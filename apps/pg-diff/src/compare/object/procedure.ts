import {
  FunctionDefinition,
  FunctionPrivileges,
} from '../../catalog/database-objects.js';
import { Config } from '../../config.js';
import objectType from '../../enums/object-type.js';
import { generateChangeCommentScript } from '../sql/misc.js';
import {
  generateDropProcedureScript,
  generateCreateProcedureScript,
  generateChangeProcedureOwnerScript,
  generateChangesProcedureRoleGrantsScript,
  generateProcedureRoleGrantsScript,
} from '../sql/procedure.js';
import { SqlResult } from '../utils.js';

export function compareProcedures(
  sourceFunctions: Record<string, Record<string, FunctionDefinition>>,
  targetFunctions: Record<string, Record<string, FunctionDefinition>>,
  config: Config,
): SqlResult[] {
  const lines: SqlResult[] = [];

  for (const procedure in sourceFunctions) {
    for (const procedureArgs in sourceFunctions[procedure]) {
      const sourceObj = sourceFunctions[procedure][procedureArgs];
      const targetObj =
        targetFunctions[procedure] && targetFunctions[procedure][procedureArgs];
      const procedureType =
        sourceObj.type === 'f' ? objectType.FUNCTION : objectType.PROCEDURE;

      if (targetObj) {
        //Procedure exists on both database, then compare procedure definition
        //TODO: Is correct that if definition is different automatically GRANTS and OWNER will not be updated also?
        //TODO: Better to match only "visible" char in order to avoid special invisible like \t, spaces, etc;
        //      the problem is that a SQL STRING can contains special char as a fix from previous function version
        const sourceFunctionDefinition = sourceObj.definition.replace(
          /\r/g,
          '',
        );
        const targetFunctionDefinition = targetObj.definition.replace(
          /\r/g,
          '',
        );
        if (sourceFunctionDefinition !== targetFunctionDefinition) {
          const generated = generateCreateProcedureScript(sourceObj);
          if (
            sourceObj.argTypes !== targetObj.argTypes ||
            sourceObj.returnType !== targetObj.returnType
          ) {
            lines.push(
              generateDropProcedureScript(sourceObj).addBefore(generated),
            );
          }
          lines.push(generated);
          const owner = config.compareOptions.mapRole(sourceObj.owner);
          if (owner !== targetObj.owner) {
            lines.push(generateChangeProcedureOwnerScript(sourceObj, owner));
          }
          if (sourceObj.comment) {
            lines.push(
              generateChangeCommentScript(
                sourceObj.id,
                procedureType,
                `${procedure}(${procedureArgs})`,
                sourceObj.comment,
              ),
            );
          }
        } else {
          lines.push(
            compareProcedurePrivileges(
              sourceObj,
              sourceObj.privileges,
              targetObj.privileges,
              config,
            ),
          );

          const owner = config.compareOptions.mapRole(sourceObj.owner);
          if (owner !== targetObj.owner) {
            lines.push(generateChangeProcedureOwnerScript(sourceObj, owner));
          }

          if (sourceObj.comment != sourceObj.comment)
            lines.push(
              generateChangeCommentScript(
                sourceObj.id,
                procedureType,
                `${procedure}(${procedureArgs})`,
                sourceObj.comment,
              ),
            );
        }
      } else {
        //Procedure not exists on target database, then generate the script to create procedure
        lines.push(generateCreateProcedureScript(sourceObj));
        lines.push(
          generateChangeProcedureOwnerScript(
            sourceObj,
            config.compareOptions.mapRole(sourceObj.owner),
          ),
        );
        if (sourceObj.comment) {
          lines.push(
            generateChangeCommentScript(
              sourceObj.id,
              procedureType,
              `${procedure}(${procedureArgs})`,
              sourceObj.comment,
            ),
          );
        }
      }
    }
  }

  if (config.compareOptions.schemaCompare.dropMissingFunction)
    for (const procedure in targetFunctions) {
      for (const procedureArgs in targetFunctions[procedure]) {
        if (
          sourceFunctions[procedure] &&
          sourceFunctions[procedure][procedureArgs]
        ) {
          continue;
        }
        lines.push(
          generateDropProcedureScript(
            targetFunctions[procedure][procedureArgs],
          ).setWeight(1),
        );
      }
    }

  return lines;
}

export function compareProcedurePrivileges(
  schema: FunctionDefinition,
  sourceProcedurePrivileges: Record<string, FunctionPrivileges>,
  targetProcedurePrivileges: Record<string, FunctionPrivileges>,
  config: Config,
): SqlResult[] {
  const lines: SqlResult[] = [];

  for (const _role in sourceProcedurePrivileges) {
    const role = config.compareOptions.mapRole(_role);
    const sourceObj = sourceProcedurePrivileges[_role];
    const targetObj = targetProcedurePrivileges[role];
    //Get new or changed role privileges
    if (targetObj) {
      //Procedure privileges for role exists on both database, then compare privileges
      let changes: { execute?: boolean } = {};
      if (sourceObj.execute !== targetObj.execute) {
        changes.execute = sourceObj.execute;
        lines.push(
          generateChangesProcedureRoleGrantsScript(schema, role, changes),
        );
      }
    } else {
      //Procedure grants for role not exists on target database, then generate script to add role privileges
      lines.push(generateProcedureRoleGrantsScript(schema, role));
    }
  }

  return lines;
}
