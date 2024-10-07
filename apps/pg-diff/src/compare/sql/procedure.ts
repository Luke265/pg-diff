import { Sql, statement } from '../stmt.js';
import { FunctionDefinition } from '../../catalog/database-objects.js';
import { hints } from './misc.js';
import { replaceLastCharacter, SqlResult } from '../utils.js';

const PROCEDURE_TYPE = {
  p: 'PROCEDURE',
  f: 'FUNCTION',
} as const;

export function generateProcedureGrantsDefinition(
  schema: FunctionDefinition,
  role: string,
): Sql | null {
  if (schema.privileges.execute) {
    return statement({
      sql: `GRANT EXECUTE ON ${PROCEDURE_TYPE[schema.type]} ${schema.fullName}(${
        schema.argTypes
      }) TO ${role};${hints.potentialRoleMissing}`,
      dependencies: [schema.id],
    });
  }
  return null;
}

export function generateCreateProcedureScript(
  schema: FunctionDefinition,
): Sql[] {
  return [
    statement({
      sql: replaceLastCharacter(schema.definition, ';'),
      dependencies: schema.fReferenceIds,
      declarations: [schema.id],
    }),
    ...Object.keys(schema.privileges)
      .map((role) => generateProcedureGrantsDefinition(schema, role))
      .filter((n) => !!n),
  ];
}

export function generateDropProcedureScript(schema: FunctionDefinition): Sql {
  return statement({
    sql: `DROP ${PROCEDURE_TYPE[schema.type]} IF EXISTS ${schema.fullName}(${schema.argTypes});`,
    before: schema.fReferenceIds,
  });
}

export function generateProcedureRoleGrantsScript(
  schema: FunctionDefinition,
  role: string,
): SqlResult {
  return generateProcedureGrantsDefinition(schema, role);
}

export function generateChangesProcedureRoleGrantsScript(
  schema: FunctionDefinition,
  role: string,
  changes: { execute?: boolean },
): SqlResult {
  if (changes.execute === undefined) {
    return null;
  }
  return statement({
    sql: `${changes.execute ? 'GRANT' : 'REVOKE'} EXECUTE ON ${
      PROCEDURE_TYPE[schema.type]
    } ${schema.fullName}(${schema.argTypes}) ${
      changes.execute ? 'TO' : 'FROM'
    } ${role};${hints.potentialRoleMissing}`,
    dependencies: [schema.id],
  });
}

export function generateChangeProcedureOwnerScript(
  schema: FunctionDefinition,
  owner: string,
): SqlResult {
  return statement({
    sql: `ALTER ${PROCEDURE_TYPE[schema.type]} ${schema.fullName}(${schema.argTypes}) OWNER TO ${owner};`,
    dependencies: [schema.id],
  });
}
