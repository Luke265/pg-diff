import { declaration, stmt } from '../../stmt';
import { FunctionDefinition } from '../../catalog/database-objects';
import { hints } from './misc';

const PROCEDURE_TYPE = {
  p: 'PROCEDURE',
  f: 'FUNCTION',
} as const;

export function generateProcedureGrantsDefinition(
  schema: FunctionDefinition,
  role: string
) {
  if (schema.privileges.execute) {
    return [
      `GRANT EXECUTE ON ${PROCEDURE_TYPE[schema.type]} ${schema.fullName}(${
        schema.argTypes
      }) TO ${role};${hints.potentialRoleMissing}`,
    ];
  }
  return [];
}

export function generateCreateProcedureScript(schema: FunctionDefinition) {
  const privileges = Object.entries(schema.privileges)
    .map(([role]) => generateProcedureGrantsDefinition(schema, role))
    .flat();
  const st = stmt`${declaration(schema.id, schema.definition)};
  ALTER ${PROCEDURE_TYPE[schema.type]} ${schema.fullName}(${
    schema.argTypes
  }) OWNER TO ${schema.owner};
  ${privileges.join('\n')}`;
  st.dependencies.push(...schema.fReferenceIds);
  return st;
}

export function generateDropProcedureScript(schema: FunctionDefinition) {
  const s = stmt`DROP ${PROCEDURE_TYPE[schema.type]} IF EXISTS ${
    schema.fullName
  }(${schema.argTypes});`;
  s.weight = 1;
  return s;
}

export function generateProcedureRoleGrantsScript(
  schema: FunctionDefinition,
  role: string
) {
  return stmt`${generateProcedureGrantsDefinition(schema, role).join('\n')}`;
}

export function generateChangesProcedureRoleGrantsScript(
  schema: FunctionDefinition,
  role: string,
  changes: { execute: boolean | null }
) {
  if (changes.execute === null) {
    return null;
  }
  return stmt`${changes.execute ? 'GRANT' : 'REVOKE'} EXECUTE ON ${
    PROCEDURE_TYPE[schema.type]
  } ${schema.fullName}(${schema.argTypes}) ${
    changes.execute ? 'TO' : 'FROM'
  } ${role};${hints.potentialRoleMissing}`;
}

export function generateChangeProcedureOwnerScript(
  procedure: string,
  argTypes: string,
  owner: string,
  type: 'p' | 'f'
) {
  return stmt`ALTER ${PROCEDURE_TYPE[type]} ${procedure}(${argTypes}) OWNER TO ${owner};`;
}
