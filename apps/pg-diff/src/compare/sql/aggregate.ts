import { stmt } from '../../stmt';
import { AggregateDefinition } from '../../catalog/database-objects';
import { generateProcedureGrantsDefinition } from './procedure';

export function generateCreateAggregateScript(schema: AggregateDefinition) {
  const privileges = Object.entries(schema.privileges)
    .map(([role]) => generateProcedureGrantsDefinition(schema, role))
    .flat();

  return stmt`CREATE AGGREGATE ${schema.fullName} (${schema.argTypes}) (\n${
    schema.definition
  }\n);
  ALTER AGGREGATE ${schema.fullName}(${schema.argTypes}) OWNER TO ${
    schema.owner
  };
  ${privileges.join('\n')}`;
}

export function generateChangeAggregateScript(schema: AggregateDefinition) {
  return stmt`DROP AGGREGATE IF EXISTS ${schema.fullName}(${
    schema.argTypes
  });\n${generateCreateAggregateScript(schema)}`;
}

export function generateDropAggregateScript(
  aggregate: string,
  aggregateArgs: string
) {
  return stmt`DROP AGGREGATE IF EXISTS ${aggregate}(${aggregateArgs});`;
}

export function generateChangeAggregateOwnerScript(
  aggregate: string,
  argTypes: string,
  owner: string
) {
  return stmt`ALTER AGGREGATE ${aggregate}(${argTypes}) OWNER TO ${owner};`;
}
