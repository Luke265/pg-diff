import { AggregateDefinition } from '../../catalog/database-objects.js';
import { joinStmt, statement } from '../stmt.js';
import { generateProcedureGrantsDefinition } from './procedure.js';

export function generateCreateAggregateScript(schema: AggregateDefinition) {
  const privileges = Object.entries(schema.privileges)
    .map(([role]) => generateProcedureGrantsDefinition(schema, role))
    .flat()
    .filter((v) => !!v);
  const sql = [
    `CREATE AGGREGATE ${schema.fullName} (${schema.argTypes}) (\n`,
    schema.definition,
    '\n);',
    '\n',
    `ALTER AGGREGATE ${schema.fullName}(${schema.argTypes}) OWNER TO ${
      schema.owner
    };`,
  ];
  joinStmt(sql, privileges, '\n');
  return statement({
    sql,
  });
}

export function generateChangeAggregateScript(schema: AggregateDefinition) {
  return statement({
    sql: `DROP AGGREGATE IF EXISTS ${schema.fullName}(${
      schema.argTypes
    });\n${generateCreateAggregateScript(schema)}`,
  });
}

export function generateDropAggregateScript(
  aggregate: string,
  aggregateArgs: string,
) {
  return statement({
    sql: `DROP AGGREGATE IF EXISTS ${aggregate}(${aggregateArgs});`,
  });
}

export function generateChangeAggregateOwnerScript(
  aggregate: string,
  argTypes: string,
  owner: string,
) {
  return statement({
    sql: `ALTER AGGREGATE ${aggregate}(${argTypes}) OWNER TO ${owner};`,
  });
}
