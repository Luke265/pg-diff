import { statement } from '../stmt.js';

export function generateCreateSchemaScript(schema: string, owner: string) {
  return statement({
    sql: `CREATE SCHEMA IF NOT EXISTS ${schema} AUTHORIZATION ${owner};`,
  });
}
