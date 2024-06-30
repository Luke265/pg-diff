import { stmt } from '../stmt.js';

export function generateCreateSchemaScript(schema: string, owner: string) {
  return stmt`CREATE SCHEMA IF NOT EXISTS ${schema} AUTHORIZATION ${owner};`;
}
