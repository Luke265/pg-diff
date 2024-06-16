import { stmt } from '../stmt';

export function generateCreateSchemaScript(schema: string, owner: string) {
  return stmt`CREATE SCHEMA IF NOT EXISTS ${schema} AUTHORIZATION ${owner};`;
}
