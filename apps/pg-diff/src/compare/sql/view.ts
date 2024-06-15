import { join, stmt } from '../../stmt';
import { ViewDefinition } from '../../catalog/database-objects';
import { generateTableGrantsDefinition } from './table';

export function generateCreateViewScript(view: string, schema: ViewDefinition) {
  const privileges = Object.entries(schema.privileges)
    .map(([role, obj]) => generateTableGrantsDefinition(view, role, obj))
    .filter((v) => !!v);
  return stmt`CREATE OR REPLACE VIEW ${view} AS ${schema.definition}
  ALTER VIEW IF EXISTS ${view} OWNER TO ${schema.owner};
  ${join(privileges, '\n')}`;
}

export function generateDropViewScript(view: string) {
  return stmt`DROP VIEW IF EXISTS ${view};`;
}
