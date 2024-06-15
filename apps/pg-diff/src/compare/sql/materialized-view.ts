import { join, stmt } from '../../stmt';
import { MaterializedViewDefinition } from '../../catalog/database-objects';
import { generateTableGrantsDefinition } from './table';

export function generateCreateMaterializedViewScript(
  view: string,
  schema: MaterializedViewDefinition
) {
  //Generate indexes script
  const indexes: string[] = [];
  for (const index in schema.indexes) {
    indexes.push(`\n${schema.indexes[index].definition};\n`);
  }

  //Generate privileges script
  const privileges = Object.entries(schema.privileges)
    .map(([role, obj]) => generateTableGrantsDefinition(view, role, obj))
    .filter((v) => !!v);

  return stmt`CREATE MATERIALIZED VIEW IF NOT EXISTS ${view} AS ${
    schema.definition
  }\n${indexes.join('\n')}
  ALTER MATERIALIZED VIEW IF EXISTS ${view} OWNER TO ${schema.owner};
  ${join(privileges, '\n')}`;
}

export function generateDropMaterializedViewScript(view: string) {
  return stmt`DROP MATERIALIZED VIEW IF EXISTS ${view};`;
}
