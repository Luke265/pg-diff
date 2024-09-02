import { statement } from '../stmt.js';
import { MaterializedViewDefinition } from '../../catalog/database-objects.js';
import { generateTableGrantsDefinition } from './table.js';
import { SqlResult } from '../utils.js';

export function generateCreateMaterializedViewScript(
  schema: MaterializedViewDefinition,
): SqlResult[] {
  //Generate indexes script
  const indexes = Object.values(schema.indexes).map((index) =>
    statement({ sql: index.definition }),
  );

  //Generate privileges script
  const privileges = Object.entries(schema.privileges)
    .map(([role, obj]) =>
      generateTableGrantsDefinition(schema.fullName, role, obj),
    )
    .flat()
    .filter((v) => !!v);
  return [
    statement({
      sql: `CREATE MATERIALIZED VIEW IF NOT EXISTS ${schema.fullName} AS ${schema.definition};`,
      declarations: [schema.id],
    }),
    ...indexes,
    statement({
      sql: `ALTER MATERIALIZED VIEW IF EXISTS ${schema.fullName} OWNER TO ${schema.owner};`,
      dependencies: [schema.id],
    }),
    ...privileges,
  ];
}

export function generateDropMaterializedViewScript(
  schema: MaterializedViewDefinition,
) {
  return statement({
    sql: `DROP MATERIALIZED VIEW IF EXISTS ${schema.fullName};`,
    before: [schema.id],
  });
}
