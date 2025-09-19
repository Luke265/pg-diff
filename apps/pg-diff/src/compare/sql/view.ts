import { statement } from '../stmt.js';
import { ViewDefinition } from '../../catalog/database-objects.js';
import { generateTableGrantsDefinition } from './table.js';
import { Config } from '../../config.js';

export function generateCreateViewScript(
  config: Config,
  schema: ViewDefinition,
) {
  const privileges = Object.entries(schema.privileges)
    .map(([role, obj]) => {
      role = config.compareOptions.mapRole(role);
      role = config.compareOptions.replaceRole(role);
      return generateTableGrantsDefinition(schema, role, obj);
    })
    .flat()
    .filter((v) => !!v);
  const owner = config.compareOptions.mapRole(
    config.compareOptions.replaceRole(schema.owner),
  );
  return [
    statement({
      sql: `CREATE OR REPLACE VIEW ${schema.fullName} AS ${schema.definition};`,
      declarations: [schema.id],
    }),
    statement({
      sql: `ALTER VIEW IF EXISTS ${schema.fullName} OWNER TO ${owner};`,
      dependencies: [schema.id],
    }),
    ...privileges,
  ];
}

export function generateDropViewScript(view: ViewDefinition) {
  return statement({
    sql: `DROP VIEW IF EXISTS ${view.fullName};`,
    before: [view.id],
  });
}
