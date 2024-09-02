import { statement } from '../stmt.js';
import { ViewDefinition } from '../../catalog/database-objects.js';
import { generateTableGrantsDefinition } from './table.js';

export function generateCreateViewScript(schema: ViewDefinition) {
  const privileges = Object.entries(schema.privileges)
    .map(([role, obj]) =>
      generateTableGrantsDefinition(schema.fullName, role, obj),
    )
    .flat()
    .filter((v) => !!v);
  return [
    statement({
      sql: `CREATE OR REPLACE VIEW ${schema.fullName} AS ${schema.definition};`,
      declarations: [schema.id],
    }),
    statement({
      sql: `ALTER VIEW IF EXISTS ${schema.fullName} OWNER TO ${schema.owner};`,
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
