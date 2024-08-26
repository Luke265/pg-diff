import { PrivilegeChanges, buildGrants } from '../../compare/utils.js';
import objectType from '../../enums/object-type.js';
import { Sql, declaration, dependency, join, stmt } from '../stmt.js';
import {
  ConstraintDefinition,
  Privileges,
  TableObject,
  TableOptions,
} from '../../catalog/database-objects.js';
import { generateColumnDefinition } from './column.js';
import { generateChangeCommentScript, hints } from './misc.js';

export function generateTableGrantsDefinition(
  table: string,
  role: string,
  privileges: Privileges,
): Sql[] {
  return buildGrants([
    ['SELECT', privileges.select],
    ['INSERT', privileges.insert],
    ['UPDATE', privileges.update],
    ['DELETE', privileges.delete],
    ['TRUNCATE', privileges.truncate],
    ['REFERENCES', privileges.references],
    ['TRIGGER', privileges.trigger],
  ]).map(
    ([type, privileges]) =>
      stmt`${type} ${privileges} ON TABLE ${table} ${
        type === 'GRANT' ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`,
  );
}

export function generateCreateTableScript(table: string, schema: TableObject) {
  //Generate columns script
  const columnArr = Object.values(schema.columns);
  const columns: Sql[] = columnArr.map((obj) => generateColumnDefinition(obj));

  //Generate constraints script
  for (const name in schema.constraints) {
    const constraint = schema.constraints[name];
    columns.push(
      stmt`CONSTRAINT ${name} ${dependency(
        constraint.definition,
        constraint.relid,
      )} `,
    );
  }

  //Generate options script
  let options = '';
  if (schema.options && schema.options.withOids)
    options = `\nWITH ( OIDS=${schema.options.withOids
      .toString()
      .toUpperCase()} )`;

  //Generate indexes script
  const indexes: string[] = Object.values(schema.indexes).map(
    (obj) =>
      obj.definition
        .replace('CREATE INDEX', 'CREATE INDEX IF NOT EXISTS')
        .replace('CREATE UNIQUE INDEX', 'CREATE UNIQUE INDEX IF NOT EXISTS') +
      ';',
  );

  const privileges = Object.entries(schema.privileges)
    .map(([role, obj]) => generateTableGrantsDefinition(table, role, obj))
    .flat()
    .filter((v) => !!v);

  const columnsComment: Sql[] = columnArr
    .filter((obj) => !!obj.comment)
    .map((obj) =>
      generateChangeCommentScript(
        obj.id,
        objectType.COLUMN,
        obj.fullName,
        obj.comment,
      ),
    );

  const constraintsComment: Sql[] = Object.values(schema.constraints)
    .filter((obj) => !!obj.comment)
    .map((obj) =>
      generateChangeCommentScript(
        obj.id,
        objectType.CONSTRAINT,
        obj.name,
        obj.comment,
        table,
      ),
    );

  const indexesComment: Sql[] = Object.values(schema.indexes)
    .filter((obj) => !!obj.comment)
    .map((obj) =>
      generateChangeCommentScript(
        obj.id,
        objectType.INDEX,
        obj.name,
        obj.comment,
      ),
    );
  return stmt`CREATE TABLE IF NOT EXISTS ${declaration(
    schema.id,
    table,
  )} (\n\t${join(columns, ',\n\t')}\n)${options};
  ${indexes.join('\n')}
ALTER TABLE IF EXISTS ${table} OWNER TO ${schema.owner};
${join(privileges, '\n')}
${join(columnsComment, '\n')}
${join(constraintsComment, '\n')}
${join(indexesComment, '\n')}`;
}

export function generateTableRoleGrantsScript(
  table: string,
  role: string,
  privileges: Privileges,
) {
  return generateTableGrantsDefinition(table, role, privileges);
}

export function generateChangesTableRoleGrantsScript(
  table: string,
  role: string,
  changes: PrivilegeChanges,
) {
  return buildGrants([
    ['SELECT', changes.select],
    ['INSERT', changes.insert],
    ['UPDATE', changes.update],
    ['DELETE', changes.delete],
    ['TRUNCATE', changes.truncate],
    ['REFERENCES', changes.references],
    ['TRIGGER', changes.trigger],
  ]).map(
    ([type, privileges]) =>
      stmt`${type} ${privileges} ON TABLE ${table} ${
        type === 'GRANT' ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`,
  );
}

export function generateChangeTableOwnerScript(table: string, owner: string) {
  return stmt`ALTER TABLE IF EXISTS ${table} OWNER TO ${owner};`;
}

export function generateAddTableConstraintScript(
  table: TableObject,
  constraint: string,
  schema: ConstraintDefinition,
) {
  return stmt`ALTER TABLE IF EXISTS ${dependency(
    table.fullName,
    table.id,
  )} ADD CONSTRAINT ${declaration(schema.id, constraint)} ${dependency(
    schema.definition,
    schema.relid,
  )};`;
}

export function generateDropTableConstraintScript(
  table: TableObject,
  constraint: ConstraintDefinition,
) {
  return stmt`ALTER TABLE IF EXISTS ${table.fullName} DROP CONSTRAINT IF EXISTS "${constraint.name}";`;
}

export function generateChangeTableOptionsScript(
  table: string,
  options: TableOptions,
) {
  return stmt`ALTER TABLE IF EXISTS ${table} SET ${
    options.withOids ? 'WITH' : 'WITHOUT'
  } OIDS;`;
}

export function generateDropTableScript(table: TableObject) {
  const s = stmt`DROP TABLE IF EXISTS ${declaration(table.id, table.fullName)};`;
  s.weight = 1;
  return s;
}
