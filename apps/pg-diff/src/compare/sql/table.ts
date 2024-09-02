import { PrivilegeChanges, buildGrants } from '../../compare/utils.js';
import objectType from '../../enums/object-type.js';
import { Sql, joinStmt, statement } from '../stmt.js';
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
  ]).map(([type, privileges]) =>
    statement({
      sql: `${type} ${privileges} ON TABLE ${table} ${
        type === 'GRANT' ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`,
    }),
  );
}

export function generateCreateTableScript(table: string, schema: TableObject) {
  //Generate columns script
  const columnArr = Object.values(schema.columns);
  const columns: (string | Sql)[] = columnArr.map((obj) =>
    generateColumnDefinition(obj),
  );

  //Generate constraints script
  for (const name in schema.constraints) {
    const constraint = schema.constraints[name];
    columns.push(
      statement({
        sql: `CONSTRAINT ${name} ${constraint.definition} `,
        dependencies: [constraint.relid],
      }),
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
  const sql: (string | Sql)[] = [`CREATE TABLE IF NOT EXISTS ${table} (`];
  sql.push('\n    ');
  joinStmt(sql, columns, ',\n    ');
  sql.push('\n)');
  sql.push(options);
  sql.push(';');
  sql.push('\n');
  if (indexes.length > 0) {
    joinStmt(sql, indexes, '\n');
    sql.push('\n');
  }
  if (privileges.length > 0) {
    joinStmt(sql, privileges, '\n');
    sql.push('\n');
  }
  if (columnsComment.length > 0) {
    joinStmt(sql, columnsComment, '\n');
    sql.push('\n');
  }
  if (constraintsComment.length > 0) {
    joinStmt(sql, constraintsComment, '\n');
    sql.push('\n');
  }
  if (indexesComment.length > 0) {
    joinStmt(sql, indexesComment, '\n');
    sql.push('\n');
  }
  sql.push(`ALTER TABLE IF EXISTS ${table} OWNER TO ${schema.owner};`);
  sql.push('\n');
  return statement({
    sql,
    declarations: [schema.id],
  });
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
  ]).map(([type, privileges]) =>
    statement({
      sql: `${type} ${privileges} ON TABLE ${table} ${
        type === 'GRANT' ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`,
    }),
  );
}

export function generateChangeTableOwnerScript(table: string, owner: string) {
  return statement({
    sql: `ALTER TABLE IF EXISTS ${table} OWNER TO ${owner};`,
  });
}

export function generateAddTableConstraintScript(
  table: TableObject,
  constraint: string,
  schema: ConstraintDefinition,
) {
  return statement({
    sql: `ALTER TABLE IF EXISTS ${table.fullName} ADD CONSTRAINT ${constraint} ${schema.definition};`,
    dependencies: [table.id, schema.relid],
  });
}

export function generateDropTableConstraintScript(
  table: TableObject,
  constraint: ConstraintDefinition,
) {
  return statement({
    sql: `ALTER TABLE IF EXISTS ${table.fullName} DROP CONSTRAINT IF EXISTS "${constraint.name}";`,
    dependencies: [table.id],
  });
}

export function generateChangeTableOptionsScript(
  table: TableObject,
  options: TableOptions,
) {
  return statement({
    sql: `ALTER TABLE IF EXISTS ${table.fullName} SET ${
      options.withOids ? 'WITH' : 'WITHOUT'
    } OIDS;`,
    dependencies: [table.id],
  });
}

export function generateDropTableScript(table: TableObject) {
  return statement({
    sql: `DROP TABLE IF EXISTS ${table.fullName};`,
    declarations: [table.id],
    weight: 1,
  });
}
