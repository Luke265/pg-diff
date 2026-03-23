import { PrivilegeChanges, buildGrants } from '../../compare/utils.js';
import objectType from '../../enums/object-type.js';
import { Sql, joinStmt, statement } from '../stmt.js';
import {
  ConstraintDefinition,
  DbObject,
  Privileges,
  TableObject,
  TableOptions,
} from '../../catalog/database-objects.js';
import { generateColumnDefinition } from './column.js';
import { generateChangeCommentScript, hints } from './misc.js';
import { Config } from '../../config.js';

export function generateTableGrantsDefinition(
  object: DbObject,
  role: string,
  privileges: Privileges,
): Sql[] {
  return buildGrants([
    [
      'SELECT',
      privileges.select === undefined
        ? undefined
        : { grant: privileges.select, revoke: false },
    ],
    [
      'INSERT',
      privileges.insert === undefined
        ? undefined
        : { grant: privileges.insert, revoke: false },
    ],
    [
      'UPDATE',
      privileges.update === undefined
        ? undefined
        : { grant: privileges.update, revoke: false },
    ],
    [
      'DELETE',
      privileges.delete === undefined
        ? undefined
        : { grant: privileges.delete, revoke: false },
    ],
    ['TRUNCATE', privileges.truncate],
    ['REFERENCES', privileges.references],
    ['TRIGGER', privileges.trigger],
  ]).map(([type, privileges]) =>
    statement({
      sql: `${type} ${privileges} ON TABLE ${object.fullName} ${
        type === 'GRANT' ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`,
      dependencies: [object.id],
    }),
  );
}

export function generateCreateTableScript(
  config: Config,
  table: TableObject,
  schema: TableObject,
) {
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
    .map(([role, obj]) => {
      role = config.compareOptions.mapRole(role);
      role = config.compareOptions.replaceRole(role);
      return generateTableGrantsDefinition(table, role, obj);
    })
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
        table.fullName,
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
  const sql: (string | Sql)[] = [
    `CREATE TABLE IF NOT EXISTS ${table.fullName} (`,
  ];
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
  sql.push(`ALTER TABLE IF EXISTS ${table.fullName} OWNER TO ${schema.owner};`);
  sql.push('\n');
  return statement({
    sql,
    declarations: [schema.id],
  });
}

export function generateTableRoleGrantsScript(
  object: DbObject,
  role: string,
  privileges: Privileges,
) {
  return generateTableGrantsDefinition(object, role, privileges);
}

export function generateChangesTableRoleGrantsScript(
  object: DbObject,
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
      sql: `${type} ${privileges} ON TABLE ${object.fullName} ${
        type === 'GRANT' ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`,
      dependencies: [object.id],
    }),
  );
}

export function generateRevokeAll(table: DbObject, role: string) {
  return statement({
    sql: `REVOKE ALL ON ${table.fullName} FROM ${role};${hints.potentialRoleMissing}`,
    dependencies: [table.id],
  });
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
