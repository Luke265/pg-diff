import objectType from '../../enums/object-type.js';
import { Id, Sql, joinStmt, statement } from '../stmt.js';
import { Column, Type } from '../../catalog/database-objects.js';
import { generateColumnDataTypeDefinition } from './column.js';
import { generateChangeCommentScript } from './misc.js';

export function generateDropTypeScript(type: Type) {
  return statement({
    sql: `DROP TYPE ${type.fullName};`,
    before: [type.id],
  });
}

export function generateCreateTypeScript(schema: Type) {
  const columnArr = Object.values(schema.columns);
  const columns = columnArr.map((obj) => generateTypeColumnDefinition(obj));
  const columnsComment: Sql[] = columnArr.map((obj) =>
    generateChangeCommentScript(
      obj.id,
      objectType.COLUMN,
      obj.fullName,
      obj.comment,
    ),
  );
  const sql: (string | Sql)[] = [`CREATE TYPE ${schema.fullName} AS `];
  if (schema.enum) {
    sql.push(`ENUM ('${schema.enum.join("','")}')`);
  } else {
    sql.push('(\n    ');
    joinStmt(sql, columns, ',\n    ');
    sql.push('\n)');
  }
  sql.push(';\n');
  joinStmt(sql, columnsComment, '\n');
  return statement({
    sql,
    declarations: [schema.id],
  });
}

export function generateDropTypeColumnScript(table: Type, column: Column) {
  return statement({
    sql: `ALTER TABLE IF EXISTS ${table.fullName} DROP COLUMN IF EXISTS ${column.name} CASCADE;`,
    dependencies: [table.id],
  });
}

export function generateAddTypeColumnScript(schema: Type, column: Column) {
  return statement({
    sql: [
      `ALTER TYPE ${schema.fullName} ADD ATTRIBUTE `,
      generateTypeColumnDefinition(column),
      ';',
    ],
    dependencies: [schema.id],
  });
}

export function generateTypeColumnDefinition(schema: Column) {
  const dataType = generateColumnDataTypeDefinition(schema);
  const sql = [schema.name, dataType];
  const dependencies: Id[] = [];
  if (schema.default) {
    sql.push(`DEFAULT ${schema.default}`);
    dependencies.push(...schema.defaultRefs);
  }
  return statement({ sql: sql.join(' '), dependencies });
}

export function generateChangeTypeOwnerScript(type: Type, owner: string) {
  return statement({
    sql: `ALTER TYPE ${type.fullName} OWNER TO ${owner};`,
    dependencies: [type.id],
  });
}
