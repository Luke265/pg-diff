import objectType from '../../enums/object-type';
import { Sql, declaration, dependency, join, stmt } from '../../stmt';
import { Column, Type } from '../../catalog/database-objects';
import { generateColumnDataTypeDefinition } from './column';
import { generateChangeCommentScript } from './misc';

export function generateDropTypeScript(type: Type) {
  return stmt`DROP TYPE ${type.fullName};`;
}
export function generateCreateTypeScript(schema: Type) {
  const columnArr = Object.values(schema.columns);
  const columns = columnArr.map((obj) => generateTypeColumnDefinition(obj));
  const columnsComment: Sql[] = columnArr.map((obj) =>
    generateChangeCommentScript(
      obj.id,
      objectType.COLUMN,
      obj.fullName,
      obj.comment
    )
  );
  let body: Sql;
  if (schema.enum) {
    body = stmt`ENUM ('${schema.enum.join("','")}')`;
  } else {
    body = stmt`(
        ${join(columns, ',\n\t')}
        )`;
  }
  return stmt`CREATE TYPE ${declaration(schema.id, schema.fullName)} AS ${body};
    ${join(columnsComment, '\n')}`;
}

export function generateDropTypeColumnScript(table: Type, column: Column) {
  return stmt`ALTER TABLE IF EXISTS ${dependency(
    table.fullName,
    table.id
  )} DROP COLUMN IF EXISTS ${column.name} CASCADE;`;
}

export function generateAddTypeColumnScript(schema: Type, column: Column) {
  return stmt`ALTER TYPE ${dependency(
    schema.fullName,
    schema.id
  )} ADD ATTRIBUTE ${generateTypeColumnDefinition(column)};`;
}

export function generateTypeColumnDefinition(schema: Column) {
  let defaultValue: Sql;
  if (schema.default) {
    defaultValue = stmt`DEFAULT ${dependency(
      schema.default,
      schema.defaultRefs
    )}`;
  }
  let dataType = generateColumnDataTypeDefinition(schema);
  return stmt`${schema.name} ${dataType} ${defaultValue}`;
}

export function generateChangeTypeOwnerScript(type: Type, owner: string) {
  return stmt`ALTER TYPE ${dependency(
    type.fullName,
    type.id
  )} OWNER TO ${owner};`;
}
