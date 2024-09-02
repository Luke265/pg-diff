import { ColumnChanges } from '../../compare/utils.js';
import { Id, Sql, statement } from '../stmt.js';
import { Column } from '../../catalog/database-objects.js';
import { hints } from './misc.js';

export function generateColumnDataTypeDefinition(schema: ColumnChanges) {
  if (!schema.datatype || !schema.dataTypeID) {
    throw new Error('Unsupported datatype');
  }
  let sql = schema.datatype;
  if (schema.precision) {
    sql += `(${schema.precision}${schema.scale ? `,${schema.scale}` : ''})`;
  }
  return statement({ sql, dependencies: [schema.dataTypeID] });
}

export function generateColumnDefinition(schema: Column): Sql {
  const dataType = generateColumnDataTypeDefinition(schema);
  const sql = [schema.name, dataType];
  const dependencies: Id[] = [];
  if (schema.generatedColumn) {
    sql.push(`GENERATED ALWAYS AS (${schema.default}) STORED`);
    dependencies.push(...schema.defaultRefs);
  } else {
    sql.push(schema.nullable ? 'NULL' : 'NOT NULL');
    if (schema.default) {
      sql.push(`DEFAULT ${schema.default}`);
      dependencies.push(...schema.defaultRefs);
    }
    if (schema.identity) {
      sql.push(`GENERATED ${schema.identity} AS IDENTITY`);
    }
  }
  return statement({ sql: sql.join(' '), dependencies });
}

export function generateAddTableColumnScript(table: string, column: Column) {
  const sql = [
    `ALTER TABLE IF EXISTS ${table} ADD COLUMN IF NOT EXISTS `,
    generateColumnDefinition(column),
    ';',
  ];
  if (!column.nullable && !column.default) {
    sql.push(hints.addColumnNotNullableWithoutDefaultValue);
  }
  return statement({
    sql,
  });
}

export function generateChangeTableColumnScript(
  table: string,
  column: string,
  changes: ColumnChanges,
) {
  const sql: (Sql | string)[] = [`ALTER TABLE IF EXISTS ${table}\n    `];
  const dependencies: Id[] = [];
  if (changes.nullable !== undefined)
    sql.push(
      `ALTER COLUMN ${column} ${
        changes.nullable ? 'DROP NOT NULL' : 'SET NOT NULL'
      }`,
    );

  if (changes.datatype) {
    const dataTypeDefinition = generateColumnDataTypeDefinition(changes);
    sql.push(hints.changeColumnDataType);
    sql.push('\n    ');
    sql.push(
      `ALTER COLUMN ${column} SET DATA TYPE ${dataTypeDefinition} USING ${column}::${dataTypeDefinition}`,
    );
    dependencies.push(...dataTypeDefinition.dependencies);
  }

  if (changes.default !== undefined && changes.defaultRefs) {
    sql.push(
      `ALTER COLUMN ${column} ${
        changes.default ? 'SET' : 'DROP'
      } DEFAULT ${changes.default}`,
    );
    dependencies.push(...changes.defaultRefs);
  }

  if (changes.identity !== undefined && changes.isNewIdentity !== undefined) {
    let identityDefinition = '';
    if (changes.identity) {
      //truly values
      identityDefinition = `${
        changes.isNewIdentity ? 'ADD' : 'SET'
      } GENERATED ${changes.identity} ${
        changes.isNewIdentity ? 'AS IDENTITY' : ''
      }`;
    } else {
      //falsy values
      identityDefinition = 'DROP IDENTITY IF EXISTS';
    }
    sql.push(`ALTER COLUMN ${column} ${identityDefinition}`);
  }
  sql.push(';');
  //TODO: Should we include COLLATE when change column data type?
  return statement({
    sql,
    dependencies,
  });
}

export function generateDropTableColumnScript(
  table: string,
  column: string,
  withoutHint = false,
) {
  return statement({
    sql: `ALTER TABLE IF EXISTS ${table} DROP COLUMN IF EXISTS ${column} CASCADE;${
      withoutHint ? '' : hints.dropColumn
    }`,
  });
}
