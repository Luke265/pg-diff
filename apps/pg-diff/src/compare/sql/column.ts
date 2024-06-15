import { ColumnChanges } from '../../compare/utils';
import { Sql, dependency, join, stmt } from '../../stmt';
import { Column } from '../../catalog/database-objects';
import { hints } from './misc';

export function generateColumnDataTypeDefinition(schema: ColumnChanges) {
  if (schema.precision) {
    const dataTypeScale = schema.scale ? `,${schema.scale}` : '';
    return stmt`${dependency(schema.datatype, schema.dataTypeID)}(${
      schema.precision
    }${dataTypeScale})`;
  }
  return stmt`${dependency(schema.datatype, schema.dataTypeID)}`;
}

export function generateColumnDefinition(schema: Column) {
  let nullableExpression = schema.nullable ? 'NULL' : 'NOT NULL';

  let defaultValue: Sql;
  if (schema.default)
    defaultValue = stmt`DEFAULT ${dependency(
      schema.default,
      schema.defaultRefs
    )}`;

  let identityValue = '';
  if (schema.identity)
    identityValue = `GENERATED ${schema.identity} AS IDENTITY`;

  if (schema.generatedColumn) {
    nullableExpression = '';
    defaultValue = stmt`GENERATED ALWAYS AS ${dependency(
      schema.default,
      schema.defaultRefs
    )} STORED`;
    identityValue = '';
  }

  const dataType = generateColumnDataTypeDefinition(schema);
  return stmt`${schema.name} ${dataType} ${nullableExpression} ${defaultValue} ${identityValue}`;
}

export function generateAddTableColumnScript(table: string, column: Column) {
  const script = stmt`ALTER TABLE IF EXISTS ${table} ADD COLUMN IF NOT EXISTS ${generateColumnDefinition(
    column
  )};`;
  if (!column.nullable && !column.default) {
    return stmt`${script} ${hints.addColumnNotNullableWithoutDefaultValue}`;
  }
  return script;
}

export function generateChangeTableColumnScript(
  table: string,
  column: string,
  changes: ColumnChanges
) {
  let definitions: Sql[] = [];
  if (changes['nullable'] !== undefined)
    definitions.push(
      stmt`ALTER COLUMN ${column} ${
        changes.nullable ? 'DROP NOT NULL' : 'SET NOT NULL'
      }`
    );

  if (changes.datatype) {
    definitions.push(stmt`${hints.changeColumnDataType}`);
    let dataTypeDefinition = generateColumnDataTypeDefinition(changes);
    definitions.push(
      stmt`ALTER COLUMN ${column} SET DATA TYPE ${dataTypeDefinition} USING ${column}::${dataTypeDefinition}`
    );
  }

  if (changes['default'] !== undefined) {
    definitions.push(
      stmt`ALTER COLUMN ${column} ${
        changes.default ? 'SET' : 'DROP'
      } DEFAULT ${dependency(changes.default, changes.defaultRefs)}`
    );
  }

  if (
    changes['identity'] !== undefined &&
    changes['isNewIdentity'] !== undefined
  ) {
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
    definitions.push(stmt`ALTER COLUMN ${column} ${identityDefinition}`);
  }

  //TODO: Should we include COLLATE when change column data type?
  return stmt`ALTER TABLE IF EXISTS ${table}\n\t${join(definitions, ',\n\t')};`;
}

export function generateDropTableColumnScript(
  table: string,
  column: string,
  withoutHint = false
) {
  return stmt`ALTER TABLE IF EXISTS ${table} DROP COLUMN IF EXISTS ${column} CASCADE;${
    withoutHint ? '' : hints.dropColumn
  }`;
}
