import { statement } from '../stmt.js';
import { hints } from './misc.js';

export function generateUpdateTableRecordScript(
  table: string,
  fields: any,
  filterConditions: any,
  changes: any,
) {
  let updates: string[] = [];
  for (let field in changes) {
    updates.push(
      `"${field}" = ${generateSqlFormattedValue(field, fields, changes[field])}`,
    );
  }

  let conditions: string[] = [];
  for (let condition in filterConditions) {
    conditions.push(
      `"${condition}" = ${generateSqlFormattedValue(
        condition,
        fields,
        filterConditions[condition],
      )}`,
    );
  }

  return statement({
    sql: `UPDATE ${table} SET ${updates.join(', ')} WHERE ${conditions.join(
      ' AND ',
    )};`,
  });
}

export function generateInsertTableRecordScript(
  table: string,
  record: any,
  fields: any[],
  isIdentityValuesAllowed: boolean,
) {
  let fieldNames: string[] = [];
  let fieldValues: string[] = [];
  for (let field in record) {
    fieldNames.push(`"${field}"`);
    fieldValues.push(generateSqlFormattedValue(field, fields, record[field]));
  }

  let sql = [
    `INSERT INTO ${table} (${fieldNames.join(', ')}) ${
      isIdentityValuesAllowed ? '' : 'OVERRIDING SYSTEM VALUE'
    } VALUES (${fieldValues.join(', ')});`,
  ];
  if (!isIdentityValuesAllowed) {
    sql.push('\n');
    sql.push(hints.identityColumnDetected);
    sql.push('\n');
  }
  return statement({
    sql,
  });
}

export function generateDeleteTableRecordScript(
  table: string,
  fields: any[],
  keyFieldsMap: any,
) {
  let conditions: string[] = [];
  for (let condition in keyFieldsMap) {
    conditions.push(
      `"${condition}" = ${generateSqlFormattedValue(
        condition,
        fields,
        keyFieldsMap[condition],
      )}`,
    );
  }

  return statement({
    sql: `DELETE FROM ${table} WHERE ${conditions.join(' AND ')};`,
  });
}

export function generateSqlFormattedValue(
  fieldName: string,
  fields: any,
  value: any,
) {
  if (value === undefined)
    throw new Error(`The field "${fieldName}" contains an "undefined" value!`);
  if (value === null) return 'NULL';

  let dataTypeName = '';
  let dataTypeCategory = 'X';

  let dataTypeIndex = fields.findIndex((field: any) => {
    return fieldName === field.name;
  });

  if (dataTypeIndex >= 0) {
    dataTypeName = fields[dataTypeIndex].datatype;
    dataTypeCategory = fields[dataTypeIndex].dataTypeCategory;
    if (fields[dataTypeIndex].isGeneratedColumn) return 'DEFAULT';
  }

  switch (dataTypeCategory) {
    case 'D': //DATE TIME
      return `'${value.toISOString()}'`;
    case 'V': //BIT
    case 'S': //STRING
      return `'${value.replace(/'/g, "''")}'`;
    // return `'${value}'`;
    case 'A': //ARRAY
      return `'{${value.join()}}'`;
    case 'R': //RANGE
      return `'${value}'`;
    case 'B': //BOOL
    case 'E': //ENUM
    case 'G': //GEOMETRIC
    case 'I': //NETWORK ADDRESS
    case 'N': //NUMERIC
    case 'T': //TIMESPAN
      return value;
    case 'U': {
      //USER TYPE
      switch (dataTypeName) {
        case 'jsonb':
        case 'json':
          return `'${JSON.stringify(value).replace(/'/g, "''")}'`;
        default:
          //like XML, UUID, GEOMETRY, etc.
          return `'${value.replace(/'/g, "''")}'`;
      }
    }
    case 'X': //UNKNOWN
    case 'P': //PSEUDO TYPE
    case 'C': //COMPOSITE TYPE
    default:
      throw new Error(
        `The data type category '${dataTypeCategory}' is not implemented yet!`,
      );
  }
}

export function generateMergeTableRecord(
  table: string,
  fields: any,
  changes: any,
  options: any,
) {
  let fieldNames: string[] = [];
  let fieldValues: string[] = [];
  let updates: string[] = [];
  for (let field in changes) {
    fieldNames.push(`"${field}"`);
    fieldValues.push(generateSqlFormattedValue(field, fields, changes[field]));
    updates.push(
      `"${field}" = ${generateSqlFormattedValue(field, fields, changes[field])}`,
    );
  }

  let conflictDefinition = '';
  if (options.constraintName)
    conflictDefinition = `ON CONSTRAINT ${options.constraintName}`;
  else if (options.uniqueFields && options.uniqueFields.length > 0)
    conflictDefinition = `("${options.uniqueFields.join('", "')}")`;
  else
    throw new Error(
      `Impossible to generate conflict definition for table ${table} record to merge!`,
    );

  let script = `INSERT INTO ${table} (${fieldNames.join(
    ', ',
  )}) VALUES (${fieldValues.join(
    ', ',
  )})\nON CONFLICT ${conflictDefinition}\nDO UPDATE SET ${updates.join(', ')}`;
  return script;
}
