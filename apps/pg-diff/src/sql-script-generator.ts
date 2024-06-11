import { ColumnChanges } from './api/compare-api';
import objectType from './enums/object-type';
import { Config } from './models/config';
import {
  AggregateDefinition,
  Column,
  ConstraintDefinition,
  FunctionDefinition,
  FunctionPrivileges,
  IndexDefinition,
  Policy,
  Sequence,
  SequencePrivileges,
  TableObject,
  TableOptions,
} from './models/database-objects';
import { Sql, declaration, dependency, join, stmt } from './stmt';

const hints = {
  addColumnNotNullableWithoutDefaultValue:
    ' --WARN: Add a new column not nullable without a default value can occure in a sql error during execution!',
  changeColumnDataType:
    ' --WARN: Change column data type can occure in a casting error, the suggested casting expression is the default one and may not fit your needs!',
  dropColumn: ' --WARN: Drop column can occure in data loss!',
  potentialRoleMissing:
    ' --WARN: Grant\\Revoke privileges to a role can occure in a sql error during execution if role is missing to the target database!',
  identityColumnDetected:
    ' --WARN: Identity column has been detected, an error can occure because constraints violation!',
  dropTable: ' --WARN: Drop table can occure in data loss!',
};
export type SequenceProperties =
  | 'startValue'
  | 'minValue'
  | 'maxValue'
  | 'increment'
  | 'cacheSize'
  | 'isCycle'
  | 'owner';
const policyForMap = {
  '*': 'ALL',
  w: 'UPDATE',
  r: 'SELECT',
  a: 'INSERT',
  d: 'DELETE',
} as const;

/**
 *
 * @param {Object} columnSchema
 */
export function generateColumnDataTypeDefinition(columnSchema: ColumnChanges) {
  let dataType = columnSchema.datatype;
  if (columnSchema.precision) {
    let dataTypeScale = columnSchema.scale ? `,${columnSchema.scale}` : '';
    dataType += `(${columnSchema.precision}${dataTypeScale})`;
  }
  return dataType;
}
/**
 *
 * @param {String} column
 * @param {Object} columnSchema
 */
export function generateColumnDefinition(column: string, columnSchema: Column) {
  let nullableExpression = columnSchema.nullable ? 'NULL' : 'NOT NULL';

  let defaultValue: Sql;
  if (columnSchema.default)
    defaultValue = stmt`DEFAULT ${columnSchema.defaultRef}`;
  //a
  let identityValue = '';
  if (columnSchema.identity)
    identityValue = `GENERATED ${columnSchema.identity} AS IDENTITY`;

  if (columnSchema.generatedColumn) {
    nullableExpression = '';
    defaultValue = stmt`GENERATED ALWAYS AS ${columnSchema.defaultRef} STORED`;
    identityValue = '';
  }

  let dataType = generateColumnDataTypeDefinition(columnSchema);
  const s = stmt`${column} ${dataType} ${nullableExpression} ${defaultValue} ${identityValue}`;
  return stmt`${column} ${dataType} ${nullableExpression} ${defaultValue} ${identityValue}`;
}
/**
 *
 * @param {String} table
 * @param {String} role
 * @param {Object} privileges
 */
export function generateTableGrantsDefinition(
  table: string,
  role: string,
  privileges: any
) {
  let definitions: string[] = [];

  if (privileges.select)
    definitions.push(
      `GRANT SELECT ON TABLE ${table} TO ${role};${hints.potentialRoleMissing}`
    );

  if (privileges.insert)
    definitions.push(
      `GRANT INSERT ON TABLE ${table} TO ${role};${hints.potentialRoleMissing}`
    );

  if (privileges.update)
    definitions.push(
      `GRANT UPDATE ON TABLE ${table} TO ${role};${hints.potentialRoleMissing}`
    );

  if (privileges.delete)
    definitions.push(
      `GRANT DELETE ON TABLE ${table} TO ${role};${hints.potentialRoleMissing}`
    );

  if (privileges.truncate)
    definitions.push(
      `GRANT TRUNCATE ON TABLE ${table} TO ${role};${hints.potentialRoleMissing}`
    );

  if (privileges.references)
    definitions.push(
      `GRANT REFERENCES ON TABLE ${table} TO ${role};${hints.potentialRoleMissing}`
    );

  if (privileges.trigger)
    definitions.push(
      `GRANT TRIGGER ON TABLE ${table} TO ${role};${hints.potentialRoleMissing}`
    );

  return definitions;
}
/**
 *
 * @param {String} procedure
 * @param {String} argTypes
 * @param {String} role
 * @param {Object} privileges
 * @param {"f"|"p"} type
 */
export function generateProcedureGrantsDefinition(
  schema: FunctionDefinition,
  role: string
) {
  const procedureType = schema.type === 'f' ? 'FUNCTION' : 'PROCEDURE';

  let definitions: string[] = [];

  if (schema.privileges.execute)
    definitions.push(
      `GRANT EXECUTE ON ${procedureType} ${schema.fullName}(${schema.argTypes}) TO ${role};${hints.potentialRoleMissing}`
    );

  return definitions;
}
/**
 *
 * @param {String} sequence
 * @param {String} role
 * @param {Object} privileges
 */
export function generateSequenceGrantsDefinition(
  sequence: string,
  role: string,
  privileges: SequencePrivileges
) {
  const definitions: string[] = [];
  if (privileges.select) {
    definitions.push(
      `GRANT SELECT ON SEQUENCE ${sequence} TO ${role};${hints.potentialRoleMissing}`
    );
  }
  if (privileges.usage) {
    definitions.push(
      `GRANT USAGE ON SEQUENCE ${sequence} TO ${role};${hints.potentialRoleMissing}`
    );
  }
  if (privileges.update) {
    definitions.push(
      `GRANT UPDATE ON SEQUENCE ${sequence} TO ${role};${hints.potentialRoleMissing}`
    );
  }
  return definitions;
}
/**
 *
 * @param {String} objectType
 * @param {String} objectName
 * @param {String} comment
 * @param {String} parentObjectName
 */
export function generateChangeCommentScript(
  id: number | string,
  objectType: string,
  objectName: string,
  comment: string,
  parentObjectName: string | null = null
) {
  const description = comment ? `'${comment.replaceAll("'", "''")}'` : 'NULL';
  const parentObject = parentObjectName ? `ON ${parentObjectName}` : '';
  return stmt`COMMENT ON ${dependency(
    id,
    objectType
  )} ${objectName} ${parentObject} IS ${description};`;
}
/**
 *
 * @param {String} schema
 * @param {String} owner
 */
export function generateCreateSchemaScript(schema: string, owner: string) {
  return stmt`CREATE SCHEMA IF NOT EXISTS ${schema} AUTHORIZATION ${owner};`;
}
/**
 *
 * @param {String} table
 */
export function generateDropTableScript(table: string) {
  return stmt`DROP TABLE IF EXISTS ${table};`;
}
/**
 *
 * @param {String} table
 * @param {Object} schema
 */
export function generateCreateTableScript(table: string, schema: TableObject) {
  //Generate columns script
  let columns: Sql[] = [];
  for (let column in schema.columns) {
    columns.push(generateColumnDefinition(column, schema.columns[column]));
  }

  //Generate constraints script
  for (let name in schema.constraints) {
    const constraint = schema.constraints[name];
    columns.push(
      stmt`CONSTRAINT ${name} ${dependency(
        constraint.relid,
        constraint.definition
      )} `
    );
  }

  //Generate options script
  let options = '';
  if (schema.options && schema.options.withOids)
    options = `\nWITH ( OIDS=${schema.options.withOids
      .toString()
      .toUpperCase()} )`;

  //Generate indexes script
  let indexes: string[] = [];
  for (let index in schema.indexes) {
    let definition = schema.indexes[index].definition;
    definition = definition.replace(
      'CREATE INDEX',
      'CREATE INDEX IF NOT EXISTS'
    );
    definition = definition.replace(
      'CREATE UNIQUE INDEX',
      'CREATE UNIQUE INDEX IF NOT EXISTS'
    );

    indexes.push(`\n${definition};\n`);
  }

  //Generate privileges script
  let privileges: string[] = [];
  privileges.push(`ALTER TABLE IF EXISTS ${table} OWNER TO ${schema.owner};\n`);
  for (let role in schema.privileges) {
    privileges = privileges.concat(
      generateTableGrantsDefinition(table, role, schema.privileges[role])
    );
  }

  let columnsComment: Sql[] = [];
  for (let name in schema.columns) {
    const obj = schema.columns[name];
    columnsComment.push(
      generateChangeCommentScript(
        obj.id,
        objectType.COLUMN,
        `${table}.${name}`,
        obj.comment
      )
    );
  }

  let constraintsComment: Sql[] = [];
  for (let name in schema.constraints) {
    const obj = schema.constraints[name];
    constraintsComment.push(
      generateChangeCommentScript(
        obj.id,
        objectType.CONSTRAINT,
        name,
        obj.comment,
        table
      )
    );
  }

  let indexesComment: Sql[] = [];
  for (let name in schema.indexes) {
    const obj = schema.indexes[name];
    indexesComment.push(
      generateChangeCommentScript(
        obj.id,
        objectType.INDEX,
        `"${obj.schema}"."${name}"`,
        obj.comment
      )
    );
  }

  return stmt`CREATE TABLE IF NOT EXISTS ${declaration(
    schema.id,
    table
  )} (\n\t${join(columns, ',\n\t')}\n)${options};\n${indexes.join(
    '\n'
  )}\n${privileges.join('\n')}\n${columnsComment.join(
    '\n'
  )}${constraintsComment.join('\n')}${join(indexesComment, '\n')}`;
}
export function generateAddTableColumnScript(
  table: string,
  name: string,
  column: Column
) {
  const script = stmt`ALTER TABLE IF EXISTS ${table} ADD COLUMN IF NOT EXISTS ${generateColumnDefinition(
    name,
    column
  )};`;
  if (!column.nullable && !column.default) {
    return stmt`${script} ${hints.addColumnNotNullableWithoutDefaultValue}`;
  }
  return script;
}
/**
 *
 * @param {String} table
 * @param {String} column
 * @param {Object} changes
 */
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
      stmt`ALTER COLUMN ${column} ${changes.default ? 'SET' : 'DROP'} DEFAULT ${
        changes.defaultRef
      }`
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
/**
 *
 * @param {String} table
 * @param {String} column
 */
export function generateDropTableColumnScript(
  table: string,
  column: string,
  withoutHint = false
) {
  return stmt`ALTER TABLE IF EXISTS ${table} DROP COLUMN IF EXISTS ${column} CASCADE;${
    withoutHint ? '' : hints.dropColumn
  }`;
}
export function generateAddTableConstraintScript(
  table: TableObject,
  constraint: string,
  schema: ConstraintDefinition
) {
  return stmt`ALTER TABLE IF EXISTS ${dependency(
    table.id,
    `"${table.schema}"."${table.name}"`
  )} ADD CONSTRAINT ${declaration(schema.id, constraint)} ${dependency(
    schema.relid,
    schema.definition
  )};`;
}
/**
 *
 * @param {String} table
 * @param {String} constraint
 */
export function generateDropTableConstraintScript(
  table: string,
  constraint: string
) {
  return stmt`ALTER TABLE IF EXISTS ${table} DROP CONSTRAINT IF EXISTS ${constraint};`;
}
/**
 *
 * @param {String} table
 * @param {Object} options
 */
export function generateChangeTableOptionsScript(
  table: string,
  options: TableOptions
) {
  return stmt`ALTER TABLE IF EXISTS ${table} SET ${
    options.withOids ? 'WITH' : 'WITHOUT'
  } OIDS;`;
}
export function generateChangeIndexScript(index: string, definition: string) {
  return stmt`DROP INDEX IF EXISTS ${index};\n${definition};`;
}
/**
 *
 * @param {String} index
 */
export function generateDropIndexScript(index: IndexDefinition) {
  return stmt`DROP INDEX IF EXISTS "${index.schema}"."${index.name}";`;
}
export function dropPolicy(schema: string, table: string, policy: string) {
  return stmt`DROP POLICY ${policy} ON "${schema}"."${table}";`;
}
export function createPolicy(schema: string, table: string, policy: Policy) {
  return stmt`CREATE POLICY ${policy.name} 
    ON ${dependency(policy.relid, `"${schema}"."${table}"`)}
    AS ${policy.permissive ? 'PERMISSIVE' : 'RESTRICTIVE'}
    FOR ${policyForMap[policy.for]}
    TO ${policy.roles.join(',')}
    ${policy.using ? `USING ${policy.using}` : ''}
    ${policy.withCheck ? `WITH CHECK ${policy.withCheck}` : ''};`;
}
/**
 *
 * @param {String} table
 * @param {String} role
 * @param {Object} privileges
 */
export function generateTableRoleGrantsScript(
  table: string,
  role: string,
  privileges: any
) {
  return stmt`${generateTableGrantsDefinition(table, role, privileges).join(
    '\n'
  )}`;
}
/**
 *
 * @param {String} table
 * @param {String} role
 * @param {Object} changes
 */
export function generateChangesTableRoleGrantsScript(
  table: string,
  role: string,
  changes: any
) {
  let privileges: string[] = [];

  if (Object.prototype.hasOwnProperty.call(changes, 'select'))
    privileges.push(
      `${changes.select ? 'GRANT' : 'REVOKE'} SELECT ON TABLE ${table} ${
        changes.select ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`
    );

  if (Object.prototype.hasOwnProperty.call(changes, 'insert'))
    privileges.push(
      `${changes.insert ? 'GRANT' : 'REVOKE'} INSERT ON TABLE ${table} ${
        changes.insert ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`
    );

  if (Object.prototype.hasOwnProperty.call(changes, 'update'))
    privileges.push(
      `${changes.update ? 'GRANT' : 'REVOKE'} UPDATE ON TABLE ${table} ${
        changes.update ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`
    );

  if (Object.prototype.hasOwnProperty.call(changes, 'delete'))
    privileges.push(
      `${changes.delete ? 'GRANT' : 'REVOKE'} DELETE ON TABLE ${table} ${
        changes.delete ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`
    );

  if (Object.prototype.hasOwnProperty.call(changes, 'truncate'))
    privileges.push(
      `${changes.truncate ? 'GRANT' : 'REVOKE'} TRUNCATE ON TABLE ${table} ${
        changes.truncate ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`
    );

  if (Object.prototype.hasOwnProperty.call(changes, 'references'))
    privileges.push(
      `${
        changes.references ? 'GRANT' : 'REVOKE'
      } REFERENCES ON TABLE ${table} ${
        changes.references ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`
    );

  if (Object.prototype.hasOwnProperty.call(changes, 'trigger'))
    privileges.push(
      `${changes.trigger ? 'GRANT' : 'REVOKE'} TRIGGER ON TABLE ${table} ${
        changes.trigger ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`
    );

  return stmt`${privileges.join('\n')}`;
}
/**
 *
 * @param {String} table
 * @param {String} owner
 */
export function generateChangeTableOwnerScript(table: string, owner: string) {
  return stmt`ALTER TABLE IF EXISTS ${table} OWNER TO ${owner};`;
}
/**
 *
 * @param {String} view
 * @param {Object} schema
 */
export function generateCreateViewScript(view: string, schema: any) {
  //Generate privileges script
  let privileges: string[] = [];
  privileges.push(`ALTER VIEW IF EXISTS ${view} OWNER TO ${schema.owner};`);
  for (let role in schema.privileges) {
    privileges = privileges.concat(
      generateTableGrantsDefinition(view, role, schema.privileges[role])
    );
  }

  return stmt`CREATE OR REPLACE VIEW ${view} AS ${
    schema.definition
  }\n${privileges.join('\n')}`;
}
/**
 *
 * @param {String} view
 */
export function generateDropViewScript(view: string) {
  return stmt`DROP VIEW IF EXISTS ${view};`;
}
/**
 *
 * @param {String} view
 * @param {Object} schema
 */
export function generateCreateMaterializedViewScript(
  view: string,
  schema: any
) {
  //Generate indexes script
  const indexes: string[] = [];
  for (const index in schema.indexes) {
    indexes.push(`\n${schema.indexes[index].definition};\n`);
  }

  //Generate privileges script
  let privileges: string[] = [];
  privileges.push(
    `ALTER MATERIALIZED VIEW IF EXISTS ${view} OWNER TO ${schema.owner};\n`
  );
  for (let role in schema.privileges) {
    privileges = privileges.concat(
      generateTableGrantsDefinition(view, role, schema.privileges[role])
    );
  }

  return stmt`CREATE MATERIALIZED VIEW IF NOT EXISTS ${view} AS ${
    schema.definition
  }\n${indexes.join('\n')}\n${privileges.join('\n')}`;
}
/**
 *
 * @param {String} view
 */
export function generateDropMaterializedViewScript(view: string) {
  return stmt`DROP MATERIALIZED VIEW IF EXISTS ${view};`;
}
/**
 *
 * @param {String} procedure
 * @param {Object} schema
 * @param {"f"|"p"} type
 */
export function generateCreateProcedureScript(schema: FunctionDefinition) {
  const procedureType = schema.type === 'f' ? 'FUNCTION' : 'PROCEDURE';

  //Generate privileges script
  let privileges: string[] = [];
  privileges.push(
    `ALTER ${procedureType} ${schema.fullName}(${schema.argTypes}) OWNER TO ${schema.owner};`
  );
  for (let role in schema.privileges) {
    privileges = privileges.concat(
      generateProcedureGrantsDefinition(schema, role)
    );
  }
  const st = stmt`${declaration(schema.id, schema.definition)};${dependency(
    schema.prorettype,
    ''
  )}\n${privileges.join('\n')}`;
  st.dependencies.push(...schema.fReferenceIds);
  return st;
}
/**
 *
 * @param {String} aggregate
 * @param {Object} schema
 */
export function generateCreateAggregateScript(schema: AggregateDefinition) {
  //Generate privileges script
  let privileges: string[] = [];
  privileges.push(
    `ALTER AGGREGATE ${schema.fullName}(${schema.argTypes}) OWNER TO ${schema.owner};`
  );
  for (let role in schema.privileges) {
    privileges = privileges.concat(
      generateProcedureGrantsDefinition(schema, role)
    );
  }

  return stmt`CREATE AGGREGATE ${schema.fullName} (${schema.argTypes}) (\n${
    schema.definition
  }\n);\n${privileges.join('\n')}`;
}

/**
 *
 * @param {String} aggregate
 * @param {Object} schema
 */
export function generateChangeAggregateScript(schema: AggregateDefinition) {
  return stmt`DROP AGGREGATE IF EXISTS ${schema.fullName}(${
    schema.argTypes
  });\n${generateCreateAggregateScript(schema)}`;
}
/**
 *
 * @param {String} procedure
 * @param {String} procedureArgs
 */
export function generateDropProcedureScript(schema: FunctionDefinition) {
  const procedureType = schema.type === 'f' ? 'FUNCTION' : 'PROCEDURE';
  return stmt`DROP ${procedureType} IF EXISTS ${schema.fullName}(${schema.argTypes});`;
}
/**
 *
 * @param {String} aggregate
 * @param {String} aggregateArgs
 */
export function generateDropAggregateScript(
  aggregate: string,
  aggregateArgs: string
) {
  return stmt`DROP AGGREGATE IF EXISTS ${aggregate}(${aggregateArgs});`;
}
/**
 *
 * @param {String} procedure
 * @param {String} argTypes
 * @param {String} role
 * @param {Object} privileges
 * @param {"f"|"p"} type
 */
export function generateProcedureRoleGrantsScript(
  schema: FunctionDefinition,
  role: string
) {
  return stmt`${generateProcedureGrantsDefinition(schema, role).join('\n')}`;
}
export function generateChangesProcedureRoleGrantsScript(
  schema: FunctionDefinition,
  role: string,
  changes: any
) {
  const procedureType = schema.type === 'f' ? 'FUNCTION' : 'PROCEDURE';
  let privileges: string[] = [];

  if (Object.prototype.hasOwnProperty.call(changes, 'execute'))
    privileges.push(
      `${changes.execute ? 'GRANT' : 'REVOKE'} EXECUTE ON ${procedureType} ${
        schema.fullName
      }(${schema.argTypes}) ${changes.execute ? 'TO' : 'FROM'} ${role};${
        hints.potentialRoleMissing
      }`
    );

  return stmt`${privileges.join('\n')}`;
}
export function generateChangeProcedureOwnerScript(
  procedure: string,
  argTypes: string,
  owner: string,
  type: 'p' | 'f'
) {
  const procedureType = type === 'f' ? 'FUNCTION' : 'PROCEDURE';

  return stmt`ALTER ${procedureType} ${procedure}(${argTypes}) OWNER TO ${owner};`;
}
export function generateChangeAggregateOwnerScript(
  aggregate: string,
  argTypes: string,
  owner: string
) {
  return stmt`ALTER AGGREGATE ${aggregate}(${argTypes}) OWNER TO ${owner};`;
}
export function generateUpdateTableRecordScript(
  table: string,
  fields: any,
  filterConditions: any,
  changes: any
) {
  let updates: string[] = [];
  for (let field in changes) {
    updates.push(
      `"${field}" = ${generateSqlFormattedValue(field, fields, changes[field])}`
    );
  }

  let conditions: string[] = [];
  for (let condition in filterConditions) {
    conditions.push(
      `"${condition}" = ${generateSqlFormattedValue(
        condition,
        fields,
        filterConditions[condition]
      )}`
    );
  }

  return stmt`UPDATE ${table} SET ${updates.join(', ')} WHERE ${conditions.join(
    ' AND '
  )};`;
}
export function generateInsertTableRecordScript(
  table: string,
  record: any,
  fields: any[],
  isIdentityValuesAllowed: boolean
) {
  let fieldNames: string[] = [];
  let fieldValues: string[] = [];
  for (let field in record) {
    fieldNames.push(`"${field}"`);
    fieldValues.push(generateSqlFormattedValue(field, fields, record[field]));
  }

  let script = stmt`INSERT INTO ${table} (${fieldNames.join(', ')}) ${
    isIdentityValuesAllowed ? '' : 'OVERRIDING SYSTEM VALUE'
  } VALUES (${fieldValues.join(', ')});`;
  if (!isIdentityValuesAllowed)
    return stmt`\n${hints.identityColumnDetected} ${script}`;
  return script;
}
export function generateDeleteTableRecordScript(
  table: string,
  fields: any[],
  keyFieldsMap: any
) {
  let conditions: string[] = [];
  for (let condition in keyFieldsMap) {
    conditions.push(
      `"${condition}" = ${generateSqlFormattedValue(
        condition,
        fields,
        keyFieldsMap[condition]
      )}`
    );
  }

  return stmt`DELETE FROM ${table} WHERE ${conditions.join(' AND ')};`;
}
export function generateSqlFormattedValue(
  fieldName: string,
  fields: any,
  value: any
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
        `The data type category '${dataTypeCategory}' is not implemented yet!`
      );
  }
}
export function generateMergeTableRecord(
  table: string,
  fields: any,
  changes: any,
  options: any
) {
  let fieldNames: string[] = [];
  let fieldValues: string[] = [];
  let updates: string[] = [];
  for (let field in changes) {
    fieldNames.push(`"${field}"`);
    fieldValues.push(generateSqlFormattedValue(field, fields, changes[field]));
    updates.push(
      `"${field}" = ${generateSqlFormattedValue(field, fields, changes[field])}`
    );
  }

  let conflictDefinition = '';
  if (options.constraintName)
    conflictDefinition = `ON CONSTRAINT ${options.constraintName}`;
  else if (options.uniqueFields && options.uniqueFields.length > 0)
    conflictDefinition = `("${options.uniqueFields.join('", "')}")`;
  else
    throw new Error(
      `Impossible to generate conflict definition for table ${table} record to merge!`
    );

  let script = `INSERT INTO ${table} (${fieldNames.join(
    ', '
  )}) VALUES (${fieldValues.join(
    ', '
  )})\nON CONFLICT ${conflictDefinition}\nDO UPDATE SET ${updates.join(', ')}`;
  return script;
}
export function generateSetSequenceValueScript(
  tableName: string,
  sequence: any
) {
  return stmt`SELECT setval(pg_get_serial_sequence('${tableName}', '${sequence.attname}'), max("${sequence.attname}"), true) FROM ${tableName};`;
}
export function sequencePropertyMap(
  property: SequenceProperties,
  value: string
) {
  switch (property) {
    case 'startValue':
      return `START WITH ${value}`;
    case 'minValue':
      return `MINVALUE ${value}`;
    case 'maxValue':
      return `MAXVALUE ${value}`;
    case 'increment':
      return `INCREMENT BY ${value}`;
    case 'cacheSize':
      return `CACHE ${value}`;
    case 'isCycle':
      return `${value ? '' : 'NO'} CYCLE`;
    case 'owner':
      return `OWNER TO ${value}`;
    default:
      throw new Error(`Unsupported property ${property}`);
  }
}
export function generateChangeSequencePropertyScript(
  sequence: string,
  property: SequenceProperties,
  value: string
) {
  return stmt`ALTER SEQUENCE IF EXISTS ${sequence} ${sequencePropertyMap(
    property,
    value
  )};`;
}
export function generateChangesSequenceRoleGrantsScript(
  sequence: string,
  role: string,
  changes: any
) {
  let privileges: string[] = [];

  if (Object.prototype.hasOwnProperty.call(changes, 'select'))
    privileges.push(
      `${changes.select ? 'GRANT' : 'REVOKE'} SELECT ON SEQUENCE ${sequence} ${
        changes.select ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`
    );

  if (Object.prototype.hasOwnProperty.call(changes, 'usage'))
    privileges.push(
      `${changes.usage ? 'GRANT' : 'REVOKE'} USAGE ON SEQUENCE ${sequence} ${
        changes.usage ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`
    );

  if (Object.prototype.hasOwnProperty.call(changes, 'update'))
    privileges.push(
      `${changes.update ? 'GRANT' : 'REVOKE'} UPDATE ON SEQUENCE ${sequence} ${
        changes.update ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`
    );

  return stmt`${privileges.join('\n')}`;
}
export function generateSequenceRoleGrantsScript(
  sequence: string,
  role: string,
  privileges: any
) {
  return stmt`${generateSequenceGrantsDefinition(
    sequence,
    role,
    privileges
  ).join('\n')}`;
}
export function generateCreateSequenceScript(
  sequence: Sequence,
  owner: string
) {
  //Generate privileges script
  const fullName = `"${sequence.schema}"."${sequence.name}"`;
  const privileges: (string | string[])[] = [
    `ALTER SEQUENCE ${fullName} OWNER TO ${owner};`,
  ];

  for (const role in sequence.privileges) {
    privileges.push(
      generateSequenceGrantsDefinition(
        fullName,
        role,
        sequence.privileges[role]
      )
    );
  }

  return stmt`
CREATE SEQUENCE IF NOT EXISTS ${fullName} 
\tINCREMENT BY ${sequence.increment} 
\tMINVALUE ${sequence.minValue}
\tMAXVALUE ${sequence.maxValue}
\tSTART WITH ${sequence.startValue}
\tCACHE ${sequence.cacheSize}
\t${sequence.isCycle ? '' : 'NO '}CYCLE;
\n${privileges.flat().join('\n')}`;
}
export function generateRenameSequenceScript(
  old_name: string,
  new_name: string
) {
  return stmt`ALTER SEQUENCE IF EXISTS ${old_name} RENAME TO ${new_name};`;
}
