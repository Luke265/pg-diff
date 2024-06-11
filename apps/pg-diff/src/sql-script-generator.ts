import { ColumnChanges, PrivilegeChanges } from './compare/utils';
import objectType from './enums/object-type';
import {
  AggregateDefinition,
  Column,
  ConstraintDefinition,
  Domain,
  FunctionDefinition,
  IndexDefinition,
  MaterializedViewDefinition,
  Policy,
  Privileges,
  Sequence,
  SequencePrivileges,
  TableObject,
  TableOptions,
  Type,
  ViewDefinition,
} from './catalog/database-objects';
import { Sql, declaration, dependency, join, stmt } from './stmt';

const PROCEDURE_TYPE = {
  p: 'PROCEDURE',
  f: 'FUNCTION',
} as const;

const POLICY_FOR = {
  '*': 'ALL',
  w: 'UPDATE',
  r: 'SELECT',
  a: 'INSERT',
  d: 'DELETE',
} as const;

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
} as const;

export type SequenceProperties =
  | 'startValue'
  | 'minValue'
  | 'maxValue'
  | 'increment'
  | 'cacheSize'
  | 'isCycle'
  | 'owner';

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
export function generateTableGrantsDefinition(
  table: string,
  role: string,
  privileges: Privileges
): Sql | null {
  const list = [
    ['SELECT', privileges.select],
    ['INSERT', privileges.insert],
    ['UPDATE', privileges.update],
    ['DELETE', privileges.delete],
    ['TRUNCATE', privileges.truncate],
    ['REFERENCES', privileges.references],
    ['TRIGGER', privileges.trigger],
  ];
  const filtered = list.filter((v) => v[1] !== undefined);
  if (filtered.length === 0) {
    return null;
  }
  const str =
    list.length === filtered.length
      ? 'ALL'
      : filtered.map(([op]) => op).join(', ');
  return stmt`GRANT ${str} ON TABLE ${table} TO ${role};${hints.potentialRoleMissing}`;
}

export function generateProcedureGrantsDefinition(
  schema: FunctionDefinition,
  role: string
) {
  if (schema.privileges.execute) {
    return [
      `GRANT EXECUTE ON ${PROCEDURE_TYPE[schema.type]} ${schema.fullName}(${
        schema.argTypes
      }) TO ${role};${hints.potentialRoleMissing}`,
    ];
  }
  return [];
}

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
    objectType,
    id
  )} ${objectName} ${parentObject} IS ${description};`;
}

export function generateCreateSchemaScript(schema: string, owner: string) {
  return stmt`CREATE SCHEMA IF NOT EXISTS ${schema} AUTHORIZATION ${owner};`;
}

export function generateDropTableScript(table: string) {
  return stmt`DROP TABLE IF EXISTS ${table};`;
}

export function generateDropTypeScript(type: Type) {
  return stmt`DROP TYPE ${type.fullName};`;
}

export function generateDropDomainScript(type: Domain) {
  return stmt`DROP DOMAIN ${type.fullName};`;
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

export function generateCreateDomainScript(schema: Domain) {
  return stmt`CREATE DOMAIN ${declaration(
    schema.id,
    schema.fullName
  )} AS ${dependency(schema.type.fullName, schema.type.id)} ${schema.check};`;
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
        constraint.relid
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
  const indexes: string[] = Object.values(schema.indexes).map(
    (obj) =>
      obj.definition
        .replace('CREATE INDEX', 'CREATE INDEX IF NOT EXISTS')
        .replace('CREATE UNIQUE INDEX', 'CREATE UNIQUE INDEX IF NOT EXISTS') +
      ';'
  );

  const privileges = Object.entries(schema.privileges)
    .map(([role, obj]) => generateTableGrantsDefinition(table, role, obj))
    .filter((v) => !!v);

  const columnsComment: Sql[] = columnArr
    .filter((obj) => !!obj.comment)
    .map((obj) =>
      generateChangeCommentScript(
        obj.id,
        objectType.COLUMN,
        obj.fullName,
        obj.comment
      )
    );

  const constraintsComment: Sql[] = Object.values(schema.constraints)
    .filter((obj) => !!obj.comment)
    .map((obj) =>
      generateChangeCommentScript(
        obj.id,
        objectType.CONSTRAINT,
        obj.name,
        obj.comment,
        table
      )
    );

  const indexesComment: Sql[] = Object.values(schema.indexes)
    .filter((obj) => !!obj.comment)
    .map((obj) =>
      generateChangeCommentScript(
        obj.id,
        objectType.INDEX,
        obj.name,
        obj.comment
      )
    );
  return stmt`CREATE TABLE IF NOT EXISTS ${declaration(
    schema.id,
    table
  )} (\n\t${join(columns, ',\n\t')}\n)${options};
${indexes.join('\n')}
ALTER TABLE IF EXISTS ${table} OWNER TO ${schema.owner};
${join(privileges, '\n')}
${join(columnsComment, '\n')}
${join(constraintsComment, '\n')}
${join(indexesComment, '\n')}`;
}

export function generateAddTypeColumnScript(schema: Type, column: Column) {
  return stmt`ALTER TYPE ${dependency(
    schema.fullName,
    schema.id
  )} ADD ATTRIBUTE ${generateTypeColumnDefinition(column)};`;
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

export function generateDropTypeColumnScript(table: Type, column: Column) {
  return stmt`ALTER TABLE IF EXISTS ${dependency(
    table.fullName,
    table.id
  )} DROP COLUMN IF EXISTS ${column.name} CASCADE;`;
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

export function generateAddTableConstraintScript(
  table: TableObject,
  constraint: string,
  schema: ConstraintDefinition
) {
  return stmt`ALTER TABLE IF EXISTS ${dependency(
    table.fullName,
    table.id
  )} ADD CONSTRAINT ${declaration(schema.id, constraint)} ${dependency(
    schema.definition,
    schema.relid
  )};`;
}

export function generateDropTableConstraintScript(
  table: TableObject,
  constraint: ConstraintDefinition
) {
  return stmt`ALTER TABLE IF EXISTS ${table.fullName} DROP CONSTRAINT IF EXISTS "${constraint.name}";`;
}

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

export function generateDropIndexScript(index: IndexDefinition) {
  return stmt`DROP INDEX IF EXISTS "${index.schema}"."${index.name}";`;
}

export function dropPolicy(schema: string, table: string, policy: string) {
  const s = stmt`DROP POLICY ${policy} ON "${schema}"."${table}";`;
  s.weight = -1;
  return s;
}

export function createPolicy(schema: string, table: string, policy: Policy) {
  const s = stmt`CREATE POLICY ${policy.name} 
  ON ${dependency(`"${schema}"."${table}"`, policy.relid, policy.dependencies)}
  AS ${policy.permissive ? 'PERMISSIVE' : 'RESTRICTIVE'}
  FOR ${POLICY_FOR[policy.for]}
  TO ${policy.roles.join(',')}
  ${policy.using ? `USING ${policy.using}` : ''}
  ${policy.withCheck ? `WITH CHECK ${policy.withCheck}` : ''};`;
  s.weight = 1;
  return s;
}

export function generateTableRoleGrantsScript(
  table: string,
  role: string,
  privileges: Privileges
) {
  return generateTableGrantsDefinition(table, role, privileges);
}

export function generateChangesTableRoleGrantsScript(
  table: string,
  role: string,
  changes: PrivilegeChanges
) {
  const list = [
    ['SELECT', changes.select],
    ['INSERT', changes.insert],
    ['UPDATE', changes.update],
    ['DELETE', changes.delete],
    ['TRUNCATE', changes.truncate],
    ['REFERENCES', changes.references],
    ['TRIGGER', changes.trigger],
  ];

  return list
    .filter((v) => v[1] !== undefined)
    .map(
      ([op, grant]) =>
        stmt`${grant ? 'GRANT' : 'REVOKE'} ${op} ON TABLE ${table} ${
          grant ? 'TO' : 'FROM'
        } ${role};${hints.potentialRoleMissing}`
    );
}

export function generateChangeTableOwnerScript(table: string, owner: string) {
  return stmt`ALTER TABLE IF EXISTS ${table} OWNER TO ${owner};`;
}

export function generateChangeTypeOwnerScript(type: Type, owner: string) {
  return stmt`ALTER TYPE ${dependency(
    type.fullName,
    type.id
  )} OWNER TO ${owner};`;
}

export function generateChangeDomainOwnerScript(type: Domain, owner: string) {
  return stmt`ALTER DOMAIN ${dependency(
    type.fullName,
    type.id
  )} OWNER TO ${owner};`;
}

export function generateChangeDomainCheckScript(type: Domain) {
  return stmt`ALTER DOMAIN DROP CONSTRAINT ${
    type.constraintName
  };\nALTER DOMAIN ${dependency(type.fullName, type.id)} ADD CONSTRAINT ${
    type.constraintName
  } ${type.check};`;
}

export function generateCreateViewScript(view: string, schema: ViewDefinition) {
  const privileges = Object.entries(schema.privileges)
    .map(([role, obj]) => generateTableGrantsDefinition(view, role, obj))
    .filter((v) => !!v);
  return stmt`CREATE OR REPLACE VIEW ${view} AS ${schema.definition}
ALTER VIEW IF EXISTS ${view} OWNER TO ${schema.owner};
${join(privileges, '\n')}`;
}

export function generateDropViewScript(view: string) {
  return stmt`DROP VIEW IF EXISTS ${view};`;
}

export function generateCreateMaterializedViewScript(
  view: string,
  schema: MaterializedViewDefinition
) {
  //Generate indexes script
  const indexes: string[] = [];
  for (const index in schema.indexes) {
    indexes.push(`\n${schema.indexes[index].definition};\n`);
  }

  //Generate privileges script
  const privileges = Object.entries(schema.privileges)
    .map(([role, obj]) => generateTableGrantsDefinition(view, role, obj))
    .filter((v) => !!v);

  return stmt`CREATE MATERIALIZED VIEW IF NOT EXISTS ${view} AS ${
    schema.definition
  }\n${indexes.join('\n')}
ALTER MATERIALIZED VIEW IF EXISTS ${view} OWNER TO ${schema.owner};
${join(privileges, '\n')}`;
}

export function generateDropMaterializedViewScript(view: string) {
  return stmt`DROP MATERIALIZED VIEW IF EXISTS ${view};`;
}

export function generateCreateProcedureScript(schema: FunctionDefinition) {
  const privileges = Object.entries(schema.privileges)
    .map(([role]) => generateProcedureGrantsDefinition(schema, role))
    .flat();
  const st = stmt`${declaration(schema.id, schema.definition)};
ALTER ${PROCEDURE_TYPE[schema.type]} ${schema.fullName}(${
    schema.argTypes
  }) OWNER TO ${schema.owner};
${privileges.join('\n')}`;
  st.dependencies.push(...schema.fReferenceIds);
  return st;
}

export function generateCreateAggregateScript(schema: AggregateDefinition) {
  const privileges = Object.entries(schema.privileges)
    .map(([role]) => generateProcedureGrantsDefinition(schema, role))
    .flat();

  return stmt`CREATE AGGREGATE ${schema.fullName} (${schema.argTypes}) (\n${
    schema.definition
  }\n);
ALTER AGGREGATE ${schema.fullName}(${schema.argTypes}) OWNER TO ${schema.owner};
${privileges.join('\n')}`;
}

export function generateChangeAggregateScript(schema: AggregateDefinition) {
  return stmt`DROP AGGREGATE IF EXISTS ${schema.fullName}(${
    schema.argTypes
  });\n${generateCreateAggregateScript(schema)}`;
}

export function generateDropProcedureScript(schema: FunctionDefinition) {
  const s = stmt`DROP ${PROCEDURE_TYPE[schema.type]} IF EXISTS ${
    schema.fullName
  }(${schema.argTypes});`;
  s.weight = 1;
  return s;
}

export function generateDropAggregateScript(
  aggregate: string,
  aggregateArgs: string
) {
  return stmt`DROP AGGREGATE IF EXISTS ${aggregate}(${aggregateArgs});`;
}

export function generateProcedureRoleGrantsScript(
  schema: FunctionDefinition,
  role: string
) {
  return stmt`${generateProcedureGrantsDefinition(schema, role).join('\n')}`;
}

export function generateChangesProcedureRoleGrantsScript(
  schema: FunctionDefinition,
  role: string,
  changes: { execute: boolean | null }
) {
  if (changes.execute === null) {
    return null;
  }
  return stmt`${changes.execute ? 'GRANT' : 'REVOKE'} EXECUTE ON ${
    PROCEDURE_TYPE[schema.type]
  } ${schema.fullName}(${schema.argTypes}) ${
    changes.execute ? 'TO' : 'FROM'
  } ${role};${hints.potentialRoleMissing}`;
}

export function generateChangeProcedureOwnerScript(
  procedure: string,
  argTypes: string,
  owner: string,
  type: 'p' | 'f'
) {
  return stmt`ALTER ${PROCEDURE_TYPE[type]} ${procedure}(${argTypes}) OWNER TO ${owner};`;
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

function sequencePropertyMap(property: SequenceProperties, value: string) {
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
