import { ClientBase } from 'pg';
import { DatabaseObjects } from '../catalog/database-objects';
import { TableDefinition } from '../models/table-definition';
import EventEmitter from 'events';
import { Config } from '../models/config';
import { Sql, stmt } from '../stmt';
import { TableData } from '../models/table-data';
import { getServerVersion } from '../utils';
import * as sql from '../sql-script-generator';
import { isEqual } from 'lodash';

export async function compareTablesRecords(
  config: Config,
  sourceClient: ClientBase,
  targetClient: ClientBase,
  addedColumns: any,
  addedTables: string[],
  dbSourceObjects: DatabaseObjects,
  dbTargetObjects: DatabaseObjects,
  eventEmitter: EventEmitter
) {
  const lines: Sql[] = [];
  let iteratorCounter = 0;
  let progressStepSize = Math.floor(
    20 / config.compareOptions.dataCompare.tables.length
  );

  for (let tableDefinition of config.compareOptions.dataCompare.tables) {
    let differentRecords = 0;
    let fullTableName = `"${tableDefinition.tableSchema || 'public'}"."${
      tableDefinition.tableName
    }"`;

    if (!(await checkIfTableExists(sourceClient, tableDefinition))) {
      lines.push(
        stmt`\n--ERROR: Table ${fullTableName} not found on SOURCE database for comparison!\n`
      );
    } else {
      let tableData: TableData = {
        sourceData: {
          records: {
            fields: [],
            rows: [],
          },
          sequences: [],
        },
        targetData: {
          records: {
            fields: [],
            rows: [],
          },
          sequences: [],
        },
      };
      tableData.sourceData.records = await collectTableRecords(
        sourceClient,
        tableDefinition,
        dbSourceObjects
      );
      tableData.sourceData.sequences = await collectTableSequences(
        sourceClient,
        tableDefinition
      );

      let isNewTable = false;
      if (addedTables.includes(fullTableName)) isNewTable = true;

      if (
        !isNewTable &&
        !(await checkIfTableExists(targetClient, tableDefinition))
      ) {
        lines.push(
          stmt`\n--ERROR: Table "${tableDefinition.tableSchema || 'public'}"."${
            tableDefinition.tableName
          }" not found on TARGET database for comparison!\n`
        );
      } else {
        tableData.targetData.records = await collectTableRecords(
          targetClient,
          tableDefinition,
          dbTargetObjects,
          isNewTable
        );
        //  tableData.targetData.sequences = await collectTableSequences(targetClient, tableDefinition);

        let compareResult = compareTableRecords(
          tableDefinition,
          tableData,
          addedColumns
        );
        lines.push(...compareResult.lines);
        differentRecords = lines.length;

        if (compareResult.isSequenceRebaseNeeded)
          lines.push(...rebaseSequences(tableDefinition, tableData));
      }
    }

    iteratorCounter += 1;

    eventEmitter.emit(
      'compare',
      `Records for table ${fullTableName} have been compared with ${differentRecords} differences`,
      70 + progressStepSize * iteratorCounter
    );
  }

  return lines;
}

export async function collectTableRecords(
  client: ClientBase,
  tableDefinition: TableDefinition,
  dbObjects: DatabaseObjects,
  isNewTable?: boolean
) {
  let result: any = {
    fields: [],
    rows: [],
  };

  if (!isNewTable) {
    let fullTableName = `"${tableDefinition.tableSchema || 'public'}"."${
      tableDefinition.tableName
    }"`;

    let misssingKeyField = '';
    let missingKeyColumns = tableDefinition.tableKeyFields.some((k) => {
      if (
        !Object.keys(dbObjects.tables[fullTableName].columns).includes(`"${k}"`)
      ) {
        misssingKeyField = k;
        return true;
      }
    });

    if (missingKeyColumns)
      throw new Error(
        `The table [${fullTableName}] doesn't contains the field [${misssingKeyField}]`
      );

    let response = await client.query(
      `SELECT MD5(ROW(${tableDefinition.tableKeyFields
        .map((c) => `"${c}"`)
        .join(',')})::text) AS "rowHash", * FROM ${fullTableName}`
    );

    for (const field of response.fields) {
      if (field.name === 'rowHash') continue;
      const { datatype, dataTypeCategory, generatedColumn } =
        dbObjects.tables[fullTableName].columns[`"${field.name}"`];
      result.fields.push({
        ...field,
        datatype,
        dataTypeCategory,
        isGeneratedColumn: !!generatedColumn,
      });
    }

    result.rows = response.rows;
  }

  return result;
}

async function checkIfTableExists(
  client: ClientBase,
  tableDefinition: TableDefinition
) {
  let response = await client.query(
    `SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = '${
      tableDefinition.tableName
    }' AND schemaname = '${tableDefinition.tableSchema || 'public'}')`
  );

  return !!response.rows[0].exists;
}

async function collectTableSequences(
  client: ClientBase,
  tableDefinition: TableDefinition
) {
  let identityFeature = `
        CASE 
            WHEN COALESCE(a.attidentity,'') = '' THEN 'SERIAL'
            WHEN a.attidentity = 'a' THEN 'ALWAYS'
            WHEN a.attidentity = 'd' THEN 'BY DEFAULT'
        END AS identitytype`;

  let response = await client.query<{}>(`
            SELECT * FROM (
                SELECT 
                    pg_get_serial_sequence(a.attrelid::regclass::name, a.attname) AS seqname,
                    a.attname,
                    ${
                      ((await getServerVersion(client))?.major ?? 0) >= 10
                        ? identityFeature
                        : "'SERIAL' AS identitytype"
                    }
                FROM pg_attribute a
                WHERE a.attrelid = '"${
                  tableDefinition.tableSchema || 'public'
                }"."${tableDefinition.tableName}"'::regclass
                AND a.attnum > 0
                AND a.attisdropped = false
            ) T WHERE T.seqname IS NOT NULL`);

  return response.rows;
}

function compareTableRecords(
  tableDefinition: TableDefinition,
  tableData: TableData,
  addedColumns: any
) {
  let ignoredRowHash: string[] = [];
  let result: { lines: Sql[]; isSequenceRebaseNeeded: boolean } = {
    lines: [],
    isSequenceRebaseNeeded: false,
  };
  let fullTableName = `"${tableDefinition.tableSchema || 'public'}"."${
    tableDefinition.tableName
  }"`;

  //Check if at least one sequence is for an ALWAYS IDENTITY in case the OVERRIDING SYSTEM VALUE must be issued
  let isIdentityValuesAllowed = !tableData.sourceData.sequences.some(
    (sequence) => sequence.identitytype === 'ALWAYS'
  );

  tableData.sourceData.records.rows.forEach((record, index) => {
    //Check if row hash has been ignored because duplicated or already processed from source
    if (ignoredRowHash.some((hash) => hash === record.rowHash)) return;

    let keyFieldsMap = getKeyFieldsMap(tableDefinition.tableKeyFields, record);

    //Check if record is duplicated in source
    if (
      tableData.sourceData.records.rows.some(
        (r, idx) => r.rowHash === record.rowHash && idx > index
      )
    ) {
      ignoredRowHash.push(record.rowHash);
      result.lines.push(
        stmt`\n--ERROR: Too many record found in SOURCE database for table ${fullTableName} and key fields ${JSON.stringify(
          keyFieldsMap
        )} !\n`
      );
      return;
    }

    //Check if record is duplicated in target
    let targetRecord: any[] = [];
    targetRecord = tableData.targetData.records.rows.filter(function (r) {
      return r.rowHash === record.rowHash;
    });

    if (targetRecord.length > 1) {
      ignoredRowHash.push(record.rowHash);
      result.lines.push(
        stmt`\n--ERROR: Too many record found in TARGET database for table ${fullTableName} and key fields ${JSON.stringify(
          keyFieldsMap
        )} !\n`
      );
      return;
    }

    ignoredRowHash.push(record.rowHash);

    //Generate sql script to add\update record in target database table
    if (targetRecord.length <= 0) {
      //A record with same KEY FIELDS not exists, then create a new record
      delete record.rowHash; //Remove property from "record" object in order to not add it on sql script
      result.lines.push(
        sql.generateInsertTableRecordScript(
          fullTableName,
          record,
          tableData.sourceData.records.fields,
          isIdentityValuesAllowed
        )
      );
      result.isSequenceRebaseNeeded = true;
    } else {
      //A record with same KEY FIELDS VALUES has been found, then update not matching fieds only
      let fieldCompareResult = compareTableRecordFields(
        fullTableName,
        keyFieldsMap,
        tableData.sourceData.records.fields,
        record,
        targetRecord[0],
        addedColumns
      );
      if (fieldCompareResult.isSequenceRebaseNeeded)
        result.isSequenceRebaseNeeded = true;
      result.lines.push(...fieldCompareResult.lines);
    }
  });

  tableData.targetData.records.rows.forEach((record, index) => {
    //Check if row hash has been ignored because duplicated or already processed from source
    if (ignoredRowHash.some((hash) => hash === record.rowHash)) return;

    let keyFieldsMap = getKeyFieldsMap(tableDefinition.tableKeyFields, record);

    if (
      tableData.targetData.records.rows.some(
        (r, idx) => r.rowHash === record.rowHash && idx > index
      )
    ) {
      ignoredRowHash.push(record.rowHash);
      result.lines.push(
        stmt`\n--ERROR: Too many record found in TARGET database for table ${fullTableName} and key fields ${JSON.stringify(
          keyFieldsMap
        )} !\n`
      );
      return;
    }

    //Generate sql script to delete record because not exists on source database table
    result.lines.push(
      sql.generateDeleteTableRecordScript(
        fullTableName,
        tableData.sourceData.records.fields,
        keyFieldsMap
      )
    );
    result.isSequenceRebaseNeeded = true;
  });

  return result;
}

function rebaseSequences(
  tableDefinition: TableDefinition,
  tableData: TableData
) {
  const lines: Sql[] = [];
  const fullTableName = `"${tableDefinition.tableSchema || 'public'}"."${
    tableDefinition.tableName
  }"`;

  tableData.sourceData.sequences.forEach((sequence) => {
    lines.push(sql.generateSetSequenceValueScript(fullTableName, sequence));
  });

  return lines;
}
function compareTableRecordFields(
  table: string,
  keyFieldsMap: any,
  fields: any[],
  sourceRecord: any,
  targetRecord: any,
  addedColumns: any
) {
  let changes: any = {};
  let result: { lines: Sql[]; isSequenceRebaseNeeded: boolean } = {
    lines: [],
    isSequenceRebaseNeeded: false,
  };

  for (const field in sourceRecord) {
    if (field === 'rowHash') continue;
    if (fields.some((f) => f.name == field && f.isGeneratedColumn == true)) {
      continue;
    }

    if (
      targetRecord[field] === undefined &&
      checkIsNewColumn(addedColumns, table, field)
    ) {
      changes[field] = sourceRecord[field];
    } else if (compareFieldValues(sourceRecord[field], targetRecord[field])) {
      changes[field] = sourceRecord[field];
    }
  }

  if (Object.keys(changes).length > 0) {
    result.isSequenceRebaseNeeded = true;
    result.lines.push(
      sql.generateUpdateTableRecordScript(table, fields, keyFieldsMap, changes)
    );
  }

  return result;
}
function getKeyFieldsMap(keyFields: string[], record: any) {
  let keyFieldsMap: Record<string, any> = {};
  keyFields.forEach((item) => {
    keyFieldsMap[item] = record[item];
  });
  return keyFieldsMap;
}

function checkIsNewColumn(addedColumns: any, table: string, field: string) {
  return !!addedColumns[table]?.some((column: any) => column == field);
}

function compareFieldValues(sourceValue: any, targetValue: any) {
  const sourceValueType = typeof sourceValue;
  const targetValueType = typeof targetValue;

  if (sourceValueType != targetValueType) return false;
  else if (sourceValue instanceof Date)
    return sourceValue.getTime() !== targetValue.getTime();
  else if (sourceValue instanceof Object)
    return !isEqual(sourceValue, targetValue);
  else return sourceValue !== targetValue;
}
