import { CatalogApi } from './catalog-api';
import { core } from '../core';
import {
  AggregateDefinition,
  Column,
  ConstraintDefinition,
  DatabaseObjects,
  FunctionDefinition,
  FunctionPrivileges,
  IndexDefinition,
  MaterializedViewDefinition,
  Policy,
  Privileges,
  Schema,
  Sequence,
  SequencePrivileges,
  TableObject,
  TableOptions,
  ViewDefinition,
} from '../models/database-objects';
import { SequenceProperties } from '../sql-script-generator';
import * as sql from '../sql-script-generator';
import { TableData } from '../models/table-data';
import objectType from '../enums/object-type';
import { add, isEqual } from 'lodash';
import path from 'path';
import fs from 'fs';
import EventEmitter from 'events';
import { Config } from '../models/config';
import { TableDefinition } from '../models/table-definition';
import { ClientBase } from 'pg';
import { getServerVersion } from '../utils';
import { Sql, SqlRef, stmt } from '../stmt';

export interface ColumnChanges {
  datatype?: any;
  dataTypeID?: any;
  dataTypeCategory?: any;
  precision?: any;
  scale?: any;
  nullable?: any;
  default?: any;
  defaultRef?: SqlRef;
  identity?: any;
  isNewIdentity?: any;
  truncate?: any;
  references?: any;
  trigger?: any;
  select?: any;
  insert?: any;
  update?: any;
  delete?: any;
  execute?: any;
  usage?: any;
}

export interface Context {
  mapRole: (input: string) => string;
}

export class CompareApi {
  /**
   *
   * @returns  Return the sql patch file pathh
   */
  static async compare(
    config: Config,
    scriptName: string,
    eventEmitter: EventEmitter
  ) {
    eventEmitter.emit('compare', 'Compare started', 0);

    eventEmitter.emit('compare', 'Connecting to source database ...', 10);
    let pgSourceClient = await core.makePgClient(config.sourceClient);
    eventEmitter.emit(
      'compare',
      `Connected to source PostgreSQL ${
        (await getServerVersion(pgSourceClient)).version
      } on [${config.sourceClient.host}:${config.sourceClient.port}/${
        config.sourceClient.database
      }] `,
      11
    );

    eventEmitter.emit('compare', 'Connecting to target database ...', 20);
    let pgTargetClient = await core.makePgClient(config.targetClient);
    eventEmitter.emit(
      'compare',
      `Connected to target PostgreSQL ${
        (await getServerVersion(pgTargetClient)).version
      } on [${config.targetClient.host}:${config.targetClient.port}/${
        config.targetClient.database
      }] `,
      21
    );

    let dbSourceObjects = await this.collectSchemaObjects(
      pgSourceClient,
      config
    );
    eventEmitter.emit('compare', 'Collected SOURCE objects', 30);
    let dbTargetObjects = await this.collectSchemaObjects(
      pgTargetClient,
      config
    );
    eventEmitter.emit('compare', 'Collected TARGET objects', 40);

    const { dropped, added, ddl } = this.compareDatabaseObjects(
      dbSourceObjects,
      dbTargetObjects,
      config,
      eventEmitter
    );

    //The progress step size is 20
    if (config.compareOptions.dataCompare.enable) {
      ddl.push(
        ...(await this.compareTablesRecords(
          config,
          pgSourceClient,
          pgTargetClient,
          added.columns,
          added.tables,
          dbSourceObjects,
          dbTargetObjects,
          eventEmitter
        ))
      );
      eventEmitter.emit('compare', 'Table records have been compared', 95);
    }

    let scriptFilePath = await this.saveSqlScript(
      ddl,
      config,
      scriptName,
      eventEmitter
    );

    eventEmitter.emit('compare', 'Compare completed', 100);

    return scriptFilePath;
  }

  static async collectSchemaObjects(client: ClientBase, config: Config) {
    const dbObjects: DatabaseObjects = {
      schemas: {},
      tables: {},
      views: {},
      materializedViews: {},
      functionMap: {},
      functions: [],
      aggregates: {},
      sequences: {},
    };

    if (typeof config.compareOptions.schemaCompare.namespaces === 'string')
      config.compareOptions.schemaCompare.namespaces = [
        config.compareOptions.schemaCompare.namespaces,
      ];
    else if (
      !config.compareOptions.schemaCompare.namespaces ||
      !Array.isArray(config.compareOptions.schemaCompare.namespaces) ||
      config.compareOptions.schemaCompare.namespaces.length <= 0
    )
      config.compareOptions.schemaCompare.namespaces =
        await CatalogApi.retrieveAllSchemas(client);

    dbObjects.schemas = await CatalogApi.retrieveSchemas(
      client,
      config.compareOptions.schemaCompare.namespaces
    );
    dbObjects.tables = await CatalogApi.retrieveTables(client, config);
    dbObjects.views = await CatalogApi.retrieveViews(client, config);
    dbObjects.materializedViews = await CatalogApi.retrieveMaterializedViews(
      client,
      config
    );
    const { map: functionMap, list: functionList } =
      await CatalogApi.retrieveFunctions(client, config);
    dbObjects.functionMap = functionMap;
    dbObjects.functions = functionList;
    for (const name in dbObjects.tables) {
      const table = dbObjects.tables[name];
      for (const columnName in table.columns) {
        const column = table.columns[columnName];
        column.functionReferences = column.defaultFunctionIds.map((id) =>
          dbObjects.functions.find((f) => f.id === id)
        );
      }
    }
    dbObjects.aggregates = await CatalogApi.retrieveAggregates(client, config);
    dbObjects.sequences = await CatalogApi.retrieveSequences(client, config);

    //TODO: Add a way to retrieve AGGREGATE and WINDOW functions
    //TODO: Do we need to retrieve roles?
    //TODO: Do we need to retieve special table like TEMPORARY and UNLOGGED? for sure not temporary, but UNLOGGED probably yes.
    //TODO: Do we need to retrieve collation for both table and columns?
    //TODO: Add a way to retrieve DOMAIN and its CONSTRAINTS

    return dbObjects;
  }

  static compareDatabaseObjects(
    dbSourceObjects: DatabaseObjects,
    dbTargetObjects: DatabaseObjects,
    config: Config,
    eventEmitter: EventEmitter
  ) {
    const droppedConstraints: string[] = [];
    const droppedIndexes: string[] = [];
    const droppedViews: string[] = [];
    const addedColumns: Record<string, any> = {};
    const addedTables: any[] = [];
    const sqlPatch: Sql[] = [];

    sqlPatch.push(
      ...this.compareSchemas(dbSourceObjects.schemas, dbTargetObjects.schemas)
    );
    eventEmitter.emit('compare', 'SCHEMA objects have been compared', 45);

    sqlPatch.push(
      ...this.compareSequences(
        config,
        dbSourceObjects.sequences,
        dbTargetObjects.sequences
      )
    );
    eventEmitter.emit('compare', 'SEQUENCE objects have been compared', 50);

    sqlPatch.push(
      ...this.compareTables(
        dbSourceObjects.tables,
        dbTargetObjects,
        droppedConstraints,
        droppedIndexes,
        droppedViews,
        addedColumns,
        addedTables,
        config
      )
    );
    eventEmitter.emit('compare', 'TABLE objects have been compared', 55);

    sqlPatch.push(
      ...this.compareViews(
        dbSourceObjects.views,
        dbTargetObjects.views,
        droppedViews,
        config
      )
    );
    eventEmitter.emit('compare', 'VIEW objects have been compared', 60);

    sqlPatch.push(
      ...this.compareMaterializedViews(
        dbSourceObjects.materializedViews,
        dbTargetObjects.materializedViews,
        droppedViews,
        droppedIndexes,
        config
      )
    );
    eventEmitter.emit(
      'compare',
      'MATERIALIZED VIEW objects have been compared',
      65
    );

    sqlPatch.push(
      ...this.compareProcedures(
        dbSourceObjects.functionMap,
        dbTargetObjects.functionMap,
        config
      )
    );
    eventEmitter.emit('compare', 'PROCEDURE objects have been compared', 70);

    sqlPatch.push(
      ...this.compareAggregates(
        dbSourceObjects.aggregates,
        dbTargetObjects.aggregates,
        config
      )
    );
    eventEmitter.emit('compare', 'AGGREGATE objects have been compared', 75);

    const policyChanges = this.comparePolicies(
      config,
      dbSourceObjects.tables,
      dbTargetObjects.tables
    );

    return {
      dropped: {
        constraints: droppedConstraints,
        indexes: droppedIndexes,
        views: droppedViews,
      },
      added: {
        columns: addedColumns,
        tables: addedTables,
      },
      ddl: [...policyChanges.drop, ...sqlPatch, ...policyChanges.create],
    };
  }

  static finalizeScript(scriptLabel: string, sqlScript: Sql[]) {
    const finalizedScript: Sql[] = [];

    if (sqlScript.length > 0) {
      finalizedScript.push(stmt`--- BEGIN ${scriptLabel} ---`);
      finalizedScript.push(...sqlScript);
      finalizedScript.push(stmt`--- END ${scriptLabel} ---`);
    }

    return finalizedScript;
  }

  static compareSchemas(
    sourceSchemas: Record<string, Schema>,
    targetSchemas: Record<string, Schema>
  ) {
    const finalizedScript: Sql[] = [];
    for (const sourceSchema in sourceSchemas) {
      const sourceObj = sourceSchemas[sourceSchema];
      const targetObj = targetSchemas[sourceSchema];
      const sqlScript: Sql[] = [];

      if (!targetObj) {
        //Schema not exists on target database, then generate script to create schema
        sqlScript.push(
          sql.generateCreateSchemaScript(sourceSchema, sourceObj.owner)
        );
        sqlScript.push(
          sql.generateChangeCommentScript(
            sourceObj.id,
            objectType.SCHEMA,
            sourceSchema,
            sourceObj.comment
          )
        );
      }

      if (targetObj && sourceObj.comment != targetObj.comment)
        sqlScript.push(
          sql.generateChangeCommentScript(
            sourceObj.id,
            objectType.SCHEMA,
            sourceSchema,
            sourceObj.comment
          )
        );

      finalizedScript.push(
        ...this.finalizeScript(
          `CREATE OR UPDATE SCHEMA ${sourceSchema}`,
          sqlScript
        )
      );
    }

    return finalizedScript;
  }

  static compareTables(
    sourceTables: Record<string, TableObject>,
    dbTargetObjects: DatabaseObjects,
    droppedConstraints: string[],
    droppedIndexes: string[],
    droppedViews: string[],
    addedColumns: any,
    addedTables: string[],
    config: Config
  ) {
    const finalizedScript: Sql[] = [];

    for (const sourceTable in sourceTables) {
      const sourceObj = sourceTables[sourceTable];
      const targetObj = dbTargetObjects.tables[sourceTable];
      const sqlScript: Sql[] = [];
      let actionLabel = '';

      if (targetObj) {
        //Table exists on both database, then compare table schema
        actionLabel = 'ALTER';

        //@mso -> relhadoids has been deprecated from PG v12.0
        if (targetObj.options)
          sqlScript.push(
            ...this.compareTableOptions(
              sourceTable,
              sourceObj.options,
              targetObj.options
            )
          );

        sqlScript.push(
          ...this.compareTableColumns(
            sourceTable,
            sourceObj.columns,
            dbTargetObjects,
            droppedConstraints,
            droppedIndexes,
            droppedViews,
            addedColumns
          )
        );

        sqlScript.push(
          ...this.compareTableConstraints(
            sourceObj,
            sourceObj.constraints,
            targetObj.constraints,
            droppedConstraints
          )
        );

        sqlScript.push(
          ...this.compareTableIndexes(
            sourceObj.indexes,
            targetObj.indexes,
            droppedIndexes
          )
        );

        sqlScript.push(
          ...this.compareTablePrivileges(
            sourceTable,
            sourceObj.privileges,
            targetObj.privileges,
            config
          )
        );

        const owner = config.compareOptions.mapRole(sourceObj.owner);
        if (owner != targetObj.owner)
          sqlScript.push(
            sql.generateChangeTableOwnerScript(sourceTable, owner)
          );
        if (!commentIsEqual(sourceObj.comment, targetObj?.comment)) {
          sqlScript.push(
            sql.generateChangeCommentScript(
              sourceObj.id,
              objectType.TABLE,
              sourceTable,
              sourceObj.comment
            )
          );
        }
      } else {
        //Table not exists on target database, then generate the script to create table
        actionLabel = 'CREATE';
        addedTables.push(sourceTable);
        sqlScript.push(sql.generateCreateTableScript(sourceTable, sourceObj));
        sqlScript.push(
          sql.generateChangeCommentScript(
            sourceObj.id,
            objectType.TABLE,
            sourceTable,
            sourceObj.comment
          )
        );
      }

      finalizedScript.push(
        ...this.finalizeScript(`${actionLabel} TABLE ${sourceTable}`, sqlScript)
      );
    }

    if (config.compareOptions.schemaCompare.dropMissingTable) {
      const migrationFullTableName = config.migrationOptions
        ? `"${config.migrationOptions.historyTableSchema}"."${config.migrationOptions.historyTableName}"`
        : '';

      for (let table in dbTargetObjects.tables) {
        let sqlScript: Sql[] = [];

        if (!sourceTables[table as any] && table != migrationFullTableName)
          sqlScript.push(sql.generateDropTableScript(table));

        finalizedScript.push(
          ...this.finalizeScript(`DROP TABLE ${table}`, sqlScript)
        );
      }
    }

    return finalizedScript;
  }

  static compareTableOptions(
    tableName: string,
    sourceTableOptions: TableOptions,
    targetTableOptions: TableOptions
  ) {
    if (sourceTableOptions.withOids === targetTableOptions.withOids) {
      return [];
    }
    return [
      sql.generateChangeTableOptionsScript(tableName, sourceTableOptions),
    ];
  }

  static compareTableColumns(
    tableName: string,
    sourceTableColumns: Record<string, Column>,
    dbTargetObjects: DatabaseObjects,
    droppedConstraints: string[],
    droppedIndexes: string[],
    droppedViews: string[],
    addedColumns: any
  ) {
    const sqlScript: Sql[] = [];
    const targetTable = dbTargetObjects.tables[tableName];
    for (const sourceTableColumn in sourceTableColumns) {
      const sourceColumnDef = sourceTableColumns[sourceTableColumn];
      const targetColumnDef = targetTable.columns[sourceTableColumn];
      if (targetColumnDef) {
        //Table column exists on both database, then compare column schema
        sqlScript.push(
          ...this.compareTableColumn(
            tableName,
            sourceTableColumn,
            sourceColumnDef,
            dbTargetObjects,
            droppedConstraints,
            droppedIndexes,
            droppedViews
          )
        );
      } else {
        //Table column not exists on target database, then generate script to add column
        sqlScript.push(
          sql.generateAddTableColumnScript(
            tableName,
            sourceTableColumn,
            sourceColumnDef
          )
        );
        sqlScript.push(
          sql.generateChangeCommentScript(
            sourceColumnDef.id,
            objectType.COLUMN,
            `${tableName}.${sourceTableColumn}`,
            sourceColumnDef.comment
          )
        );

        addedColumns[tableName] ??= [];
        addedColumns[tableName].push(sourceTableColumn);
      }
    }

    for (const targetColumn in targetTable.columns) {
      if (!sourceTableColumns[targetColumn])
        //Table column not exists on source, then generate script to drop column
        sqlScript.push(
          sql.generateDropTableColumnScript(tableName, targetColumn)
        );
    }

    return sqlScript;
  }

  static compareTableColumn(
    tableName: string,
    columnName: string,
    sourceTableColumn: Column,
    dbTargetObjects: DatabaseObjects,
    droppedConstraints: string[],
    droppedIndexes: string[],
    droppedViews: string[]
  ) {
    let sqlScript: Sql[] = [];
    let changes: ColumnChanges = {};
    let targetTable = dbTargetObjects.tables[tableName];
    let targetTableColumn = targetTable.columns[columnName];

    if (sourceTableColumn.nullable != targetTableColumn.nullable)
      changes.nullable = sourceTableColumn.nullable;

    if (
      sourceTableColumn.datatype != targetTableColumn.datatype ||
      sourceTableColumn.precision != targetTableColumn.precision ||
      sourceTableColumn.scale != targetTableColumn.scale
    ) {
      changes.datatype = sourceTableColumn.datatype;
      changes.dataTypeID = sourceTableColumn.dataTypeID;
      changes.dataTypeCategory = sourceTableColumn.dataTypeCategory;
      changes.precision = sourceTableColumn.precision;
      changes.scale = sourceTableColumn.scale;
    }

    if (sourceTableColumn.default != targetTableColumn.default) {
      changes.default = sourceTableColumn.default;
      changes.defaultRef = sourceTableColumn.defaultRef;
    }

    if (sourceTableColumn.identity != targetTableColumn.identity) {
      changes.identity = sourceTableColumn.identity;

      if (targetTableColumn.identity == null) changes.isNewIdentity = true;
      else changes.isNewIdentity = false;
    }

    if (
      sourceTableColumn.generatedColumn &&
      (sourceTableColumn.generatedColumn != targetTableColumn.generatedColumn ||
        sourceTableColumn.default != targetTableColumn.default)
    ) {
      changes = {};
      sqlScript.push(
        sql.generateDropTableColumnScript(tableName, columnName, true)
      );
      sqlScript.push(
        sql.generateAddTableColumnScript(
          tableName,
          columnName,
          sourceTableColumn
        )
      );
    }

    if (Object.keys(changes).length > 0) {
      let rawColumnName = columnName.substring(1).slice(0, -1);

      //Check if the column has constraint
      for (let constraint in targetTable.constraints) {
        if (droppedConstraints.includes(constraint)) continue;

        let constraintDefinition =
          targetTable.constraints[constraint].definition;
        let searchStartingIndex = constraintDefinition.indexOf('(');

        if (
          constraintDefinition.includes(
            `${rawColumnName},`,
            searchStartingIndex
          ) ||
          constraintDefinition.includes(
            `${rawColumnName})`,
            searchStartingIndex
          ) ||
          constraintDefinition.includes(`${columnName}`, searchStartingIndex)
        ) {
          sqlScript.push(
            sql.generateDropTableConstraintScript(tableName, constraint)
          );
          droppedConstraints.push(constraint);
        }
      }

      //Check if the column is part of indexes
      for (let index in targetTable.indexes) {
        let indexDefinition = targetTable.indexes[index].definition;
        let serachStartingIndex = indexDefinition.indexOf('(');

        if (
          indexDefinition.includes(`${rawColumnName},`, serachStartingIndex) ||
          indexDefinition.includes(`${rawColumnName})`, serachStartingIndex) ||
          indexDefinition.includes(`${columnName}`, serachStartingIndex)
        ) {
          sqlScript.push(
            sql.generateDropIndexScript(targetTable.indexes[index])
          );
          droppedIndexes.push(index);
        }
      }

      //Check if the column is used into view
      for (let view in dbTargetObjects.views) {
        dbTargetObjects.views[view].dependencies.forEach((dependency: any) => {
          let fullDependencyName = `"${dependency.schemaName}"."${dependency.tableName}"`;
          if (
            fullDependencyName == tableName &&
            dependency.columnName == columnName
          ) {
            sqlScript.push(sql.generateDropViewScript(view));
            droppedViews.push(view);
          }
        });
      }

      //Check if the column is used into materialized view
      for (let view in dbTargetObjects.materializedViews) {
        dbTargetObjects.materializedViews[view].dependencies.forEach(
          (dependency: any) => {
            let fullDependencyName = `"${dependency.schemaName}"."${dependency.tableName}"`;
            if (
              fullDependencyName == tableName &&
              dependency.columnName == columnName
            ) {
              sqlScript.push(sql.generateDropMaterializedViewScript(view));
              droppedViews.push(view);
            }
          }
        );
      }

      sqlScript.push(
        sql.generateChangeTableColumnScript(tableName, columnName, changes)
      );
    }

    if (sourceTableColumn.comment != targetTableColumn.comment)
      sqlScript.push(
        sql.generateChangeCommentScript(
          sourceTableColumn.id,
          objectType.COLUMN,
          `${tableName}.${columnName}`,
          sourceTableColumn.comment
        )
      );

    return sqlScript;
  }

  static compareTableConstraints(
    table: TableObject,
    sourceTableConstraints: Record<string, ConstraintDefinition>,
    targetTableConstraints: Record<string, ConstraintDefinition>,
    droppedConstraints: string[]
  ) {
    const sqlScript: Sql[] = [];
    for (const constraint in sourceTableConstraints) {
      const sourceObj = sourceTableConstraints[constraint];
      const targetObj = targetTableConstraints[constraint];
      //Get new or changed constraint
      if (targetObj) {
        //Table constraint exists on both database, then compare column schema
        if (sourceObj.definition !== targetObj.definition) {
          if (!droppedConstraints.includes(constraint)) {
            sqlScript.push(
              sql.generateDropTableConstraintScript(table.name, constraint)
            );
          }
          sqlScript.push(
            sql.generateAddTableConstraintScript(table, constraint, sourceObj)
          );
          if (sourceObj.comment) {
            sqlScript.push(
              sql.generateChangeCommentScript(
                sourceObj.id,
                objectType.CONSTRAINT,
                constraint,
                sourceObj.comment,
                table.fullName
              )
            );
          }
        } else {
          if (droppedConstraints.includes(constraint)) {
            //It will recreate a dropped constraints because changes happens on involved columns
            sqlScript.push(
              sql.generateAddTableConstraintScript(table, constraint, sourceObj)
            );
            if (sourceObj.comment) {
              sqlScript.push(
                sql.generateChangeCommentScript(
                  sourceObj.id,
                  objectType.CONSTRAINT,
                  constraint,
                  sourceObj.comment,
                  table.fullName
                )
              );
            }
          } else {
            if (!commentIsEqual(sourceObj.comment, targetObj.comment)) {
              sqlScript.push(
                sql.generateChangeCommentScript(
                  sourceObj.id,
                  objectType.CONSTRAINT,
                  constraint,
                  sourceObj.comment,
                  table.fullName
                )
              );
            }
          }
        }
      } else {
        //Table constraint not exists on target database, then generate script to add constraint
        sqlScript.push(
          sql.generateAddTableConstraintScript(table, constraint, sourceObj)
        );
        sqlScript.push(
          sql.generateChangeCommentScript(
            sourceObj.id,
            objectType.CONSTRAINT,
            constraint,
            sourceObj.comment,
            table.fullName
          )
        );
      }
    }

    for (const constraint in targetTableConstraints) {
      //Get dropped constraints
      if (
        !sourceTableConstraints[constraint] &&
        !droppedConstraints.includes(constraint)
      )
        //Table constraint not exists on source, then generate script to drop constraint
        sqlScript.push(
          sql.generateDropTableConstraintScript(table.name, constraint)
        );
    }

    return sqlScript;
  }

  static compareTableIndexes(
    sourceTableIndexes: Record<string, IndexDefinition>,
    targetTableIndexes: Record<string, IndexDefinition>,
    droppedIndexes: string[]
  ) {
    let sqlScript: Sql[] = [];

    for (let index in sourceTableIndexes) {
      const sourceObj = sourceTableIndexes[index];
      const targetObj = targetTableIndexes[index];
      //Get new or changed indexes
      if (targetObj) {
        //Table index exists on both database, then compare index definition
        if (sourceObj.definition != targetObj.definition) {
          if (!droppedIndexes.includes(index)) {
            sqlScript.push(sql.generateDropIndexScript(sourceObj));
          }
          sqlScript.push(stmt`${sourceObj.definition};`);
          sqlScript.push(
            sql.generateChangeCommentScript(
              sourceObj.id,
              objectType.INDEX,
              `"${sourceObj.schema}"."${index}"`,
              sourceObj.comment
            )
          );
        } else {
          if (droppedIndexes.includes(index)) {
            //It will recreate a dropped index because changes happens on involved columns
            sqlScript.push(stmt`${sourceObj.definition};`);
            sqlScript.push(
              sql.generateChangeCommentScript(
                sourceObj.id,
                objectType.INDEX,
                `"${sourceObj.schema}"."${index}"`,
                sourceObj.comment
              )
            );
          } else {
            if (sourceObj.comment != targetObj.comment)
              sqlScript.push(
                sql.generateChangeCommentScript(
                  sourceObj.id,
                  objectType.INDEX,
                  `"${sourceObj.schema}"."${index}"`,
                  sourceObj.comment
                )
              );
          }
        }
      } else {
        //Table index not exists on target database, then generate script to add index
        sqlScript.push(stmt`${sourceObj.definition};`);
        sqlScript.push(
          sql.generateChangeCommentScript(
            sourceObj.id,
            objectType.INDEX,
            `"${sourceObj.schema}"."${index}"`,
            sourceObj.comment
          )
        );
      }
    }

    for (let index in targetTableIndexes) {
      //Get dropped indexes
      if (!sourceTableIndexes[index] && !droppedIndexes.includes(index))
        //Table index not exists on source, then generate script to drop index
        sqlScript.push(sql.generateDropIndexScript(targetTableIndexes[index]));
    }

    return sqlScript;
  }
  /*
  static _compareTablePolicies(
    config: Config,
    table: TableObject,
    source: Record<string, Policy>,
    target: Record<string, Policy>
  ) {
    const sqlScript: string[] = [];
    for (const name in source) {
      const sourceObj = source[name];
      const targetObj = target[name];
      const roles = sourceObj.roles.map(config.compareOptions.mapRole);
      const isSame =
        targetObj &&
        sourceObj.using === targetObj.using &&
        sourceObj.permissive === targetObj.permissive &&
        sourceObj.withCheck === targetObj.withCheck &&
        sourceObj.for === targetObj.for &&
        isEqual(roles, targetObj.roles);
      if (!isSame) {
        if (targetObj) {
          sqlScript.push(sql.dropPolicy(table.schema, table.name, name));
        }
        sqlScript.push(
          sql.createPolicy(table.schema, table.name, {
            ...sourceObj,
            roles,
          })
        );
      }
      if (!commentIsEqual(sourceObj.comment, targetObj?.comment)) {
        sqlScript.push(
          sql.generateChangeCommentScript(
            objectType.POLICY,
            name,
            sourceObj.comment,
            `"${table.schema}"."${table.name}"`
          )
        );
      }
    }

    for (const name in target) {
      if (source[name]) {
        continue;
      }
      sqlScript.push(sql.dropPolicy(table.schema, table.name, name));
    }

    return sqlScript;
  }
*/
  static compareTablePolicies(
    config: Config,
    table: TableObject,
    source: Record<string, Policy>,
    target: Record<string, Policy>
  ) {
    const create: Sql[] = [];
    const drop: Sql[] = [];
    for (const name in source) {
      const sourceObj = source[name];
      const targetObj = target[name];
      const roles = sourceObj.roles.map(config.compareOptions.mapRole);
      const isSame =
        targetObj &&
        sourceObj.using === targetObj.using &&
        sourceObj.permissive === targetObj.permissive &&
        sourceObj.withCheck === targetObj.withCheck &&
        sourceObj.for === targetObj.for &&
        isEqual(roles, targetObj.roles);
      if (!isSame) {
        if (targetObj) {
          drop.push(sql.dropPolicy(table.schema, table.name, name));
        }
        create.push(
          sql.createPolicy(table.schema, table.name, {
            ...sourceObj,
            roles,
          })
        );
      }
      if (!commentIsEqual(sourceObj.comment, targetObj?.comment)) {
        create.push(
          sql.generateChangeCommentScript(
            sourceObj.id,
            objectType.POLICY,
            name,
            sourceObj.comment,
            `"${table.schema}"."${table.name}"`
          )
        );
      }
    }

    for (const name in target) {
      if (source[name]) {
        continue;
      }
      drop.push(sql.dropPolicy(table.schema, table.name, name));
    }

    return {
      drop,
      create,
    };
  }

  static comparePolicies(
    config: Config,
    source: Record<string, TableObject>,
    target: Record<string, TableObject>
  ) {
    const drop: Sql[][] = [];
    const create: Sql[][] = [];
    for (const name in source) {
      const sourceObj = source[name];
      const targetObj = target[name];
      const policies = this.compareTablePolicies(
        config,
        sourceObj,
        sourceObj.policies,
        targetObj?.policies ?? {}
      );
      drop.push(policies.drop);
      create.push(policies.create);
    }
    return {
      drop: drop.flat(),
      create: create.flat(),
    };
  }

  static compareTablePrivileges(
    tableName: string,
    sourceTablePrivileges: Record<string, Privileges>,
    targetTablePrivileges: Record<string, Privileges>,
    config: Config
  ) {
    let sqlScript: Sql[] = [];

    for (let role in sourceTablePrivileges) {
      // In case a list of specific roles hve been configured, the check will only contains those roles eventually.
      if (
        config.compareOptions.schemaCompare.roles.length > 0 &&
        !config.compareOptions.schemaCompare.roles.includes(role)
      )
        continue;

      //Get new or changed role privileges
      if (targetTablePrivileges[role]) {
        //Table privileges for role exists on both database, then compare privileges
        let changes: ColumnChanges = {};

        if (
          sourceTablePrivileges[role].select !=
          targetTablePrivileges[role].select
        )
          changes.select = sourceTablePrivileges[role].select;

        if (
          sourceTablePrivileges[role].insert !=
          targetTablePrivileges[role].insert
        )
          changes.insert = sourceTablePrivileges[role].insert;

        if (
          sourceTablePrivileges[role].update !=
          targetTablePrivileges[role].update
        )
          changes.update = sourceTablePrivileges[role].update;

        if (
          sourceTablePrivileges[role].delete !=
          targetTablePrivileges[role].delete
        )
          changes.delete = sourceTablePrivileges[role].delete;

        if (
          sourceTablePrivileges[role].truncate !=
          targetTablePrivileges[role].truncate
        )
          changes.truncate = sourceTablePrivileges[role].truncate;

        if (
          sourceTablePrivileges[role].references !=
          targetTablePrivileges[role].references
        )
          changes.references = sourceTablePrivileges[role].references;

        if (
          sourceTablePrivileges[role].trigger !=
          targetTablePrivileges[role].trigger
        )
          changes.trigger = sourceTablePrivileges[role].trigger;

        if (Object.keys(changes).length > 0)
          sqlScript.push(
            sql.generateChangesTableRoleGrantsScript(tableName, role, changes)
          );
      } else {
        //Table grants for role not exists on target database, then generate script to add role privileges
        sqlScript.push(
          sql.generateTableRoleGrantsScript(
            tableName,
            role,
            sourceTablePrivileges[role]
          )
        );
      }
    }

    return sqlScript;
  }

  static compareViews(
    sourceViews: Record<string, ViewDefinition>,
    targetViews: Record<string, ViewDefinition>,
    droppedViews: string[],
    config: Config
  ) {
    let finalizedScript: Sql[] = [];

    for (let view in sourceViews) {
      const sourceObj = sourceViews[view];
      const targetObj = targetViews[view];
      let sqlScript: Sql[] = [];
      let actionLabel = '';

      if (targetObj) {
        //View exists on both database, then compare view schema
        actionLabel = 'ALTER';

        let sourceViewDefinition = sourceObj.definition.replace(/\r/g, '');
        let targetViewDefinition = targetObj.definition.replace(/\r/g, '');
        if (sourceViewDefinition != targetViewDefinition) {
          if (!droppedViews.includes(view))
            sqlScript.push(sql.generateDropViewScript(view));
          sqlScript.push(sql.generateCreateViewScript(view, sourceObj));
          sqlScript.push(
            sql.generateChangeCommentScript(
              sourceObj.id,
              objectType.VIEW,
              view,
              sourceObj.comment
            )
          );
        } else {
          if (droppedViews.includes(view))
            //It will recreate a dropped view because changes happens on involved columns
            sqlScript.push(sql.generateCreateViewScript(view, sourceObj));

          sqlScript.push(
            ...this.compareTablePrivileges(
              view,
              sourceObj.privileges,
              targetObj.privileges,
              config
            )
          );

          if (sourceObj.owner != targetObj.owner)
            sqlScript.push(
              sql.generateChangeTableOwnerScript(view, sourceObj.owner)
            );

          if (sourceObj.comment != targetObj.comment)
            sqlScript.push(
              sql.generateChangeCommentScript(
                sourceObj.id,
                objectType.VIEW,
                view,
                sourceObj.comment
              )
            );
        }
      } else {
        //View not exists on target database, then generate the script to create view
        actionLabel = 'CREATE';

        sqlScript.push(sql.generateCreateViewScript(view, sourceObj));
        sqlScript.push(
          sql.generateChangeCommentScript(
            sourceObj.id,
            objectType.VIEW,
            view,
            sourceObj.comment
          )
        );
      }

      finalizedScript.push(
        ...this.finalizeScript(`${actionLabel} VIEW ${view}`, sqlScript)
      );
    }

    if (config.compareOptions.schemaCompare.dropMissingView)
      for (let view in targetViews) {
        if (sourceViews[view]) {
          continue;
        }

        finalizedScript.push(
          ...this.finalizeScript(`DROP VIEW ${view}`, [
            sql.generateDropViewScript(view),
          ])
        );
      }

    return finalizedScript;
  }

  static compareMaterializedViews(
    sourceMaterializedViews: Record<string, MaterializedViewDefinition>,
    targetMaterializedViews: Record<string, MaterializedViewDefinition>,
    droppedViews: string[],
    droppedIndexes: string[],
    config: Config
  ) {
    let finalizedScript: Sql[] = [];

    for (let view in sourceMaterializedViews) {
      const sourceObj = sourceMaterializedViews[view];
      const targetObj = targetMaterializedViews[view];
      //Get new or changed materialized views
      let sqlScript: Sql[] = [];
      let actionLabel = '';

      if (targetObj) {
        //Materialized view exists on both database, then compare materialized view schema
        actionLabel = 'ALTER';

        let sourceViewDefinition = sourceObj.definition.replace(/\r/g, '');
        let targetViewDefinition = targetObj.definition.replace(/\r/g, '');
        if (sourceViewDefinition != targetViewDefinition) {
          if (!droppedViews.includes(view))
            sqlScript.push(sql.generateDropMaterializedViewScript(view));
          sqlScript.push(
            sql.generateCreateMaterializedViewScript(view, sourceObj)
          );
          sqlScript.push(
            sql.generateChangeCommentScript(
              sourceObj.id,
              objectType.MATERIALIZED_VIEW,
              view,
              sourceObj.comment
            )
          );
        } else {
          if (droppedViews.includes(view))
            //It will recreate a dropped materialized view because changes happens on involved columns
            sqlScript.push(
              sql.generateCreateMaterializedViewScript(view, sourceObj)
            );

          sqlScript.push(
            ...this.compareTableIndexes(
              sourceObj.indexes,
              targetObj.indexes,
              droppedIndexes
            )
          );

          sqlScript.push(
            ...this.compareTablePrivileges(
              view,
              sourceObj.privileges,
              targetObj.privileges,
              config
            )
          );

          if (sourceObj.owner != targetObj.owner)
            sqlScript.push(
              sql.generateChangeTableOwnerScript(view, sourceObj.owner)
            );

          if (sourceObj.comment != targetObj.comment)
            sqlScript.push(
              sql.generateChangeCommentScript(
                sourceObj.id,
                objectType.MATERIALIZED_VIEW,
                view,
                sourceObj.comment
              )
            );
        }
      } else {
        //Materialized view not exists on target database, then generate the script to create materialized view
        actionLabel = 'CREATE';

        sqlScript.push(
          sql.generateCreateMaterializedViewScript(view, sourceObj)
        );
        sqlScript.push(
          sql.generateChangeCommentScript(
            sourceObj.id,
            objectType.MATERIALIZED_VIEW,
            view,
            sourceObj.comment
          )
        );
      }

      finalizedScript.push(
        ...this.finalizeScript(
          `${actionLabel} MATERIALIZED VIEW ${view}`,
          sqlScript
        )
      );
    }

    if (config.compareOptions.schemaCompare.dropMissingView)
      for (let view in targetMaterializedViews) {
        if (sourceMaterializedViews[view]) {
          continue;
        }
        finalizedScript.push(
          ...this.finalizeScript(`DROP MATERIALIZED VIEW ${view}`, [
            sql.generateDropMaterializedViewScript(view),
          ])
        );
      }

    return finalizedScript;
  }

  static compareProcedures(
    sourceFunctions: Record<string, Record<string, FunctionDefinition>>,
    targetFunctions: Record<string, Record<string, FunctionDefinition>>,
    config: Config
  ) {
    let finalizedScript: Sql[] = [];

    for (let procedure in sourceFunctions) {
      for (const procedureArgs in sourceFunctions[procedure]) {
        let sqlScript: Sql[] = [];
        let actionLabel = '';
        const sourceObj = sourceFunctions[procedure][procedureArgs];
        const targetObj =
          targetFunctions[procedure] &&
          targetFunctions[procedure][procedureArgs];
        const procedureType =
          sourceObj.type === 'f' ? objectType.FUNCTION : objectType.PROCEDURE;

        if (targetObj) {
          //Procedure exists on both database, then compare procedure definition
          actionLabel = 'ALTER';

          //TODO: Is correct that if definition is different automatically GRANTS and OWNER will not be updated also?
          //TODO: Better to match only "visible" char in order to avoid special invisible like \t, spaces, etc;
          //      the problem is that a SQL STRING can contains special char as a fix from previous function version
          const sourceFunctionDefinition = sourceObj.definition.replace(
            /\r/g,
            ''
          );
          const targetFunctionDefinition = targetObj.definition.replace(
            /\r/g,
            ''
          );
          if (sourceFunctionDefinition !== targetFunctionDefinition) {
            if (sourceObj.argTypes !== targetObj.argTypes) {
              sqlScript.push(sql.generateDropProcedureScript(sourceObj));
            }
            sqlScript.push(sql.generateCreateProcedureScript(sourceObj));
            sqlScript.push(
              sql.generateChangeCommentScript(
                sourceObj.id,
                procedureType,
                `${procedure}(${procedureArgs})`,
                sourceObj.comment
              )
            );
          } else {
            sqlScript.push(
              ...this.compareProcedurePrivileges(
                sourceObj,
                sourceObj.privileges,
                targetObj.privileges
              )
            );

            if (sourceObj.owner != targetObj.owner)
              sqlScript.push(
                sql.generateChangeProcedureOwnerScript(
                  procedure,
                  procedureArgs,
                  sourceObj.owner,
                  sourceObj.type
                )
              );

            if (sourceObj.comment != sourceObj.comment)
              sqlScript.push(
                sql.generateChangeCommentScript(
                  sourceObj.id,
                  procedureType,
                  `${procedure}(${procedureArgs})`,
                  sourceObj.comment
                )
              );
          }
        } else {
          //Procedure not exists on target database, then generate the script to create procedure
          actionLabel = 'CREATE';

          sqlScript.push(sql.generateCreateProcedureScript(sourceObj));
          sqlScript.push(
            sql.generateChangeCommentScript(
              sourceObj.id,
              procedureType,
              `${procedure}(${procedureArgs})`,
              sourceObj.comment
            )
          );
        }

        finalizedScript.push(
          ...this.finalizeScript(
            `${actionLabel} ${procedureType} ${procedure}(${procedureArgs})`,
            sqlScript
          )
        );
      }
    }

    if (config.compareOptions.schemaCompare.dropMissingFunction)
      for (let procedure in targetFunctions) {
        for (const procedureArgs in targetFunctions[procedure]) {
          if (
            sourceFunctions[procedure] &&
            sourceFunctions[procedure][procedureArgs]
          ) {
            continue;
          }
          finalizedScript.push(
            ...this.finalizeScript(
              `DROP FUNCTION ${procedure}(${procedureArgs})`,
              [
                sql.generateDropProcedureScript(
                  targetFunctions[procedure][procedureArgs]
                ),
              ]
            )
          );
        }
      }

    return finalizedScript;
  }

  static compareAggregates(
    sourceAggregates: Record<string, Record<string, AggregateDefinition>>,
    targetAggregates: Record<string, Record<string, AggregateDefinition>>,
    config: Config
  ) {
    let finalizedScript: Sql[] = [];

    for (let aggregate in sourceAggregates) {
      for (const aggregateArgs in sourceAggregates[aggregate]) {
        const sourceObj = sourceAggregates[aggregate][aggregateArgs];
        const targetObj =
          targetAggregates[aggregate] &&
          targetAggregates[aggregate][aggregateArgs];
        let sqlScript: Sql[] = [];
        let actionLabel = '';

        if (targetObj) {
          //Aggregate exists on both database, then compare procedure definition
          actionLabel = 'ALTER';

          //TODO: Is correct that if definition is different automatically GRANTS and OWNER will not be updated also?
          if (sourceObj.definition != targetObj.definition) {
            sqlScript.push(sql.generateChangeAggregateScript(sourceObj));
            sqlScript.push(
              sql.generateChangeCommentScript(
                sourceObj.id,
                objectType.AGGREGATE,
                `${aggregate}(${aggregateArgs})`,
                sourceObj.comment
              )
            );
          } else {
            throw new Error('Not implemented');
            /*sqlScript.push(
              ...this.compareProcedurePrivileges(
                aggregate,
                aggregateArgs,
                sourceFunctions[procedure][procedureArgs].type,
                sourceObj.privileges,
                targetObj.privileges,
              ),
            );

            if (
              sourceObj.owner !=
              targetObj.owner
            )
              sqlScript.push(
                sql.generateChangeAggregateOwnerScript(
                  aggregate,
                  aggregateArgs,
                  sourceObj.owner,
                ),
              );

            if (
              sourceObj.comment !=
              targetObj.comment
            )
              sqlScript.push(
                sql.generateChangeCommentScript(
                  objectType.AGGREGATE,
                  `${aggregate}(${aggregateArgs})`,
                  sourceObj.comment,
                ),
              );*/
          }
        } else {
          //Aggregate not exists on target database, then generate the script to create aggregate
          actionLabel = 'CREATE';

          sqlScript.push(sql.generateCreateAggregateScript(sourceObj));
          sqlScript.push(
            sql.generateChangeCommentScript(
              sourceObj.id,
              objectType.FUNCTION,
              `${aggregate}(${aggregateArgs})`,
              sourceObj.comment
            )
          );
        }

        finalizedScript.push(
          ...this.finalizeScript(
            `${actionLabel} AGGREGATE ${aggregate}(${aggregateArgs})`,
            sqlScript
          )
        );
      }
    }

    if (config.compareOptions.schemaCompare.dropMissingAggregate)
      for (let aggregate in targetAggregates) {
        for (const aggregateArgs in targetAggregates[aggregate]) {
          let sqlScript: Sql[] = [];

          if (
            !sourceAggregates[aggregate] ||
            !sourceAggregates[aggregate][aggregateArgs]
          )
            sqlScript.push(
              sql.generateDropAggregateScript(aggregate, aggregateArgs)
            );

          finalizedScript.push(
            ...this.finalizeScript(
              `DROP AGGREGATE ${aggregate}(${aggregateArgs})`,
              sqlScript
            )
          );
        }
      }

    return finalizedScript;
  }

  static compareProcedurePrivileges(
    schema: FunctionDefinition,
    sourceProcedurePrivileges: Record<string, FunctionPrivileges>,
    targetProcedurePrivileges: Record<string, FunctionPrivileges>
  ) {
    let sqlScript: Sql[] = [];

    for (let role in sourceProcedurePrivileges) {
      const sourceObj = sourceProcedurePrivileges[role];
      const targetObj = targetProcedurePrivileges[role];
      //Get new or changed role privileges
      if (targetObj) {
        //Procedure privileges for role exists on both database, then compare privileges
        let changes: ColumnChanges = {};
        if (sourceObj.execute != targetObj.execute)
          changes.execute = sourceObj.execute;

        if (Object.keys(changes).length > 0)
          sqlScript.push(
            sql.generateChangesProcedureRoleGrantsScript(schema, role, changes)
          );
      } else {
        //Procedure grants for role not exists on target database, then generate script to add role privileges
        sqlScript.push(sql.generateProcedureRoleGrantsScript(schema, role));
      }
    }

    return sqlScript;
  }

  static compareSequences(
    config: Config,
    sourceSequences: Record<string, Sequence>,
    targetSequences: Record<string, Sequence>
  ) {
    const full = config.compareOptions.schemaCompare.sequence !== false;
    const finalizedScript: Sql[] = [];
    for (const sequence in sourceSequences) {
      const sqlScript: Sql[] = [];
      const sourceObj = sourceSequences[sequence];
      const targetSequence =
        this.findRenamedSequenceOwnedByTargetTableColumn(
          sequence,
          sourceObj.ownedBy,
          targetSequences
        ) ?? sequence;
      const targetObj = targetSequences[targetSequence];
      let actionLabel = '';

      if (targetObj) {
        //Sequence exists on both database, then compare sequence definition
        actionLabel = 'ALTER';

        if (sequence !== targetSequence)
          sqlScript.push(
            sql.generateRenameSequenceScript(
              targetSequence,
              `"${sourceObj.name}"`
            )
          );

        sqlScript.push(
          ...this.compareSequenceDefinition(
            config,
            sequence,
            sourceObj,
            targetObj
          )
        );

        sqlScript.push(
          ...this.compareSequencePrivileges(
            sequence,
            sourceObj.privileges,
            targetObj.privileges
          )
        );

        if (sourceObj.comment != targetObj.comment)
          sqlScript.push(
            sql.generateChangeCommentScript(
              sourceObj.id,
              objectType.SEQUENCE,
              sequence,
              sourceObj.comment
            )
          );
      } else {
        //Sequence not exists on target database, then generate the script to create sequence
        actionLabel = 'CREATE';

        sqlScript.push(
          sql.generateCreateSequenceScript(
            sourceObj,
            config.compareOptions.mapRole(sourceObj.owner)
          )
        );
        sqlScript.push(
          sql.generateChangeCommentScript(
            sourceObj.id,
            objectType.SEQUENCE,
            sequence,
            sourceObj.comment
          )
        );
      }

      //TODO: @mso -> add a way to drop missing sequence if exists only on target db
      finalizedScript.push(
        ...this.finalizeScript(`${actionLabel} SEQUENCE ${sequence}`, sqlScript)
      );
    }

    return finalizedScript;
  }

  static findRenamedSequenceOwnedByTargetTableColumn(
    sequenceName: string,
    tableColumn: string,
    targetSequences: Record<string, Sequence>
  ) {
    for (let sequence in targetSequences.sequences) {
      if (
        targetSequences[sequence].ownedBy == tableColumn &&
        sequence != sequenceName
      ) {
        return sequence;
      }
    }
    return null;
  }

  static compareSequenceDefinition(
    config: Config,
    sequence: string,
    sourceSequenceDefinition: Sequence,
    targetSequenceDefinition: Sequence
  ) {
    const sqlScript: Sql[] = [];

    for (const property in sourceSequenceDefinition) {
      let sourceObj = sourceSequenceDefinition[property];
      const targetObj = targetSequenceDefinition[property];
      if (property === 'owner') {
        sourceObj = config.compareOptions.mapRole(sourceObj);
      }
      if (
        property == 'privileges' ||
        property == 'ownedBy' ||
        property == 'name' ||
        property == 'comment' ||
        property == 'id' ||
        sourceObj === targetObj
      ) {
        continue;
      }
      sqlScript.push(
        sql.generateChangeSequencePropertyScript(
          sequence,
          property as SequenceProperties,
          sourceObj
        )
      );
    }

    return sqlScript;
  }

  static compareSequencePrivileges(
    sequence: string,
    sourceSequencePrivileges: SequencePrivileges,
    targetSequencePrivileges: SequencePrivileges
  ) {
    let sqlScript: Sql[] = [];

    for (let role in sourceSequencePrivileges) {
      //Get new or changed role privileges
      if (targetSequencePrivileges[role]) {
        //Sequence privileges for role exists on both database, then compare privileges
        let changes: ColumnChanges = {};
        if (
          sourceSequencePrivileges[role].select !=
          targetSequencePrivileges[role].select
        )
          changes.select = sourceSequencePrivileges[role].select;

        if (
          sourceSequencePrivileges[role].usage !=
          targetSequencePrivileges[role].usage
        )
          changes.usage = sourceSequencePrivileges[role].usage;

        if (
          sourceSequencePrivileges[role].update !=
          targetSequencePrivileges[role].update
        )
          changes.update = sourceSequencePrivileges[role].update;

        if (Object.keys(changes).length > 0)
          sqlScript.push(
            sql.generateChangesSequenceRoleGrantsScript(sequence, role, changes)
          );
      } else {
        //Sequence grants for role not exists on target database, then generate script to add role privileges
        sqlScript.push(
          sql.generateSequenceRoleGrantsScript(
            sequence,
            role,
            sourceSequencePrivileges[role]
          )
        );
      }
    }

    return sqlScript;
  }

  static async compareTablesRecords(
    config: Config,
    sourceClient: ClientBase,
    targetClient: ClientBase,
    addedColumns: any,
    addedTables: string[],
    dbSourceObjects: DatabaseObjects,
    dbTargetObjects: DatabaseObjects,
    eventEmitter: EventEmitter
  ) {
    let finalizedScript: Sql[] = [];
    let iteratorCounter = 0;
    let progressStepSize = Math.floor(
      20 / config.compareOptions.dataCompare.tables.length
    );

    for (let tableDefinition of config.compareOptions.dataCompare.tables) {
      let differentRecords = 0;
      let sqlScript: Sql[] = [];
      let fullTableName = `"${tableDefinition.tableSchema || 'public'}"."${
        tableDefinition.tableName
      }"`;

      if (!(await this.checkIfTableExists(sourceClient, tableDefinition))) {
        sqlScript.push(
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
        tableData.sourceData.records = await this.collectTableRecords(
          sourceClient,
          tableDefinition,
          dbSourceObjects
        );
        tableData.sourceData.sequences = await this.collectTableSequences(
          sourceClient,
          tableDefinition
        );

        let isNewTable = false;
        if (addedTables.includes(fullTableName)) isNewTable = true;

        if (
          !isNewTable &&
          !(await this.checkIfTableExists(targetClient, tableDefinition))
        ) {
          sqlScript.push(
            stmt`\n--ERROR: Table "${
              tableDefinition.tableSchema || 'public'
            }"."${
              tableDefinition.tableName
            }" not found on TARGET database for comparison!\n`
          );
        } else {
          tableData.targetData.records = await this.collectTableRecords(
            targetClient,
            tableDefinition,
            dbTargetObjects,
            isNewTable
          );
          //  tableData.targetData.sequences = await this.collectTableSequences(targetClient, tableDefinition);

          let compareResult = this.compareTableRecords(
            tableDefinition,
            tableData,
            addedColumns
          );
          sqlScript.push(...compareResult.sqlScript);
          differentRecords = sqlScript.length;

          if (compareResult.isSequenceRebaseNeeded)
            sqlScript.push(...this.rebaseSequences(tableDefinition, tableData));
        }
      }
      finalizedScript.push(
        ...this.finalizeScript(
          `SYNCHRONIZE TABLE "${tableDefinition.tableSchema || 'public'}"."${
            tableDefinition.tableName
          }" RECORDS`,
          sqlScript
        )
      );

      iteratorCounter += 1;

      eventEmitter.emit(
        'compare',
        `Records for table ${fullTableName} have been compared with ${differentRecords} differences`,
        70 + progressStepSize * iteratorCounter
      );
    }

    return finalizedScript;
  }

  static async checkIfTableExists(
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

  static async collectTableRecords(
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
          !Object.keys(dbObjects.tables[fullTableName].columns).includes(
            `"${k}"`
          )
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

  static async collectTableSequences(
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

  static compareTableRecords(
    tableDefinition: TableDefinition,
    tableData: TableData,
    addedColumns: any
  ) {
    let ignoredRowHash: string[] = [];
    let result: { sqlScript: Sql[]; isSequenceRebaseNeeded: boolean } = {
      sqlScript: [],
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

      let keyFieldsMap = this.getKeyFieldsMap(
        tableDefinition.tableKeyFields,
        record
      );

      //Check if record is duplicated in source
      if (
        tableData.sourceData.records.rows.some(
          (r, idx) => r.rowHash === record.rowHash && idx > index
        )
      ) {
        ignoredRowHash.push(record.rowHash);
        result.sqlScript.push(
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
        result.sqlScript.push(
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
        result.sqlScript.push(
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
        let fieldCompareResult = this.compareTableRecordFields(
          fullTableName,
          keyFieldsMap,
          tableData.sourceData.records.fields,
          record,
          targetRecord[0],
          addedColumns
        );
        if (fieldCompareResult.isSequenceRebaseNeeded)
          result.isSequenceRebaseNeeded = true;
        result.sqlScript.push(...fieldCompareResult.sqlScript);
      }
    });

    tableData.targetData.records.rows.forEach((record, index) => {
      //Check if row hash has been ignored because duplicated or already processed from source
      if (ignoredRowHash.some((hash) => hash === record.rowHash)) return;

      let keyFieldsMap = this.getKeyFieldsMap(
        tableDefinition.tableKeyFields,
        record
      );

      if (
        tableData.targetData.records.rows.some(
          (r, idx) => r.rowHash === record.rowHash && idx > index
        )
      ) {
        ignoredRowHash.push(record.rowHash);
        result.sqlScript.push(
          stmt`\n--ERROR: Too many record found in TARGET database for table ${fullTableName} and key fields ${JSON.stringify(
            keyFieldsMap
          )} !\n`
        );
        return;
      }

      //Generate sql script to delete record because not exists on source database table
      result.sqlScript.push(
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

  /**
   *
   * @param {String[]} keyFields
   * @param {Object} record
   */
  static getKeyFieldsMap(keyFields: string[], record: any) {
    let keyFieldsMap: Record<string, any> = {};
    keyFields.forEach((item) => {
      keyFieldsMap[item] = record[item];
    });
    return keyFieldsMap;
  }

  static compareTableRecordFields(
    table: string,
    keyFieldsMap: any,
    fields: any[],
    sourceRecord: any,
    targetRecord: any,
    addedColumns: any
  ) {
    let changes: any = {};
    let result: { sqlScript: Sql[]; isSequenceRebaseNeeded: boolean } = {
      sqlScript: [],
      isSequenceRebaseNeeded: false,
    };

    for (const field in sourceRecord) {
      if (field === 'rowHash') continue;
      if (fields.some((f) => f.name == field && f.isGeneratedColumn == true)) {
        continue;
      }

      if (
        targetRecord[field] === undefined &&
        this.checkIsNewColumn(addedColumns, table, field)
      ) {
        changes[field] = sourceRecord[field];
      } else if (
        this.compareFieldValues(sourceRecord[field], targetRecord[field])
      ) {
        changes[field] = sourceRecord[field];
      }
    }

    if (Object.keys(changes).length > 0) {
      result.isSequenceRebaseNeeded = true;
      result.sqlScript.push(
        sql.generateUpdateTableRecordScript(
          table,
          fields,
          keyFieldsMap,
          changes
        )
      );
    }

    return result;
  }

  static checkIsNewColumn(addedColumns: any, table: string, field: string) {
    return !!addedColumns[table]?.some((column: any) => column == field);
  }

  static compareFieldValues(sourceValue: any, targetValue: any) {
    const sourceValueType = typeof sourceValue;
    const targetValueType = typeof targetValue;

    if (sourceValueType != targetValueType) return false;
    else if (sourceValue instanceof Date)
      return sourceValue.getTime() !== targetValue.getTime();
    else if (sourceValue instanceof Object)
      return !isEqual(sourceValue, targetValue);
    else return sourceValue !== targetValue;
  }

  static rebaseSequences(
    tableDefinition: TableDefinition,
    tableData: TableData
  ) {
    let sqlScript: Sql[] = [];
    let fullTableName = `"${tableDefinition.tableSchema || 'public'}"."${
      tableDefinition.tableName
    }"`;

    tableData.sourceData.sequences.forEach((sequence) => {
      sqlScript.push(
        sql.generateSetSequenceValueScript(fullTableName, sequence)
      );
    });

    return sqlScript;
  }

  static async saveSqlScript(
    scriptLines: Sql[],
    config: Config,
    scriptName: string,
    eventEmitter: EventEmitter
  ) {
    if (scriptLines.length <= 0) return null;

    const now = new Date();
    const fileName = `${now
      .toISOString()
      .replace(/[-:.TZ]/g, '')}_${scriptName}.sql`;

    if (typeof config.compareOptions.outputDirectory !== 'string')
      config.compareOptions.outputDirectory = '';

    const scriptPath = path.resolve(
      config.compareOptions.outputDirectory || '',
      fileName
    );
    if (config.compareOptions.getAuthorFromGit) {
      config.compareOptions.author = await core.getGitAuthor();
    }
    const datetime = now.toISOString();
    const titleLength =
      config.compareOptions.author.length > now.toISOString().length
        ? config.compareOptions.author.length
        : datetime.length;

    return new Promise((resolve, reject) => {
      try {
        const file = fs.createWriteStream(scriptPath);

        file.on('error', reject);

        file.on('finish', () => {
          eventEmitter.emit('compare', 'Patch file have been created', 99);
          resolve(scriptPath);
        });

        file.write(`/******************${'*'.repeat(titleLength + 2)}***/\n`);
        file.write(
          `/*** SCRIPT AUTHOR: ${config.compareOptions.author.padEnd(
            titleLength
          )} ***/\n`
        );
        file.write(
          `/***    CREATED ON: ${datetime.padEnd(titleLength)} ***/\n`
        );
        file.write(`/******************${'*'.repeat(titleLength + 2)}***/\n`);

        scriptLines.forEach((line) => {
          file.write(line.toString());
        });

        file.end();
      } catch (err) {
        reject(err);
      }
    });
  }
}

function commentIsEqual(
  a: string | null | undefined,
  b: string | null | undefined
) {
  return a === b || (!a && !b);
}
