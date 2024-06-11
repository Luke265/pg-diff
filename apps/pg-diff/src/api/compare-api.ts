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
  Type,
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
import {
  ColumnChanges,
  commentIsEqual,
  extractFunctionCalls,
} from './compare/utils';
import { compareTypes } from './compare/types';
import { compareDomains } from './compare/domain';

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
      types: {},
      domains: {},
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
    const tableIdMap = Object.values(dbObjects.tables).map(
      (t) => [t.id, `${t.schema}.${t.name}`] as [number, string]
    );
    for (const fn of functionList) {
      if (fn.languageName !== 'sql') {
        continue;
      }
      for (const [id, name] of tableIdMap) {
        if (fn.definition.includes(name)) {
          fn.fReferenceIds.push(id);
        }
      }
      fn.fReferenceIds.push(
        ...extractFunctionCalls(fn.definition)
          .filter((name) => functionMap[name])
          .map((name) => Object.values(functionMap[name]).map((f) => f.id))
          .flat()
      );
    }
    for (const name in dbObjects.tables) {
      const table = dbObjects.tables[name];
      for (const policyName in table.policies) {
        const policy = table.policies[policyName];
        if (policy.using) {
          for (const [id, name] of tableIdMap) {
            if (policy.using.includes(name)) {
              policy.dependencies.push(id);
            }
          }
        }
        if (policy.withCheck) {
          for (const [id, name] of tableIdMap) {
            if (policy.withCheck.includes(name)) {
              policy.dependencies.push(id);
            }
          }
        }
      }
      for (const columnName in table.columns) {
        const column = table.columns[columnName];
        column.functionReferences = column.defaultFunctionIds.map((id) =>
          dbObjects.functions.find((f) => f.id === id)
        );
      }
    }
    dbObjects.aggregates = await CatalogApi.retrieveAggregates(client, config);
    dbObjects.sequences = await CatalogApi.retrieveSequences(client, config);
    dbObjects.types = await CatalogApi.retrieveTypes(client, config);
    dbObjects.domains = await CatalogApi.retrieveDomains(client, config);

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

    if (config.compareOptions.schemaCompare.sequence !== false) {
      sqlPatch.push(
        ...this.compareSequences(
          config,
          dbSourceObjects.sequences,
          dbTargetObjects.sequences
        )
      );
      eventEmitter.emit('compare', 'SEQUENCE objects have been compared', 50);
    }

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

    sqlPatch.push(
      ...compareTypes(dbSourceObjects.types, dbTargetObjects.types, config)
    );

    sqlPatch.push(
      ...compareDomains(
        dbSourceObjects.domains,
        dbTargetObjects.domains,
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

    sqlPatch.push(
      ...this.comparePolicies(
        config,
        dbSourceObjects.tables,
        dbTargetObjects.tables
      )
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
      ddl: sqlPatch.filter((v) => !!v),
    };
  }

  static compareSchemas(
    sourceSchemas: Record<string, Schema>,
    targetSchemas: Record<string, Schema>
  ) {
    const lines: Sql[] = [];
    for (const sourceSchema in sourceSchemas) {
      const sourceObj = sourceSchemas[sourceSchema];
      const targetObj = targetSchemas[sourceSchema];

      if (!targetObj) {
        //Schema not exists on target database, then generate script to create schema
        lines.push(
          sql.generateCreateSchemaScript(sourceSchema, sourceObj.owner)
        );
        lines.push(
          sql.generateChangeCommentScript(
            sourceObj.id,
            objectType.SCHEMA,
            sourceSchema,
            sourceObj.comment
          )
        );
      }

      if (targetObj && sourceObj.comment != targetObj.comment)
        lines.push(
          sql.generateChangeCommentScript(
            sourceObj.id,
            objectType.SCHEMA,
            sourceSchema,
            sourceObj.comment
          )
        );
    }

    return lines;
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
    const lines: Sql[] = [];

    for (const sourceTable in sourceTables) {
      const sourceObj = sourceTables[sourceTable];
      const targetObj = dbTargetObjects.tables[sourceTable];
      let actionLabel = '';

      if (targetObj) {
        //Table exists on both database, then compare table schema
        actionLabel = 'ALTER';

        //@mso -> relhadoids has been deprecated from PG v12.0
        if (targetObj.options)
          lines.push(
            ...this.compareTableOptions(
              sourceTable,
              sourceObj.options,
              targetObj.options
            )
          );

        lines.push(
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

        lines.push(
          ...this.compareTableConstraints(
            sourceObj,
            sourceObj.constraints,
            targetObj.constraints,
            droppedConstraints
          )
        );

        lines.push(
          ...this.compareTableIndexes(
            sourceObj.indexes,
            targetObj.indexes,
            droppedIndexes
          )
        );

        lines.push(
          ...this.compareTablePrivileges(
            sourceTable,
            sourceObj.privileges,
            targetObj.privileges,
            config
          )
        );

        const owner = config.compareOptions.mapRole(sourceObj.owner);
        if (owner != targetObj.owner)
          lines.push(sql.generateChangeTableOwnerScript(sourceTable, owner));
        if (!commentIsEqual(sourceObj.comment, targetObj?.comment)) {
          lines.push(
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
        lines.push(sql.generateCreateTableScript(sourceTable, sourceObj));
        if (sourceObj.comment) {
          lines.push(
            sql.generateChangeCommentScript(
              sourceObj.id,
              objectType.TABLE,
              sourceTable,
              sourceObj.comment
            )
          );
        }
      }
    }

    if (config.compareOptions.schemaCompare.dropMissingTable) {
      const migrationFullTableName = config.migrationOptions
        ? `"${config.migrationOptions.historyTableSchema}"."${config.migrationOptions.historyTableName}"`
        : '';

      for (let table in dbTargetObjects.tables) {
        if (!sourceTables[table as any] && table != migrationFullTableName)
          lines.push(sql.generateDropTableScript(table));
      }
    }

    return lines;
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
    const lines: Sql[] = [];
    const targetTable = dbTargetObjects.tables[tableName];
    for (const sourceTableColumn in sourceTableColumns) {
      const sourceColumnDef = sourceTableColumns[sourceTableColumn];
      const targetColumnDef = targetTable.columns[sourceTableColumn];
      if (targetColumnDef) {
        //Table column exists on both database, then compare column schema
        lines.push(
          ...this.compareTableColumn(
            targetTable,
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
        lines.push(
          sql.generateAddTableColumnScript(tableName, sourceColumnDef)
        );
        if (sourceColumnDef.comment) {
          lines.push(
            sql.generateChangeCommentScript(
              sourceColumnDef.id,
              objectType.COLUMN,
              `${tableName}.${sourceTableColumn}`,
              sourceColumnDef.comment
            )
          );
        }

        addedColumns[tableName] ??= [];
        addedColumns[tableName].push(sourceTableColumn);
      }
    }

    for (const targetColumn in targetTable.columns) {
      if (!sourceTableColumns[targetColumn])
        //Table column not exists on source, then generate script to drop column
        lines.push(sql.generateDropTableColumnScript(tableName, targetColumn));
    }

    return lines;
  }

  static compareTableColumn(
    table: TableObject,
    columnName: string,
    sourceTableColumn: Column,
    dbTargetObjects: DatabaseObjects,
    droppedConstraints: string[],
    droppedIndexes: string[],
    droppedViews: string[]
  ) {
    const lines: Sql[] = [];
    const targetTable = dbTargetObjects.tables[table.fullName];
    const targetTableColumn = targetTable.columns[columnName];
    let changes: ColumnChanges = {};

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
      lines.push(
        sql.generateDropTableColumnScript(table.fullName, columnName, true)
      );
      lines.push(
        sql.generateAddTableColumnScript(table.fullName, sourceTableColumn)
      );
    }

    if (Object.keys(changes).length > 0) {
      let rawColumnName = columnName.substring(1).slice(0, -1);

      /* //Check if the column has constraint
      for (let constraint in targetTable.constraints) {
        const targetObj = targetTable.constraints[constraint];
        if (droppedConstraints.includes(constraint)) continue;

        let constraintDefinition = targetObj.definition;
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
            sql.generateDropTableConstraintScript(table, targetObj)
          );
          droppedConstraints.push(constraint);
        }
      }*/

      //Check if the column is part of indexes
      for (let index in targetTable.indexes) {
        let indexDefinition = targetTable.indexes[index].definition;
        let serachStartingIndex = indexDefinition.indexOf('(');

        if (
          indexDefinition.includes(`${rawColumnName},`, serachStartingIndex) ||
          indexDefinition.includes(`${rawColumnName})`, serachStartingIndex) ||
          indexDefinition.includes(`${columnName}`, serachStartingIndex)
        ) {
          lines.push(sql.generateDropIndexScript(targetTable.indexes[index]));
          droppedIndexes.push(index);
        }
      }

      //Check if the column is used into view
      for (let view in dbTargetObjects.views) {
        dbTargetObjects.views[view].dependencies.forEach((dependency: any) => {
          let fullDependencyName = `"${dependency.schemaName}"."${dependency.tableName}"`;
          if (
            fullDependencyName == table.fullName &&
            dependency.columnName == columnName
          ) {
            lines.push(sql.generateDropViewScript(view));
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
              fullDependencyName == table.fullName &&
              dependency.columnName == columnName
            ) {
              lines.push(sql.generateDropMaterializedViewScript(view));
              droppedViews.push(view);
            }
          }
        );
      }

      lines.push(
        sql.generateChangeTableColumnScript(table.fullName, columnName, changes)
      );
    }

    if (sourceTableColumn.comment != targetTableColumn.comment)
      lines.push(
        sql.generateChangeCommentScript(
          sourceTableColumn.id,
          objectType.COLUMN,
          `${table.fullName}.${columnName}`,
          sourceTableColumn.comment
        )
      );

    return lines;
  }

  static compareTableConstraints(
    table: TableObject,
    sourceTableConstraints: Record<string, ConstraintDefinition>,
    targetTableConstraints: Record<string, ConstraintDefinition>,
    droppedConstraints: string[]
  ) {
    const lines: Sql[] = [];
    for (const constraint in sourceTableConstraints) {
      const sourceObj = sourceTableConstraints[constraint];
      const targetObj = targetTableConstraints[constraint];
      //Get new or changed constraint
      if (targetObj) {
        //Table constraint exists on both database, then compare column schema
        if (sourceObj.definition !== targetObj.definition) {
          if (!droppedConstraints.includes(constraint)) {
            lines.push(sql.generateDropTableConstraintScript(table, sourceObj));
          }
          lines.push(
            sql.generateAddTableConstraintScript(table, constraint, sourceObj)
          );
          if (sourceObj.comment) {
            lines.push(
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
            lines.push(
              sql.generateAddTableConstraintScript(table, constraint, sourceObj)
            );
            if (sourceObj.comment) {
              lines.push(
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
              lines.push(
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
        lines.push(
          sql.generateAddTableConstraintScript(table, constraint, sourceObj)
        );
        if (sourceObj.comment) {
          lines.push(
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

    for (const constraint in targetTableConstraints) {
      //Get dropped constraints
      if (
        !sourceTableConstraints[constraint] &&
        !droppedConstraints.includes(constraint)
      )
        //Table constraint not exists on source, then generate script to drop constraint
        lines.push(
          sql.generateDropTableConstraintScript(
            table,
            targetTableConstraints[constraint]
          )
        );
    }

    return lines;
  }

  static compareTableIndexes(
    sourceTableIndexes: Record<string, IndexDefinition>,
    targetTableIndexes: Record<string, IndexDefinition>,
    droppedIndexes: string[]
  ) {
    const lines: Sql[] = [];

    for (const index in sourceTableIndexes) {
      const sourceObj = sourceTableIndexes[index];
      const targetObj = targetTableIndexes[index];
      //Get new or changed indexes
      if (targetObj) {
        //Table index exists on both database, then compare index definition
        if (sourceObj.definition != targetObj.definition) {
          if (!droppedIndexes.includes(index)) {
            lines.push(sql.generateDropIndexScript(sourceObj));
          }
          lines.push(stmt`${sourceObj.definition};`);
          lines.push(
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
            lines.push(stmt`${sourceObj.definition};`);
            lines.push(
              sql.generateChangeCommentScript(
                sourceObj.id,
                objectType.INDEX,
                `"${sourceObj.schema}"."${index}"`,
                sourceObj.comment
              )
            );
          } else {
            if (sourceObj.comment != targetObj.comment)
              lines.push(
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
        lines.push(stmt`${sourceObj.definition};`);
        lines.push(
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
        lines.push(sql.generateDropIndexScript(targetTableIndexes[index]));
    }

    return lines;
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
    const lines: Sql[] = [];
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
          lines.push(sql.dropPolicy(table.schema, table.name, name));
        }
        lines.push(
          sql.createPolicy(table.schema, table.name, {
            ...sourceObj,
            roles,
          })
        );
      }
      if (!commentIsEqual(sourceObj.comment, targetObj?.comment)) {
        lines.push(
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
      lines.push(sql.dropPolicy(table.schema, table.name, name));
    }

    return lines;
  }

  static comparePolicies(
    config: Config,
    source: Record<string, TableObject>,
    target: Record<string, TableObject>
  ) {
    const lines: Sql[][] = [];
    for (const name in source) {
      const sourceObj = source[name];
      const targetObj = target[name];
      const policies = this.compareTablePolicies(
        config,
        sourceObj,
        sourceObj.policies,
        targetObj?.policies ?? {}
      );
      lines.push(policies);
    }
    return lines.flat();
  }

  static compareTablePrivileges(
    tableName: string,
    sourceTablePrivileges: Record<string, Privileges>,
    targetTablePrivileges: Record<string, Privileges>,
    config: Config
  ) {
    const lines: Sql[] = [];

    for (const role in sourceTablePrivileges) {
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
          lines.push(
            sql.generateChangesTableRoleGrantsScript(tableName, role, changes)
          );
      } else {
        //Table grants for role not exists on target database, then generate script to add role privileges
        lines.push(
          sql.generateTableRoleGrantsScript(
            tableName,
            role,
            sourceTablePrivileges[role]
          )
        );
      }
    }

    return lines;
  }

  static compareViews(
    sourceViews: Record<string, ViewDefinition>,
    targetViews: Record<string, ViewDefinition>,
    droppedViews: string[],
    config: Config
  ) {
    const lines: Sql[] = [];

    for (const view in sourceViews) {
      const sourceObj = sourceViews[view];
      const targetObj = targetViews[view];
      let actionLabel = '';

      if (targetObj) {
        //View exists on both database, then compare view schema
        actionLabel = 'ALTER';

        let sourceViewDefinition = sourceObj.definition.replace(/\r/g, '');
        let targetViewDefinition = targetObj.definition.replace(/\r/g, '');
        if (sourceViewDefinition != targetViewDefinition) {
          if (!droppedViews.includes(view))
            lines.push(sql.generateDropViewScript(view));
          lines.push(sql.generateCreateViewScript(view, sourceObj));
          lines.push(
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
            lines.push(sql.generateCreateViewScript(view, sourceObj));

          lines.push(
            ...this.compareTablePrivileges(
              view,
              sourceObj.privileges,
              targetObj.privileges,
              config
            )
          );

          if (sourceObj.owner != targetObj.owner)
            lines.push(
              sql.generateChangeTableOwnerScript(view, sourceObj.owner)
            );

          if (sourceObj.comment != targetObj.comment)
            lines.push(
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

        lines.push(sql.generateCreateViewScript(view, sourceObj));
        lines.push(
          sql.generateChangeCommentScript(
            sourceObj.id,
            objectType.VIEW,
            view,
            sourceObj.comment
          )
        );
      }
    }

    if (config.compareOptions.schemaCompare.dropMissingView)
      for (let view in targetViews) {
        if (sourceViews[view]) {
          continue;
        }

        lines.push(sql.generateDropViewScript(view));
      }

    return lines;
  }

  static compareMaterializedViews(
    sourceMaterializedViews: Record<string, MaterializedViewDefinition>,
    targetMaterializedViews: Record<string, MaterializedViewDefinition>,
    droppedViews: string[],
    droppedIndexes: string[],
    config: Config
  ) {
    const lines: Sql[] = [];
    for (let view in sourceMaterializedViews) {
      const sourceObj = sourceMaterializedViews[view];
      const targetObj = targetMaterializedViews[view];
      //Get new or changed materialized views
      let actionLabel = '';

      if (targetObj) {
        //Materialized view exists on both database, then compare materialized view schema
        actionLabel = 'ALTER';

        let sourceViewDefinition = sourceObj.definition.replace(/\r/g, '');
        let targetViewDefinition = targetObj.definition.replace(/\r/g, '');
        if (sourceViewDefinition != targetViewDefinition) {
          if (!droppedViews.includes(view))
            lines.push(sql.generateDropMaterializedViewScript(view));
          lines.push(sql.generateCreateMaterializedViewScript(view, sourceObj));
          lines.push(
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
            lines.push(
              sql.generateCreateMaterializedViewScript(view, sourceObj)
            );

          lines.push(
            ...this.compareTableIndexes(
              sourceObj.indexes,
              targetObj.indexes,
              droppedIndexes
            )
          );

          lines.push(
            ...this.compareTablePrivileges(
              view,
              sourceObj.privileges,
              targetObj.privileges,
              config
            )
          );

          if (sourceObj.owner != targetObj.owner)
            lines.push(
              sql.generateChangeTableOwnerScript(view, sourceObj.owner)
            );

          if (sourceObj.comment != targetObj.comment)
            lines.push(
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

        lines.push(sql.generateCreateMaterializedViewScript(view, sourceObj));
        lines.push(
          sql.generateChangeCommentScript(
            sourceObj.id,
            objectType.MATERIALIZED_VIEW,
            view,
            sourceObj.comment
          )
        );
      }
    }

    if (config.compareOptions.schemaCompare.dropMissingView)
      for (let view in targetMaterializedViews) {
        if (sourceMaterializedViews[view]) {
          continue;
        }
        lines.push(sql.generateDropMaterializedViewScript(view));
      }

    return lines;
  }

  static compareProcedures(
    sourceFunctions: Record<string, Record<string, FunctionDefinition>>,
    targetFunctions: Record<string, Record<string, FunctionDefinition>>,
    config: Config
  ) {
    const lines: Sql[] = [];

    for (let procedure in sourceFunctions) {
      for (const procedureArgs in sourceFunctions[procedure]) {
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
              lines.push(sql.generateDropProcedureScript(sourceObj));
            }
            lines.push(sql.generateCreateProcedureScript(sourceObj));
            if (sourceObj.comment) {
              lines.push(
                sql.generateChangeCommentScript(
                  sourceObj.id,
                  procedureType,
                  `${procedure}(${procedureArgs})`,
                  sourceObj.comment
                )
              );
            }
          } else {
            lines.push(
              ...this.compareProcedurePrivileges(
                sourceObj,
                sourceObj.privileges,
                targetObj.privileges
              )
            );

            if (sourceObj.owner != targetObj.owner)
              lines.push(
                sql.generateChangeProcedureOwnerScript(
                  procedure,
                  procedureArgs,
                  sourceObj.owner,
                  sourceObj.type
                )
              );

            if (sourceObj.comment != sourceObj.comment)
              lines.push(
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

          lines.push(sql.generateCreateProcedureScript(sourceObj));
          if (sourceObj.comment) {
            lines.push(
              sql.generateChangeCommentScript(
                sourceObj.id,
                procedureType,
                `${procedure}(${procedureArgs})`,
                sourceObj.comment
              )
            );
          }
        }
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
          lines.push(
            sql.generateDropProcedureScript(
              targetFunctions[procedure][procedureArgs]
            )
          );
        }
      }

    return lines;
  }

  static compareAggregates(
    sourceAggregates: Record<string, Record<string, AggregateDefinition>>,
    targetAggregates: Record<string, Record<string, AggregateDefinition>>,
    config: Config
  ) {
    const lines: Sql[] = [];

    for (let aggregate in sourceAggregates) {
      for (const aggregateArgs in sourceAggregates[aggregate]) {
        const sourceObj = sourceAggregates[aggregate][aggregateArgs];
        const targetObj =
          targetAggregates[aggregate] &&
          targetAggregates[aggregate][aggregateArgs];
        let actionLabel = '';

        if (targetObj) {
          //Aggregate exists on both database, then compare procedure definition
          actionLabel = 'ALTER';

          //TODO: Is correct that if definition is different automatically GRANTS and OWNER will not be updated also?
          if (sourceObj.definition != targetObj.definition) {
            lines.push(sql.generateChangeAggregateScript(sourceObj));
            lines.push(
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

          lines.push(sql.generateCreateAggregateScript(sourceObj));
          if (sourceObj.comment) {
            lines.push(
              sql.generateChangeCommentScript(
                sourceObj.id,
                objectType.FUNCTION,
                `${aggregate}(${aggregateArgs})`,
                sourceObj.comment
              )
            );
          }
        }
      }
    }

    if (config.compareOptions.schemaCompare.dropMissingAggregate)
      for (let aggregate in targetAggregates) {
        for (const aggregateArgs in targetAggregates[aggregate]) {
          if (
            !sourceAggregates[aggregate] ||
            !sourceAggregates[aggregate][aggregateArgs]
          )
            lines.push(
              sql.generateDropAggregateScript(aggregate, aggregateArgs)
            );
        }
      }

    return lines;
  }

  static compareProcedurePrivileges(
    schema: FunctionDefinition,
    sourceProcedurePrivileges: Record<string, FunctionPrivileges>,
    targetProcedurePrivileges: Record<string, FunctionPrivileges>
  ) {
    const lines: Sql[] = [];

    for (const role in sourceProcedurePrivileges) {
      const sourceObj = sourceProcedurePrivileges[role];
      const targetObj = targetProcedurePrivileges[role];
      //Get new or changed role privileges
      if (targetObj) {
        //Procedure privileges for role exists on both database, then compare privileges
        let changes: ColumnChanges = {};
        if (sourceObj.execute != targetObj.execute)
          changes.execute = sourceObj.execute;

        if (Object.keys(changes).length > 0)
          lines.push(
            sql.generateChangesProcedureRoleGrantsScript(schema, role, changes)
          );
      } else {
        //Procedure grants for role not exists on target database, then generate script to add role privileges
        lines.push(sql.generateProcedureRoleGrantsScript(schema, role));
      }
    }

    return lines;
  }

  static compareSequences(
    config: Config,
    sourceSequences: Record<string, Sequence>,
    targetSequences: Record<string, Sequence>
  ) {
    const lines: Sql[] = [];
    for (const sequence in sourceSequences) {
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
          lines.push(
            sql.generateRenameSequenceScript(
              targetSequence,
              `"${sourceObj.name}"`
            )
          );

        lines.push(
          ...this.compareSequenceDefinition(
            config,
            sequence,
            sourceObj,
            targetObj
          )
        );

        lines.push(
          ...this.compareSequencePrivileges(
            sequence,
            sourceObj.privileges,
            targetObj.privileges
          )
        );

        if (sourceObj.comment != targetObj.comment)
          lines.push(
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

        lines.push(
          sql.generateCreateSequenceScript(
            sourceObj,
            config.compareOptions.mapRole(sourceObj.owner)
          )
        );
        if (sourceObj.comment) {
          lines.push(
            sql.generateChangeCommentScript(
              sourceObj.id,
              objectType.SEQUENCE,
              sequence,
              sourceObj.comment
            )
          );
        }
      }

      //TODO: @mso -> add a way to drop missing sequence if exists only on target db
    }

    return lines;
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
    const lines: Sql[] = [];

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
      lines.push(
        sql.generateChangeSequencePropertyScript(
          sequence,
          property as SequenceProperties,
          sourceObj
        )
      );
    }

    return lines;
  }

  static compareSequencePrivileges(
    sequence: string,
    sourceSequencePrivileges: SequencePrivileges,
    targetSequencePrivileges: SequencePrivileges
  ) {
    const lines: Sql[] = [];

    for (const role in sourceSequencePrivileges) {
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
          lines.push(
            sql.generateChangesSequenceRoleGrantsScript(sequence, role, changes)
          );
      } else {
        //Sequence grants for role not exists on target database, then generate script to add role privileges
        lines.push(
          sql.generateSequenceRoleGrantsScript(
            sequence,
            role,
            sourceSequencePrivileges[role]
          )
        );
      }
    }

    return lines;
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

      if (!(await this.checkIfTableExists(sourceClient, tableDefinition))) {
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
          lines.push(
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
          lines.push(...compareResult.lines);
          differentRecords = lines.length;

          if (compareResult.isSequenceRebaseNeeded)
            lines.push(...this.rebaseSequences(tableDefinition, tableData));
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
        result.lines.push(...fieldCompareResult.lines);
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
      result.lines.push(
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
    const lines: Sql[] = [];
    const fullTableName = `"${tableDefinition.tableSchema || 'public'}"."${
      tableDefinition.tableName
    }"`;

    tableData.sourceData.sequences.forEach((sequence) => {
      lines.push(sql.generateSetSequenceValueScript(fullTableName, sequence));
    });

    return lines;
  }

  static async saveSqlScript(
    lines: Sql[],
    config: Config,
    scriptName: string,
    eventEmitter: EventEmitter
  ) {
    if (lines.length <= 0) return null;

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

        lines.forEach((line) => {
          file.write(line.toString());
        });

        file.end();
      } catch (err) {
        reject(err);
      }
    });
  }
}
