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
  Privileges,
  Schema,
  Sequence,
  SequencePrivileges,
  TableObject,
  TableOptions,
  ViewDefinition,
} from '../models/database-objects';
import sql from '../sql-script-generator';
import { TableData } from '../models/table-data';
import objectType from '../enums/object-type';
import { isEqual } from 'lodash';
import path from 'path';
import fs from 'fs';
import EventEmitter from 'events';
import { Config } from '../models/config';
import { TableDefinition } from '../models/table-definition';
import { ClientBase } from 'pg';
import { getServerVersion } from '../utils';

interface Changes {
  datatype?: any;
  dataTypeID?: any;
  dataTypeCategory?: any;
  precision?: any;
  scale?: any;
  nullable?: any;
  default?: any;
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

    let droppedConstraints: string[] = [];
    let droppedIndexes: string[] = [];
    let droppedViews: string[] = [];
    let addedColumns: any = {};
    let addedTables: any[] = [];

    let scripts = this.compareDatabaseObjects(
      dbSourceObjects,
      dbTargetObjects,
      droppedConstraints,
      droppedIndexes,
      droppedViews,
      addedColumns,
      addedTables,
      config,
      eventEmitter
    );

    //The progress step size is 20
    if (config.compareOptions.dataCompare.enable) {
      scripts.push(
        ...(await this.compareTablesRecords(
          config,
          pgSourceClient,
          pgTargetClient,
          addedColumns,
          addedTables,
          dbSourceObjects,
          dbTargetObjects,
          eventEmitter
        ))
      );
      eventEmitter.emit('compare', 'Table records have been compared', 95);
    }

    let scriptFilePath = await this.saveSqlScript(
      scripts,
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
      functions: {},
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
    dbObjects.functions = await CatalogApi.retrieveFunctions(client, config);
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
    droppedConstraints: string[],
    droppedIndexes: string[],
    droppedViews: string[],
    addedColumns: any,
    addedTables: string[],
    config: Config,
    eventEmitter: EventEmitter
  ) {
    let sqlPatch: string[] = [];

    sqlPatch.push(
      ...this.compareSchemas(dbSourceObjects.schemas, dbTargetObjects.schemas)
    );
    eventEmitter.emit('compare', 'SCHEMA objects have been compared', 45);

    sqlPatch.push(
      ...this.compareSequences(
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
        dbSourceObjects.functions,
        dbTargetObjects.functions,
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

    return sqlPatch;
  }

  static finalizeScript(scriptLabel: string, sqlScript: string[]) {
    let finalizedScript: string[] = [];

    if (sqlScript.length > 0) {
      finalizedScript.push(`\n--- BEGIN ${scriptLabel} ---\n`);
      finalizedScript.push(...sqlScript);
      finalizedScript.push(`\n--- END ${scriptLabel} ---\n`);
    }

    return finalizedScript;
  }

  static compareSchemas(
    sourceSchemas: Record<string, Schema>,
    targetSchemas: Record<string, Schema>
  ) {
    const finalizedScript: string[] = [];
    for (const sourceSchema in sourceSchemas) {
      const sourceDef = sourceSchemas[sourceSchema];
      const targetDef = targetSchemas[sourceSchema];
      const sqlScript: string[] = [];

      if (!targetDef) {
        //Schema not exists on target database, then generate script to create schema
        sqlScript.push(
          sql.generateCreateSchemaScript(sourceSchema, sourceDef.owner)
        );
        sqlScript.push(
          sql.generateChangeCommentScript(
            objectType.SCHEMA,
            sourceSchema,
            sourceDef.comment
          )
        );
      }

      if (targetDef && sourceDef.comment != targetDef.comment)
        sqlScript.push(
          sql.generateChangeCommentScript(
            objectType.SCHEMA,
            sourceSchema,
            sourceDef.comment
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
    let finalizedScript: string[] = [];

    for (const sourceTable in sourceTables) {
      const sourceTableDef = sourceTables[sourceTable];
      const targetTableDef = dbTargetObjects.tables[sourceTable];
      let sqlScript: string[] = [];
      let actionLabel = '';

      if (targetTableDef) {
        //Table exists on both database, then compare table schema
        actionLabel = 'ALTER';

        //@mso -> relhadoids has been deprecated from PG v12.0
        if (targetTableDef.options)
          sqlScript.push(
            ...this.compareTableOptions(
              sourceTable,
              sourceTableDef.options,
              targetTableDef.options
            )
          );

        sqlScript.push(
          ...this.compareTableColumns(
            sourceTable,
            sourceTableDef.columns,
            dbTargetObjects,
            droppedConstraints,
            droppedIndexes,
            droppedViews,
            addedColumns
          )
        );

        sqlScript.push(
          ...this.compareTableConstraints(
            sourceTable,
            sourceTableDef.constraints,
            targetTableDef.constraints,
            droppedConstraints
          )
        );

        sqlScript.push(
          ...this.compareTableIndexes(
            sourceTableDef.indexes,
            targetTableDef.indexes,
            droppedIndexes
          )
        );

        sqlScript.push(
          ...this.compareTablePrivileges(
            sourceTable,
            sourceTableDef.privileges,
            targetTableDef.privileges,
            config
          )
        );

        if (sourceTableDef.owner != targetTableDef.owner)
          sqlScript.push(
            sql.generateChangeTableOwnerScript(
              sourceTable,
              sourceTableDef.owner
            )
          );

        if (sourceTableDef.comment != targetTableDef.comment)
          sqlScript.push(
            sql.generateChangeCommentScript(
              objectType.TABLE,
              sourceTable,
              sourceTableDef.comment
            )
          );
      } else {
        //Table not exists on target database, then generate the script to create table
        actionLabel = 'CREATE';
        addedTables.push(sourceTable);
        sqlScript.push(
          sql.generateCreateTableScript(sourceTable, sourceTableDef)
        );
        sqlScript.push(
          sql.generateChangeCommentScript(
            objectType.TABLE,
            sourceTable,
            sourceTableDef.comment
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
        let sqlScript: string[] = [];

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
    const sqlScript: string[] = [];
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
    let sqlScript: string[] = [];
    let changes: Changes = {};
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

    if (sourceTableColumn.default != targetTableColumn.default)
      changes.default = sourceTableColumn.default;

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
          sqlScript.push(sql.generateDropIndexScript(index));
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
          objectType.COLUMN,
          `${tableName}.${columnName}`,
          sourceTableColumn.comment
        )
      );

    return sqlScript;
  }

  static compareTableConstraints(
    tableName: string,
    sourceTableConstraints: Record<string, ConstraintDefinition>,
    targetTableConstraints: Record<string, ConstraintDefinition>,
    droppedConstraints: string[]
  ) {
    let sqlScript: string[] = [];

    for (let constraint in sourceTableConstraints) {
      //Get new or changed constraint
      if (targetTableConstraints[constraint]) {
        //Table constraint exists on both database, then compare column schema
        if (
          sourceTableConstraints[constraint].definition !=
          targetTableConstraints[constraint].definition
        ) {
          if (!droppedConstraints.includes(constraint)) {
            sqlScript.push(
              sql.generateDropTableConstraintScript(tableName, constraint)
            );
          }
          sqlScript.push(
            sql.generateAddTableConstraintScript(
              tableName,
              constraint,
              sourceTableConstraints[constraint]
            )
          );
          sqlScript.push(
            sql.generateChangeCommentScript(
              objectType.CONSTRAINT,
              constraint,
              sourceTableConstraints[constraint].comment,
              tableName
            )
          );
        } else {
          if (droppedConstraints.includes(constraint)) {
            //It will recreate a dropped constraints because changes happens on involved columns
            sqlScript.push(
              sql.generateAddTableConstraintScript(
                tableName,
                constraint,
                sourceTableConstraints[constraint]
              )
            );
            sqlScript.push(
              sql.generateChangeCommentScript(
                objectType.CONSTRAINT,
                constraint,
                sourceTableConstraints[constraint].comment,
                tableName
              )
            );
          } else {
            if (
              sourceTableConstraints[constraint].comment !=
              targetTableConstraints[constraint].comment
            )
              sqlScript.push(
                sql.generateChangeCommentScript(
                  objectType.CONSTRAINT,
                  constraint,
                  sourceTableConstraints[constraint].comment,
                  tableName
                )
              );
          }
        }
      } else {
        //Table constraint not exists on target database, then generate script to add constraint
        sqlScript.push(
          sql.generateAddTableConstraintScript(
            tableName,
            constraint,
            sourceTableConstraints[constraint]
          )
        );
        sqlScript.push(
          sql.generateChangeCommentScript(
            objectType.CONSTRAINT,
            constraint,
            sourceTableConstraints[constraint].comment,
            tableName
          )
        );
      }
    }

    for (let constraint in targetTableConstraints) {
      //Get dropped constraints
      if (
        !sourceTableConstraints[constraint] &&
        !droppedConstraints.includes(constraint)
      )
        //Table constraint not exists on source, then generate script to drop constraint
        sqlScript.push(
          sql.generateDropTableConstraintScript(tableName, constraint)
        );
    }

    return sqlScript;
  }

  static compareTableIndexes(
    sourceTableIndexes: Record<string, IndexDefinition>,
    targetTableIndexes: Record<string, IndexDefinition>,
    droppedIndexes: string[]
  ) {
    let sqlScript: string[] = [];

    for (let index in sourceTableIndexes) {
      //Get new or changed indexes
      if (targetTableIndexes[index]) {
        //Table index exists on both database, then compare index definition
        if (
          sourceTableIndexes[index].definition !=
          targetTableIndexes[index].definition
        ) {
          if (!droppedIndexes.includes(index)) {
            sqlScript.push(sql.generateDropIndexScript(index));
          }
          sqlScript.push(`\n${sourceTableIndexes[index].definition};\n`);
          sqlScript.push(
            sql.generateChangeCommentScript(
              objectType.INDEX,
              `"${sourceTableIndexes[index].schema}"."${index}"`,
              sourceTableIndexes[index].comment
            )
          );
        } else {
          if (droppedIndexes.includes(index)) {
            //It will recreate a dropped index because changes happens on involved columns
            sqlScript.push(`\n${sourceTableIndexes[index].definition};\n`);
            sqlScript.push(
              sql.generateChangeCommentScript(
                objectType.INDEX,
                `"${sourceTableIndexes[index].schema}"."${index}"`,
                sourceTableIndexes[index].comment
              )
            );
          } else {
            if (
              sourceTableIndexes[index].comment !=
              targetTableIndexes[index].comment
            )
              sqlScript.push(
                sql.generateChangeCommentScript(
                  objectType.INDEX,
                  `"${sourceTableIndexes[index].schema}"."${index}"`,
                  sourceTableIndexes[index].comment
                )
              );
          }
        }
      } else {
        //Table index not exists on target database, then generate script to add index
        sqlScript.push(`\n${sourceTableIndexes[index].definition};\n`);
        sqlScript.push(
          sql.generateChangeCommentScript(
            objectType.INDEX,
            `"${sourceTableIndexes[index].schema}"."${index}"`,
            sourceTableIndexes[index].comment
          )
        );
      }
    }

    for (let index in targetTableIndexes) {
      //Get dropped indexes
      if (!sourceTableIndexes[index] && !droppedIndexes.includes(index))
        //Table index not exists on source, then generate script to drop index
        sqlScript.push(sql.generateDropIndexScript(index));
    }

    return sqlScript;
  }

  static compareTablePrivileges(
    tableName: string,
    sourceTablePrivileges: Record<string, Privileges>,
    targetTablePrivileges: Record<string, Privileges>,
    config: Config
  ) {
    let sqlScript: string[] = [];

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
        let changes: Changes = {};

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
    let finalizedScript: string[] = [];

    for (let view in sourceViews) {
      let sqlScript: string[] = [];
      let actionLabel = '';

      if (targetViews[view]) {
        //View exists on both database, then compare view schema
        actionLabel = 'ALTER';

        let sourceViewDefinition = sourceViews[view].definition.replace(
          /\r/g,
          ''
        );
        let targetViewDefinition = targetViews[view].definition.replace(
          /\r/g,
          ''
        );
        if (sourceViewDefinition != targetViewDefinition) {
          if (!droppedViews.includes(view))
            sqlScript.push(sql.generateDropViewScript(view));
          sqlScript.push(sql.generateCreateViewScript(view, sourceViews[view]));
          sqlScript.push(
            sql.generateChangeCommentScript(
              objectType.VIEW,
              view,
              sourceViews[view].comment
            )
          );
        } else {
          if (droppedViews.includes(view))
            //It will recreate a dropped view because changes happens on involved columns
            sqlScript.push(
              sql.generateCreateViewScript(view, sourceViews[view])
            );

          sqlScript.push(
            ...this.compareTablePrivileges(
              view,
              sourceViews[view].privileges,
              targetViews[view].privileges,
              config
            )
          );

          if (sourceViews[view].owner != targetViews[view].owner)
            sqlScript.push(
              sql.generateChangeTableOwnerScript(view, sourceViews[view].owner)
            );

          if (sourceViews[view].comment != targetViews[view].comment)
            sqlScript.push(
              sql.generateChangeCommentScript(
                objectType.VIEW,
                view,
                sourceViews[view].comment
              )
            );
        }
      } else {
        //View not exists on target database, then generate the script to create view
        actionLabel = 'CREATE';

        sqlScript.push(sql.generateCreateViewScript(view, sourceViews[view]));
        sqlScript.push(
          sql.generateChangeCommentScript(
            objectType.VIEW,
            view,
            sourceViews[view].comment
          )
        );
      }

      finalizedScript.push(
        ...this.finalizeScript(`${actionLabel} VIEW ${view}`, sqlScript)
      );
    }

    if (config.compareOptions.schemaCompare.dropMissingView)
      for (let view in targetViews) {
        //Get missing views
        let sqlScript: string[] = [];

        if (!sourceViews[view])
          sqlScript.push(sql.generateDropViewScript(view));

        finalizedScript.push(
          ...this.finalizeScript(`DROP VIEW ${view}`, sqlScript)
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
    let finalizedScript: string[] = [];

    for (let view in sourceMaterializedViews) {
      //Get new or changed materialized views
      let sqlScript: string[] = [];
      let actionLabel = '';

      if (targetMaterializedViews[view]) {
        //Materialized view exists on both database, then compare materialized view schema
        actionLabel = 'ALTER';

        let sourceViewDefinition = sourceMaterializedViews[
          view
        ].definition.replace(/\r/g, '');
        let targetViewDefinition = targetMaterializedViews[
          view
        ].definition.replace(/\r/g, '');
        if (sourceViewDefinition != targetViewDefinition) {
          if (!droppedViews.includes(view))
            sqlScript.push(sql.generateDropMaterializedViewScript(view));
          sqlScript.push(
            sql.generateCreateMaterializedViewScript(
              view,
              sourceMaterializedViews[view]
            )
          );
          sqlScript.push(
            sql.generateChangeCommentScript(
              objectType.MATERIALIZED_VIEW,
              view,
              sourceMaterializedViews[view].comment
            )
          );
        } else {
          if (droppedViews.includes(view))
            //It will recreate a dropped materialized view because changes happens on involved columns
            sqlScript.push(
              sql.generateCreateMaterializedViewScript(
                view,
                sourceMaterializedViews[view]
              )
            );

          sqlScript.push(
            ...this.compareTableIndexes(
              sourceMaterializedViews[view].indexes,
              targetMaterializedViews[view].indexes,
              droppedIndexes
            )
          );

          sqlScript.push(
            ...this.compareTablePrivileges(
              view,
              sourceMaterializedViews[view].privileges,
              targetMaterializedViews[view].privileges,
              config
            )
          );

          if (
            sourceMaterializedViews[view].owner !=
            targetMaterializedViews[view].owner
          )
            sqlScript.push(
              sql.generateChangeTableOwnerScript(
                view,
                sourceMaterializedViews[view].owner
              )
            );

          if (
            sourceMaterializedViews[view].comment !=
            targetMaterializedViews[view].comment
          )
            sqlScript.push(
              sql.generateChangeCommentScript(
                objectType.MATERIALIZED_VIEW,
                view,
                sourceMaterializedViews[view].comment
              )
            );
        }
      } else {
        //Materialized view not exists on target database, then generate the script to create materialized view
        actionLabel = 'CREATE';

        sqlScript.push(
          sql.generateCreateMaterializedViewScript(
            view,
            sourceMaterializedViews[view]
          )
        );
        sqlScript.push(
          sql.generateChangeCommentScript(
            objectType.MATERIALIZED_VIEW,
            view,
            sourceMaterializedViews[view].comment
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
        let sqlScript: string[] = [];

        if (!sourceMaterializedViews[view])
          sqlScript.push(sql.generateDropMaterializedViewScript(view));

        finalizedScript.push(
          ...this.finalizeScript(`DROP MATERIALIZED VIEW ${view}`, sqlScript)
        );
      }

    return finalizedScript;
  }

  static compareProcedures(
    sourceFunctions: Record<string, Record<string, FunctionDefinition>>,
    targetFunctions: Record<string, Record<string, FunctionDefinition>>,
    config: Config
  ) {
    let finalizedScript: string[] = [];

    for (let procedure in sourceFunctions) {
      for (const procedureArgs in sourceFunctions[procedure]) {
        let sqlScript: string[] = [];
        let actionLabel = '';
        const procedureType =
          sourceFunctions[procedure][procedureArgs].type === 'f'
            ? objectType.FUNCTION
            : objectType.PROCEDURE;

        if (
          targetFunctions[procedure] &&
          targetFunctions[procedure][procedureArgs]
        ) {
          //Procedure exists on both database, then compare procedure definition
          actionLabel = 'ALTER';

          //TODO: Is correct that if definition is different automatically GRANTS and OWNER will not be updated also?
          //TODO: Better to match only "visible" char in order to avoid special invisible like \t, spaces, etc;
          //      the problem is that a SQL STRING can contains special char as a fix from previous function version
          let sourceFunctionDefinition = sourceFunctions[procedure][
            procedureArgs
          ].definition.replace(/\r/g, '');
          let targetFunctionDefinition = targetFunctions[procedure][
            procedureArgs
          ].definition.replace(/\r/g, '');
          if (sourceFunctionDefinition != targetFunctionDefinition) {
            sqlScript.push(
              sql.generateChangeProcedureScript(
                procedure,
                sourceFunctions[procedure][procedureArgs]
              )
            );
            sqlScript.push(
              sql.generateChangeCommentScript(
                procedureType,
                `${procedure}(${procedureArgs})`,
                sourceFunctions[procedure][procedureArgs].comment
              )
            );
          } else {
            sqlScript.push(
              ...this.compareProcedurePrivileges(
                procedure,
                procedureArgs,
                sourceFunctions[procedure][procedureArgs].type,
                sourceFunctions[procedure][procedureArgs].privileges,
                targetFunctions[procedure][procedureArgs].privileges
              )
            );

            if (
              sourceFunctions[procedure][procedureArgs].owner !=
              targetFunctions[procedure][procedureArgs].owner
            )
              sqlScript.push(
                sql.generateChangeProcedureOwnerScript(
                  procedure,
                  procedureArgs,
                  sourceFunctions[procedure][procedureArgs].owner,
                  sourceFunctions[procedure][procedureArgs].type
                )
              );

            if (
              sourceFunctions[procedure][procedureArgs].comment !=
              sourceFunctions[procedure][procedureArgs].comment
            )
              sqlScript.push(
                sql.generateChangeCommentScript(
                  procedureType,
                  `${procedure}(${procedureArgs})`,
                  sourceFunctions[procedure][procedureArgs].comment
                )
              );
          }
        } else {
          //Procedure not exists on target database, then generate the script to create procedure
          actionLabel = 'CREATE';

          sqlScript.push(
            sql.generateCreateProcedureScript(
              procedure,
              sourceFunctions[procedure][procedureArgs]
            )
          );
          sqlScript.push(
            sql.generateChangeCommentScript(
              procedureType,
              `${procedure}(${procedureArgs})`,
              sourceFunctions[procedure][procedureArgs].comment
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
          let sqlScript: string[] = [];

          if (
            !sourceFunctions[procedure] ||
            !sourceFunctions[procedure][procedureArgs]
          )
            sqlScript.push(
              sql.generateDropProcedureScript(procedure, procedureArgs)
            );

          finalizedScript.push(
            ...this.finalizeScript(
              `DROP FUNCTION ${procedure}(${procedureArgs})`,
              sqlScript
            )
          );
        }
      }

    return finalizedScript;
  }

  static compareAggregates(
    sourceAggregates: Record<string, AggregateDefinition>,
    targetAggregates: Record<string, AggregateDefinition>,
    config: Config
  ) {
    let finalizedScript: string[] = [];

    for (let aggregate in sourceAggregates) {
      for (const aggregateArgs in sourceAggregates[aggregate]) {
        let sqlScript: string[] = [];
        let actionLabel = '';

        if (
          targetAggregates[aggregate] &&
          targetAggregates[aggregate][aggregateArgs]
        ) {
          //Aggregate exists on both database, then compare procedure definition
          actionLabel = 'ALTER';

          //TODO: Is correct that if definition is different automatically GRANTS and OWNER will not be updated also?
          if (
            sourceAggregates[aggregate][aggregateArgs].definition !=
            targetAggregates[aggregate][aggregateArgs].definition
          ) {
            sqlScript.push(
              sql.generateChangeAggregateScript(
                aggregate,
                sourceAggregates[aggregate][aggregateArgs]
              )
            );
            sqlScript.push(
              sql.generateChangeCommentScript(
                objectType.AGGREGATE,
                `${aggregate}(${aggregateArgs})`,
                sourceAggregates[aggregate][aggregateArgs].comment
              )
            );
          } else {
            throw new Error('Not implemented');
            /*sqlScript.push(
              ...this.compareProcedurePrivileges(
                aggregate,
                aggregateArgs,
                sourceFunctions[procedure][procedureArgs].type,
                sourceAggregates[aggregate][aggregateArgs].privileges,
                targetAggregates[aggregate][aggregateArgs].privileges,
              ),
            );

            if (
              sourceAggregates[aggregate][aggregateArgs].owner !=
              targetAggregates[aggregate][aggregateArgs].owner
            )
              sqlScript.push(
                sql.generateChangeAggregateOwnerScript(
                  aggregate,
                  aggregateArgs,
                  sourceAggregates[aggregate][aggregateArgs].owner,
                ),
              );

            if (
              sourceAggregates[aggregate][aggregateArgs].comment !=
              targetAggregates[aggregate][aggregateArgs].comment
            )
              sqlScript.push(
                sql.generateChangeCommentScript(
                  objectType.AGGREGATE,
                  `${aggregate}(${aggregateArgs})`,
                  sourceAggregates[aggregate][aggregateArgs].comment,
                ),
              );*/
          }
        } else {
          //Aggregate not exists on target database, then generate the script to create aggregate
          actionLabel = 'CREATE';

          sqlScript.push(
            sql.generateCreateAggregateScript(
              aggregate,
              sourceAggregates[aggregate][aggregateArgs]
            )
          );
          sqlScript.push(
            sql.generateChangeCommentScript(
              objectType.FUNCTION,
              `${aggregate}(${aggregateArgs})`,
              sourceAggregates[aggregate][aggregateArgs].comment
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
          let sqlScript: string[] = [];

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
    procedure: string,
    argTypes: string,
    type: 'f' | 'p',
    sourceProcedurePrivileges: Record<string, FunctionPrivileges>,
    targetProcedurePrivileges: Record<string, FunctionPrivileges>
  ) {
    let sqlScript: string[] = [];

    for (let role in sourceProcedurePrivileges) {
      //Get new or changed role privileges
      if (targetProcedurePrivileges[role]) {
        //Procedure privileges for role exists on both database, then compare privileges
        let changes: Changes = {};
        if (
          sourceProcedurePrivileges[role].execute !=
          targetProcedurePrivileges[role].execute
        )
          changes.execute = sourceProcedurePrivileges[role].execute;

        if (Object.keys(changes).length > 0)
          sqlScript.push(
            sql.generateChangesProcedureRoleGrantsScript(
              procedure,
              argTypes,
              role,
              changes,
              type
            )
          );
      } else {
        //Procedure grants for role not exists on target database, then generate script to add role privileges
        sqlScript.push(
          sql.generateProcedureRoleGrantsScript(
            procedure,
            argTypes,
            role,
            sourceProcedurePrivileges[role],
            type
          )
        );
      }
    }

    return sqlScript;
  }

  static compareSequences(
    sourceSequences: Record<string, Sequence>,
    targetSequences: Record<string, Sequence>
  ) {
    let finalizedScript: string[] = [];

    for (let sequence in sourceSequences) {
      let sqlScript: string[] = [];
      let actionLabel = '';
      let targetSequence =
        this.findRenamedSequenceOwnedByTargetTableColumn(
          sequence,
          sourceSequences[sequence].ownedBy,
          targetSequences
        ) || sequence;

      if (targetSequences[targetSequence]) {
        //Sequence exists on both database, then compare sequence definition
        actionLabel = 'ALTER';

        if (sequence != targetSequence)
          sqlScript.push(
            sql.generateRenameSequenceScript(
              targetSequence,
              `"${sourceSequences[sequence].name}"`
            )
          );

        sqlScript.push(
          ...this.compareSequenceDefinition(
            sequence,
            sourceSequences[sequence],
            targetSequences[targetSequence]
          )
        );

        sqlScript.push(
          ...this.compareSequencePrivileges(
            sequence,
            sourceSequences[sequence].privileges,
            targetSequences[targetSequence].privileges
          )
        );

        if (
          sourceSequences[sequence].comment !=
          targetSequences[targetSequence].comment
        )
          sqlScript.push(
            sql.generateChangeCommentScript(
              objectType.SEQUENCE,
              sequence,
              sourceSequences[sequence].comment
            )
          );
      } else {
        //Sequence not exists on target database, then generate the script to create sequence
        actionLabel = 'CREATE';

        sqlScript.push(
          sql.generateCreateSequenceScript(sequence, sourceSequences[sequence])
        );
        sqlScript.push(
          sql.generateChangeCommentScript(
            objectType.SEQUENCE,
            sequence,
            sourceSequences[sequence].comment
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
    sequence: string,
    sourceSequenceDefinition: Sequence,
    targetSequenceDefinition: Sequence
  ) {
    let sqlScript: string[] = [];

    for (let property in sourceSequenceDefinition) {
      //Get new or changed properties

      if (
        property == 'privileges' ||
        property == 'ownedBy' ||
        property == 'name' ||
        property == 'comment'
      )
        //skip these properties from compare
        continue;

      if (
        sourceSequenceDefinition[property] != targetSequenceDefinition[property]
      )
        sqlScript.push(
          sql.generateChangeSequencePropertyScript(
            sequence,
            property,
            sourceSequenceDefinition[property]
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
    let sqlScript: string[] = [];

    for (let role in sourceSequencePrivileges) {
      //Get new or changed role privileges
      if (targetSequencePrivileges[role]) {
        //Sequence privileges for role exists on both database, then compare privileges
        let changes: Changes = {};
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
    let finalizedScript: string[] = [];
    let iteratorCounter = 0;
    let progressStepSize = Math.floor(
      20 / config.compareOptions.dataCompare.tables.length
    );

    for (let tableDefinition of config.compareOptions.dataCompare.tables) {
      let differentRecords = 0;
      let sqlScript: string[] = [];
      let fullTableName = `"${tableDefinition.tableSchema || 'public'}"."${
        tableDefinition.tableName
      }"`;

      if (!(await this.checkIfTableExists(sourceClient, tableDefinition))) {
        sqlScript.push(
          `\n--ERROR: Table ${fullTableName} not found on SOURCE database for comparison!\n`
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
            `\n--ERROR: Table "${tableDefinition.tableSchema || 'public'}"."${
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
    let result: { sqlScript: string[]; isSequenceRebaseNeeded: boolean } = {
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
          `\n--ERROR: Too many record found in SOURCE database for table ${fullTableName} and key fields ${JSON.stringify(
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
          `\n--ERROR: Too many record found in TARGET database for table ${fullTableName} and key fields ${JSON.stringify(
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
          `\n--ERROR: Too many record found in TARGET database for table ${fullTableName} and key fields ${JSON.stringify(
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
    let result: { sqlScript: string[]; isSequenceRebaseNeeded: boolean } = {
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
    let sqlScript: string[] = [];
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
    scriptLines: string[],
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

        scriptLines.forEach(function (line: string) {
          file.write(line);
        });

        file.end();
      } catch (err) {
        reject(err);
      }
    });
  }
}
