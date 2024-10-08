import _ from 'lodash';
import objectType from '../../enums/object-type.js';
import {
  TableObject,
  DatabaseObjects,
  TableOptions,
  ConstraintDefinition,
  IndexDefinition,
  Policy,
  Column,
  Privileges,
  Trigger,
} from '../../catalog/database-objects.js';
import { Sql, statement } from '../stmt.js';
import { commentIsEqual, ColumnChanges } from '../utils.js';
import {
  generateAddTableColumnScript,
  generateDropTableColumnScript,
  generateChangeTableColumnScript,
} from '../sql/column.js';
import { generateDropIndexScript } from '../sql/index-db.js';
import { generateDropMaterializedViewScript } from '../sql/materialized-view.js';
import { generateChangeCommentScript } from '../sql/misc.js';
import { dropPolicy, createPolicy } from '../sql/policy.js';
import {
  generateChangeTableOwnerScript,
  generateCreateTableScript,
  generateDropTableScript,
  generateChangeTableOptionsScript,
  generateDropTableConstraintScript,
  generateAddTableConstraintScript,
  generateChangesTableRoleGrantsScript,
  generateTableRoleGrantsScript,
} from '../sql/table.js';
import { generateDropViewScript } from '../sql/view.js';
import { Config } from '../../config.js';
import { createTrigger, dropTrigger } from '../sql/trigger.js';

export function compareTables(
  dbSourceObjects: DatabaseObjects,
  dbTargetObjects: DatabaseObjects,
  droppedConstraints: string[],
  droppedIndexes: string[],
  droppedViews: string[],
  addedColumns: Record<string, string[]>,
  addedTables: string[],
  config: Config,
) {
  const lines: Sql[] = [];

  for (const sourceTable in dbSourceObjects.tables) {
    const sourceObj = dbSourceObjects.tables[sourceTable];
    const targetObj = dbTargetObjects.tables[sourceTable];
    if (targetObj) {
      //Table exists on both database, then compare table schema
      //@mso -> relhadoids has been deprecated from PG v12.0
      if (targetObj.options)
        lines.push(
          ...compareTableOptions(
            sourceObj,
            sourceObj.options,
            targetObj.options,
          ),
        );

      lines.push(
        ...compareTableColumns(
          sourceTable,
          sourceObj.columns,
          dbTargetObjects,
          droppedConstraints,
          droppedIndexes,
          droppedViews,
          addedColumns,
        ),
      );

      lines.push(
        ...compareTableConstraints(
          sourceObj,
          sourceObj.constraints,
          targetObj.constraints,
          droppedConstraints,
        ),
      );

      lines.push(
        ...compareTableIndexes(
          sourceObj.indexes,
          targetObj.indexes,
          droppedIndexes,
        ),
      );

      lines.push(
        ...compareTablePrivileges(
          sourceTable,
          sourceObj.privileges,
          targetObj.privileges,
          config,
        ),
      );

      const owner = config.compareOptions.mapRole(sourceObj.owner);
      if (owner !== targetObj.owner) {
        lines.push(generateChangeTableOwnerScript(sourceTable, owner));
      }
      if (!commentIsEqual(sourceObj.comment, targetObj?.comment)) {
        lines.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.TABLE,
            sourceTable,
            sourceObj.comment,
          ),
        );
      }
    } else {
      //Table not exists on target database, then generate the script to create table
      addedTables.push(sourceTable);
      lines.push(generateCreateTableScript(sourceTable, sourceObj));
      if (sourceObj.comment) {
        lines.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.TABLE,
            sourceTable,
            sourceObj.comment,
          ),
        );
      }
    }
  }

  if (config.compareOptions.schemaCompare.dropMissingTable) {
    const migrationFullTableName = config.migrationOptions
      ? `"${config.migrationOptions.historyTableSchema}"."${config.migrationOptions.historyTableName}"`
      : '';

    for (const table in dbTargetObjects.tables) {
      if (dbSourceObjects.tables[table] || table === migrationFullTableName) {
        continue;
      }
      lines.push(generateDropTableScript(dbTargetObjects.tables[table]));
    }
  }

  return lines;
}

function compareTableOptions(
  table: TableObject,
  sourceTableOptions: TableOptions,
  targetTableOptions: TableOptions,
) {
  if (sourceTableOptions.withOids === targetTableOptions.withOids) {
    return [];
  }
  return [generateChangeTableOptionsScript(table, sourceTableOptions)];
}

function compareTableColumns(
  tableName: string,
  sourceTableColumns: Record<string, Column>,
  dbTargetObjects: DatabaseObjects,
  droppedConstraints: string[],
  droppedIndexes: string[],
  droppedViews: string[],
  addedColumns: Record<string, string[]>,
) {
  const lines: Sql[] = [];
  const targetTable = dbTargetObjects.tables[tableName];
  for (const sourceTableColumn in sourceTableColumns) {
    const sourceColumnDef = sourceTableColumns[sourceTableColumn];
    const targetColumnDef = targetTable.columns[sourceTableColumn];
    if (targetColumnDef) {
      //Table column exists on both database, then compare column schema
      lines.push(
        ...compareTableColumn(
          targetTable,
          sourceTableColumn,
          sourceColumnDef,
          dbTargetObjects,
          droppedConstraints,
          droppedIndexes,
          droppedViews,
        ),
      );
    } else {
      //Table column not exists on target database, then generate script to add column
      lines.push(generateAddTableColumnScript(tableName, sourceColumnDef));
      if (sourceColumnDef.comment) {
        lines.push(
          generateChangeCommentScript(
            sourceColumnDef.id,
            objectType.COLUMN,
            `${tableName}.${sourceTableColumn}`,
            sourceColumnDef.comment,
          ),
        );
      }

      addedColumns[tableName] ??= [];
      addedColumns[tableName].push(sourceTableColumn);
    }
  }

  for (const targetColumn in targetTable.columns) {
    if (!sourceTableColumns[targetColumn])
      //Table column not exists on source, then generate script to drop column
      lines.push(generateDropTableColumnScript(tableName, targetColumn));
  }

  return lines;
}

function compareTableColumn(
  table: TableObject,
  columnName: string,
  sourceTableColumn: Column,
  dbTargetObjects: DatabaseObjects,
  droppedConstraints: string[],
  droppedIndexes: string[],
  droppedViews: string[],
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
    changes.defaultRefs = sourceTableColumn.defaultRefs;
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
    lines.push(generateDropTableColumnScript(table.fullName, columnName, true));
    lines.push(generateAddTableColumnScript(table.fullName, sourceTableColumn));
  }

  if (Object.keys(changes).length > 0) {
    let rawColumnName = columnName.substring(1).slice(0, -1);

    //Check if the column is part of indexes
    for (let index in targetTable.indexes) {
      let indexDefinition = targetTable.indexes[index].definition;
      let serachStartingIndex = indexDefinition.indexOf('(');

      if (
        indexDefinition.includes(`${rawColumnName},`, serachStartingIndex) ||
        indexDefinition.includes(`${rawColumnName})`, serachStartingIndex) ||
        indexDefinition.includes(`${columnName}`, serachStartingIndex)
      ) {
        lines.push(generateDropIndexScript(targetTable.indexes[index]));
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
          lines.push(generateDropViewScript(dbTargetObjects.views[view]));
          droppedViews.push(view);
        }
      });
    }

    //Check if the column is used into materialized view
    for (let view in dbTargetObjects.materializedViews) {
      dbTargetObjects.materializedViews[view].dependencies.forEach(
        (dependency) => {
          let fullDependencyName = `"${dependency.schemaName}"."${dependency.tableName}"`;
          if (
            fullDependencyName == table.fullName &&
            dependency.columnName == columnName
          ) {
            lines.push(
              generateDropMaterializedViewScript(
                dbTargetObjects.materializedViews[view],
              ),
            );
            droppedViews.push(view);
          }
        },
      );
    }

    lines.push(
      generateChangeTableColumnScript(table.fullName, columnName, changes),
    );
  }

  if (sourceTableColumn.comment != targetTableColumn.comment)
    lines.push(
      generateChangeCommentScript(
        sourceTableColumn.id,
        objectType.COLUMN,
        `${table.fullName}.${columnName}`,
        sourceTableColumn.comment,
      ),
    );

  return lines;
}

function compareTableConstraints(
  table: TableObject,
  sourceTableConstraints: Record<string, ConstraintDefinition>,
  targetTableConstraints: Record<string, ConstraintDefinition>,
  droppedConstraints: string[],
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
          lines.push(generateDropTableConstraintScript(table, sourceObj));
        }
        lines.push(
          generateAddTableConstraintScript(table, constraint, sourceObj),
        );
        if (sourceObj.comment) {
          lines.push(
            generateChangeCommentScript(
              sourceObj.id,
              objectType.CONSTRAINT,
              constraint,
              sourceObj.comment,
              table.fullName,
            ),
          );
        }
      } else {
        if (droppedConstraints.includes(constraint)) {
          //It will recreate a dropped constraints because changes happens on involved columns
          lines.push(
            generateAddTableConstraintScript(table, constraint, sourceObj),
          );
          if (sourceObj.comment) {
            lines.push(
              generateChangeCommentScript(
                sourceObj.id,
                objectType.CONSTRAINT,
                constraint,
                sourceObj.comment,
                table.fullName,
              ),
            );
          }
        } else {
          if (!commentIsEqual(sourceObj.comment, targetObj.comment)) {
            lines.push(
              generateChangeCommentScript(
                sourceObj.id,
                objectType.CONSTRAINT,
                constraint,
                sourceObj.comment,
                table.fullName,
              ),
            );
          }
        }
      }
    } else {
      //Table constraint not exists on target database, then generate script to add constraint
      lines.push(
        generateAddTableConstraintScript(table, constraint, sourceObj),
      );
      if (sourceObj.comment) {
        lines.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.CONSTRAINT,
            constraint,
            sourceObj.comment,
            table.fullName,
          ),
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
        generateDropTableConstraintScript(
          table,
          targetTableConstraints[constraint],
        ),
      );
  }

  return lines;
}

export function compareTableIndexes(
  sourceTableIndexes: Record<string, IndexDefinition>,
  targetTableIndexes: Record<string, IndexDefinition>,
  droppedIndexes: string[],
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
          lines.push(generateDropIndexScript(sourceObj));
        }
        lines.push(
          statement({
            sql: [sourceObj.definition, ';'],
          }),
        );
        lines.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.INDEX,
            `"${sourceObj.schema}"."${index}"`,
            sourceObj.comment,
          ),
        );
      } else {
        if (droppedIndexes.includes(index)) {
          //It will recreate a dropped index because changes happens on involved columns
          lines.push(
            statement({
              sql: [sourceObj.definition, ';'],
            }),
          );
          lines.push(
            generateChangeCommentScript(
              sourceObj.id,
              objectType.INDEX,
              `"${sourceObj.schema}"."${index}"`,
              sourceObj.comment,
            ),
          );
        } else {
          if (sourceObj.comment != targetObj.comment)
            lines.push(
              generateChangeCommentScript(
                sourceObj.id,
                objectType.INDEX,
                `"${sourceObj.schema}"."${index}"`,
                sourceObj.comment,
              ),
            );
        }
      }
    } else {
      //Table index not exists on target database, then generate script to add index
      lines.push(
        statement({
          sql: [sourceObj.definition, ';'],
        }),
      );
      lines.push(
        generateChangeCommentScript(
          sourceObj.id,
          objectType.INDEX,
          `"${sourceObj.schema}"."${index}"`,
          sourceObj.comment,
        ),
      );
    }
  }

  for (let index in targetTableIndexes) {
    //Get dropped indexes
    if (!sourceTableIndexes[index] && !droppedIndexes.includes(index))
      //Table index not exists on source, then generate script to drop index
      lines.push(generateDropIndexScript(targetTableIndexes[index]));
  }

  return lines;
}

export function compareTablePolicies(
  config: Config,
  table: TableObject,
  source: Record<string, Policy>,
  target: Record<string, Policy>,
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
      _.isEqual(roles, targetObj.roles);
    if (!isSame) {
      if (targetObj) {
        lines.push(dropPolicy(targetObj));
      }
      lines.push(
        createPolicy({
          ...sourceObj,
          roles,
        }),
      );
    }
    if (!commentIsEqual(sourceObj.comment, targetObj?.comment)) {
      lines.push(
        generateChangeCommentScript(
          sourceObj.id,
          objectType.POLICY,
          name,
          sourceObj.comment,
          `"${table.schema}"."${table.name}"`,
        ),
      );
    }
  }

  for (const name in target) {
    if (source[name]) {
      continue;
    }
    const targetObj = target[name];
    lines.push(dropPolicy(targetObj));
  }

  return lines;
}

export function compareTableTriggers(
  config: Config,
  table: TableObject,
  source: Record<string, Trigger>,
  target: Record<string, Trigger>,
) {
  const lines: Sql[] = [];
  for (const name in source) {
    const sourceObj = source[name];
    const targetObj = target[name];
    const isSame =
      targetObj &&
      sourceObj.actionOrientation === targetObj.actionOrientation &&
      sourceObj.actionStatement === targetObj.actionStatement &&
      sourceObj.actionTiming === targetObj.actionTiming &&
      sourceObj.whenExpr === targetObj.whenExpr &&
      _.isEqual(sourceObj.eventManipulation, targetObj.eventManipulation);
    if (!isSame) {
      if (targetObj) {
        lines.push(dropTrigger(targetObj));
      }
      lines.push(createTrigger(sourceObj));
    }
    if (!commentIsEqual(sourceObj.comment, targetObj?.comment)) {
      lines.push(
        generateChangeCommentScript(
          sourceObj.id,
          objectType.TRIGGER,
          name,
          sourceObj.comment,
          table.fullName,
        ),
      );
    }
  }

  for (const name in target) {
    if (source[name]) {
      continue;
    }
    const targetObj = target[name];
    lines.push(dropTrigger(targetObj));
  }

  return lines;
}

export function compareTablePrivileges(
  tableName: string,
  sourceTablePrivileges: Record<string, Privileges>,
  targetTablePrivileges: Record<string, Privileges>,
  config: Config,
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
        sourceTablePrivileges[role].select != targetTablePrivileges[role].select
      )
        changes.select = sourceTablePrivileges[role].select;

      if (
        sourceTablePrivileges[role].insert != targetTablePrivileges[role].insert
      )
        changes.insert = sourceTablePrivileges[role].insert;

      if (
        sourceTablePrivileges[role].update != targetTablePrivileges[role].update
      )
        changes.update = sourceTablePrivileges[role].update;

      if (
        sourceTablePrivileges[role].delete != targetTablePrivileges[role].delete
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
          ...generateChangesTableRoleGrantsScript(tableName, role, changes),
        );
    } else {
      //Table grants for role not exists on target database, then generate script to add role privileges
      lines.push(
        ...generateTableRoleGrantsScript(
          tableName,
          role,
          sourceTablePrivileges[role],
        ),
      );
    }
  }

  return lines;
}
