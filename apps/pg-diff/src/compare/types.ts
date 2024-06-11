import objectType from '../enums/object-type';
import { Config } from '../models/config';
import { Type, Column } from '../catalog/database-objects';
import { Sql } from '../stmt';
import { ColumnChanges, commentIsEqual } from './utils';
import * as sql from '../sql-script-generator';

export function compareTypes(
  source: Record<string, Type>,
  target: Record<string, Type>,
  config: Config
) {
  const sqlScript: Sql[] = [];
  for (const name in source) {
    const sourceObj = source[name];
    const targetObj = target[name];

    if (targetObj) {
      //Table exists on both database, then compare table schema

      if (targetObj.enum) {
        //TODO: alter enum
      } else {
        sqlScript.push(
          ...compareTypeColumns(sourceObj, sourceObj.columns, targetObj.columns)
        );
      }

      const owner = config.compareOptions.mapRole(sourceObj.owner);
      if (owner !== targetObj.owner) {
        sqlScript.push(sql.generateChangeTypeOwnerScript(sourceObj, owner));
      }
      if (!commentIsEqual(sourceObj.comment, targetObj?.comment)) {
        sqlScript.push(
          sql.generateChangeCommentScript(
            sourceObj.id,
            objectType.TYPE,
            name,
            sourceObj.comment
          )
        );
      }
    } else {
      //Table not exists on target database, then generate the script to create table
      sqlScript.push(sql.generateCreateTypeScript(sourceObj));
      if (sourceObj.comment) {
        sqlScript.push(
          sql.generateChangeCommentScript(
            sourceObj.id,
            objectType.TYPE,
            name,
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

    for (const name in target) {
      if (source[name] || name === migrationFullTableName) {
        continue;
      }
      sqlScript.push(sql.generateDropTypeScript(target[name]));
    }
  }

  return sqlScript;
}

function compareTypeColumns(
  type: Type,
  source: Record<string, Column>,
  target: Record<string, Column>
) {
  const sqlScript: Sql[] = [];
  for (const sourceTableColumn in source) {
    const sourceObj = source[sourceTableColumn];
    const targetObj = target[sourceTableColumn];
    if (targetObj) {
      //Table column exists on both database, then compare column schema
      sqlScript.push(
        ...compareTableColumn(type.fullName, sourceObj, targetObj)
      );
    } else {
      //Table column not exists on target database, then generate script to add column
      sqlScript.push(sql.generateAddTypeColumnScript(type, sourceObj));
      if (sourceObj.comment) {
        sqlScript.push(
          sql.generateChangeCommentScript(
            sourceObj.id,
            objectType.COLUMN,
            sourceObj.fullName,
            sourceObj.comment
          )
        );
      }
    }
  }

  for (const name in target) {
    if (source[name]) {
      continue;
    }
    //Table column not exists on source, then generate script to drop column
    sqlScript.push(sql.generateDropTypeColumnScript(type, target[name]));
  }

  return sqlScript;
}

function compareTableColumn(table: string, source: Column, target: Column) {
  let sqlScript: Sql[] = [];
  let changes: ColumnChanges = {};

  if (source.nullable != target.nullable) changes.nullable = source.nullable;

  if (
    source.datatype != target.datatype ||
    source.precision != target.precision ||
    source.scale != target.scale
  ) {
    changes.datatype = source.datatype;
    changes.dataTypeID = source.dataTypeID;
    changes.dataTypeCategory = source.dataTypeCategory;
    changes.precision = source.precision;
    changes.scale = source.scale;
  }

  if (source.default != target.default) {
    changes.default = source.default;
    changes.defaultRefs = source.defaultRefs;
  }

  if (source.identity != target.identity) {
    changes.identity = source.identity;

    if (target.identity == null) changes.isNewIdentity = true;
    else changes.isNewIdentity = false;
  }

  if (
    source.generatedColumn &&
    (source.generatedColumn != target.generatedColumn ||
      source.default != target.default)
  ) {
    changes = {};
    sqlScript.push(sql.generateDropTableColumnScript(table, source.name, true));
    sqlScript.push(sql.generateAddTableColumnScript(table, source));
  }

  if (Object.keys(changes).length > 0) {
    let rawColumnName = source.name.substring(1).slice(0, -1);

    sqlScript.push(
      sql.generateChangeTableColumnScript(table, source.name, changes)
    );
  }

  if (source.comment != target.comment)
    sqlScript.push(
      sql.generateChangeCommentScript(
        source.id,
        objectType.COLUMN,
        source.fullName,
        source.comment
      )
    );

  return sqlScript;
}
