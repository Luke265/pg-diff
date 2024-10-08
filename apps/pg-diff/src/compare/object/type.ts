import objectType from '../../enums/object-type.js';
import { Type, Column } from '../../catalog/database-objects.js';
import { Sql } from '../stmt.js';
import { ColumnChanges, commentIsEqual } from '../utils.js';
import {
  generateDropTableColumnScript,
  generateAddTableColumnScript,
  generateChangeTableColumnScript,
} from '../sql/column.js';
import { generateChangeCommentScript } from '../sql/misc.js';
import {
  generateChangeTypeOwnerScript,
  generateCreateTypeScript,
  generateDropTypeScript,
  generateAddTypeColumnScript,
  generateDropTypeColumnScript,
} from '../sql/type.js';
import { Config } from '../../config.js';

export function compareTypes(
  source: Record<string, Type>,
  target: Record<string, Type>,
  config: Config,
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
          ...compareTypeColumns(
            sourceObj,
            sourceObj.columns,
            targetObj.columns,
          ),
        );
      }

      const owner = config.compareOptions.mapRole(sourceObj.owner);
      if (owner !== targetObj.owner) {
        sqlScript.push(generateChangeTypeOwnerScript(sourceObj, owner));
      }
      if (!commentIsEqual(sourceObj.comment, targetObj?.comment)) {
        sqlScript.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.TYPE,
            name,
            sourceObj.comment,
          ),
        );
      }
    } else {
      //Table not exists on target database, then generate the script to create table
      sqlScript.push(generateCreateTypeScript(sourceObj));
      if (sourceObj.comment) {
        sqlScript.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.TYPE,
            name,
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

    for (const name in target) {
      if (source[name] || name === migrationFullTableName) {
        continue;
      }
      sqlScript.push(generateDropTypeScript(target[name]));
    }
  }

  return sqlScript;
}

function compareTypeColumns(
  type: Type,
  source: Record<string, Column>,
  target: Record<string, Column>,
) {
  const sqlScript: Sql[] = [];
  for (const sourceTableColumn in source) {
    const sourceObj = source[sourceTableColumn];
    const targetObj = target[sourceTableColumn];
    if (targetObj) {
      //Table column exists on both database, then compare column schema
      sqlScript.push(
        ...compareTableColumn(type.fullName, sourceObj, targetObj),
      );
    } else {
      //Table column not exists on target database, then generate script to add column
      sqlScript.push(generateAddTypeColumnScript(type, sourceObj));
      if (sourceObj.comment) {
        sqlScript.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.COLUMN,
            sourceObj.fullName,
            sourceObj.comment,
          ),
        );
      }
    }
  }

  for (const name in target) {
    if (source[name]) {
      continue;
    }
    //Table column not exists on source, then generate script to drop column
    sqlScript.push(generateDropTypeColumnScript(type, target[name]));
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
    sqlScript.push(generateDropTableColumnScript(table, source.name, true));
    sqlScript.push(generateAddTableColumnScript(table, source));
  }

  if (Object.keys(changes).length > 0) {
    let rawColumnName = source.name.substring(1).slice(0, -1);

    sqlScript.push(
      generateChangeTableColumnScript(table, source.name, changes),
    );
  }

  if (source.comment != target.comment)
    sqlScript.push(
      generateChangeCommentScript(
        source.id,
        objectType.COLUMN,
        source.fullName,
        source.comment,
      ),
    );

  return sqlScript;
}
