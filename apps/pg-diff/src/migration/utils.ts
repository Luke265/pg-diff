import path from 'path';
import {
  MigrationHistoryTableSchema,
  migrationHistoryTableSchema,
} from './migration-history-table-schema';
import { PatchInfo } from './patch-info';
import { Client } from 'pg';
import { MigrationConfig } from './migration-config';
import { TableObject } from '../catalog/database-objects';
import { generateCreateTableScript } from '../compare/sql/table';
import { Config } from '../config';

export function prepareMigrationConfig(config: Config): MigrationConfig {
  if (!config.migrationOptions.patchesDirectory)
    throw new Error('Missing configuration property "patchesFolder"!');

  return {
    patchesFolder: path.isAbsolute(config.migrationOptions.patchesDirectory)
      ? config.migrationOptions.patchesDirectory
      : path.resolve(process.cwd(), config.migrationOptions.patchesDirectory),
    migrationHistory: {
      tableName: config.migrationOptions.historyTableName,
      tableSchema: config.migrationOptions.historyTableSchema,
      fullTableName: `"${config.migrationOptions.historyTableSchema}"."${config.migrationOptions.historyTableName}"`,
      primaryKeyName: `"${config.migrationOptions.historyTableName}_pkey"`,
      tableOwner: config.targetClient.user,
      tableColumns: extractColumnsDefinitionFromSchema(
        migrationHistoryTableSchema,
      ),
    },
  };
}

function extractColumnsDefinitionFromSchema(
  schema: MigrationHistoryTableSchema,
) {
  return Object.values(schema.columns).map((c) => ({
    name: c,
    dataTypeCategory: c.dataTypeCategory,
  }));
}

export async function prepareMigrationsHistoryTable(
  pgClient: Client,
  config: MigrationConfig,
) {
  migrationHistoryTableSchema.constraints[
    config.migrationHistory.primaryKeyName
  ] = {
    type: 'p',
    definition: 'PRIMARY KEY ("version")',
  };

  migrationHistoryTableSchema.privileges[config.migrationHistory.tableOwner] = {
    select: true,
    insert: true,
    update: true,
    delete: true,
    truncate: true,
    references: true,
    trigger: true,
  };

  migrationHistoryTableSchema.owner = config.migrationHistory.tableOwner;

  let sqlScript = generateCreateTableScript(
    config.migrationHistory.tableName,
    migrationHistoryTableSchema as unknown as TableObject,
  );
  await pgClient.query(sqlScript.toString());
}

export function getPatchFileInfo(filename: string, filepath: string) {
  let indexOfSeparator = filename.indexOf('_');
  let version = filename.substring(0, indexOfSeparator);
  let name = filename.substring(indexOfSeparator + 1).replace('.sql', '');

  if (indexOfSeparator < 0 || !/^\d+$/.test(version))
    throw new Error(
      `The patch file name ${filename} is not compatible with conventioned pattern {version}_{path name}.sql !`,
    );

  return new PatchInfo(filename, filepath, version, name);
}
