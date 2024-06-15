import path from 'path';
import pg from 'pg';
import { migrationHistoryTableSchema } from './models/migration-history-table-schema';
import { PatchInfo } from './models/patch-info';
import { ServerVersion } from './models/server-version';
import { Config } from './models/config';
import { Client } from 'pg';
import { ClientConfig } from './models/client-config';
import { MigrationConfig } from './models/migration-config';
import { TableObject } from './catalog/database-objects';
import { generateCreateTableScript } from './compare/sql/table';

export class core {
  static prepareMigrationConfig(config: Config): MigrationConfig {
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
        tableColumns: this.extractColumnsDefinitionFromSchema(
          migrationHistoryTableSchema
        ),
      },
    };
  }

  static extractColumnsDefinitionFromSchema(schema: any) {
    let fields: { name: string; dataTypeCategory: string }[] = [];
    for (let column in schema.columns) {
      fields.push({
        name: column,
        dataTypeCategory: schema.columns[column].dataTypeCategory,
      });
    }
    return fields;
  }

  static async prepareMigrationsHistoryTable(
    pgClient: Client,
    config: MigrationConfig
  ) {
    migrationHistoryTableSchema.constraints[
      config.migrationHistory.primaryKeyName
    ] = {
      type: 'p',
      definition: 'PRIMARY KEY ("version")',
    };

    migrationHistoryTableSchema.privileges[config.migrationHistory.tableOwner] =
      {
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
      migrationHistoryTableSchema as unknown as TableObject
    );
    await pgClient.query(sqlScript.toString());
  }

  static getPatchFileInfo(filename: string, filepath: string) {
    let indexOfSeparator = filename.indexOf('_');
    let version = filename.substring(0, indexOfSeparator);
    let name = filename.substring(indexOfSeparator + 1).replace('.sql', '');

    if (indexOfSeparator < 0 || !/^\d+$/.test(version))
      throw new Error(
        `The patch file name ${filename} is not compatible with conventioned pattern {version}_{path name}.sql !`
      );

    return new PatchInfo(filename, filepath, version, name);
  }

  static async makePgClient(config: ClientConfig) {
    if (!config.database)
      throw new Error(
        `The client config parameter [database] cannot be empty! `
      );

    const client = new pg.Client({
      user: config.user,
      host: config.host,
      database: config.database,
      password: config.password,
      port: config.port,
      application_name: config.applicationName,
    });

    await client.connect();

    return client;
  }

  static async getServerVersion(client: Client) {
    const queryResult = await client.query<{ current_setting: string | null }>(
      "SELECT current_setting('server_version')"
    );
    const version = queryResult.rows.at(0)?.current_setting;
    if (!version) {
      throw new Error('Failed to retrieve server version');
    }
    return new ServerVersion(version);
  }

  /**
   * Check the server version
   * @returns Return true if connected server has greater or equal version
   */
  static checkServerCompatibility(
    serverVersion: ServerVersion,
    majorVersion: number,
    minorVersion: number
  ) {
    return (
      serverVersion != null &&
      serverVersion.major >= majorVersion &&
      serverVersion.minor >= minorVersion
    );
  }
}
