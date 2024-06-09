import sql from '../sql-script-generator';
import { core } from '../core';
import patchStatus from '../enums/patch-status';
import path from 'path';
import fs from 'fs';
import { PatchInfo } from '../models/patch-info';
import { Client } from 'pg';
import { MigrationConfig } from '../models/migration-config';
import { Config } from '../models/config';
import { EventEmitter, once } from 'events';
import readline from 'readline';
import { getServerVersion } from '../utils';

export class MigrationApi {
  static async migrate(
    config: Config,
    force: boolean,
    toSourceClient: boolean,
    eventEmitter: EventEmitter
  ) {
    eventEmitter.emit('migrate', 'Migration started', 0);

    let migrationConfig = core.prepareMigrationConfig(config);

    eventEmitter.emit('migrate', 'Connecting to database ...', 20);
    let clientConfig = toSourceClient
      ? config.sourceClient
      : config.targetClient;
    let pgClient = await core.makePgClient(clientConfig);
    eventEmitter.emit(
      'migrate',
      `Connected to PostgreSQL ${
        (await getServerVersion(pgClient)).version
      } on [${clientConfig.host}:${clientConfig.port}/${
        clientConfig.database
      }] `,
      25
    );

    eventEmitter.emit('migrate', 'Preparing migration history table ...', 30);
    await core.prepareMigrationsHistoryTable(pgClient, migrationConfig);
    eventEmitter.emit(
      'migrate',
      'Migration history table has been prepared',
      35
    );

    eventEmitter.emit('migrate', 'Collecting patches ...', 40);
    let patchesFiles = fs
      .readdirSync(migrationConfig.patchesFolder)
      .sort()
      .filter((file) => {
        return file.match(/.*\.(sql)/gi);
      });
    eventEmitter.emit('migrate', 'Patches collected', 45);

    if (patchesFiles.length <= 0) {
      eventEmitter.emit('migrate', 'The patch folder is empty', 100);
      return [];
    }

    let result: PatchInfo[] = [];

    eventEmitter.emit('migrate', 'Executing patches ...', 50);
    const progressStep = 50 / patchesFiles.length / 3;
    let progressValue = 50;
    for (let index in patchesFiles) {
      progressValue += progressStep;
      eventEmitter.emit('migrate', 'Reading patch status ...', progressValue);

      let patchFileInfo = core.getPatchFileInfo(
        patchesFiles[index],
        migrationConfig.patchesFolder
      );
      let patchFileStatus = await this.checkPatchStatus(
        pgClient,
        patchFileInfo,
        migrationConfig
      );

      switch (patchFileStatus) {
        case patchStatus.IN_PROGRESS:
          {
            if (!force)
              throw new Error(
                `The patch version={${patchFileInfo.version}} and name={${patchFileInfo.name}} is still in progress!`
              );

            progressValue += progressStep;
            eventEmitter.emit(
              'migrate',
              `Executing patch ${patchFileInfo.filename} ...`,
              progressValue
            );

            await this.applyPatch(pgClient, patchFileInfo, migrationConfig);
            result.push(patchFileInfo);

            progressValue += progressStep;
            eventEmitter.emit(
              'migrate',
              `Patch ${patchFileInfo.filename} has been executed`,
              progressValue
            );
          }
          break;
        case patchStatus.ERROR:
          {
            if (!force)
              throw new Error(
                `The patch version={${patchFileInfo.version}} and name={${patchFileInfo.name}} previously encountered an error! Try to "force" migration with argument -mr.`
              );

            progressValue += progressStep;
            eventEmitter.emit(
              'migrate',
              `Executing patch ${patchFileInfo.filename} ...`,
              progressValue
            );

            await this.applyPatch(pgClient, patchFileInfo, migrationConfig);
            result.push(patchFileInfo);

            progressValue += progressStep;
            eventEmitter.emit(
              'migrate',
              `Patch ${patchFileInfo.filename} has been executed`,
              progressValue
            );
          }
          break;
        case patchStatus.DONE:
          progressValue += progressStep * 2;
          eventEmitter.emit(
            'migrate',
            `Skip patch ${patchFileInfo.filename} because already executed`,
            progressValue
          );
          break;
        case patchStatus.TO_APPLY:
          progressValue += progressStep;
          eventEmitter.emit(
            'migrate',
            `Executing patch ${patchFileInfo.filename} ...`,
            progressValue
          );

          await this.applyPatch(pgClient, patchFileInfo, migrationConfig);
          result.push(patchFileInfo);

          progressValue += progressStep;
          eventEmitter.emit(
            'migrate',
            `Patch ${patchFileInfo.filename} has been executed`,
            progressValue
          );
          break;
        default:
          throw new Error(
            `The status "${patchFileStatus}" not recognized! Impossible to apply patch version={${patchFileInfo.version}} and name={${patchFileInfo.name}}.`
          );
      }
    }

    eventEmitter.emit('migrate', 'Migration completed', 100);

    return result;
  }

  static async checkPatchStatus(
    pgClient: Client,
    patchFileInfo: PatchInfo,
    config: MigrationConfig
  ) {
    let sql = `SELECT "status" FROM ${config.migrationHistory.fullTableName} WHERE "version" = '${patchFileInfo.version}' AND "name" = '${patchFileInfo.name}'`;
    let response = await pgClient.query(sql);

    if (response.rows.length > 1)
      throw new Error(
        `Too many patches found on migrations history table "${config.migrationHistory.fullTableName}" for patch version=${patchFileInfo.version} and name=${patchFileInfo.name}!`
      );

    if (response.rows.length < 1) return patchStatus.TO_APPLY;
    else return response.rows[0].status;
  }

  static async applyPatch(
    pgClient: Client,
    patchFileInfo: any,
    config: MigrationConfig
  ) {
    await this.addRecordToHistoryTable(pgClient, patchFileInfo, config);
    try {
      let patchScript = await this.readPatch(pgClient, patchFileInfo, config);
      await this.updateRecordToHistoryTable(pgClient, patchScript, config);
    } catch (err: any) {
      let patchScript = patchFileInfo;
      patchScript.status = patchStatus.ERROR;
      patchScript.message = err.toString();
      await this.updateRecordToHistoryTable(pgClient, patchScript, config);
      throw err;
    }
  }

  static async readPatch(
    pgClient: Client,
    patchFileInfo: any,
    config: MigrationConfig
  ) {
    return new Promise(async (resolve, reject) => {
      try {
        const reader = readline.createInterface({
          input: fs.createReadStream(
            path.resolve(patchFileInfo.filepath, patchFileInfo.filename)
          ),
          crlfDelay: Infinity,
        });
        let readingBlock = false;
        let readLines = 0;
        let commandExecuted = 0;
        let patchError: Error | null = null;

        let patchScript = patchFileInfo;
        patchScript.command = '';
        patchScript.message = '';

        reader.on('error', (err) => reject(err));

        reader.on('line', (line: string) => {
          readLines += 1;
          if (readingBlock) {
            if (line.startsWith('--- END')) {
              readingBlock = false;
              reader.pause();
              this.executePatchScript(pgClient, patchScript, config)
                .then(() => {
                  commandExecuted += 1;
                  reader.resume();
                })
                .catch((err) => {
                  commandExecuted += 1;
                  patchError = err;
                  reader.close();
                  reader.resume();
                });
            } else {
              patchScript.command += `${line}\n`;
            }
          }

          if (!readingBlock && line.startsWith('--- BEGIN')) {
            readingBlock = true;
            patchScript.command = '';
            patchScript.message = line;
          }
        });
        reader.on('close', function () {
          if (readLines <= 0)
            patchError = new Error(
              `The patch "${patchFileInfo.name}" version "${patchFileInfo.version}" is empty!`
            );
          else if (commandExecuted <= 0)
            patchError = new Error(
              `The patch "${patchFileInfo.name}" version "${patchFileInfo.version}" is malformed. Missing BEGIN/END comments!`
            );

          if (patchError) {
            reject(patchError);
          } else {
            patchScript.status = patchStatus.DONE;
            patchScript.message = '';
            patchScript.command = '';
            resolve(patchScript);
          }
        });
      } catch (e) {
        reject(e);
      }
    });
  }

  static async savePatch(config: Config, patchFileName: string) {
    let migrationConfig = core.prepareMigrationConfig(config);
    let pgClient = await core.makePgClient(config.sourceClient);

    await core.prepareMigrationsHistoryTable(pgClient, migrationConfig);

    let patchFilePath = path.resolve(
      migrationConfig.patchesFolder,
      patchFileName
    );

    if (!fs.existsSync(patchFilePath))
      throw new Error(`The patch file ${patchFilePath} does not exists!`);

    let patchFileInfo = core.getPatchFileInfo(patchFileName, patchFilePath);
    patchFileInfo.status = patchStatus.DONE;
    await this.addRecordToHistoryTable(
      pgClient,
      patchFileInfo,
      migrationConfig
    );
  }

  static async executePatchScript(
    pgClient: Client,
    patchScript: any,
    config: MigrationConfig
  ) {
    patchScript.status = patchStatus.IN_PROGRESS;
    await this.updateRecordToHistoryTable(pgClient, patchScript, config);
    await pgClient.query(patchScript.command);
  }

  static async updateRecordToHistoryTable(
    pgClient: Client,
    patchScript: any,
    config: MigrationConfig
  ) {
    let changes = {
      status: patchScript.status,
      last_message: patchScript.message,
      applied_on: new Date(),
      script: null,
    };

    if (patchScript.status != patchStatus.ERROR)
      changes.script = patchScript.command;

    let filterConditions = {
      version: patchScript.version,
      name: patchScript.name,
    };

    let command = sql.generateUpdateTableRecordScript(
      config.migrationHistory.fullTableName,
      config.migrationHistory.tableColumns,
      filterConditions,
      changes
    );

    await pgClient.query(command);
  }

  static async addRecordToHistoryTable(
    pgClient: Client,
    patchFileInfo: PatchInfo,
    config: MigrationConfig
  ) {
    let changes = {
      version: patchFileInfo.version,
      name: patchFileInfo.name,
      status: patchFileInfo.status || patchStatus.TO_APPLY,
      last_message: '',
      script: '',
      applied_on: null,
    };

    let options = {
      constraintName: config.migrationHistory.primaryKeyName,
    };

    let command = sql.generateMergeTableRecord(
      config.migrationHistory.fullTableName,
      config.migrationHistory.tableColumns,
      changes,
      options
    );
    await pgClient.query(command);
  }
}
