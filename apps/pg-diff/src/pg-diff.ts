import { CompareApi } from './api/compare-api';
import { MigrationApi } from './api/migration-api';
import { Config } from './models/config';
import EventEmitter from 'events';

export class PgDiff {
  events = new EventEmitter();
  constructor(private config: Config) {}

  /**
   *
   * @param force True to force execution even for patches encountered an error
   * @param toSourceClient True to execute patches on source client
   * @returns Return a list of PatchInfo.
   */
  async migrate(force: boolean, toSourceClient: boolean) {
    force = force || false;
    toSourceClient = toSourceClient || false;
    return await MigrationApi.migrate(
      this.config,
      force,
      toSourceClient,
      this.events
    );
  }

  /**
   *
   * @returns Return null if no patch has been created.
   */
  async compare(scriptName: string) {
    if (!scriptName) throw new Error('The script name must be specified!');
    return await CompareApi.compare(this.config, scriptName, this.events);
  }

  async save(patchFileName: string) {
    if (!patchFileName)
      throw new Error('The patch file name must be specified!');
    return await MigrationApi.savePatch(this.config, patchFileName);
  }
}
