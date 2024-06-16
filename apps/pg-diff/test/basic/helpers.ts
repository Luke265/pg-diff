import { sourceDb, targetDb } from './setup-db';
import fs from 'fs';
import path from 'path';
import EventEmitter from 'events';
import { Config } from 'apps/pg-diff/src/config';
import {
  collectDatabaseObject,
  compareDatabaseObjects,
} from 'apps/pg-diff/src/compare';
import { sortByDependencies } from 'apps/pg-diff/src/utils';

export async function compare(dir: string) {
  await sourceDb().query(
    fs.readFileSync(path.join(dir, 'source.sql')).toString(),
  );
  await targetDb().query(
    fs.readFileSync(path.join(dir, 'target.sql')).toString(),
  );
  const config: Config = {
    targetClient: {} as any,
    sourceClient: {} as any,
    migrationOptions: {} as any,
    compareOptions: {
      outputDirectory: '',
      author: 'test',
      getAuthorFromGit: false,
      mapRole: (role) => {
        return role;
      },
      schemaCompare: {
        namespaces: ['public'],
        sequence: false,
        dropMissingTable: true,
        dropMissingView: true,
        dropMissingFunction: true,
        dropMissingAggregate: true,
        roles: [],
      },
      dataCompare: {
        enable: false,
        tables: [],
      },
    },
  };
  const dbSourceObjects = await collectDatabaseObject(sourceDb(), config);
  const dbTargetObjects = await collectDatabaseObject(targetDb(), config);

  const { ddl } = compareDatabaseObjects(
    dbSourceObjects,
    dbTargetObjects,
    config,
    new EventEmitter(),
  );

  const sorted = sortByDependencies(ddl);
  const result = sorted.join('\n');
  const tmp = path.join('tmp', 'test', dir.replace(__dirname, ''));
  fs.mkdirSync(tmp, { recursive: true });
  fs.writeFileSync(path.join(tmp, 'result.sql'), result);
  await targetDb().query(result.toString());
}
