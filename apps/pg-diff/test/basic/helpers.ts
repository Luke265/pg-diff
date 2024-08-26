import { sourceDb, targetDb } from './setup-db.js';
import fs from 'fs';
import path from 'path';
import EventEmitter from 'events';
import { Config } from 'apps/pg-diff/src/config.js';
import {
  collectDatabaseObject,
  compareDatabaseObjects,
} from 'apps/pg-diff/src/compare/index.js';
import { sortByDependencies } from 'apps/pg-diff/src/utils.js';

export async function sync(dir: string) {
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
  const result = sorted.join('\n').toString();
  const tmp = path.join('tmp', 'test', dir.replace(__dirname, ''));
  fs.mkdirSync(tmp, { recursive: true });
  fs.writeFileSync(path.join(tmp, 'result.sql'), result);
  try {
    await targetDb().query(result);
  } catch (e: any) {
    throw new Error(e.message + '\n' + result);
  }
  return result;
}

export async function compare(dir: string) {
  const patch = fs.readFileSync(path.join(dir, 'patch.sql')).toString();
  const resultPatch = await sync(dir);
  expect(patch).toBe(resultPatch);
}
