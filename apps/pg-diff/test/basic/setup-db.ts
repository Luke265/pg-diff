import { Client } from 'pg';

export const source = 'dev_pg_diff_test_source';
export const target = 'dev_pg_diff_test_target';

let _sourceDb: Client;
let _targetDb: Client;
let db: Client;

beforeAll(async () => {
  db = new Client(process.env['ROOT_DATABASE_URL']);
  await db.connect();
  await db.query(`DROP DATABASE IF EXISTS ${source}`);
  await db.query(`DROP DATABASE IF EXISTS ${target}`);
  await db.query(`CREATE DATABASE ${source}`);
  await db.query(`CREATE DATABASE ${target}`);
  const sourceUrl = new URL(process.env['ROOT_DATABASE_URL']!!);
  sourceUrl.pathname = source;
  const targetUrl = new URL(process.env['ROOT_DATABASE_URL']!!);
  targetUrl.pathname = target;
  _sourceDb = new Client(sourceUrl.toString());
  await _sourceDb.connect();
  _targetDb = new Client(targetUrl.toString());
  await _targetDb.connect();
});

afterAll(async () => {
  await db.end();
  await _sourceDb.end();
  await _targetDb.end();
});

export function sourceDb() {
  return _sourceDb;
}

export function targetDb() {
  return _targetDb;
}
