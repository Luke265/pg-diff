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
  await db.query(`DROP ROLE IF EXISTS dev_r_pg_diff_visitor`);
  await db.query(`DROP ROLE IF EXISTS dev_r_pg_diff_admin`);
  await db.query(`DROP ROLE IF EXISTS dev_r_pg_diff_owner`);
  await db.query(`CREATE ROLE dev_r_pg_diff_owner`);
  await db.query(`CREATE ROLE dev_r_pg_diff_admin`);
  await db.query(`CREATE ROLE dev_r_pg_diff_visitor`);
  await db.query(`
  GRANT CONNECT ON DATABASE ${source} TO dev_r_pg_diff_owner;
  GRANT CONNECT ON DATABASE ${target} TO dev_r_pg_diff_owner;
GRANT ALL ON DATABASE  ${source} TO dev_r_pg_diff_owner;
GRANT ALL ON DATABASE  ${target} TO dev_r_pg_diff_owner;`);
  const sourceUrl = new URL(process.env['ROOT_DATABASE_URL']!!);
  sourceUrl.pathname = source;
  const targetUrl = new URL(process.env['ROOT_DATABASE_URL']!!);
  targetUrl.pathname = target;
  _sourceDb = new Client(sourceUrl.toString());
  await _sourceDb.connect();
  _targetDb = new Client(targetUrl.toString());
  await _targetDb.connect();
  await _sourceDb.query(`ALTER SCHEMA public OWNER TO dev_r_pg_diff_owner;
  -- Some extensions require superuser privileges, so we create them before migration time.
  CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
  CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;
  CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;
  CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;
  CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;`);
  await _targetDb.query(`ALTER SCHEMA public OWNER TO dev_r_pg_diff_owner;
  -- Some extensions require superuser privileges, so we create them before migration time.
  CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
  CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;
  CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;
  CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;
  CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;`);
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
