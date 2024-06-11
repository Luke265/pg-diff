import { ClientBase } from 'pg';
import { core } from '../core';
import { ServerVersion } from '../models/server-version';
import { DataTypeCategory } from '../models/database-objects';

export interface PrivilegeRow {
  schemaname: string;
  tablename: string;
  usename: string;
  select: boolean;
  insert: boolean;
  update: boolean;
  delete: boolean;
  truncate: boolean;
  references: boolean;
  trigger: boolean;
}

export interface ViewPrivilegeRow {
  schemaname: string;
  viewname: string;
  usename: string;
  select: boolean;
  insert: boolean;
  update: boolean;
  delete: boolean;
  truncate: boolean;
  references: boolean;
  trigger: boolean;
}

export interface FunctionPrivilegeRow {
  pronamespace: string;
  proname: string;
  usename: string;
  execute: boolean;
}
export interface SequencePrivilegeRow {
  sequence_schema: string;
  sequence_name: string;
  usename: string;
  select: boolean;
  usage: boolean;
  update: boolean;
  cache_value: string | null;
}

export interface ViewDependencyRow {
  schemaname: string;
  tablename: string;
  columnname: string;
}
export interface MaterializedViewPrivilegeRow {
  schemaname: string;
  matviewname: string;
  usename: string;
  select: boolean;
  insert: boolean;
  update: boolean;
  delete: boolean;
  truncate: boolean;
  references: boolean;
  trigger: boolean;
}

export function getAllSchemas(client: ClientBase) {
  //TODO: Instead of using ::regrole casting, for better performance join with pg_roles
  return client.query<{ nspname: string }>(`SELECT nspname FROM pg_namespace 
      WHERE nspname NOT IN ('pg_catalog','information_schema')
      AND nspname NOT LIKE 'pg_toast%'
      AND nspname NOT LIKE 'pg_temp%'`);
}

export function getSchemas(client: ClientBase, schemas: string[]) {
  //TODO: Instead of using ::regrole casting, for better performance join with pg_roles
  return client.query<{
    id: number;
    nspname: string;
    owner: string;
    comment: string | null;
  }>(`SELECT n.oid AS id, n.nspname, n.nspowner::regrole::name as owner, d.description as comment
      FROM pg_namespace n
      LEFT JOIN pg_description d ON d.objoid = n."oid" AND d.objsubid = 0
      WHERE nspname IN ('${schemas.join("','")}')`);
}
export interface TableInfoRow {
  id: number;
  schemaname: string;
  tablename: string;
  tableowner: string;
  comment: string | null;
}
export function getTables(client: ClientBase, schemas: string[]) {
  return client.query<TableInfoRow>(`SELECT c.oid AS id, t.schemaname, t.tablename, t.tableowner, d.description as comment
      FROM pg_tables t
      INNER JOIN pg_namespace n ON t.schemaname = n.nspname 
              INNER JOIN pg_class c ON t.tablename = c.relname AND c.relnamespace = n."oid" 
      LEFT JOIN pg_description d ON d.objoid = c."oid" AND d.objsubid = 0
              WHERE t.schemaname IN ('${schemas.join("','")}')
              AND c.oid NOT IN (
                  SELECT d.objid 
                  FROM pg_depend d
                  WHERE d.deptype = 'e'
              )`);
}
export function getTableOptions(
  client: ClientBase,
  schemaName: string,
  tableName: string
) {
  return client.query<{ relhasoids: string }>(`SELECT relhasoids 
                  FROM pg_class c
                  INNER JOIN pg_namespace n ON n."oid" = c.relnamespace AND n.nspname = '${schemaName}'
                  WHERE c.relname = '${tableName}'`);
}
export interface TableColumnRow {
  id: string;
  attname: string;
  attnotnull: boolean;
  typname: string;
  typeid: number;
  typcategory: DataTypeCategory;
  adsrc: string | null;
  attidentity: string;
  adbin: string | null;
  attgenerated: string;
  precision: number | null;
  scale: number | null;
  comment: string | null;
}
export function getTableColumns(
  client: ClientBase,
  schemaName: string,
  tableName: string,
  serverVersion: ServerVersion
) {
  return client.query<TableColumnRow>(
    `SELECT 
        CONCAT(attrelid, '-', attname) AS id,
        a.attname, 
        a.attnotnull, 
        t.typname, 
        t.oid as typeid, 
        t.typcategory, 
        pg_get_expr(ad.adbin ,ad.adrelid ) as adsrc, 
        ad.adbin,
                  ${
                    core.checkServerCompatibility(serverVersion, 10, 0)
                      ? 'a.attidentity'
                      : 'NULL as attidentity'
                  },
                  ${
                    core.checkServerCompatibility(serverVersion, 12, 0)
                      ? 'a.attgenerated'
                      : 'NULL as attgenerated'
                  },
                  CASE 
                      WHEN t.typname = 'numeric' AND a.atttypmod > 0 THEN (a.atttypmod-4) >> 16
                      WHEN (t.typname = 'bpchar' or t.typname = 'varchar') AND a.atttypmod > 0 THEN a.atttypmod-4
                      ELSE null
                  END AS precision,
                  CASE
                      WHEN t.typname = 'numeric' AND a.atttypmod > 0 THEN (a.atttypmod-4) & 65535
                      ELSE null
                  END AS scale,
                  d.description AS comment
                  FROM pg_attribute a
                  INNER JOIN pg_type t ON t.oid = a.atttypid
                  LEFT JOIN pg_attrdef ad on ad.adrelid = a.attrelid AND a.attnum = ad.adnum
                  INNER JOIN pg_namespace n ON n.nspname = '${schemaName}'
                  INNER JOIN pg_class c ON c.relname = '${tableName}' AND c.relnamespace = n."oid"
                  LEFT JOIN pg_description d ON d.objoid = c."oid" AND d.objsubid = a.attnum
                  WHERE attrelid = c."oid" AND attnum > 0 AND attisdropped = false
                  ORDER BY a.attnum ASC`
  );
}
export interface ContraintRow {
  id: number;
  relid: number;
  conname: string;
  contype: string;
  foreign_schema: string;
  foreign_table: string;
  definition: string;
  comment: string | null;
}
export function getTableConstraints(
  client: ClientBase,
  schemaName: string,
  tableName: string
) {
  return client.query<ContraintRow>(`SELECT c.oid AS id, c.confrelid AS relid, c.conname, c.contype, f_sch.nspname AS foreign_schema, f_tbl.relname AS foreign_table, 
                  pg_get_constraintdef(c.oid) as definition, d.description AS comment
                  FROM pg_constraint c
                  INNER JOIN pg_namespace n ON n.nspname = '${schemaName}'
                  INNER JOIN pg_class cl ON cl.relname ='${tableName}' AND cl.relnamespace = n.oid
                  LEFT JOIN pg_class f_tbl ON f_tbl.oid = c.confrelid
                  LEFT JOIN pg_namespace f_sch ON f_sch.oid = f_tbl.relnamespace
                  LEFT JOIN pg_description d ON d.objoid = c."oid" AND d.objsubid = 0
                  WHERE c.conrelid = cl.oid`);
}
export interface IndexRow {
  id: string;
  relid: number;
  indexname: string;
  indexdef: string;
  comment: string | null;
}
export function getTableIndexes(
  client: ClientBase,
  schemaName: string,
  tableName: string
) {
  return client.query<IndexRow>(`SELECT CONCAT(i.indexrelid, '-', i.indrelid) AS id, i.indexrelid AS relid, idx.relname as indexname, pg_get_indexdef(idx.oid) AS indexdef, d.description AS comment
                  FROM pg_index i
                  INNER JOIN pg_class tbl ON tbl.oid = i.indrelid
                  INNER JOIN pg_namespace tbln ON tbl.relnamespace = tbln.oid
                  INNER JOIN pg_class idx ON idx.oid = i.indexrelid
                  LEFT JOIN pg_description d ON d.objoid = idx."oid" AND d.objsubid = 0
                  WHERE tbln.nspname = '${schemaName}' AND tbl.relname='${tableName}' AND i.indisprimary = false AND i.indisunique = FALSE`);
}
export function getTablePrivileges(
  client: ClientBase,
  schemaName: string,
  tableName: string
) {
  return client.query<PrivilegeRow>(`SELECT t.schemaname, t.tablename, u.usename, 
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${tableName}"', 'SELECT') as select,
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${tableName}"', 'INSERT') as insert,
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${tableName}"', 'UPDATE') as update,
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${tableName}"', 'DELETE') as delete, 
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${tableName}"', 'TRUNCATE') as truncate,
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${tableName}"', 'REFERENCES') as references,
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${tableName}"', 'TRIGGER') as trigger
                  FROM pg_tables t, pg_user u 
                  WHERE t.schemaname = '${schemaName}' and t.tablename='${tableName}'`);
}
export interface ViewRow {
  id: number;
  schemaname: string;
  viewname: string;
  viewowner: string;
  definition: string;
  comment: string | null;
}
export function getViews(client: ClientBase, schemas: string[]) {
  return client.query<ViewRow>(`SELECT c.oid AS id, v.schemaname, v.viewname, v.viewowner, v.definition, d.description AS comment 
                  FROM pg_views v
                  INNER JOIN pg_namespace n ON v.schemaname = n.nspname 
                  INNER JOIN pg_class c ON v.viewname = c.relname AND c.relnamespace = n."oid" 
                  LEFT JOIN pg_description d ON d.objoid = c."oid" AND d.objsubid = 0
                  WHERE v.schemaname IN ('${schemas.join("','")}')
                  AND c.oid NOT IN (
                      SELECT d.objid 
                      FROM pg_depend d
                      WHERE d.deptype = 'e'
                  )`);
}
export function getViewPrivileges(
  client: ClientBase,
  schemaName: string,
  viewName: string
) {
  return client.query<ViewPrivilegeRow>(`SELECT v.schemaname, v.viewname, u.usename, 
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'SELECT') as select,
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'INSERT') as insert,
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'UPDATE') as update,
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'DELETE') as delete, 
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'TRUNCATE') as truncate,
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'REFERENCES') as references,
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'TRIGGER') as trigger
                  FROM pg_views v, pg_user u 
                  WHERE v.schemaname = '${schemaName}' and v.viewname='${viewName}'`);
}
export interface MaterializedViewRow {
  id: number;
  schemaname: string;
  matviewname: string;
  matviewowner: string;
  definition: string;
  comment: string | null;
}
export function getMaterializedViews(client: ClientBase, schemas: string[]) {
  return client.query<MaterializedViewRow>(`SELECT c.oid AS id, m.schemaname, m.matviewname, m.matviewowner, m.definition, d.description AS comment
                  FROM pg_matviews m
                  INNER JOIN pg_namespace n ON m.schemaname = n.nspname 
                  INNER JOIN pg_class c ON m.matviewname = c.relname AND c.relnamespace = n."oid" 
                  LEFT JOIN pg_description d ON d.objoid = c."oid" AND d.objsubid = 0
                  WHERE schemaname IN ('${schemas.join("','")}')`);
}
export function getMaterializedViewPrivileges(
  client: ClientBase,
  schemaName: string,
  viewName: string
) {
  return client.query<MaterializedViewPrivilegeRow>(`SELECT v.schemaname, v.matviewname, u.usename, 
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'SELECT') as select,
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'INSERT') as insert,
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'UPDATE') as update,
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'DELETE') as delete, 
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'TRUNCATE') as truncate,
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'REFERENCES') as references,
                  HAS_TABLE_PRIVILEGE(u.usename,'"${schemaName}"."${viewName}"', 'TRIGGER') as trigger
                  FROM pg_matviews v, pg_user u 
                  WHERE v.schemaname = '${schemaName}' and v.matviewname='${viewName}'`);
}
export function getViewDependencies(
  client: ClientBase,
  schemaName: string,
  viewName: string
) {
  return client.query<ViewDependencyRow>(`SELECT                 
                  n.nspname AS schemaname,
                  c.relname AS tablename,
                  a.attname AS columnname
                  FROM pg_rewrite AS r
                  INNER JOIN pg_depend AS d ON r.oid=d.objid
                  INNER JOIN pg_attribute a ON a.attnum = d.refobjsubid AND a.attrelid = d.refobjid AND a.attisdropped = false
                  INNER JOIN pg_class c ON c.oid = d.refobjid
                  INNER JOIN pg_namespace n ON n.oid = c.relnamespace
                  INNER JOIN pg_namespace vn ON vn.nspname = '${schemaName}'
                  INNER JOIN pg_class vc ON vc.relname = '${viewName}' AND vc.relnamespace = vn."oid" 
                  WHERE r.ev_class = vc.oid AND d.refobjid <> vc.oid`);
}
export interface FunctionRow {
  id: number;
  prorettype: number;
  proname: string;
  nspname: string;
  definition: string;
  owner: string;
  argtypes: string;
  comment: string;
  prokind: 'f' | 'p';
}
export function getFunctions(
  client: ClientBase,
  schemas: string[],
  serverVersion: ServerVersion
) {
  //TODO: Instead of using ::regrole casting, for better performance join with pg_roles
  return client.query<FunctionRow>(`SELECT p.oid AS id, 
  p.prorettype,
  p.proname, 
  n.nspname,
  pg_get_functiondef(p.oid) as definition,
  p.proowner::regrole::name as owner, 
  oidvectortypes(proargtypes) as argtypes, 
  d.description AS comment,
  p.prokind
                  FROM pg_proc p
                  INNER JOIN pg_namespace n ON n.oid = p.pronamespace
                  LEFT JOIN pg_description d ON d.objoid = p."oid" AND d.objsubid = 0
                  WHERE n.nspname IN ('${schemas.join(
                    "','"
                  )}') AND p.probin IS NULL 
                  ${
                    core.checkServerCompatibility(serverVersion, 11, 0)
                      ? "AND p.prokind IN ('f','p')"
                      : 'AND p.proisagg = false AND p.proiswindow = false'
                  } 
                  AND p."oid" NOT IN (
                      SELECT d.objid 
                      FROM pg_depend d
                      WHERE d.deptype = 'e'
                  )`);
}
export interface PolicyRow {
  id: number;
  polname: string;
  polrelid: number;
  polroles: number[];
  polpermissive: boolean;
  policy_qual: string | null;
  policy_with_check: string | null;
  comment: string | null;
  schema: string;
  role_names: string[];
  polcmd: '*' | 'w' | 'r' | 'a' | 'd';
  table: string;
}
export function getTablePolicies(
  client: ClientBase,
  schema: string,
  table: string
) {
  //TODO: Instead of using ::regrole casting, for better performance join with pg_roles
  return client.query<PolicyRow>(`SELECT
  p.oid AS id,
  p.polname, 
  p.polrelid,
  p.polroles, 
  ARRAY(
    SELECT r.rolname
    FROM
        pg_roles r
    WHERE
        r.oid = ANY (p.polroles)
    )::TEXT[] AS role_names,
  p.polcmd,
  p.polpermissive,
  d.description AS comment, 
  pg_get_expr(polqual, polrelid) AS policy_qual,
  pg_get_expr(polwithcheck, polrelid) AS policy_with_check,
  n.nspname AS schema,
  t.relname AS table
                  FROM pg_policy p
                  INNER JOIN pg_class t ON t.oid = p.polrelid
                  INNER JOIN pg_class c ON c."oid" = p.polrelid
                  INNER JOIN pg_namespace n ON n.oid = c.relnamespace 
                  LEFT JOIN pg_description d ON d.objoid = p."oid" AND d.objsubid = 0
                  WHERE n.nspname = '${schema}' AND t.relname = '${table}'`);
}
export interface AggregateRow {
  id: number;
  proname: string;
  nspname: string;
  owner: string;
  argtypes: string;
  prorettype: number;
  definition: string;
  comment: string | null;
}
export function getAggregates(
  client: ClientBase,
  schemas: string[],
  serverVersion: ServerVersion
) {
  //TODO: Instead of using ::regrole casting, for better performance join with pg_roles
  return client.query<AggregateRow>(`SELECT p.oid AS id, p.prorettype, p.proname, n.nspname, p.proowner::regrole::name as owner, oidvectortypes(p.proargtypes) as argtypes,
                  format('%s', array_to_string(
                      ARRAY[
                          format(E'\\tSFUNC = %s', a.aggtransfn::text)
                          , format(E'\\tSTYPE = %s', format_type(a.aggtranstype, NULL))	 
                          , format(E'\\tSSPACE = %s',a.aggtransspace)
                          , CASE a.aggfinalfn WHEN '-'::regproc THEN NULL ELSE format(E'\\tFINALFUNC = %s',a.aggfinalfn::text) END	     
                          , CASE WHEN a.aggfinalfn != '-'::regproc AND a.aggfinalextra = true THEN format(E'\\tFINALFUNC_EXTRA') ELSE NULL END
                          ${
                            core.checkServerCompatibility(serverVersion, 11, 0)
                              ? `, CASE WHEN a.aggfinalfn != '-'::regproc THEN format(E'\\tFINALFUNC_MODIFY = %s', 
                              CASE 
                                   WHEN a.aggfinalmodify = 'r' THEN 'READ_ONLY'
                                   WHEN a.aggfinalmodify = 's' THEN 'SHAREABLE'
                                   WHEN a.aggfinalmodify = 'w' THEN 'READ_WRITE'
                              END
                          ) ELSE NULL END`
                              : ''
                          }
                          , CASE WHEN a.agginitval IS NULL THEN NULL ELSE format(E'\\tINITCOND = %s', a.agginitval) END
                          , format(E'\\tPARALLEL = %s', 
                              CASE 
                                  WHEN p.proparallel = 'u' THEN 'UNSAFE'
                                  WHEN p.proparallel = 's' THEN 'SAFE'
                                  WHEN p.proparallel = 'r' THEN 'RESTRICTED'
                              END
                          ) 	     
                          , CASE a.aggcombinefn WHEN '-'::regproc THEN NULL ELSE format(E'\\tCOMBINEFUNC = %s',a.aggcombinefn::text) END
                          , CASE a.aggserialfn WHEN '-'::regproc THEN NULL ELSE format(E'\\tSERIALFUNC = %s',a.aggserialfn::text) END
                          , CASE a.aggdeserialfn WHEN '-'::regproc THEN NULL ELSE format(E'\\tDESERIALFUNC = %s',a.aggdeserialfn::text) END
                          , CASE a.aggmtransfn WHEN '-'::regproc THEN NULL ELSE format(E'\\tMSFUNC = %s',a.aggmtransfn::text) END
                          , case a.aggmtranstype WHEN '-'::regtype THEN NULL ELSE format(E'\\tMSTYPE = %s', format_type(a.aggmtranstype, NULL)) END
                          , case WHEN a.aggmfinalfn != '-'::regproc THEN format(E'\\tMSSPACE = %s',a.aggmtransspace) ELSE NULL END
                          , CASE a.aggminvtransfn WHEN '-'::regproc THEN NULL ELSE format(E'\\tMINVFUNC = %s',a.aggminvtransfn::text) END
                          , CASE a.aggmfinalfn WHEN '-'::regproc THEN NULL ELSE format(E'\\tMFINALFUNC = %s',a.aggmfinalfn::text) END
                          , CASE WHEN a.aggmfinalfn != '-'::regproc and a.aggmfinalextra = true THEN format(E'\\tMFINALFUNC_EXTRA') ELSE NULL END
                          ${
                            core.checkServerCompatibility(serverVersion, 11, 0)
                              ? `, CASE WHEN a.aggmfinalfn != '-'::regproc THEN format(E'\\tMFINALFUNC_MODIFY  = %s', 
                              CASE 
                                  WHEN a.aggmfinalmodify = 'r' THEN 'READ_ONLY'
                                  WHEN a.aggmfinalmodify = 's' THEN 'SHAREABLE'
                                  WHEN a.aggmfinalmodify = 'w' THEN 'READ_WRITE'
                              END
                           ) ELSE NULL END`
                              : ''
                          }
                          , CASE WHEN a.aggminitval IS NULL THEN NULL ELSE format(E'\\tMINITCOND = %s', a.aggminitval) END
                          , CASE a.aggsortop WHEN 0 THEN NULL ELSE format(E'\\tSORTOP = %s', o.oprname) END		 
                      ]
                      , E',\\n'
                      )
                  ) as definition,
                  d.description AS comment
                  FROM pg_proc p
                  INNER JOIN pg_namespace n ON n.oid = p.pronamespace
                  INNER JOIN pg_aggregate a on p.oid = a.aggfnoid 
                  LEFT JOIN pg_operator o ON o.oid = a.aggsortop
                  LEFT JOIN pg_description d ON d.objoid = p."oid" AND d.objsubid = 0
                  WHERE n.nspname IN ('${schemas.join("','")}')
                  AND a.aggkind = 'n'
                  ${
                    core.checkServerCompatibility(serverVersion, 11, 0)
                      ? " AND p.prokind = 'a' "
                      : ' AND p.proisagg = true AND p.proiswindow = false '
                  } 
                  AND p."oid" NOT IN (
                      SELECT d.objid 
                      FROM pg_depend d
                      WHERE d.deptype = 'e'
                  )`);
}
export function getFunctionPrivileges(
  client: ClientBase,
  schemaName: string,
  functionName: string,
  argTypes: string
) {
  return client.query<FunctionPrivilegeRow>(`SELECT n.nspname as pronamespace, p.proname, u.usename, 
                  HAS_FUNCTION_PRIVILEGE(u.usename,'"${schemaName}"."${functionName}"(${argTypes})','EXECUTE') as execute  
                  FROM pg_proc p, pg_user u 
                  INNER JOIN pg_namespace n ON n.nspname = '${schemaName}' 				
                  WHERE p.proname='${functionName}' AND p.pronamespace = n.oid`);
}
export interface SequenceRow {
  id: number;
  seq_nspname: string;
  seq_name: string;
  owner: string;
  ownedby_table: string | null;
  ownedby_column: string | null;
  start_value: string;
  minimum_value: string;
  maximum_value: string;
  increment: string;
  cycle_option: boolean;
  cache_size: string;
  comment: string | null;
}

export function getSequences(
  client: ClientBase,
  schemas: string[],
  serverVersion: ServerVersion
) {
  return client.query<SequenceRow>(`SELECT s.oid AS id, s.seq_nspname, s.seq_name, s.owner, s.ownedby_table, s.ownedby_column, p.start_value, p.minimum_value, p.maximum_value, p.increment, p.cycle_option, 
                  ${
                    core.checkServerCompatibility(serverVersion, 10, 0)
                      ? 'p.cache_size'
                      : '1 as cache_size'
                  },
                  s.comment 
                  FROM (
                      SELECT   
                          c.oid, ns.nspname AS seq_nspname, c.relname AS seq_name, r.rolname as owner, sc.relname AS ownedby_table, a.attname AS ownedby_column, ds.description AS comment
                      FROM pg_class c
                      INNER JOIN pg_namespace ns ON ns.oid = c.relnamespace 
                      INNER JOIN pg_roles r ON r.oid = c.relowner 
                      LEFT JOIN pg_depend d ON d.objid = c.oid AND d.refobjsubid > 0 AND d.deptype ='a'
                      LEFT JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid	
                      LEFT JOIN pg_class sc ON sc."oid" = d.refobjid
                      LEFT JOIN pg_description ds ON ds.objoid = c."oid" AND ds.objsubid = 0
                      WHERE c.relkind = 'S' AND ns.nspname IN ('${schemas.join(
                        "','"
                      )}') 
                      ${
                        core.checkServerCompatibility(serverVersion, 10, 0)
                          ? "AND (a.attidentity IS NULL OR a.attidentity = '')"
                          : ''
                      }
                  ) s, LATERAL pg_sequence_parameters(s.oid) p`);
}
export function getSequencePrivileges(
  client: ClientBase,
  schemaName: string,
  sequenceName: string,
  serverVersion: ServerVersion
) {
  return client.query<SequencePrivilegeRow>(`SELECT s.sequence_schema, s.sequence_name, u.usename, ${
    core.checkServerCompatibility(serverVersion, 10, 0)
      ? 'NULL AS cache_value,'
      : 'p.cache_value,'
  }
                  HAS_SEQUENCE_PRIVILEGE(u.usename,'"${schemaName}"."${sequenceName}"', 'SELECT') as select,
                  HAS_SEQUENCE_PRIVILEGE(u.usename,'"${schemaName}"."${sequenceName}"', 'USAGE') as usage,
                  HAS_SEQUENCE_PRIVILEGE(u.usename,'"${schemaName}"."${sequenceName}"', 'UPDATE') as update
                  FROM information_schema.sequences s, pg_user u ${
                    core.checkServerCompatibility(serverVersion, 10, 0)
                      ? ''
                      : ', "' + schemaName + '"."' + sequenceName + '" p'
                  }
                  WHERE s.sequence_schema = '${schemaName}' and s.sequence_name='${sequenceName}'`);
}
