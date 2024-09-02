import { statement } from '../stmt.js';
import { Policy } from '../../catalog/database-objects.js';

const POLICY_FOR = {
  '*': 'ALL',
  w: 'UPDATE',
  r: 'SELECT',
  a: 'INSERT',
  d: 'DELETE',
} as const;

export function dropPolicy(policy: Policy) {
  return statement({
    sql: `DROP POLICY ${policy.name} ON "${policy.schema}"."${policy.table}";`,
    before: [policy.relid],
    weight: -1,
  });
}

export function createPolicy(policy: Policy) {
  const sql = ['CREATE POLICY ', policy.name];
  sql.push('\n    ');
  sql.push(`ON "${policy.schema}"."${policy.table}"`);
  sql.push('\n    ');
  sql.push(`AS ${policy.permissive ? 'PERMISSIVE' : 'RESTRICTIVE'}`);
  sql.push('\n    ');
  sql.push(`FOR ${POLICY_FOR[policy.for]}`);
  sql.push('\n    ');
  sql.push(`TO ${policy.roles.join(',')}`);
  if (policy.using) {
    sql.push('\n    ');
    sql.push(`USING ${policy.using}`);
  }
  if (policy.withCheck) {
    sql.push('\n    ');
    sql.push(`WITH CHECK ${policy.withCheck}`);
  }
  sql.push(';');
  return statement({
    sql,
    dependencies: [policy.relid, ...policy.dependencies],
    weight: 1,
  });
}
