import { dependency, stmt } from '../stmt';
import { Policy } from '../../catalog/database-objects';

const POLICY_FOR = {
  '*': 'ALL',
  w: 'UPDATE',
  r: 'SELECT',
  a: 'INSERT',
  d: 'DELETE',
} as const;

export function dropPolicy(schema: string, table: string, policy: string) {
  const s = stmt`DROP POLICY ${policy} ON "${schema}"."${table}";`;
  s.weight = -1;
  return s;
}

export function createPolicy(schema: string, table: string, policy: Policy) {
  const s = stmt`CREATE POLICY ${policy.name} 
    ON ${dependency(
      `"${schema}"."${table}"`,
      policy.relid,
      policy.dependencies,
    )}
    AS ${policy.permissive ? 'PERMISSIVE' : 'RESTRICTIVE'}
    FOR ${POLICY_FOR[policy.for]}
    TO ${policy.roles.join(',')}
    ${policy.using ? `USING ${policy.using}` : ''}
    ${policy.withCheck ? `WITH CHECK ${policy.withCheck}` : ''};`;
  s.weight = 1;
  return s;
}
