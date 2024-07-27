import { TableObject } from '../../catalog/database-objects.js';
import { Config } from '../../config.js';
import { Sql } from '../stmt.js';
import { compareTablePolicies } from './table.js';

export function comparePolicies(
  config: Config,
  source: Record<string, TableObject>,
  target: Record<string, TableObject>,
) {
  const lines: Sql[][] = [];
  for (const name in source) {
    const sourceObj = source[name];
    const targetObj = target[name];
    const policies = compareTablePolicies(
      config,
      sourceObj,
      sourceObj.policies,
      targetObj?.policies ?? {},
    );
    lines.push(policies);
  }
  return lines.flat();
}
