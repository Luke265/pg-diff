import { TableObject } from '../../catalog/database-objects.js';
import { Config } from '../../config.js';
import { Sql } from '../stmt.js';
import { compareTableTriggers } from './table.js';

export function compareTriggers(
  config: Config,
  source: Record<string, TableObject>,
  target: Record<string, TableObject>,
) {
  const lines: Sql[][] = [];
  for (const name in source) {
    const sourceObj = source[name];
    const targetObj = target[name];
    const policies = compareTableTriggers(
      config,
      sourceObj,
      sourceObj.triggers,
      targetObj?.triggers ?? {},
    );
    lines.push(policies);
  }
  return lines.flat();
}
