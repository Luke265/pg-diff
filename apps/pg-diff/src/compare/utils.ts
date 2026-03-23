import { Sql } from './stmt.js';

export type SqlResult = Sql | null | SqlResult[];

export function commentIsEqual(
  a: string | null | undefined,
  b: string | null | undefined,
) {
  return a === b || (!a && !b);
}

export interface SequenceChanges {
  select?: boolean;
  usage?: boolean;
  update?: boolean;
}

export interface ColumnChanges extends PrivilegeChanges {
  datatype?: string;
  dataTypeID?: number;
  dataTypeCategory?: any;
  precision?: any;
  scale?: any;
  nullable?: any;
  default?: any;
  defaultRefs?: (number | string)[];
  identity?: any;
  isNewIdentity?: any;
}

export type PrivilegeChange = {
  grant: boolean | string[];
  revoke: boolean | string[];
};
export interface PrivilegeChanges {
  select?: PrivilegeChange;
  insert?: PrivilegeChange;
  update?: PrivilegeChange;
  delete?: PrivilegeChange;
  truncate?: boolean;
  references?: boolean;
  trigger?: boolean;
  execute?: boolean;
  usage?: boolean;
}

export function buildGrants(
  list: [string, PrivilegeChange | boolean | undefined][],
): ['GRANT' | 'REVOKE', string][] {
  const result: ['GRANT' | 'REVOKE', string][] = [];
  let revokes: string[] = [];
  let grants: string[] = [];
  for (const [type, defined] of list) {
    if (defined === undefined) {
      continue;
    }
    if (defined) {
      if (typeof defined === 'object') {
        if (Array.isArray(defined.grant)) {
          if (defined.grant.length > 0) {
            grants.push(`${type} (${defined.grant.map((k) => k).join(', ')})`);
          }
        } else if (defined.grant) {
          grants.push(type);
        }
        if (Array.isArray(defined.revoke)) {
          if (defined.revoke.length > 0) {
            revokes.push(
              `${type} (${defined.revoke.map((k) => k).join(', ')})`,
            );
          }
        } else if (defined.revoke) {
          revokes.push(type);
        }
      } else {
        grants.push(type);
      }
    } else {
      revokes.push(type);
    }
  }
  if (grants.length === list.length) {
    grants = ['ALL'];
  }
  if (revokes.length === list.length) {
    revokes = ['ALL'];
  }
  if (revokes.length > 0) {
    result.push(['REVOKE', revokes.join(', ')]);
  }
  if (grants.length > 0) {
    result.push(['GRANT', grants.join(', ')]);
  }
  return result;
}

export function replaceLastCharacter(
  input: string,
  replacement: string,
): string {
  if (input.length === 0) {
    return input;
  }
  return input.slice(0, -1) + replacement;
}
