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

export interface PrivilegeChanges {
  truncate?: boolean;
  references?: boolean;
  trigger?: boolean;
  select?: boolean;
  insert?: boolean;
  update?: boolean;
  delete?: boolean;
  execute?: boolean;
  usage?: boolean;
}

export function buildGrants(
  list: [string, boolean | undefined][],
): ['GRANT' | 'REVOKE', string][] {
  const result: ['GRANT' | 'REVOKE', string][] = [];
  let revokes: string[] = [];
  let grants: string[] = [];
  for (const [type, defined] of list) {
    if (defined === undefined) {
      continue;
    }
    if (defined) {
      grants.push(type);
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
  if (grants.length > 0) {
    result.push(['GRANT', grants.join(', ')]);
  }
  if (revokes.length > 0) {
    result.push(['REVOKE', revokes.join(', ')]);
  }
  return result;
}
