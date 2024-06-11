import { SqlRef } from '../stmt';

export function commentIsEqual(
  a: string | null | undefined,
  b: string | null | undefined
) {
  return a === b || (!a && !b);
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
  truncate?: any;
  references?: any;
  trigger?: any;
  select?: any;
  insert?: any;
  update?: any;
  delete?: any;
  execute?: any;
  usage?: any;
}
