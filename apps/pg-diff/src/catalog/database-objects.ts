import { SqlRef } from '../compare/stmt';

export interface FunctionDefinition {
  id: number;
  argTypes: string;
  fullName: string;
  returnTypeId: number;
  argtypeids: number[];
  languageName: string;
  comment: string | null;
  fReferenceIds: number[];
  definition: string;
  owner: string;
  privileges: Record<string, FunctionPrivileges>;
  type: 'f' | 'p';
}
export interface Privileges {
  select?: boolean;
  insert?: boolean;
  update?: boolean;
  delete?: boolean;
  truncate?: boolean;
  references?: boolean;
  trigger?: boolean;
}
export interface SequencePrivileges {
  usage?: boolean;
  select?: boolean;
  update?: boolean;
}
export interface FunctionPrivileges {
  execute: boolean;
}
export interface ConstraintDefinition {
  id: number;
  name: string;
  relid: number;
  type: string;
  definition: string;
  comment: string | null;
  foreign_schema: string | null;
  foreign_table: string | null;
}
export interface IndexDefinition {
  id: string;
  definition: string;
  comment: string | null;
  schema: string;
  name: string;
}
export interface ViewDependency {
  schemaName: string;
  tableName: string;
  columnName: string;
}
export interface ViewDefinition {
  id: number;
  definition: string;
  owner: string;
  privileges: Record<string, Privileges>;
  dependencies: ViewDependency[];
  comment: string | null;
}
export interface Schema {
  id: number;
  owner: string;
  comment: string | null;
}
export type DataTypeCategory =
  | 'D' // DATE TIME
  | 'V' // BIT
  | 'S' // STRING
  | 'A' // ARRAY
  | 'R' // RANGE
  | 'B' // BOOL
  | 'E' // ENUM
  | 'G' // GEOMETRIC
  | 'I' // NETWORK ADDRESS
  | 'N' // NUMERIC
  | 'T' // TIMESPAN
  | 'U' // USER TYPE
  | 'X' // UNKNOWN
  | 'P' // PSEUDO TYPE
  | 'C'; // COMPOSITE TYPE
export type ColumnType =
  | 'int4'
  | 'text'
  | 'bool'
  | 'citext'
  | 'timestamptz'
  | 'jsonb'
  | 'numeric'
  | 'float8'
  | 'tstzrange'
  | 'json'
  | 'uuid'
  | 'timestamp'
  | 'modifier_operation'
  | 'interval'
  | '_int4'
  | 'int8';
export interface Column {
  id: string;
  nullable: boolean;
  fullName: string;
  name: string;
  datatype: ColumnType | string;
  dataTypeID: number;
  dataTypeCategory: DataTypeCategory;
  default: string | null;
  defaultRefs: number[];
  precision: number | null;
  scale: number | null;
  identity: string | null;
  comment: string | null;
  generatedColumn: string | null;
}
export interface TableOptions {
  withOids?: string;
}
export interface Policy {
  id: number;
  relid: number;
  permissive: boolean;
  for: '*' | 'w' | 'r' | 'a' | 'd';
  name: string;
  comment: string | null;
  using: string | null;
  dependencies: (string | number)[];
  withCheck: string | null;
  roles: string[];
}
export interface TableObject {
  id: number;
  schema: string;
  name: string;
  fullName: string;
  columns: Record<string, Column>;
  constraints: Record<string, ConstraintDefinition>;
  options: TableOptions;
  indexes: Record<string, IndexDefinition>;
  policies: Record<string, Policy>;
  privileges: Record<string, Privileges>;
  owner: string;
  comment: string | null;
}
export interface MaterializedViewDefinition extends ViewDefinition {
  indexes: Record<string, IndexDefinition>;
}
export interface AggregateDefinition {
  id: number;
  definition: string;
  fullName: string;
  returnTypeId: number;
  argtypeids: number[];
  languageName: string;
  type: 'f';
  owner: string;
  fReferences: string[];
  fReferenceIds: number[];
  argTypes: string;
  privileges: Record<string, FunctionPrivileges>;
  comment: string | null;
}
export interface Sequence {
  id: number;
  owner: string;
  startValue: string;
  minValue: string;
  maxValue: string;
  increment: string;
  cacheSize: string;
  isCycle: boolean;
  name: string;
  schema: string;
  ownedBy: null | string;
  privileges: Record<string, SequencePrivileges>;
  comment: null | string;
}
export interface Type {
  id: number;
  schema: string;
  name: string;
  owner: string;
  comment: string | null;
  enum?: string[];
  fullName: string;
  columns: Record<string, Column>;
}
export interface Domain {
  id: number;
  type: {
    id: number;
    fullName: string;
  };
  constraintName: string;
  schema: string;
  name: string;
  owner: string;
  comment: string | null;
  fullName: string;
  check: string;
}
export interface DatabaseObjects {
  schemas: Record<string, Schema>;
  tables: Record<string, TableObject>;
  views: Record<string, ViewDefinition>;
  materializedViews: Record<string, MaterializedViewDefinition>;
  functionMap: Record<string, Record<string, FunctionDefinition>>;
  aggregates: Record<string, Record<string, AggregateDefinition>>;
  sequences: Record<string, Sequence>;
  types: Record<string, Type>;
  domains: Record<string, Domain>;
}
