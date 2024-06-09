export interface FunctionDefinition {
  argTypes: string;
  comment: string | null;
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
}
export interface FunctionPrivileges {
  execute: string;
}
export interface ConstraintDefinition {
  type: string;
  definition: string;
  comment: string | null;
  foreign_schema: string | null;
  foreign_table: string | null;
}
export interface IndexDefinition {
  definition: string;
  comment: string | null;
  schema: string;
}
export interface ViewDependency {
  schemaName: string;
  tableName: string;
  columnName: string;
}
export interface ViewDefinition {
  definition: string;
  owner: string;
  privileges: Record<string, Privileges>;
  dependencies: ViewDependency[];
  comment: string;
}
export interface Schema {
  owner: string;
  startValue: string;
  minValue: string;
  maxValue: string;
  increment: string;
  cacheSize: string;
  isCycle: boolean;
  name: string;
  ownedBy: string | null;
  privileges: Privileges;
  comment: string | null;
}
type DataTypeCategory =
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
  nullable: boolean;
  datatype: ColumnType | string;
  dataTypeID: number;
  dataTypeCategory: DataTypeCategory;
  default: string | null;
  precision: number | null;
  scale: number | null;
  identity: string | null;
  comment: string | null;
  generatedColumn: string | null;
}
export interface TableOptions {
  withOids?: number;
}
export interface TableObject {
  columns: Record<string, Column>;
  constraints: Record<string, ConstraintDefinition>;
  options: TableOptions;
  indexes: Record<string, IndexDefinition>;
  privileges: Record<string, Privileges>;
  owner: string;
  comment: string | null;
}
export interface MaterializedViewDefinition extends ViewDefinition {
  indexes: Record<string, IndexDefinition>;
}
export interface Aggregate {}
export interface Sequence {
  owner: string;
  startValue: string;
  minValue: string;
  maxValue: string;
  increment: string;
  cacheSize: string;
  isCycle: boolean;
  name: string;
  ownedBy: null | string;
  privileges: Record<string, SequencePrivileges>;
  comment: null | string;
}
export interface DatabaseObjects {
  schemas: Record<string, Schema>;
  tables: Record<string, TableObject>;
  views: Record<string, ViewDefinition>;
  materializedViews: Record<string, MaterializedViewDefinition>;
  functions: { [args: string]: FunctionDefinition };
  aggregates: Record<string, Aggregate>;
  sequences: Record<string, Sequence>;
}
