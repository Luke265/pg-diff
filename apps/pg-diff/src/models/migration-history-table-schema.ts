export interface Column {
  nullable: boolean;
  datatype: string;
  dataTypeCategory: string;
  default: string | null;
  precision: number | null;
  scale: number | null;
}
export interface Columns {
  version: Column;
  name: Column;
  status: Column;
  last_message: Column;
  script: Column;
  applied_on: Column;
}
export interface MigrationHistoryTableSchema {
  columns: Columns;
  constraints: Record<string, unknown>;
  indexes: Record<string, unknown>;
  privileges: Record<string, unknown>;
  owner: string | null;
}
export const migrationHistoryTableSchema: MigrationHistoryTableSchema = {
  columns: {
    version: {
      nullable: false,
      datatype: 'varchar',
      dataTypeCategory: 'S',
      default: '',
      precision: 17,
      scale: null,
    },
    name: {
      nullable: false,
      datatype: 'varchar',
      dataTypeCategory: 'S',
      default: null,
      precision: null,
      scale: null,
    },
    status: {
      nullable: false,
      datatype: 'varchar',
      dataTypeCategory: 'S',
      default: "''",
      precision: 5,
      scale: null,
    },
    last_message: {
      nullable: true,
      datatype: 'varchar',
      dataTypeCategory: 'S',
      default: null,
      precision: null,
      scale: null,
    },
    script: {
      nullable: false,
      datatype: 'varchar',
      dataTypeCategory: 'S',
      default: "''",
      precision: null,
      scale: null,
    },
    applied_on: {
      nullable: true,
      datatype: 'timestamp',
      dataTypeCategory: 'D',
      default: null,
      precision: null,
      scale: null,
    },
  },
  constraints: {},
  indexes: {},
  privileges: {},
  owner: null,
};
