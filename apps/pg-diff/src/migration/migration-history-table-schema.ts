export interface Column {
  name: string;
  nullable: boolean;
  datatype: string;
  dataTypeCategory: string;
  default: string | null;
  precision: number | null;
  scale: number | null;
}
export interface MigrationHistoryTableSchema {
  columns: Record<string, Column>;
  constraints: Record<string, unknown>;
  indexes: Record<string, unknown>;
  privileges: Record<string, unknown>;
  owner: string | null;
}
export const migrationHistoryTableSchema: MigrationHistoryTableSchema = {
  columns: {
    version: {
      name: 'version',
      nullable: false,
      datatype: 'varchar',
      dataTypeCategory: 'S',
      default: '',
      precision: 17,
      scale: null,
    },
    name: {
      name: 'name',
      nullable: false,
      datatype: 'varchar',
      dataTypeCategory: 'S',
      default: null,
      precision: null,
      scale: null,
    },
    status: {
      name: 'status',
      nullable: false,
      datatype: 'varchar',
      dataTypeCategory: 'S',
      default: "''",
      precision: 5,
      scale: null,
    },
    last_message: {
      name: 'last_message',
      nullable: true,
      datatype: 'varchar',
      dataTypeCategory: 'S',
      default: null,
      precision: null,
      scale: null,
    },
    script: {
      name: 'script',
      nullable: false,
      datatype: 'varchar',
      dataTypeCategory: 'S',
      default: "''",
      precision: null,
      scale: null,
    },
    applied_on: {
      name: 'applied_on',
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
