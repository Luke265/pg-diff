export interface MigrationConfig {
  patchesFolder: string;
  migrationHistory: {
    tableName: string;
    tableSchema: string;
    fullTableName: string;
    primaryKeyName: string;
    tableOwner: string;
    tableColumns: any[];
  };
}
