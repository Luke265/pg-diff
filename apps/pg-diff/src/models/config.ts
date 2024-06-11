import { ClientConfig } from './client-config';
import { TableDefinition } from './table-definition';

export interface Config {
  targetClient: ClientConfig;
  sourceClient: ClientConfig;
  compareOptions: {
    outputDirectory: string;
    author: string;
    getAuthorFromGit: boolean;
    mapRole: (input: string) => string;
    schemaCompare: {
      namespaces: string[];
      sequence: boolean;
      dropMissingTable: boolean;
      dropMissingView: boolean;
      dropMissingFunction: boolean;
      dropMissingAggregate: boolean;
      roles: string[];
    };
    dataCompare: {
      enable: boolean;
      tables: TableDefinition[];
    };
  };
  migrationOptions: {
    patchesDirectory: string;
    historyTableName: string;
    historyTableSchema: string;
  };
}
