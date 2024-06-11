import { ClientBase } from 'pg';
import { core } from '../core';
import { Config } from '../models/config';
import { getServerVersion } from '../utils';
import {
  getAllSchemas,
  getSchemas,
  getTables,
  getTableColumns,
  getTableConstraints,
  getTableOptions,
  getTableIndexes,
  getTablePrivileges,
  getViews,
  getViewPrivileges,
  getViewDependencies,
  getMaterializedViews,
  getMaterializedViewPrivileges,
  getFunctions,
  getFunctionPrivileges,
  getAggregates,
  getSequences,
  getSequencePrivileges,
  getTablePolicies,
} from './introspection';
import {
  AggregateDefinition,
  FunctionDefinition,
  MaterializedViewDefinition,
  Sequence,
  TableObject,
  ViewDefinition,
} from '../models/database-objects';
import { SqlRef } from '../stmt';

const COLUMN_TYPE_MAP = {
  int4: 'INTEGER',
  bool: 'BOOLEAN',
  timestamptz: 'TIMESTAMP WITH TIME ZONE',
};
export class CatalogApi {
  static async retrieveAllSchemas(client: ClientBase) {
    const namespaces = await getAllSchemas(client);
    return namespaces.rows.map((namespace) => namespace.nspname);
  }

  static async retrieveSchemas(
    client: ClientBase,
    schemas: string[]
  ): Promise<
    Record<string, { id: number; owner: string; comment: string | null }>
  > {
    const result = {};
    const namespaces = await getSchemas(client, schemas);
    for (const row of namespaces.rows) {
      result[row.nspname] = row;
    }
    return result;
  }

  static async retrieveTables(client: ClientBase, config: Config) {
    const result: Record<string, TableObject> = {};
    const tableNamesPriority: string[] = [];
    const serverVersion = await getServerVersion(client);
    const tables = await getTables(
      client,
      config.compareOptions.schemaCompare.namespaces
    );
    await Promise.all(
      tables.rows.map(async (table) => {
        const fullTableName = `"${table.schemaname}"."${table.tablename}"`;
        const def: TableObject = (result[fullTableName] = {
          id: table.id,
          name: table.tablename,
          schema: table.schemaname,
          fullName: `"${table.tablename}"."${table.schemaname}"`,
          columns: {},
          constraints: {},
          options: {},
          indexes: {},
          privileges: {},
          policies: {},
          owner: table.tableowner,
          comment: table.comment,
        });

        const columns = await getTableColumns(
          client,
          table.schemaname,
          table.tablename,
          await getServerVersion(client)
        );
        columns.rows.forEach((column) => {
          let columnName = `"${column.attname}"`;
          let columnIdentity: 'ALWAYS' | 'BY DEFAULT' | null = null;
          let defaultValue = column.adsrc;
          let dataType = COLUMN_TYPE_MAP[column.typname] ?? column.typname;
          let generatedColumn: 'STORED' | null = null;

          switch (column.attidentity) {
            case 'a':
              columnIdentity = 'ALWAYS';
              defaultValue = '';
              break;
            case 'd':
              columnIdentity = 'BY DEFAULT';
              defaultValue = '';
              break;
          }

          switch (column.attgenerated) {
            case 's':
              generatedColumn = 'STORED';
              break;
          }
          const functIdsMatch = column.adbin?.matchAll(
            /FUNCEXPR :funcid (\d+)/g
          );
          const defaultFunctionIds = Array.from(functIdsMatch ?? []).map((m) =>
            parseInt(m[1])
          );
          def.columns[columnName] = {
            id: column.id,
            nullable: !column.attnotnull,
            datatype: dataType,
            dataTypeID: column.typeid,
            dataTypeCategory: column.typcategory,
            default: defaultValue,
            defaultFunctionIds,
            defaultRef: new SqlRef(defaultValue, defaultFunctionIds),
            functionReferences: [],
            precision: column.precision,
            scale: column.scale,
            identity: columnIdentity,
            comment: column.comment,
            generatedColumn: generatedColumn,
          };
        });

        const constraints = await getTableConstraints(
          client,
          table.schemaname,
          table.tablename
        );
        constraints.rows.forEach((constraint) => {
          let constraintName = `"${constraint.conname}"`;
          def.constraints[constraintName] = {
            id: constraint.id,
            relid: constraint.relid,
            type: constraint.contype,
            definition: constraint.definition,
            comment: constraint.comment,
            foreign_schema: constraint.foreign_schema,
            foreign_table: constraint.foreign_table,
          };

          //REFERENCED tables have to be created before
          if (constraint.contype == 'f') {
            const tableNameToReorder = `"${constraint.foreign_schema}"."${constraint.foreign_table}"`;
            const indexOfTableNameToReorder =
              tableNamesPriority.indexOf(tableNameToReorder);
            const indexOfCurrentTableName =
              tableNamesPriority.indexOf(fullTableName);

            if (indexOfCurrentTableName >= 0) {
              if (indexOfTableNameToReorder < 0) {
                tableNamesPriority.splice(
                  indexOfCurrentTableName,
                  0,
                  tableNameToReorder
                );
              } else if (indexOfCurrentTableName < indexOfTableNameToReorder) {
                tableNamesPriority.splice(
                  indexOfCurrentTableName,
                  0,
                  tableNamesPriority.splice(indexOfTableNameToReorder, 1)[0]
                );
              }
            } else if (indexOfTableNameToReorder < 0) {
              tableNamesPriority.push(
                `"${constraint.foreign_schema}"."${constraint.foreign_table}"`
              );
            }
          }
        });

        //@mso -> relhadoids has been deprecated from PG v12.0
        if (!core.checkServerCompatibility(serverVersion, 12, 0)) {
          const options = await getTableOptions(
            client,
            table.schemaname,
            table.tablename
          );
          options.rows.forEach((option) => {
            def.options = {
              withOids: option.relhasoids,
            };
          });
        }

        const indexes = await getTableIndexes(
          client,
          table.schemaname,
          table.tablename
        );
        indexes.rows.forEach((index) => {
          def.indexes[index.indexname] = {
            id: index.id,
            definition: index.indexdef,
            comment: index.comment,
            name: index.indexname,
            schema: table.schemaname,
          };
        });

        const privileges = await getTablePrivileges(
          client,
          table.schemaname,
          table.tablename
        );
        privileges.rows
          .filter(
            (row) =>
              config.compareOptions.schemaCompare.roles.length <= 0 ||
              config.compareOptions.schemaCompare.roles.includes(row.usename)
          )
          .forEach((privilege) => {
            def.privileges[privilege.usename] = {
              select: privilege.select,
              insert: privilege.insert,
              update: privilege.update,
              delete: privilege.delete,
              truncate: privilege.truncate,
              references: privilege.references,
              trigger: privilege.trigger,
            };
          });

        const policies = await getTablePolicies(
          client,
          table.schemaname,
          table.tablename
        );
        policies.rows.forEach((row) => {
          let using = row.policy_qual;
          let withCheck = row.policy_with_check;
          if (using && !using.startsWith('(')) {
            using = `(${using})`;
          }
          if (withCheck && !withCheck.startsWith('(')) {
            withCheck = `(${withCheck})`;
          }
          def.policies[row.polname] = {
            id: row.id,
            relid: row.polrelid,
            using,
            withCheck,
            permissive: row.polpermissive,
            comment: row.comment,
            name: row.polname,
            roles: row.role_names,
            for: row.polcmd,
          };
        });
        //TODO: Missing discovering of PARTITIONv
        //TODO: Missing discovering of TRIGGER
        //TODO: Missing discovering of GRANTS for COLUMNS
        //TODO: Missing discovering of WITH GRANT OPTION, that is used to indicate if user\role can add GRANTS to other users
      })
    );

    //Re-order tables based on priority
    const reorderedResult: Record<string, TableObject> = {};
    for (const tableName of tableNamesPriority) {
      reorderedResult[tableName] = result[tableName];
      delete result[tableName];
    }

    return { ...reorderedResult, ...result };
  }

  static async retrieveViews(client: ClientBase, config: Config) {
    const result: Record<string, ViewDefinition> = {};

    //Get views
    const views = await getViews(
      client,
      config.compareOptions.schemaCompare.namespaces
    );

    await Promise.all(
      views.rows.map(async (view) => {
        const fullViewName = `"${view.schemaname}"."${view.viewname}"`;
        const def = (result[fullViewName] = {
          id: view.id,
          definition: view.definition,
          owner: view.viewowner,
          privileges: {},
          dependencies: [],
          comment: view.comment,
        });

        const privileges = await getViewPrivileges(
          client,
          view.schemaname,
          view.viewname
        );
        privileges.rows
          .filter(
            (row) =>
              config.compareOptions.schemaCompare.roles.length <= 0 ||
              config.compareOptions.schemaCompare.roles.includes(row.usename)
          )
          .forEach((privilege) => {
            def.privileges[privilege.usename] = {
              select: privilege.select,
              insert: privilege.insert,
              update: privilege.update,
              delete: privilege.delete,
              truncate: privilege.truncate,
              references: privilege.references,
              trigger: privilege.trigger,
            };
          });

        const dependencies = await getViewDependencies(
          client,
          view.schemaname,
          view.viewname
        );
        dependencies.rows.forEach((dependency) => {
          def.dependencies.push({
            schemaName: dependency.schemaname,
            tableName: dependency.tablename,
            columnName: dependency.columnname,
          });
        });
      })
    );

    //TODO: Missing discovering of TRIGGER
    //TODO: Missing discovering of GRANTS for COLUMNS
    //TODO: Should we get TEMPORARY VIEW?

    return result;
  }

  static async retrieveMaterializedViews(client: ClientBase, config: Config) {
    const result: Record<string, MaterializedViewDefinition> = {};
    const views = await getMaterializedViews(
      client,
      config.compareOptions.schemaCompare.namespaces
    );
    await Promise.all(
      views.rows.map(async (view) => {
        const fullViewName = `"${view.schemaname}"."${view.matviewname}"`;
        const def = (result[fullViewName] = {
          id: view.id,
          definition: view.definition,
          indexes: {},
          owner: view.matviewowner,
          privileges: {},
          dependencies: [],
          comment: view.comment,
        });

        const indexes = await getTableIndexes(
          client,
          view.schemaname,
          view.matviewname
        );
        indexes.rows.forEach((index) => {
          def.indexes[index.indexname] = {
            definition: index.indexdef,
            comment: index.comment,
            schema: view.schemaname,
          };
        });

        let privileges = await getMaterializedViewPrivileges(
          client,
          view.schemaname,
          view.matviewname
        );
        privileges.rows
          .filter(
            (row) =>
              config.compareOptions.schemaCompare.roles.length <= 0 ||
              config.compareOptions.schemaCompare.roles.includes(row.usename)
          )
          .forEach((privilege) => {
            def.privileges[privilege.usename] = {
              select: privilege.select,
              insert: privilege.insert,
              update: privilege.update,
              delete: privilege.delete,
              truncate: privilege.truncate,
              references: privilege.references,
              trigger: privilege.trigger,
            };
          });

        const dependencies = await getViewDependencies(
          client,
          view.schemaname,
          view.matviewname
        );
        dependencies.rows.forEach((dependency) => {
          def.dependencies.push({
            schemaName: dependency.schemaname,
            tableName: dependency.tablename,
            columnName: dependency.columnname,
          });
        });
      })
    );

    //TODO: Missing discovering of GRANTS for COLUMNS

    return result;
  }

  static async retrieveFunctions(client: ClientBase, config: Config) {
    const result: Record<string, Record<string, FunctionDefinition>> = {};
    const list: FunctionDefinition[] = [];
    const procedures = await getFunctions(
      client,
      config.compareOptions.schemaCompare.namespaces,
      await getServerVersion(client)
    );

    await Promise.all(
      procedures.rows.map(async (row) => {
        const fullProcedureName = `"${row.nspname}"."${row.proname}"`;
        const map = (result[fullProcedureName] ??= {});
        const def = (map[row.argtypes] = {
          id: row.id,
          definition: row.definition,
          owner: row.owner,
          prorettype: row.prorettype,
          fullName: fullProcedureName,
          argTypes: row.argtypes,
          privileges: {},
          comment: row.comment,
          fReferences: extractFunctionCalls(row.definition),
          fReferenceIds: [],
          type: row.prokind,
        });

        list.push(def);
        const privileges = await getFunctionPrivileges(
          client,
          row.nspname,
          row.proname,
          row.argtypes
        );

        privileges.rows
          .filter(
            (row) =>
              config.compareOptions.schemaCompare.roles.length <= 0 ||
              config.compareOptions.schemaCompare.roles.includes(row.usename)
          )
          .forEach((privilege) => {
            def.privileges[privilege.usename] = {
              execute: privilege.execute,
            };
          });
      })
    );
    for (const f of list) {
      if (f.fReferences.length === 0) {
        continue;
      }
      /*f.fReferenceIds = f.fReferences
        .filter((ref) => result[ref])
        .map((ref) => Object.values(result[ref]).map((f) => f.id))
        .flat();*/
    }

    return { map: result, list };
  }

  static async retrieveAggregates(client: ClientBase, config: Config) {
    const result: Record<string, Record<string, AggregateDefinition>> = {};
    const aggregates = await getAggregates(
      client,
      config.compareOptions.schemaCompare.namespaces,
      await getServerVersion(client)
    );
    await Promise.all(
      aggregates.rows.map(async (aggregate) => {
        const fullAggregateName = `"${aggregate.nspname}"."${aggregate.proname}"`;
        const map = (result[fullAggregateName] ??= {});
        const def = (map[aggregate.argtypes] = {
          id: aggregate.id,
          definition: aggregate.definition,
          prorettype: aggregate.prorettype,
          owner: aggregate.owner,
          fullName: fullAggregateName,
          fReferences: [],
          fReferenceIds: [],
          type: 'f',
          argTypes: aggregate.argtypes,
          privileges: {},
          comment: aggregate.comment,
        });

        const privileges = await getFunctionPrivileges(
          client,
          aggregate.nspname,
          aggregate.proname,
          aggregate.argtypes
        );

        privileges.rows
          .filter(
            (row) =>
              config.compareOptions.schemaCompare.roles.length <= 0 ||
              config.compareOptions.schemaCompare.roles.includes(row.usename)
          )
          .forEach((privilege: any) => {
            def.privileges[privilege.usename] = {
              execute: privilege.execute,
            };
          });
      })
    );

    return result;
  }

  static async retrieveSequences(client: ClientBase, config: Config) {
    const result: Record<string, Sequence> = {};

    const sequences = await getSequences(
      client,
      config.compareOptions.schemaCompare.namespaces,
      await getServerVersion(client)
    );

    await Promise.all(
      sequences.rows.map(async (sequence) => {
        const fullSequenceName = `"${sequence.seq_nspname}"."${sequence.seq_name}"`;
        const def = (result[fullSequenceName] = {
          id: sequence.id,
          owner: sequence.owner,
          startValue: sequence.start_value,
          minValue: sequence.minimum_value,
          maxValue: sequence.maximum_value,
          increment: sequence.increment,
          cacheSize: sequence.cache_size,
          isCycle: sequence.cycle_option,
          name: sequence.seq_name,
          schema: sequence.seq_nspname,
          ownedBy:
            sequence.ownedby_table && sequence.ownedby_column
              ? `${sequence.ownedby_table}.${sequence.ownedby_column}`
              : null,
          privileges: {},
          comment: sequence.comment,
        });

        const privileges = await getSequencePrivileges(
          client,
          sequence.seq_nspname,
          sequence.seq_name,
          await getServerVersion(client)
        );

        privileges.rows.forEach((privilege) => {
          if (privilege.cache_value != null)
            def.cacheSize = privilege.cache_value;

          if (
            config.compareOptions.schemaCompare.roles.length <= 0 ||
            config.compareOptions.schemaCompare.roles.includes(
              privilege.usename
            )
          ) {
            def.privileges[privilege.usename] = {
              select: privilege.select,
              usage: privilege.usage,
              update: privilege.update,
            };
          }
        });
      })
    );
    return result;
  }
}

function extractFunctionCalls(sqlFunctionBody: string): string[] {
  sqlFunctionBody = sqlFunctionBody.substring(
    sqlFunctionBody.indexOf('RETURNS')
  );
  // Define a regex to match function calls
  const functionCallRegex = /(\w+(\.\w+)+)\s*\(/g;
  let functionCalls = [];
  let match;

  // Use the regex to find all function calls in the SQL function body
  while ((match = functionCallRegex.exec(sqlFunctionBody)) !== null) {
    // match[1] contains the function name (e.g., "app_hidden.on_order_paid")
    functionCalls.push(
      match[1]
        .split('.')
        .map((v) => `"${v}"`)
        .join('.')
    );
  }

  return functionCalls;
}
