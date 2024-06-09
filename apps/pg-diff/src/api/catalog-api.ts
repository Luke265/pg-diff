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
} from './introspection';
import {
  AggregateDefinition,
  FunctionDefinition,
  MaterializedViewDefinition,
  Sequence,
  TableObject,
  ViewDefinition,
} from '../models/database-objects';

export class CatalogApi {
  static async retrieveAllSchemas(client: ClientBase) {
    const namespaces = await getAllSchemas(client);
    return namespaces.rows.map((namespace) => namespace.nspname);
  }

  static async retrieveSchemas(
    client: ClientBase,
    schemas: string[]
  ): Promise<Record<string, { owner: string; comment: string | null }>> {
    const result = {};
    const namespaces = await getSchemas(client, schemas);
    for (const row of namespaces.rows) {
      result[row.nspname] = {
        owner: row.owner,
        comment: row.comment,
      };
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
        const def = (result[fullTableName] = {
          columns: {},
          constraints: {},
          options: {},
          indexes: {},
          privileges: {},
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
          let dataType = column.typname;
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

          def.columns[columnName] = {
            nullable: !column.attnotnull,
            datatype: dataType,
            dataTypeID: column.typeid,
            dataTypeCategory: column.typcategory,
            default: defaultValue,
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
            definition: index.indexdef,
            comment: index.comment,
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

        //TODO: Missing discovering of PARTITION
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

    const procedures = await getFunctions(
      client,
      config.compareOptions.schemaCompare.namespaces,
      await getServerVersion(client)
    );

    await Promise.all(
      procedures.rows.map(async (procedure) => {
        const fullProcedureName = `"${procedure.nspname}"."${procedure.proname}"`;
        const map = (result[fullProcedureName] ??= {});
        const def = (map[procedure.argtypes] = {
          definition: procedure.definition,
          owner: procedure.owner,
          argTypes: procedure.argtypes,
          privileges: {},
          comment: procedure.comment,
          type: procedure.prokind,
        });
        const privileges = await getFunctionPrivileges(
          client,
          procedure.nspname,
          procedure.proname,
          procedure.argtypes
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

    return result;
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
          definition: aggregate.definition,
          owner: aggregate.owner,
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
          owner: sequence.owner,
          startValue: sequence.start_value,
          minValue: sequence.minimum_value,
          maxValue: sequence.maximum_value,
          increment: sequence.increment,
          cacheSize: sequence.cache_size,
          isCycle: sequence.cycle_option,
          name: sequence.seq_name,
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
