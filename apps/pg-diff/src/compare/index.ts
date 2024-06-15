import { core } from '../core';
import {
  AggregateDefinition,
  DatabaseObjects,
  FunctionDefinition,
  FunctionPrivileges,
  MaterializedViewDefinition,
  Schema,
  Sequence,
  SequencePrivileges,
  TableObject,
  ViewDefinition,
} from '../catalog/database-objects';
import objectType from '../enums/object-type';

import EventEmitter from 'events';
import { Config } from '../models/config';
import { ClientBase } from 'pg';
import { getServerVersion } from '../utils';
import { Sql } from '../stmt';
import { ColumnChanges } from './utils';
import { compareTypes } from './type';
import { compareDomains } from './domain';
import { compareTablesRecords } from './table-record';
import {
  compareTableIndexes,
  compareTablePolicies,
  compareTablePrivileges,
  compareTables,
} from './table';
import { loadCatalog } from '../catalog';
import { retrieveAllSchemas } from '../catalog/catalog-api';
import {
  generateChangeAggregateScript,
  generateCreateAggregateScript,
  generateDropAggregateScript,
} from './sql/aggregate';
import {
  generateDropMaterializedViewScript,
  generateCreateMaterializedViewScript,
} from './sql/materialized-view';
import { generateChangeCommentScript } from './sql/misc';
import {
  generateDropProcedureScript,
  generateCreateProcedureScript,
  generateChangeProcedureOwnerScript,
  generateChangesProcedureRoleGrantsScript,
  generateProcedureRoleGrantsScript,
} from './sql/procedure';
import { generateCreateSchemaScript } from './sql/schema';
import {
  generateRenameSequenceScript,
  generateCreateSequenceScript,
  generateChangeSequencePropertyScript,
  SequenceProperties,
  generateChangesSequenceRoleGrantsScript,
  generateSequenceRoleGrantsScript,
} from './sql/sequence';
import { generateChangeTableOwnerScript } from './sql/table';
import { generateDropViewScript, generateCreateViewScript } from './sql/view';

export async function compare(config: Config, eventEmitter: EventEmitter) {
  eventEmitter.emit('compare', 'Compare started', 0);
  eventEmitter.emit('compare', 'Connecting to source database ...', 10);
  const pgSourceClient = await core.makePgClient(config.sourceClient);
  eventEmitter.emit(
    'compare',
    `Connected to source PostgreSQL ${
      (await getServerVersion(pgSourceClient)).version
    } on [${config.sourceClient.host}:${config.sourceClient.port}/${
      config.sourceClient.database
    }] `,
    11
  );

  eventEmitter.emit('compare', 'Connecting to target database ...', 20);
  const pgTargetClient = await core.makePgClient(config.targetClient);
  eventEmitter.emit(
    'compare',
    `Connected to target PostgreSQL ${
      (await getServerVersion(pgTargetClient)).version
    } on [${config.targetClient.host}:${config.targetClient.port}/${
      config.targetClient.database
    }] `,
    21
  );

  const dbSourceObjects = await collectDatabaseObject(pgSourceClient, config);
  eventEmitter.emit('compare', 'Collected SOURCE objects', 30);
  const dbTargetObjects = await collectDatabaseObject(pgTargetClient, config);
  eventEmitter.emit('compare', 'Collected TARGET objects', 40);

  const { added, ddl } = compareDatabaseObjects(
    dbSourceObjects,
    dbTargetObjects,
    config,
    eventEmitter
  );

  //The progress step size is 20
  if (config.compareOptions.dataCompare.enable) {
    ddl.push(
      ...(await compareTablesRecords(
        config,
        pgSourceClient,
        pgTargetClient,
        added.columns,
        added.tables,
        dbSourceObjects,
        dbTargetObjects,
        eventEmitter
      ))
    );
    eventEmitter.emit('compare', 'Table records have been compared', 95);
  }
  eventEmitter.emit('compare', 'Compare completed', 100);
  return ddl;
}

export async function collectDatabaseObject(
  client: ClientBase,
  config: Config
) {
  if (typeof config.compareOptions.schemaCompare.namespaces === 'string')
    config.compareOptions.schemaCompare.namespaces = [
      config.compareOptions.schemaCompare.namespaces,
    ];
  else if (
    !config.compareOptions.schemaCompare.namespaces ||
    !Array.isArray(config.compareOptions.schemaCompare.namespaces) ||
    config.compareOptions.schemaCompare.namespaces.length <= 0
  )
    config.compareOptions.schemaCompare.namespaces = await retrieveAllSchemas(
      client
    );
  return loadCatalog(client, {
    schemas: config.compareOptions.schemaCompare.namespaces,
    roles: config.compareOptions.schemaCompare.roles,
  });
}

export function compareDatabaseObjects(
  dbSourceObjects: DatabaseObjects,
  dbTargetObjects: DatabaseObjects,
  config: Config,
  eventEmitter: EventEmitter
) {
  const droppedConstraints: string[] = [];
  const droppedIndexes: string[] = [];
  const droppedViews: string[] = [];
  const addedColumns: Record<string, any> = {};
  const addedTables: any[] = [];
  const sqlPatch: Sql[] = [];

  sqlPatch.push(
    ...compareSchemas(dbSourceObjects.schemas, dbTargetObjects.schemas)
  );
  eventEmitter.emit('compare', 'SCHEMA objects have been compared', 45);

  if (config.compareOptions.schemaCompare.sequence !== false) {
    sqlPatch.push(
      ...compareSequences(
        config,
        dbSourceObjects.sequences,
        dbTargetObjects.sequences
      )
    );
    eventEmitter.emit('compare', 'SEQUENCE objects have been compared', 50);
  }

  sqlPatch.push(
    ...compareTables(
      dbSourceObjects.tables,
      dbTargetObjects,
      droppedConstraints,
      droppedIndexes,
      droppedViews,
      addedColumns,
      addedTables,
      config
    )
  );

  sqlPatch.push(
    ...compareTypes(dbSourceObjects.types, dbTargetObjects.types, config)
  );

  sqlPatch.push(
    ...compareDomains(dbSourceObjects.domains, dbTargetObjects.domains, config)
  );
  eventEmitter.emit('compare', 'TABLE objects have been compared', 55);

  sqlPatch.push(
    ...compareViews(
      dbSourceObjects.views,
      dbTargetObjects.views,
      droppedViews,
      config
    )
  );
  eventEmitter.emit('compare', 'VIEW objects have been compared', 60);

  sqlPatch.push(
    ...compareMaterializedViews(
      dbSourceObjects.materializedViews,
      dbTargetObjects.materializedViews,
      droppedViews,
      droppedIndexes,
      config
    )
  );
  eventEmitter.emit(
    'compare',
    'MATERIALIZED VIEW objects have been compared',
    65
  );

  sqlPatch.push(
    ...compareProcedures(
      dbSourceObjects.functionMap,
      dbTargetObjects.functionMap,
      config
    )
  );
  eventEmitter.emit('compare', 'PROCEDURE objects have been compared', 70);

  sqlPatch.push(
    ...compareAggregates(
      dbSourceObjects.aggregates,
      dbTargetObjects.aggregates,
      config
    )
  );
  eventEmitter.emit('compare', 'AGGREGATE objects have been compared', 75);

  sqlPatch.push(
    ...comparePolicies(config, dbSourceObjects.tables, dbTargetObjects.tables)
  );

  return {
    dropped: {
      constraints: droppedConstraints,
      indexes: droppedIndexes,
      views: droppedViews,
    },
    added: {
      columns: addedColumns,
      tables: addedTables,
    },
    ddl: sqlPatch.filter((v) => !!v),
  };
}

export function compareSchemas(
  sourceSchemas: Record<string, Schema>,
  targetSchemas: Record<string, Schema>
) {
  const lines: Sql[] = [];
  for (const sourceSchema in sourceSchemas) {
    const sourceObj = sourceSchemas[sourceSchema];
    const targetObj = targetSchemas[sourceSchema];

    if (!targetObj) {
      //Schema not exists on target database, then generate script to create schema
      lines.push(generateCreateSchemaScript(sourceSchema, sourceObj.owner));
      lines.push(
        generateChangeCommentScript(
          sourceObj.id,
          objectType.SCHEMA,
          sourceSchema,
          sourceObj.comment
        )
      );
    }

    if (targetObj && sourceObj.comment != targetObj.comment)
      lines.push(
        generateChangeCommentScript(
          sourceObj.id,
          objectType.SCHEMA,
          sourceSchema,
          sourceObj.comment
        )
      );
  }

  return lines;
}

export function comparePolicies(
  config: Config,
  source: Record<string, TableObject>,
  target: Record<string, TableObject>
) {
  const lines: Sql[][] = [];
  for (const name in source) {
    const sourceObj = source[name];
    const targetObj = target[name];
    const policies = compareTablePolicies(
      config,
      sourceObj,
      sourceObj.policies,
      targetObj?.policies ?? {}
    );
    lines.push(policies);
  }
  return lines.flat();
}

export function compareViews(
  sourceViews: Record<string, ViewDefinition>,
  targetViews: Record<string, ViewDefinition>,
  droppedViews: string[],
  config: Config
) {
  const lines: Sql[] = [];

  for (const view in sourceViews) {
    const sourceObj = sourceViews[view];
    const targetObj = targetViews[view];

    if (targetObj) {
      //View exists on both database, then compare view schema
      let sourceViewDefinition = sourceObj.definition.replace(/\r/g, '');
      let targetViewDefinition = targetObj.definition.replace(/\r/g, '');
      if (sourceViewDefinition != targetViewDefinition) {
        if (!droppedViews.includes(view))
          lines.push(generateDropViewScript(view));
        lines.push(generateCreateViewScript(view, sourceObj));
        lines.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.VIEW,
            view,
            sourceObj.comment
          )
        );
      } else {
        if (droppedViews.includes(view))
          //It will recreate a dropped view because changes happens on involved columns
          lines.push(generateCreateViewScript(view, sourceObj));

        lines.push(
          ...compareTablePrivileges(
            view,
            sourceObj.privileges,
            targetObj.privileges,
            config
          )
        );

        if (sourceObj.owner != targetObj.owner)
          lines.push(generateChangeTableOwnerScript(view, sourceObj.owner));

        if (sourceObj.comment != targetObj.comment)
          lines.push(
            generateChangeCommentScript(
              sourceObj.id,
              objectType.VIEW,
              view,
              sourceObj.comment
            )
          );
      }
    } else {
      //View not exists on target database, then generate the script to create view
      lines.push(generateCreateViewScript(view, sourceObj));
      lines.push(
        generateChangeCommentScript(
          sourceObj.id,
          objectType.VIEW,
          view,
          sourceObj.comment
        )
      );
    }
  }

  if (config.compareOptions.schemaCompare.dropMissingView)
    for (let view in targetViews) {
      if (sourceViews[view]) {
        continue;
      }

      lines.push(generateDropViewScript(view));
    }

  return lines;
}

export function compareMaterializedViews(
  sourceMaterializedViews: Record<string, MaterializedViewDefinition>,
  targetMaterializedViews: Record<string, MaterializedViewDefinition>,
  droppedViews: string[],
  droppedIndexes: string[],
  config: Config
) {
  const lines: Sql[] = [];
  for (let view in sourceMaterializedViews) {
    const sourceObj = sourceMaterializedViews[view];
    const targetObj = targetMaterializedViews[view];
    //Get new or changed materialized views
    if (targetObj) {
      //Materialized view exists on both database, then compare materialized view schema
      let sourceViewDefinition = sourceObj.definition.replace(/\r/g, '');
      let targetViewDefinition = targetObj.definition.replace(/\r/g, '');
      if (sourceViewDefinition != targetViewDefinition) {
        if (!droppedViews.includes(view))
          lines.push(generateDropMaterializedViewScript(view));
        lines.push(generateCreateMaterializedViewScript(view, sourceObj));
        lines.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.MATERIALIZED_VIEW,
            view,
            sourceObj.comment
          )
        );
      } else {
        if (droppedViews.includes(view))
          //It will recreate a dropped materialized view because changes happens on involved columns
          lines.push(generateCreateMaterializedViewScript(view, sourceObj));

        lines.push(
          ...compareTableIndexes(
            sourceObj.indexes,
            targetObj.indexes,
            droppedIndexes
          )
        );

        lines.push(
          ...compareTablePrivileges(
            view,
            sourceObj.privileges,
            targetObj.privileges,
            config
          )
        );

        if (sourceObj.owner != targetObj.owner)
          lines.push(generateChangeTableOwnerScript(view, sourceObj.owner));

        if (sourceObj.comment != targetObj.comment)
          lines.push(
            generateChangeCommentScript(
              sourceObj.id,
              objectType.MATERIALIZED_VIEW,
              view,
              sourceObj.comment
            )
          );
      }
    } else {
      //Materialized view not exists on target database, then generate the script to create materialized view
      lines.push(generateCreateMaterializedViewScript(view, sourceObj));
      lines.push(
        generateChangeCommentScript(
          sourceObj.id,
          objectType.MATERIALIZED_VIEW,
          view,
          sourceObj.comment
        )
      );
    }
  }

  if (config.compareOptions.schemaCompare.dropMissingView)
    for (let view in targetMaterializedViews) {
      if (sourceMaterializedViews[view]) {
        continue;
      }
      lines.push(generateDropMaterializedViewScript(view));
    }

  return lines;
}

export function compareProcedures(
  sourceFunctions: Record<string, Record<string, FunctionDefinition>>,
  targetFunctions: Record<string, Record<string, FunctionDefinition>>,
  config: Config
) {
  const lines: Sql[] = [];

  for (let procedure in sourceFunctions) {
    for (const procedureArgs in sourceFunctions[procedure]) {
      const sourceObj = sourceFunctions[procedure][procedureArgs];
      const targetObj =
        targetFunctions[procedure] && targetFunctions[procedure][procedureArgs];
      const procedureType =
        sourceObj.type === 'f' ? objectType.FUNCTION : objectType.PROCEDURE;

      if (targetObj) {
        //Procedure exists on both database, then compare procedure definition
        //TODO: Is correct that if definition is different automatically GRANTS and OWNER will not be updated also?
        //TODO: Better to match only "visible" char in order to avoid special invisible like \t, spaces, etc;
        //      the problem is that a SQL STRING can contains special char as a fix from previous function version
        const sourceFunctionDefinition = sourceObj.definition.replace(
          /\r/g,
          ''
        );
        const targetFunctionDefinition = targetObj.definition.replace(
          /\r/g,
          ''
        );
        if (sourceFunctionDefinition !== targetFunctionDefinition) {
          if (sourceObj.argTypes !== targetObj.argTypes) {
            lines.push(generateDropProcedureScript(sourceObj));
          }
          lines.push(generateCreateProcedureScript(sourceObj));
          if (sourceObj.comment) {
            lines.push(
              generateChangeCommentScript(
                sourceObj.id,
                procedureType,
                `${procedure}(${procedureArgs})`,
                sourceObj.comment
              )
            );
          }
        } else {
          lines.push(
            ...compareProcedurePrivileges(
              sourceObj,
              sourceObj.privileges,
              targetObj.privileges
            )
          );

          if (sourceObj.owner != targetObj.owner)
            lines.push(
              generateChangeProcedureOwnerScript(
                procedure,
                procedureArgs,
                sourceObj.owner,
                sourceObj.type
              )
            );

          if (sourceObj.comment != sourceObj.comment)
            lines.push(
              generateChangeCommentScript(
                sourceObj.id,
                procedureType,
                `${procedure}(${procedureArgs})`,
                sourceObj.comment
              )
            );
        }
      } else {
        //Procedure not exists on target database, then generate the script to create procedure
        lines.push(generateCreateProcedureScript(sourceObj));
        if (sourceObj.comment) {
          lines.push(
            generateChangeCommentScript(
              sourceObj.id,
              procedureType,
              `${procedure}(${procedureArgs})`,
              sourceObj.comment
            )
          );
        }
      }
    }
  }

  if (config.compareOptions.schemaCompare.dropMissingFunction)
    for (let procedure in targetFunctions) {
      for (const procedureArgs in targetFunctions[procedure]) {
        if (
          sourceFunctions[procedure] &&
          sourceFunctions[procedure][procedureArgs]
        ) {
          continue;
        }
        lines.push(
          generateDropProcedureScript(targetFunctions[procedure][procedureArgs])
        );
      }
    }

  return lines;
}

export function compareAggregates(
  sourceAggregates: Record<string, Record<string, AggregateDefinition>>,
  targetAggregates: Record<string, Record<string, AggregateDefinition>>,
  config: Config
) {
  const lines: Sql[] = [];

  for (let aggregate in sourceAggregates) {
    for (const aggregateArgs in sourceAggregates[aggregate]) {
      const sourceObj = sourceAggregates[aggregate][aggregateArgs];
      const targetObj =
        targetAggregates[aggregate] &&
        targetAggregates[aggregate][aggregateArgs];
      if (targetObj) {
        //Aggregate exists on both database, then compare procedure definition
        //TODO: Is correct that if definition is different automatically GRANTS and OWNER will not be updated also?
        if (sourceObj.definition != targetObj.definition) {
          lines.push(generateChangeAggregateScript(sourceObj));
          lines.push(
            generateChangeCommentScript(
              sourceObj.id,
              objectType.AGGREGATE,
              `${aggregate}(${aggregateArgs})`,
              sourceObj.comment
            )
          );
        } else {
          throw new Error('Not implemented');
          /*sqlScript.push(
              ...compareProcedurePrivileges(
                aggregate,
                aggregateArgs,
                sourceFunctions[procedure][procedureArgs].type,
                sourceObj.privileges,
                targetObj.privileges,
              ),
            );

            if (
              sourceObj.owner !=
              targetObj.owner
            )
              sqlScript.push(
                generateChangeAggregateOwnerScript(
                  aggregate,
                  aggregateArgs,
                  sourceObj.owner,
                ),
              );

            if (
              sourceObj.comment !=
              targetObj.comment
            )
              sqlScript.push(
                generateChangeCommentScript(
                  objectType.AGGREGATE,
                  `${aggregate}(${aggregateArgs})`,
                  sourceObj.comment,
                ),
              );*/
        }
      } else {
        //Aggregate not exists on target database, then generate the script to create aggregate
        lines.push(generateCreateAggregateScript(sourceObj));
        if (sourceObj.comment) {
          lines.push(
            generateChangeCommentScript(
              sourceObj.id,
              objectType.FUNCTION,
              `${aggregate}(${aggregateArgs})`,
              sourceObj.comment
            )
          );
        }
      }
    }
  }

  if (config.compareOptions.schemaCompare.dropMissingAggregate)
    for (let aggregate in targetAggregates) {
      for (const aggregateArgs in targetAggregates[aggregate]) {
        if (
          !sourceAggregates[aggregate] ||
          !sourceAggregates[aggregate][aggregateArgs]
        )
          lines.push(generateDropAggregateScript(aggregate, aggregateArgs));
      }
    }

  return lines;
}

export function compareProcedurePrivileges(
  schema: FunctionDefinition,
  sourceProcedurePrivileges: Record<string, FunctionPrivileges>,
  targetProcedurePrivileges: Record<string, FunctionPrivileges>
) {
  const lines: Sql[] = [];

  for (const role in sourceProcedurePrivileges) {
    const sourceObj = sourceProcedurePrivileges[role];
    const targetObj = targetProcedurePrivileges[role];
    //Get new or changed role privileges
    if (targetObj) {
      //Procedure privileges for role exists on both database, then compare privileges
      let changes = { execute: null };
      if (sourceObj.execute !== targetObj.execute) {
        changes.execute = sourceObj.execute;
        lines.push(
          generateChangesProcedureRoleGrantsScript(schema, role, changes)
        );
      }
    } else {
      //Procedure grants for role not exists on target database, then generate script to add role privileges
      lines.push(generateProcedureRoleGrantsScript(schema, role));
    }
  }

  return lines;
}

export function compareSequences(
  config: Config,
  sourceSequences: Record<string, Sequence>,
  targetSequences: Record<string, Sequence>
) {
  const lines: Sql[] = [];
  for (const sequence in sourceSequences) {
    const sourceObj = sourceSequences[sequence];
    const targetSequence =
      findRenamedSequenceOwnedByTargetTableColumn(
        sequence,
        sourceObj.ownedBy,
        targetSequences
      ) ?? sequence;
    const targetObj = targetSequences[targetSequence];

    if (targetObj) {
      //Sequence exists on both database, then compare sequence definition
      if (sequence !== targetSequence)
        lines.push(
          generateRenameSequenceScript(targetSequence, `"${sourceObj.name}"`)
        );

      lines.push(
        ...compareSequenceDefinition(config, sequence, sourceObj, targetObj)
      );

      lines.push(
        ...compareSequencePrivileges(
          sequence,
          sourceObj.privileges,
          targetObj.privileges
        )
      );

      if (sourceObj.comment != targetObj.comment)
        lines.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.SEQUENCE,
            sequence,
            sourceObj.comment
          )
        );
    } else {
      //Sequence not exists on target database, then generate the script to create sequence
      lines.push(
        generateCreateSequenceScript(
          sourceObj,
          config.compareOptions.mapRole(sourceObj.owner)
        )
      );
      if (sourceObj.comment) {
        lines.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.SEQUENCE,
            sequence,
            sourceObj.comment
          )
        );
      }
    }

    //TODO: @mso -> add a way to drop missing sequence if exists only on target db
  }

  return lines;
}

export function findRenamedSequenceOwnedByTargetTableColumn(
  sequenceName: string,
  tableColumn: string,
  targetSequences: Record<string, Sequence>
) {
  for (let sequence in targetSequences.sequences) {
    if (
      targetSequences[sequence].ownedBy == tableColumn &&
      sequence != sequenceName
    ) {
      return sequence;
    }
  }
  return null;
}

export function compareSequenceDefinition(
  config: Config,
  sequence: string,
  sourceSequenceDefinition: Sequence,
  targetSequenceDefinition: Sequence
) {
  const lines: Sql[] = [];

  for (const property in sourceSequenceDefinition) {
    let sourceObj = sourceSequenceDefinition[property];
    const targetObj = targetSequenceDefinition[property];
    if (property === 'owner') {
      sourceObj = config.compareOptions.mapRole(sourceObj);
    }
    if (
      property == 'privileges' ||
      property == 'ownedBy' ||
      property == 'name' ||
      property == 'comment' ||
      property == 'id' ||
      sourceObj === targetObj
    ) {
      continue;
    }
    lines.push(
      generateChangeSequencePropertyScript(
        sequence,
        property as SequenceProperties,
        sourceObj
      )
    );
  }

  return lines;
}

export function compareSequencePrivileges(
  sequence: string,
  sourceSequencePrivileges: SequencePrivileges,
  targetSequencePrivileges: SequencePrivileges
) {
  const lines: Sql[] = [];

  for (const role in sourceSequencePrivileges) {
    //Get new or changed role privileges
    if (targetSequencePrivileges[role]) {
      //Sequence privileges for role exists on both database, then compare privileges
      let changes: ColumnChanges = {};
      if (
        sourceSequencePrivileges[role].select !=
        targetSequencePrivileges[role].select
      )
        changes.select = sourceSequencePrivileges[role].select;

      if (
        sourceSequencePrivileges[role].usage !=
        targetSequencePrivileges[role].usage
      )
        changes.usage = sourceSequencePrivileges[role].usage;

      if (
        sourceSequencePrivileges[role].update !=
        targetSequencePrivileges[role].update
      )
        changes.update = sourceSequencePrivileges[role].update;

      if (Object.keys(changes).length > 0)
        lines.push(
          generateChangesSequenceRoleGrantsScript(sequence, role, changes)
        );
    } else {
      //Sequence grants for role not exists on target database, then generate script to add role privileges
      lines.push(
        generateSequenceRoleGrantsScript(
          sequence,
          role,
          sourceSequencePrivileges[role]
        )
      );
    }
  }

  return lines;
}
