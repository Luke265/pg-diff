import { DatabaseObjects } from '../catalog/database-objects.js';

import EventEmitter from 'events';
import { ClientBase } from 'pg';
import pg from 'pg';
import { getServerVersion } from '../utils.js';
import { Sql } from './stmt.js';
import { compareTypes } from './object/type.js';
import { compareDomains } from './object/domain.js';
import { compareTables } from './object/table.js';
import { loadCatalog } from '../catalog/index.js';
import { retrieveAllSchemas } from '../catalog/catalog-api.js';
import { Config } from '../config.js';
import { compareSequences } from './object/sequence.js';
import { comparePolicies } from './object/policy.js';
import { compareViews } from './object/view.js';
import { compareMaterializedViews } from './object/materialized-view.js';
import { compareProcedures } from './object/procedure.js';
import { compareAggregates } from './object/aggregate.js';
import { compareSchemas } from './object/schema.js';
import { compareTriggers } from './object/trigger.js';

export async function compare(config: Config, eventEmitter: EventEmitter) {
  eventEmitter.emit('compare', 'Compare started', 0);
  eventEmitter.emit('compare', 'Connecting to source database ...', 10);
  const pgSourceClient = new pg.Client({
    user: config.sourceClient.user,
    host: config.sourceClient.host,
    database: config.sourceClient.database,
    password: config.sourceClient.password,
    port: config.sourceClient.port,
    application_name: config.sourceClient.applicationName,
  });
  await pgSourceClient.connect();
  eventEmitter.emit(
    'compare',
    `Connected to source PostgreSQL ${
      (await getServerVersion(pgSourceClient)).version
    } on [${config.sourceClient.host}:${config.sourceClient.port}/${
      config.sourceClient.database
    }] `,
    11,
  );

  eventEmitter.emit('compare', 'Connecting to target database ...', 20);
  const pgTargetClient = new pg.Client({
    user: config.targetClient.user,
    host: config.targetClient.host,
    database: config.targetClient.database,
    password: config.targetClient.password,
    port: config.targetClient.port,
    application_name: config.targetClient.applicationName,
  });
  await pgTargetClient.connect();
  eventEmitter.emit(
    'compare',
    `Connected to target PostgreSQL ${
      (await getServerVersion(pgTargetClient)).version
    } on [${config.targetClient.host}:${config.targetClient.port}/${
      config.targetClient.database
    }] `,
    21,
  );

  const dbSourceObjects = await collectDatabaseObject(pgSourceClient, config);
  eventEmitter.emit('compare', 'Collected SOURCE objects', 30);
  const dbTargetObjects = await collectDatabaseObject(pgTargetClient, config);
  eventEmitter.emit('compare', 'Collected TARGET objects', 40);

  const { added, ddl } = compareDatabaseObjects(
    dbSourceObjects,
    dbTargetObjects,
    config,
    eventEmitter,
  );
  eventEmitter.emit('compare', 'Compare completed', 100);
  return ddl;
}

export async function collectDatabaseObject(
  client: ClientBase,
  config: Config,
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
    config.compareOptions.schemaCompare.namespaces =
      await retrieveAllSchemas(client);
  return loadCatalog(client, {
    schemas: config.compareOptions.schemaCompare.namespaces,
    roles: config.compareOptions.schemaCompare.roles,
  });
}

export function compareDatabaseObjects(
  dbSourceObjects: DatabaseObjects,
  dbTargetObjects: DatabaseObjects,
  config: Config,
  eventEmitter: EventEmitter,
) {
  const droppedConstraints: string[] = [];
  const droppedIndexes: string[] = [];
  const droppedViews: string[] = [];
  const addedColumns: Record<string, string[]> = {};
  const addedTables: string[] = [];
  const sqlPatch: (Sql | null)[] = [];

  sqlPatch.push(
    ...compareSchemas(dbSourceObjects.schemas, dbTargetObjects.schemas),
  );
  eventEmitter.emit('compare', 'SCHEMA objects have been compared', 45);

  if (config.compareOptions.schemaCompare.sequence !== false) {
    sqlPatch.push(
      ...compareSequences(
        config,
        dbSourceObjects.sequences,
        dbTargetObjects.sequences,
      ),
    );
    eventEmitter.emit('compare', 'SEQUENCE objects have been compared', 50);
  }

  sqlPatch.push(
    ...compareTables(
      dbSourceObjects,
      dbTargetObjects,
      droppedConstraints,
      droppedIndexes,
      droppedViews,
      addedColumns,
      addedTables,
      config,
    ),
  );

  sqlPatch.push(
    ...compareTypes(dbSourceObjects.types, dbTargetObjects.types, config),
  );

  sqlPatch.push(
    ...compareDomains(dbSourceObjects.domains, dbTargetObjects.domains, config),
  );
  eventEmitter.emit('compare', 'TABLE objects have been compared', 55);

  sqlPatch.push(
    ...compareViews(
      dbSourceObjects.views,
      dbTargetObjects.views,
      droppedViews,
      config,
    ),
  );
  eventEmitter.emit('compare', 'VIEW objects have been compared', 60);

  sqlPatch.push(
    ...compareMaterializedViews(
      dbSourceObjects.materializedViews,
      dbTargetObjects.materializedViews,
      droppedViews,
      droppedIndexes,
      config,
    ),
  );
  eventEmitter.emit(
    'compare',
    'MATERIALIZED VIEW objects have been compared',
    65,
  );

  sqlPatch.push(
    ...compareProcedures(
      dbSourceObjects.functionMap,
      dbTargetObjects.functionMap,
      config,
    ),
  );
  eventEmitter.emit('compare', 'PROCEDURE objects have been compared', 70);

  sqlPatch.push(
    ...compareAggregates(
      dbSourceObjects.aggregates,
      dbTargetObjects.aggregates,
      config,
    ),
  );
  eventEmitter.emit('compare', 'AGGREGATE objects have been compared', 75);

  sqlPatch.push(
    ...comparePolicies(config, dbSourceObjects.tables, dbTargetObjects.tables),
  );

  sqlPatch.push(
    ...compareTriggers(config, dbSourceObjects.tables, dbTargetObjects.tables),
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
    ddl: sqlPatch.filter((v): v is Sql => !!v),
  };
}
