import { ClientBase } from 'pg';
import { DatabaseObjects } from './database-objects';
import { Config } from './config';
import { extractFunctionCalls } from './utils';
import {
  retrieveSchemas,
  retrieveTables,
  retrieveViews,
  retrieveMaterializedViews,
  retrieveFunctions,
  retrieveAggregates,
  retrieveSequences,
  retrieveTypes,
  retrieveDomains,
} from './catalog-api';

export async function loadCatalog(client: ClientBase, config: Config) {
  const dbObjects: DatabaseObjects = {
    schemas: {},
    tables: {},
    views: {},
    materializedViews: {},
    functionMap: {},
    aggregates: {},
    sequences: {},
    types: {},
    domains: {},
  };

  dbObjects.schemas = await retrieveSchemas(client, config.schemas);
  dbObjects.tables = await retrieveTables(client, config);
  dbObjects.views = await retrieveViews(client, config);
  dbObjects.materializedViews = await retrieveMaterializedViews(client, config);
  dbObjects.aggregates = await retrieveAggregates(client, config);
  dbObjects.sequences = await retrieveSequences(client, config);
  dbObjects.types = await retrieveTypes(client, config);
  dbObjects.domains = await retrieveDomains(client, config);
  const { map: functionMap, list: functionList } = await retrieveFunctions(
    client,
    config
  );
  dbObjects.functionMap = functionMap;
  const tableIdMap = Object.values(dbObjects.tables).map(
    (t) => [t.id, `${t.schema}.${t.name}`] as [number, string]
  );
  // mark mentioned tables in a function definition as dependencies
  for (const fn of functionList) {
    if (fn.languageName !== 'sql') {
      continue;
    }
    for (const [id, name] of tableIdMap) {
      if (fn.definition.includes(name)) {
        fn.fReferenceIds.push(id);
      }
    }
    fn.fReferenceIds.push(
      ...extractFunctionCalls(fn.definition)
        .filter((name) => functionMap[name])
        .map((name) => Object.values(functionMap[name]).map((f) => f.id))
        .flat()
    );
  }
  for (const name in dbObjects.tables) {
    const table = dbObjects.tables[name];
    // mark mentioned tables in a policy definition as dependencies
    for (const policyName in table.policies) {
      const policy = table.policies[policyName];
      if (policy.using) {
        for (const [id, name] of tableIdMap) {
          if (policy.using.includes(name)) {
            policy.dependencies.push(id);
          }
        }
      }
      if (policy.withCheck) {
        for (const [id, name] of tableIdMap) {
          if (policy.withCheck.includes(name)) {
            policy.dependencies.push(id);
          }
        }
      }
    }
  }

  //TODO: Add a way to retrieve AGGREGATE and WINDOW functions
  //TODO: Do we need to retrieve roles?
  //TODO: Do we need to retieve special table like TEMPORARY and UNLOGGED? for sure not temporary, but UNLOGGED probably yes.
  //TODO: Do we need to retrieve collation for both table and columns?
  //TODO: Add a way to retrieve DOMAIN and its CONSTRAINTS
  return dbObjects;
}
