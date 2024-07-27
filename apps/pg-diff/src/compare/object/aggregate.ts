import { AggregateDefinition } from '../../catalog/database-objects.js';
import { Config } from '../../config.js';
import objectType from '../../enums/object-type.js';
import {
  generateChangeAggregateScript,
  generateCreateAggregateScript,
  generateDropAggregateScript,
} from '../sql/aggregate.js';
import { generateChangeCommentScript } from '../sql/misc.js';
import { Sql } from '../stmt.js';

export function compareAggregates(
  sourceAggregates: Record<string, Record<string, AggregateDefinition>>,
  targetAggregates: Record<string, Record<string, AggregateDefinition>>,
  config: Config,
) {
  const lines: Sql[] = [];

  for (const aggregate in sourceAggregates) {
    for (const aggregateArgs in sourceAggregates[aggregate]) {
      const sourceObj = sourceAggregates[aggregate][aggregateArgs];
      const targetObj = targetAggregates[aggregate]?.[aggregateArgs];
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
              sourceObj.comment,
            ),
          );
        } else {
          throw new Error('Not implemented');
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
              sourceObj.comment,
            ),
          );
        }
      }
    }
  }

  if (config.compareOptions.schemaCompare.dropMissingAggregate) {
    for (const aggregate in targetAggregates) {
      for (const aggregateArgs in targetAggregates[aggregate]) {
        const sourceObj = sourceAggregates[aggregate]?.[aggregateArgs];
        if (sourceObj) {
          continue;
        }
        lines.push(generateDropAggregateScript(aggregate, aggregateArgs));
      }
    }
  }

  return lines;
}
