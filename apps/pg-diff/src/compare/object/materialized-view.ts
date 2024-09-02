import { MaterializedViewDefinition } from '../../catalog/database-objects.js';
import { Config } from '../../config.js';
import objectType from '../../enums/object-type.js';
import {
  generateDropMaterializedViewScript,
  generateCreateMaterializedViewScript,
} from '../sql/materialized-view.js';
import { generateChangeCommentScript } from '../sql/misc.js';
import { generateChangeTableOwnerScript } from '../sql/table.js';
import { SqlResult } from '../utils.js';
import { compareTableIndexes, compareTablePrivileges } from './table.js';

export function compareMaterializedViews(
  sourceMaterializedViews: Record<string, MaterializedViewDefinition>,
  targetMaterializedViews: Record<string, MaterializedViewDefinition>,
  droppedViews: string[],
  droppedIndexes: string[],
  config: Config,
): SqlResult[] {
  const lines: SqlResult[] = [];
  for (const view in sourceMaterializedViews) {
    const sourceObj = sourceMaterializedViews[view];
    const targetObj = targetMaterializedViews[view];
    //Get new or changed materialized views
    if (targetObj) {
      //Materialized view exists on both database, then compare materialized view schema
      const sourceViewDefinition = sourceObj.definition.replace(/\r/g, '');
      const targetViewDefinition = targetObj.definition.replace(/\r/g, '');
      if (sourceViewDefinition != targetViewDefinition) {
        if (!droppedViews.includes(view)) {
          lines.push(generateDropMaterializedViewScript(sourceObj));
        }
        lines.push(generateCreateMaterializedViewScript(sourceObj));
        lines.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.MATERIALIZED_VIEW,
            view,
            sourceObj.comment,
          ),
        );
      } else {
        if (droppedViews.includes(view)) {
          //It will recreate a dropped materialized view because changes happens on involved columns
          lines.push(generateCreateMaterializedViewScript(sourceObj));
        }
        lines.push(
          ...compareTableIndexes(
            sourceObj.indexes,
            targetObj.indexes,
            droppedIndexes,
          ),
        );

        lines.push(
          ...compareTablePrivileges(
            view,
            sourceObj.privileges,
            targetObj.privileges,
            config,
          ),
        );

        if (sourceObj.owner != targetObj.owner)
          lines.push(generateChangeTableOwnerScript(view, sourceObj.owner));

        if (sourceObj.comment != targetObj.comment)
          lines.push(
            generateChangeCommentScript(
              sourceObj.id,
              objectType.MATERIALIZED_VIEW,
              view,
              sourceObj.comment,
            ),
          );
      }
    } else {
      //Materialized view not exists on target database, then generate the script to create materialized view
      lines.push(generateCreateMaterializedViewScript(sourceObj));
      lines.push(
        generateChangeCommentScript(
          sourceObj.id,
          objectType.MATERIALIZED_VIEW,
          view,
          sourceObj.comment,
        ),
      );
    }
  }

  if (config.compareOptions.schemaCompare.dropMissingView) {
    for (const view in targetMaterializedViews) {
      if (sourceMaterializedViews[view]) {
        continue;
      }
      lines.push(
        generateDropMaterializedViewScript(targetMaterializedViews[view]),
      );
    }
  }

  return lines;
}
