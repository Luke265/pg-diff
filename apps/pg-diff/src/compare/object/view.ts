import { ViewDefinition } from '../../catalog/database-objects.js';
import { Config } from '../../config.js';
import objectType from '../../enums/object-type.js';
import { generateChangeCommentScript } from '../sql/misc.js';
import { generateChangeTableOwnerScript } from '../sql/table.js';
import {
  generateDropViewScript,
  generateCreateViewScript,
} from '../sql/view.js';
import { SqlResult } from '../utils.js';
import { compareTablePrivileges } from './table.js';

export function compareViews(
  sourceViews: Record<string, ViewDefinition>,
  targetViews: Record<string, ViewDefinition>,
  droppedViews: string[],
  config: Config,
): SqlResult[] {
  const lines: SqlResult[] = [];

  for (const view in sourceViews) {
    const sourceObj = sourceViews[view];
    const targetObj = targetViews[view];

    if (targetObj) {
      //View exists on both database, then compare view schema
      let sourceViewDefinition = sourceObj.definition.replace(/\r/g, '');
      let targetViewDefinition = targetObj.definition.replace(/\r/g, '');
      if (sourceViewDefinition != targetViewDefinition) {
        if (!droppedViews.includes(view))
          lines.push(generateDropViewScript(sourceObj));
        lines.push(generateCreateViewScript(config, sourceObj));
        lines.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.VIEW,
            view,
            sourceObj.comment,
          ),
        );
      } else {
        if (droppedViews.includes(view))
          //It will recreate a dropped view because changes happens on involved columns
          lines.push(generateCreateViewScript(config, sourceObj));

        lines.push(
          ...compareTablePrivileges(
            sourceObj,
            sourceObj.privileges,
            targetObj.privileges,
            config,
          ),
        );

        const owner = config.compareOptions.mapRole(sourceObj.owner);
        if (owner !== targetObj.owner) {
          lines.push(
            generateChangeTableOwnerScript(
              view,
              config.compareOptions.replaceRole(owner),
            ),
          );
        }

        if (sourceObj.comment != targetObj.comment)
          lines.push(
            generateChangeCommentScript(
              sourceObj.id,
              objectType.VIEW,
              view,
              sourceObj.comment,
            ),
          );
      }
    } else {
      //View not exists on target database, then generate the script to create view
      lines.push(generateCreateViewScript(config, sourceObj));
      lines.push(
        generateChangeCommentScript(
          sourceObj.id,
          objectType.VIEW,
          view,
          sourceObj.comment,
        ),
      );
    }
  }

  if (config.compareOptions.schemaCompare.dropMissingView)
    for (const view in targetViews) {
      if (sourceViews[view]) {
        continue;
      }

      lines.push(generateDropViewScript(targetViews[view]));
    }

  return lines;
}
