import { Schema } from '../../catalog/database-objects.js';
import objectType from '../../enums/object-type.js';
import { generateChangeCommentScript } from '../sql/misc.js';
import { generateCreateSchemaScript } from '../sql/schema.js';
import { Sql } from '../stmt.js';

export function compareSchemas(
  sourceSchemas: Record<string, Schema>,
  targetSchemas: Record<string, Schema>,
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
          sourceObj.comment,
        ),
      );
    }

    if (targetObj && sourceObj.comment != targetObj.comment)
      lines.push(
        generateChangeCommentScript(
          sourceObj.id,
          objectType.SCHEMA,
          sourceSchema,
          sourceObj.comment,
        ),
      );
  }

  return lines;
}
