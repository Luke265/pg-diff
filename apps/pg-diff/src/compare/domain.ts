import objectType from '../enums/object-type.js';
import { Domain } from '../catalog/database-objects.js';
import { Sql } from './stmt.js';
import { commentIsEqual } from './utils.js';
import {
  generateChangeDomainCheckScript,
  generateChangeDomainOwnerScript,
  generateCreateDomainScript,
  generateDropDomainScript,
} from './sql/domain.js';
import { generateChangeCommentScript } from './sql/misc.js';
import { Config } from '../config.js';

export function compareDomains(
  source: Record<string, Domain>,
  target: Record<string, Domain>,
  config: Config,
) {
  const sqlScript: Sql[] = [];
  for (const name in source) {
    const sourceObj = source[name];
    const targetObj = target[name];
    if (targetObj) {
      //Table exists on both database, then compare table schema
      if (sourceObj.check !== targetObj.check) {
        sqlScript.push(generateChangeDomainCheckScript(sourceObj));
      }

      const owner = config.compareOptions.mapRole(sourceObj.owner);
      if (owner !== targetObj.owner) {
        sqlScript.push(generateChangeDomainOwnerScript(sourceObj, owner));
      }
      if (!commentIsEqual(sourceObj.comment, targetObj?.comment)) {
        sqlScript.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.DOMAIN,
            name,
            sourceObj.comment,
          ),
        );
      }
    } else {
      //Table not exists on target database, then generate the script to create table
      sqlScript.push(generateCreateDomainScript(sourceObj));
      if (sourceObj.comment) {
        sqlScript.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.DOMAIN,
            name,
            sourceObj.comment,
          ),
        );
      }
    }
  }

  if (config.compareOptions.schemaCompare.dropMissingTable) {
    const migrationFullTableName = config.migrationOptions
      ? `"${config.migrationOptions.historyTableSchema}"."${config.migrationOptions.historyTableName}"`
      : '';

    for (const name in target) {
      if (source[name] || name === migrationFullTableName) {
        continue;
      }
      sqlScript.push(generateDropDomainScript(target[name]));
    }
  }

  return sqlScript;
}
