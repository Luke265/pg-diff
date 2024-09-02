import { statement } from '../stmt.js';

export function generateChangeCommentScript(
  id: number | string,
  objectType: string,
  objectName: string,
  comment: string | null,
  parentObjectName: string | null = null,
) {
  const description = comment ? `'${comment.replaceAll("'", "''")}'` : 'NULL';
  const parentObject = parentObjectName ? `ON ${parentObjectName}` : '';
  return statement({
    sql: `COMMENT ON ${objectType} ${objectName} ${parentObject} IS ${description};`,
    dependencies: [id],
  });
}

export const hints = {
  addColumnNotNullableWithoutDefaultValue:
    '--WARN: Add a new column not nullable without a default value can occure in a sql error during execution!',
  changeColumnDataType:
    '--WARN: Change column data type can occure in a casting error, the suggested casting expression is the default one and may not fit your needs!',
  dropColumn: ' --WARN: Drop column can occure in data loss!',
  potentialRoleMissing:
    '--WARN: Grant\\Revoke privileges to a role can occure in a sql error during execution if role is missing to the target database!',
  identityColumnDetected:
    '--WARN: Identity column has been detected, an error can occure because constraints violation!',
  dropTable: '--WARN: Drop table can occure in data loss!',
} as const;
