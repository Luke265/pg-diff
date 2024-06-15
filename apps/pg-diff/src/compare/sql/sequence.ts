import { stmt } from '../../stmt';
import { Sequence, SequencePrivileges } from '../../catalog/database-objects';
import { hints } from './misc';

export type SequenceProperties =
  | 'startValue'
  | 'minValue'
  | 'maxValue'
  | 'increment'
  | 'cacheSize'
  | 'isCycle'
  | 'owner';

export function generateSequenceGrantsDefinition(
  sequence: string,
  role: string,
  privileges: SequencePrivileges
) {
  const definitions: string[] = [];
  if (privileges.select) {
    definitions.push(
      `GRANT SELECT ON SEQUENCE ${sequence} TO ${role};${hints.potentialRoleMissing}`
    );
  }
  if (privileges.usage) {
    definitions.push(
      `GRANT USAGE ON SEQUENCE ${sequence} TO ${role};${hints.potentialRoleMissing}`
    );
  }
  if (privileges.update) {
    definitions.push(
      `GRANT UPDATE ON SEQUENCE ${sequence} TO ${role};${hints.potentialRoleMissing}`
    );
  }
  return definitions;
}

export function generateSetSequenceValueScript(
  tableName: string,
  sequence: any
) {
  return stmt`SELECT setval(pg_get_serial_sequence('${tableName}', '${sequence.attname}'), max("${sequence.attname}"), true) FROM ${tableName};`;
}

function sequencePropertyMap(property: SequenceProperties, value: string) {
  switch (property) {
    case 'startValue':
      return `START WITH ${value}`;
    case 'minValue':
      return `MINVALUE ${value}`;
    case 'maxValue':
      return `MAXVALUE ${value}`;
    case 'increment':
      return `INCREMENT BY ${value}`;
    case 'cacheSize':
      return `CACHE ${value}`;
    case 'isCycle':
      return `${value ? '' : 'NO'} CYCLE`;
    case 'owner':
      return `OWNER TO ${value}`;
    default:
      throw new Error(`Unsupported property ${property}`);
  }
}

export function generateChangeSequencePropertyScript(
  sequence: string,
  property: SequenceProperties,
  value: string
) {
  return stmt`ALTER SEQUENCE IF EXISTS ${sequence} ${sequencePropertyMap(
    property,
    value
  )};`;
}

export function generateChangesSequenceRoleGrantsScript(
  sequence: string,
  role: string,
  changes: any
) {
  let privileges: string[] = [];

  if (Object.prototype.hasOwnProperty.call(changes, 'select'))
    privileges.push(
      `${changes.select ? 'GRANT' : 'REVOKE'} SELECT ON SEQUENCE ${sequence} ${
        changes.select ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`
    );

  if (Object.prototype.hasOwnProperty.call(changes, 'usage'))
    privileges.push(
      `${changes.usage ? 'GRANT' : 'REVOKE'} USAGE ON SEQUENCE ${sequence} ${
        changes.usage ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`
    );

  if (Object.prototype.hasOwnProperty.call(changes, 'update'))
    privileges.push(
      `${changes.update ? 'GRANT' : 'REVOKE'} UPDATE ON SEQUENCE ${sequence} ${
        changes.update ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`
    );

  return stmt`${privileges.join('\n')}`;
}

export function generateSequenceRoleGrantsScript(
  sequence: string,
  role: string,
  privileges: any
) {
  return stmt`${generateSequenceGrantsDefinition(
    sequence,
    role,
    privileges
  ).join('\n')}`;
}

export function generateCreateSequenceScript(
  sequence: Sequence,
  owner: string
) {
  //Generate privileges script
  const fullName = `"${sequence.schema}"."${sequence.name}"`;
  const privileges: (string | string[])[] = [
    `ALTER SEQUENCE ${fullName} OWNER TO ${owner};`,
  ];

  for (const role in sequence.privileges) {
    privileges.push(
      generateSequenceGrantsDefinition(
        fullName,
        role,
        sequence.privileges[role]
      )
    );
  }

  return stmt`
  CREATE SEQUENCE IF NOT EXISTS ${fullName} 
  \tINCREMENT BY ${sequence.increment} 
  \tMINVALUE ${sequence.minValue}
  \tMAXVALUE ${sequence.maxValue}
  \tSTART WITH ${sequence.startValue}
  \tCACHE ${sequence.cacheSize}
  \t${sequence.isCycle ? '' : 'NO '}CYCLE;
  \n${privileges.flat().join('\n')}`;
}

export function generateRenameSequenceScript(
  old_name: string,
  new_name: string
) {
  return stmt`ALTER SEQUENCE IF EXISTS ${old_name} RENAME TO ${new_name};`;
}
