import {
  Sequence,
  SequencePrivileges,
} from '../../catalog/database-objects.js';
import { hints } from './misc.js';
import { SequenceChanges, buildGrants } from '../utils.js';
import { joinStmt, statement } from '../stmt.js';

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
  privileges: SequencePrivileges,
) {
  return buildGrants([
    ['SELECT', privileges.select],
    ['USAGE', privileges.usage],
    ['UPDATE', privileges.update],
  ]).map(([type, privileges]) =>
    statement({
      sql: `${type} ${privileges} ON SEQUENCE ${sequence} ${
        type === 'GRANT' ? 'TO' : 'FROM'
      } ${role};${hints.potentialRoleMissing}`,
    }),
  );
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
  value: string,
) {
  return statement({
    sql: `ALTER SEQUENCE IF EXISTS ${sequence} ${sequencePropertyMap(
      property,
      value,
    )};`,
  });
}

export function generateChangesSequenceRoleGrantsScript(
  sequence: string,
  role: string,
  changes: SequenceChanges,
) {
  return [
    [changes.select, 'SELECT'],
    [changes.usage, 'USAGE'],
    [changes.update, 'UPDATE'],
  ]
    .filter(([defined]) => defined !== undefined)
    .map(([defined, type]) =>
      statement({
        sql: `${defined ? 'GRANT' : 'REVOKE'} ${type} ON SEQUENCE ${sequence} ${
          defined ? 'TO' : 'FROM'
        } ${role};${hints.potentialRoleMissing}`,
      }),
    );
}

export function generateSequenceRoleGrantsScript(
  sequence: string,
  role: string,
  privileges: SequencePrivileges,
) {
  return generateSequenceGrantsDefinition(sequence, role, privileges);
}

export function generateCreateSequenceScript(
  sequence: Sequence,
  owner: string,
) {
  //Generate privileges script
  const fullName = `"${sequence.schema}"."${sequence.name}"`;
  const privileges = Object.entries(sequence.privileges)
    .map(([role, privileges]) =>
      generateSequenceGrantsDefinition(fullName, role, privileges),
    )
    .flat();
  const sql = [
    `CREATE SEQUENCE IF NOT EXISTS ${fullName} 
  \tINCREMENT BY ${sequence.increment} 
  \tMINVALUE ${sequence.minValue}
  \tMAXVALUE ${sequence.maxValue}
  \tSTART WITH ${sequence.startValue}
  \tCACHE ${sequence.cacheSize}
  \t${sequence.isCycle ? '' : 'NO '}CYCLE;\n`,
    `ALTER SEQUENCE ${fullName} OWNER TO ${owner};\n`,
  ];
  joinStmt(sql, privileges, '\n');
  return statement({
    sql,
  });
}

export function generateRenameSequenceScript(
  old_name: string,
  new_name: string,
) {
  return statement({
    sql: `ALTER SEQUENCE IF EXISTS ${old_name} RENAME TO ${new_name};`,
  });
}
