import {
  Sequence,
  SequencePrivileges,
} from '../../catalog/database-objects.js';
import { Config } from '../../config.js';
import objectType from '../../enums/object-type.js';
import { generateChangeCommentScript } from '../sql/misc.js';
import {
  SequenceProperties,
  generateChangeSequencePropertyScript,
  generateChangesSequenceRoleGrantsScript,
  generateCreateSequenceScript,
  generateRenameSequenceScript,
  generateSequenceRoleGrantsScript,
} from '../sql/sequence.js';
import { Sql } from '../stmt.js';
import { ColumnChanges } from '../utils.js';

export function compareSequences(
  config: Config,
  sourceSequences: Record<string, Sequence>,
  targetSequences: Record<string, Sequence>,
) {
  const lines: Sql[] = [];
  for (const sequence in sourceSequences) {
    const sourceObj = sourceSequences[sequence];
    const targetSequence =
      findRenamedSequenceOwnedByTargetTableColumn(
        sequence,
        sourceObj.ownedBy,
        targetSequences,
      ) ?? sequence;
    const targetObj = targetSequences[targetSequence];

    if (targetObj) {
      //Sequence exists on both database, then compare sequence definition
      if (sequence !== targetSequence)
        lines.push(
          generateRenameSequenceScript(targetSequence, `"${sourceObj.name}"`),
        );

      lines.push(
        ...compareSequenceDefinition(config, sequence, sourceObj, targetObj),
      );

      lines.push(
        ...compareSequencePrivileges(
          sequence,
          sourceObj.privileges,
          targetObj.privileges,
        ),
      );

      if (sourceObj.comment != targetObj.comment)
        lines.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.SEQUENCE,
            sequence,
            sourceObj.comment,
          ),
        );
    } else {
      //Sequence not exists on target database, then generate the script to create sequence
      lines.push(
        generateCreateSequenceScript(
          sourceObj,
          config.compareOptions.mapRole(sourceObj.owner),
        ),
      );
      if (sourceObj.comment) {
        lines.push(
          generateChangeCommentScript(
            sourceObj.id,
            objectType.SEQUENCE,
            sequence,
            sourceObj.comment,
          ),
        );
      }
    }

    //TODO: @mso -> add a way to drop missing sequence if exists only on target db
  }

  return lines;
}

function findRenamedSequenceOwnedByTargetTableColumn(
  sequenceName: string,
  ownedBy: string | null,
  targetSequences: Record<string, Sequence>,
) {
  for (let sequence in targetSequences.sequences) {
    if (
      targetSequences[sequence].ownedBy == ownedBy &&
      sequence != sequenceName
    ) {
      return sequence;
    }
  }
  return null;
}

function compareSequenceDefinition(
  config: Config,
  sequence: string,
  sourceSequenceDefinition: Sequence,
  targetSequenceDefinition: Sequence,
) {
  const lines: Sql[] = [];
  const props: SequenceProperties[] = [
    'startValue',
    'minValue',
    'maxValue',
    'increment',
    'cacheSize',
    'isCycle',
    'owner',
  ];
  for (const p of props) {
    const sourceObj = sourceSequenceDefinition[p];
    const targetObj = targetSequenceDefinition[p];
    if (sourceObj === targetObj) {
      continue;
    }
    let value = sourceObj + '';
    if (p === 'owner') {
      value = config.compareOptions.mapRole(sourceObj as string);
    }
    lines.push(generateChangeSequencePropertyScript(sequence, p, value));
  }
  return lines;
}

function compareSequencePrivileges(
  sequence: string,
  sourceSequencePrivileges: Record<string, SequencePrivileges>,
  targetSequencePrivileges: Record<string, SequencePrivileges>,
) {
  const lines: Sql[] = [];

  for (const role in sourceSequencePrivileges) {
    const sourceObj = sourceSequencePrivileges[role];
    const targetObj = targetSequencePrivileges[role];
    //Get new or changed role privileges
    if (targetObj) {
      //Sequence privileges for role exists on both database, then compare privileges
      let changes: ColumnChanges = {};
      if (sourceObj.select != targetObj.select)
        changes.select = sourceObj.select;

      if (sourceObj.usage != targetObj.usage) changes.usage = sourceObj.usage;

      if (sourceObj.update != targetObj.update)
        changes.update = sourceObj.update;

      if (Object.keys(changes).length > 0)
        lines.push(
          generateChangesSequenceRoleGrantsScript(sequence, role, changes),
        );
    } else {
      //Sequence grants for role not exists on target database, then generate script to add role privileges
      lines.push(generateSequenceRoleGrantsScript(sequence, role, sourceObj));
    }
  }

  return lines;
}
