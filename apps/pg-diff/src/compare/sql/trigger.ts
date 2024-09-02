import { statement } from '../stmt.js';
import { Trigger } from '../../catalog/database-objects.js';

export function dropTrigger(trigger: Trigger) {
  return statement({
    sql: `DROP TRIGGER ${trigger.name} ON "${trigger.schema}"."${trigger.table}";`,
    before: [trigger.tableId],
    weight: -1,
  });
}

export function createTrigger(trigger: Trigger) {
  const sql = ['CREATE TRIGGER ', trigger.name];
  sql.push('\n    ');
  sql.push(trigger.actionTiming);
  sql.push(' ');
  const manipulation = trigger.eventManipulation.map((v) => {
    if (v === 'UPDATE' && trigger.attributes.length > 0) {
      return v + ' OF ' + trigger.attributes.join(',');
    }
    return v;
  });
  sql.push(manipulation.join(' OR '));
  sql.push('\n    ');
  sql.push(`ON "${trigger.schema}"."${trigger.table}"`);
  if (trigger.newTable || trigger.oldTable) {
    sql.push('REFERENCING ');
    if (trigger.newTable) {
      sql.push('NEW TABLE AS ' + trigger.newTable);
    }
    if (trigger.oldTable) {
      sql.push('OLD TABLE AS ' + trigger.oldTable);
    }
  }
  if (sql.length > 0) {
    sql.push('\n');
  }
  sql.push('    FOR ');
  if (trigger.actionOrientation === 'ROW') {
    sql.push('EACH ');
  }
  sql.push(trigger.actionOrientation);
  if (trigger.whenExpr) {
    sql.push('\n    ');
    sql.push(`WHEN ${trigger.whenExpr}`);
  }
  sql.push('\n    ');
  sql.push(trigger.actionStatement + ';');
  return statement({
    sql: sql.join(''),
    dependencies: [trigger.tableId, trigger.functionId],
  });
}
