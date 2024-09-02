import { IndexDefinition } from '../../catalog/database-objects.js';
import { statement } from '../stmt.js';

export function generateChangeIndexScript(index: string, definition: string) {
  return statement({
    sql: `DROP INDEX IF EXISTS ${index};\n${definition};`,
  });
}

export function generateDropIndexScript(index: IndexDefinition) {
  return statement({
    sql: `DROP INDEX IF EXISTS "${index.schema}"."${index.name}";`,
  });
}
