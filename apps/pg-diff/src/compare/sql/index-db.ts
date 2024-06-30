import { stmt } from '../stmt.js';
import { IndexDefinition } from '../../catalog/database-objects.js';

export function generateChangeIndexScript(index: string, definition: string) {
  return stmt`DROP INDEX IF EXISTS ${index};\n${definition};`;
}

export function generateDropIndexScript(index: IndexDefinition) {
  return stmt`DROP INDEX IF EXISTS "${index.schema}"."${index.name}";`;
}
