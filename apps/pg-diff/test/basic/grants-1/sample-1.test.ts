import { compare } from '../helpers.js';
import { sourceDb } from '../setup-db.js';

describe('compare source - target', () => {
  it('should succeed', () => compare(__dirname));
});
