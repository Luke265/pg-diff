import { compare } from '../helpers.js';

describe('compare source - target', () => {
  it('should succeed', async () => {
    await compare(__dirname);
  });
});
