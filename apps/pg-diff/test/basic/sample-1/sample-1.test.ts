import { compare } from '../helpers';

describe('compare source - target', () => {
  it('should succeed', async () => {
    await compare(__dirname);
  });
});
