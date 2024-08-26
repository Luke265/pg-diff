/* eslint-disable */
export default {
  displayName: 'pg-diff',
  preset: '../../jest.preset.js',
  testEnvironment: 'node',
  transform: {
    '^.+\\.ts$': ['ts-jest', { tsconfig: '<rootDir>/tsconfig.spec.json' }],
  },
  moduleFileExtensions: ['ts', 'js', 'html', 'sql'],
  coverageDirectory: '../../coverage/apps/pg-diff',
};
