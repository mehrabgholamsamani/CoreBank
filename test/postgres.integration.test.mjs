import assert from 'node:assert/strict';
import test from 'node:test';
import testcontainers from 'testcontainers';
const { GenericContainer, Wait } = testcontainers;

test(
  'PostgreSQL Testcontainer starts for service integration tests',
  { skip: process.env.RUN_INTEGRATION !== '1' },
  async () => {
    const database = await new GenericContainer('postgres:16-alpine')
      .withEnvironment({ POSTGRES_USER: 'test', POSTGRES_PASSWORD: 'test', POSTGRES_DB: 'test' })
      .withExposedPorts(5432)
      .withWaitStrategy(Wait.forLogMessage('database system is ready to accept connections'))
      .start();
    try {
      assert.ok(database.getMappedPort(5432) > 0);
    } finally {
      await database.stop();
    }
  },
);
