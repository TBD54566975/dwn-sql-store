import chai, { expect } from 'chai';
import { testMysqlDialect, testPostgresDialect, testSqliteDialect } from './test-dialects.js';

import { Kysely } from 'kysely';
import { DwnDatabaseType } from '../src/types.js';

import chaiAsPromised from 'chai-as-promised';
import { TestDataGenerator } from '@tbd54566975/dwn-sdk-js';

chai.use(chaiAsPromised);

describe('Dialect tests', () => {
  const databaseDialects = [testMysqlDialect, testPostgresDialect, testSqliteDialect];
  for (const dialect of databaseDialects) {
    it(`should check if table exists correctly: ${dialect.name}`, async () => {
      const database = new Kysely<DwnDatabaseType>({ dialect });

      const randomTableName = `test_table_${TestDataGenerator.randomString(10)}`;

      let tableExists = await dialect.hasTable(database, randomTableName);
      expect(tableExists).to.be.false;

      await database.schema
        .createTable(randomTableName)
        .addColumn('anyColumn', 'text')
        .execute();

      tableExists = await dialect.hasTable(database, randomTableName);
      expect(tableExists).to.be.true;

      await database.schema.dropTable(randomTableName).execute();

      tableExists = await dialect.hasTable(database, randomTableName);
      expect(tableExists).to.be.false;
    });
  }
});