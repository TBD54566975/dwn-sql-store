import { expect } from 'chai';
import { testMysqlDialect, testPostgresDialect, testSqliteDialect } from '../test-dialects.js';

import { executeWithRetryIfDatabaseIsLocked } from '../../src/utils/transaction.js';
import { Kysely } from 'kysely';
import { DwnDatabaseType } from '../../src/types.js';

describe('executeWithRetryIfDatabaseIsLocked', () => {
  // There is an opportunity to improve how 3 different database dialects are tested if SQL specific tests start to expand.
  const databaseDialects = [testMysqlDialect, testPostgresDialect, testSqliteDialect];
  for (const dialect of databaseDialects) {
    it('should rethrow error if its not due to database being locked', async () => {
      const database = new Kysely<DwnDatabaseType>({ dialect });
      const operation = async (_transaction) => {
        throw new Error('Some error');
      };

      try {
        await executeWithRetryIfDatabaseIsLocked(database, operation);
      } catch (error) {
        expect(error.message).to.contain('Some error');
      }
    });
  }
});