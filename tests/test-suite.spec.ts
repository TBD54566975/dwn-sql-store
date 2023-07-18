import { TestSuite } from '@tbd54566975/dwn-sdk-js/tests';
import { testMysqlDialect, testPostgresDialect, testSqliteDialect } from './test-dialects.js';
import { MessageStoreSql } from '../src/message-store-sql.js';
import { DataStoreSql } from '../src/data-store-sql.js';
import { EventLogSql } from '../src/event-log-sql.js';

describe('SQL Store Test Suite', () => {
  describe('MysqlDialect Support', () => {
    TestSuite.runStoreDependentTests({
      messageStore : new MessageStoreSql(testMysqlDialect),
      dataStore    : new DataStoreSql(testMysqlDialect),
      eventLog     : new EventLogSql(testMysqlDialect),
    });
  });

  describe('PostgresDialect Support', () => {
    TestSuite.runStoreDependentTests({
      messageStore : new MessageStoreSql(testPostgresDialect),
      dataStore    : new DataStoreSql(testPostgresDialect),
      eventLog     : new EventLogSql(testPostgresDialect),
    });
  });

  describe('SqliteDialect Support', () => {
    TestSuite.runStoreDependentTests({
      messageStore : new MessageStoreSql(testSqliteDialect),
      dataStore    : new DataStoreSql(testSqliteDialect),
      eventLog     : new EventLogSql(testSqliteDialect),
    });
  });
});