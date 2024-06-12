import type { DwnDatabaseType, KeyValues } from './types.js';
import type { EventLog, Filter, PaginationCursor } from '@tbd54566975/dwn-sdk-js';

import { Dialect } from './dialect/dialect.js';
import { filterSelectQuery } from './utils/filter.js';
import { Kysely, Transaction } from 'kysely';
import { executeWithRetryIfDatabaseIsLocked } from './utils/transaction.js';
import { extractTagsAndSanitizeIndexes } from './utils/sanitize.js';
import { TagTables } from './utils/tags.js';

export class EventLogSql implements EventLog {
  #dialect: Dialect;
  #db: Kysely<DwnDatabaseType> | null = null;
  #tags: TagTables;

  constructor(dialect: Dialect) {
    this.#dialect = dialect;
    this.#tags = new TagTables(dialect, 'eventLogMessages');
  }

  async open(): Promise<void> {
    if (this.#db) {
      return;
    }

    this.#db = new Kysely<DwnDatabaseType>({ dialect: this.#dialect });
    let createTable = this.#db.schema
      .createTable('eventLogMessages')
      .ifNotExists()
      .addColumn('tenant', 'text', (col) => col.notNull())
      .addColumn('messageCid', 'varchar(60)', (col) => col.notNull())
      // "indexes" start
      .addColumn('interface', 'text')
      .addColumn('method', 'text')
      .addColumn('schema', 'text')
      .addColumn('dataCid', 'text')
      .addColumn('dataSize', 'integer')
      .addColumn('dateCreated', 'text')
      .addColumn('delegated', 'text')
      .addColumn('messageTimestamp', 'text')
      .addColumn('dataFormat', 'text')
      .addColumn('isLatestBaseState', 'text')
      .addColumn('published', 'text')
      .addColumn('author', 'text')
      .addColumn('recordId', 'text')
      .addColumn('entryId', 'text')
      .addColumn('datePublished', 'text')
      .addColumn('latest', 'text')
      .addColumn('protocol', 'text')
      .addColumn('dateExpires', 'text')
      .addColumn('description', 'text')
      .addColumn('grantedTo', 'text')
      .addColumn('grantedBy', 'text')
      .addColumn('grantedFor', 'text')
      .addColumn('permissionsRequestId', 'text')
      .addColumn('attester', 'text')
      .addColumn('protocolPath', 'text')
      .addColumn('recipient', 'text')
      .addColumn('contextId', 'text')
      .addColumn('parentId', 'text')
      .addColumn('permissionGrantId', 'text')
      .addColumn('prune', 'text');
      // "indexes" end

    let createRecordsTagsTable = this.#db.schema
      .createTable('eventLogRecordsTags')
      .ifNotExists()
      .addColumn('tag', 'text', (col) => col.notNull())
      .addColumn('valueString', 'text')
      .addColumn('valueNumber', 'decimal');
    // Add columns that have dialect-specific constraints
    createTable = this.#dialect.addAutoIncrementingColumn(createTable, 'watermark', (col) => col.primaryKey());
    createRecordsTagsTable = this.#dialect.addAutoIncrementingColumn(createRecordsTagsTable, 'id', (col) => col.primaryKey());
    createRecordsTagsTable = this.#dialect.addReferencedColumn(createRecordsTagsTable, 'eventLogRecordsTags', 'eventWatermark', 'integer', 'eventLogMessages', 'watermark', 'cascade');

    await createTable.execute();
    await createRecordsTagsTable.execute();
  }

  async close(): Promise<void> {
    await this.#db?.destroy();
    this.#db = null;
  }

  async append(
    tenant: string,
    messageCid: string,
    indexes: Record<string, string | boolean | number>
  ): Promise<void> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `append`.'
      );
    }

    // we execute the insert in a transaction as we are making multiple inserts into multiple tables.
    // if any of these inserts would throw, the whole transaction would be rolled back.
    // otherwise it is committed.
    const putEventOperation = this.constructPutEventOperation({ tenant, messageCid, indexes });
    await executeWithRetryIfDatabaseIsLocked(this.#db, putEventOperation);
  }

  /**
   * Constructs a transactional operation to insert an event into the database.
   */
  private constructPutEventOperation(queryOptions: {
    tenant: string;
    messageCid: string;
    indexes: KeyValues;
  }): (tx: Transaction<DwnDatabaseType>) => Promise<void> {
    const { tenant, messageCid, indexes } = queryOptions;

    // we extract the tag indexes into their own object to be inserted separately.
    // we also sanitize the indexes to convert any `boolean` values to `text` representations.
    const { indexes: appendIndexes, tags } = extractTagsAndSanitizeIndexes(indexes);

    return async (tx) => {

      const eventIndexValues = {
        tenant,
        messageCid,
        ...appendIndexes,
      };

      // we use the dialect-specific `insertThenReturnId` in order to be able to extract the `insertId`
      const result = await this.#dialect
        .insertThenReturnId(tx, 'eventLogMessages', eventIndexValues, 'watermark as insertId')
        .executeTakeFirstOrThrow();

      // if tags exist, we execute those within the transaction associating them with the `insertId`.
      if (Object.keys(tags).length > 0) {
        await this.#tags.executeTagsInsert(result.insertId, tags, tx);
      }
    };
  }

  async getEvents(
    tenant: string,
    cursor?: PaginationCursor
  ): Promise<{events: string[], cursor?: PaginationCursor }> {

    // get events is simply a query without any filters. gets all events beyond the cursor.
    return this.queryEvents(tenant, [], cursor);
  }

  async queryEvents(
    tenant: string,
    filters: Filter[],
    cursor?: PaginationCursor
  ): Promise<{events: string[], cursor?: PaginationCursor }> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `queryEvents`.'
      );
    }

    let query = this.#db
      .selectFrom('eventLogMessages')
      .leftJoin('eventLogRecordsTags', 'eventLogRecordsTags.eventWatermark', 'eventLogMessages.watermark')
      .select('messageCid')
      .distinct()
      .select('watermark')
      .where('tenant', '=', tenant);

    if (filters.length > 0) {
      // filter sanitization takes place within `filterSelectQuery`
      query = filterSelectQuery(filters, query);
    }

    if(cursor !== undefined) {
      // eventLogMessages in the sql store uses the watermark cursor value which is a number in SQL
      // if not we will return empty results
      const cursorValue = cursor.value as number;
      const cursorMessageCid = cursor.messageCid;

      query = query.where(({ eb, refTuple, tuple }) => {
        // https://kysely-org.github.io/kysely-apidoc/interfaces/ExpressionBuilder.html#refTuple
        return eb(refTuple('watermark', 'messageCid'), '>', tuple(cursorValue, cursorMessageCid));
      });
    }

    query = query.orderBy('watermark', 'asc').orderBy('messageCid', 'asc');

    const events: string[] = [];
    // we always return a cursor with the event log query, so we set the return cursor to the properties of the last item.
    let returnCursor: PaginationCursor | undefined;
    if (this.#dialect.isStreamingSupported) {
      for await (let { messageCid, watermark: value } of query.stream()) {
        events.push(messageCid);
        returnCursor = { messageCid, value };
      }
    } else {
      const results = await query.execute();
      for (let { messageCid, watermark: value } of results) {
        events.push(messageCid);
        returnCursor = { messageCid, value };
      }
    }

    return { events, cursor: returnCursor };
  }

  async deleteEventsByCid(
    tenant: string,
    messageCids: Array<string>
  ): Promise<void> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `deleteEventsByCid`.'
      );
    }

    if (messageCids.length === 0) {
      return;
    }

    await this.#db
      .deleteFrom('eventLogMessages')
      .where('tenant', '=', tenant)
      .where('messageCid', 'in', messageCids)
      .execute();
  }

  async clear(): Promise<void> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `clear`.'
      );
    }

    await this.#db
      .deleteFrom('eventLogMessages')
      .execute();
  }
}