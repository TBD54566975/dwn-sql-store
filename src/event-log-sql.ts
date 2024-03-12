import type { DwnDatabaseType, KeyValues } from './types.js';
import type { EventLog, Filter, PaginationCursor } from '@tbd54566975/dwn-sdk-js';

import { Dialect } from './dialect/dialect.js';
import { filterSelectQuery } from './utils/filter.js';
import { InsertResult, Kysely, Transaction } from 'kysely';
import { extractTagsAndSanitizeIndexes } from './utils/sanitize.js';
import { executeTagsInsert } from './utils/tags.js';

export class EventLogSql implements EventLog {
  #dialect: Dialect;
  #db: Kysely<DwnDatabaseType> | null = null;

  constructor(dialect: Dialect) {
    this.#dialect = dialect;
  }

  async open(): Promise<void> {
    if (this.#db) {
      return;
    }

    this.#db = new Kysely<DwnDatabaseType>({ dialect: this.#dialect });
    let createTable = this.#db.schema
      .createTable('eventLog')
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
      .addColumn('permissionsGrantId', 'text');
      // "indexes" end

    let createRecordsTagsTable = this.#db.schema
      .createTable('eventLogRecordsTags')
      .ifNotExists()
      .addColumn('eventLogWatermark', 'integer', (col) => this.#dialect.addReferencedColumn(col, 'eventLog', 'watermark'))
      .addColumn('tag', 'text', (col) => col.notNull());

    let createRecordsTagValuesTable = this.#db.schema
      .createTable('eventLogRecordsTagValues')
      .ifNotExists()
      .addColumn('tagId', 'integer', (col) => this.#dialect.addReferencedColumn(col, 'eventLogRecordsTags', 'id'))
      .addColumn('valueString', 'text')
      .addColumn('valueNumber', 'integer');

    // Add columns that have dialect-specific constraints
    createTable = this.#dialect.addAutoIncrementingColumn(createTable, 'watermark', (col) => col.primaryKey());
    createRecordsTagsTable = this.#dialect.addAutoIncrementingColumn(createRecordsTagsTable, 'id', (col) => col.primaryKey());

    await createTable.execute();
    await createRecordsTagsTable.execute();
    await createRecordsTagValuesTable.execute();
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

    await this.#db.transaction().execute(this.executePutTransaction({
      tenant, messageCid, indexes
    }));
  }

  private executePutTransaction(queryOptions: {
    tenant: string;
    messageCid: string;
    indexes: KeyValues;
  }): (tx: Transaction<DwnDatabaseType>) => Promise<InsertResult> {
    const { tenant, messageCid, indexes } = queryOptions;
    const { indexes: appendIndexes, tags } = extractTagsAndSanitizeIndexes(indexes);

    return async (tx) => {
      const result = await tx
        .insertInto('eventLog')
        .values({
          tenant,
          messageCid,
          ...appendIndexes
        })
        .executeTakeFirstOrThrow();

      const messageStoreId = Number(result.insertId);

      // if tags exist, we execute those within the transaction
      if (Object.keys(tags).length > 0) {
        await executeTagsInsert({ store: 'eventLog'  ,tx, tags, id: messageStoreId });
      }

      return result;
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
      .selectFrom('eventLog')
      .leftJoin('eventLogRecordsTags', 'eventLogRecordsTags.eventLogWatermark', 'eventLog.watermark')
      .leftJoin('eventLogRecordsTagValues', 'eventLogRecordsTagValues.tagId', 'eventLogRecordsTags.id')
      .select('messageCid')
      .distinct()
      .select('watermark')
      .where('tenant', '=', tenant);

    if (filters.length > 0) {
      // filter sanitization takes place within `filterSelectQuery`
      query = filterSelectQuery(filters, query);
    }

    if(cursor !== undefined) {
      // eventLog in the sql store uses the watermark cursor value which is a number in SQL
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
      .deleteFrom('eventLog')
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
      .deleteFrom('eventLog')
      .execute();
  }
}