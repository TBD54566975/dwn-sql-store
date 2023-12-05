import type { Database } from './database.js';
import type { EventLog, GetEventsOptions, Filter } from '@tbd54566975/dwn-sdk-js';
import { Kysely } from 'kysely';
import { Dialect } from './dialect/dialect.js';
import { filterSelectQuery } from './utils/filter.js';
import { sanitizeFilters, sanitizeIndexes } from './utils/sanitize.js';

export class EventLogSql implements EventLog {
  #dialect: Dialect;
  #db: Kysely<Database> | null = null;

  constructor(dialect: Dialect) {
    this.#dialect = dialect;
  }

  async open(): Promise<void> {
    if (this.#db) {
      return;
    }

    this.#db = new Kysely<Database>({ dialect: this.#dialect });
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

    // Add columns that have dialect-specific constraints
    createTable = this.#dialect.addAutoIncrementingColumn(createTable, 'watermark', (col) => col.primaryKey());

    await createTable.execute();
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

    sanitizeIndexes(indexes);

    await this.#db
      .insertInto('eventLog')
      .values({
        tenant,
        messageCid,
        ...indexes
      }).execute();
  }

  async getEvents(
    tenant: string,
    options?: GetEventsOptions
  ): Promise<string[]> {

    // get events is simply a query without any filters. gets all events beyond the cursor.
    return this.queryEvents(tenant, [], options?.cursor);
  }

  async queryEvents(
    tenant: string,
    filters: Filter[],
    cursor?: string
  ): Promise<string[]> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `queryEvents`.'
      );
    }

    let query = this.#db
      .selectFrom('eventLog')
      .select('messageCid')
      .where('tenant', '=', tenant);

    if (filters.length > 0) {
      // sqlite3 dialect does not support `boolean` types, so we convert the filter to match our index
      sanitizeFilters(filters);
      query = filterSelectQuery(filters, query);
    }

    if(cursor !== undefined) {
      const messageCid = cursor;
      query = query.where(({ eb, selectFrom }) => {

        // fetch the watermark of the messageCid cursor
        const cursor = selectFrom('eventLog')
          .select('watermark')
          .where('tenant', '=', tenant)
          .where('messageCid', '=', messageCid)
          .limit(1);

        return eb('watermark', '>' , cursor);
      });
    }

    query = query.orderBy('watermark', 'asc');

    const events: string[] = [];
    if (this.#dialect.isStreamingSupported) {
      for await (let { messageCid } of query.stream()) {
        events.push(messageCid);
      }
    } else {
      const results = await query.execute();
      for (let { messageCid } of results) {
        events.push(messageCid);
      }
    }

    return events;
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