import type { Database } from './database.js';
import type { EventLog, Event, GetEventsOptions } from '@tbd54566975/dwn-sdk-js';
import { Kysely } from 'kysely';
import { Dialect } from './dialect/dialect.js';

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
      .addColumn('messageCid', 'varchar(60)', (col) => col.notNull());

    // Add columns that have dialect-specific constraints
    createTable = this.#dialect.addAutoIncrementingColumn(createTable, 'id', (col) => col.primaryKey());

    await createTable.execute();
  }

  async close(): Promise<void> {
    await this.#db?.destroy();
    this.#db = null;
  }

  async append(
    tenant: string,
    messageCid: string
  ): Promise<string> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `append`.'
      );
    }

    const result = await this.#db
      .insertInto('eventLog')
      .values({ tenant, messageCid })
      .executeTakeFirstOrThrow();

    return result.insertId?.toString() ?? '';
  }

  async getEvents(
    tenant: string,
    options?: GetEventsOptions
  ): Promise<Array<Event>> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `getEvents`.'
      );
    }

    let query = this.#db
      .selectFrom('eventLog')
      .selectAll()
      .where('tenant', '=', tenant);

    if (options && options.gt) {
      query = query.where('id', '>', parseInt(options.gt));
    }

    const events: Event[] = [];

    if (this.#dialect.isStreamingSupported) {
      for await (let result of query.stream()) {
        events.push({
          watermark  : result.id.toString(),
          messageCid : result.messageCid
        });
      }
    } else {
      const results = await query.execute();
      for (let result of results) {
        events.push({
          watermark  : result.id.toString(),
          messageCid : result.messageCid
        });
      }
    }

    return events;
  }

  async deleteEventsByCid(
    tenant: string,
    cids: string[]
  ): Promise<number> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `deleteEventsByCid`.'
      );
    }

    if (cids.length === 0) {
      return 0;
    }

    const result = await this.#db
      .deleteFrom('eventLog')
      .where('tenant', '=', tenant)
      .where('messageCid', 'in', cids)
      .executeTakeFirstOrThrow();

    // TODO: numDeletedRows is a bigint. need to decide what to return here
    return result.numDeletedRows as any;
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