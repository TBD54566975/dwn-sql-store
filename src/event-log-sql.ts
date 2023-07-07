import type { Generated } from 'kysely';
import type { EventLog, Event } from '@tbd54566975/dwn-sdk-js';

import * as mysql2 from 'mysql2';

import { Kysely, MysqlDialect } from 'kysely';

interface EventLogTable {
  id: Generated<number>;
  tenant: string;
  messageCid: string;
}

interface Database {
  eventlog: EventLogTable;
}

type GetEventsOpts = {
  gt: string;
}

export class EventLogSql implements EventLog {
  #db: Kysely<Database>;
  #mysqlConnectionPool: mysql2.Pool;

  constructor() {
    // TODO: have `Kysely.Dialect` passed in as an arg by caller so that caller can choose
    //       sql flavor and provide connection details
    this.#mysqlConnectionPool = mysql2.createPool({
      host     : 'localhost',
      port     : 3306,
      database : 'dwn',
      user     : 'root',
      password : 'dwn'
    });

    this.#db = new Kysely<Database>({
      dialect: new MysqlDialect({
        pool: this.#mysqlConnectionPool
      })
    });
  }
  async open(): Promise<void> {
    // const connection = await this.#mysqlConnectionPool.getConnection();
    // console.log('pinging mysql..');
    // await connection.ping();
    // console.log('ping successful! creating table if not exists');

    await this.#db.schema
      .createTable('eventlog')
      .ifNotExists()
      .addColumn('id', 'integer', (col) => col.autoIncrement().primaryKey())
      .addColumn('tenant', 'text', (col) => col.notNull())
      .addColumn('messageCid', 'varchar(60)', (col) => col.notNull())
      .execute();
  }

  close(): Promise<void> {
    this.#mysqlConnectionPool.end();

    return;
  }

  async append(tenant: string, messageCid: string): Promise<string> {
    const result = await this.#db
      .insertInto('eventlog')
      .values({ tenant, messageCid })
      .executeTakeFirstOrThrow();

    return result.insertId.toString();
  }

  async getEvents(tenant: string, options?: GetEventsOpts): Promise<Array<Event>> {
    const query = this.#db
      .selectFrom('eventlog')
      .selectAll()
      .where('tenant', '=', tenant);

    if (options && options.gt) {
      query.where('id', '>', parseInt(options.gt));
    }

    const events: Event[] = [];
    for await (let dbEvent of query.stream()) {
      const event: Event = {
        watermark  : dbEvent.id.toString(),
        messageCid : dbEvent.messageCid
      };

      events.push(event);
    }

    return events;
  }

  async deleteEventsByCid(tenant: string, cids: string[]): Promise<number> {
    const result = await this.#db
      .deleteFrom('eventlog')
      .where('tenant', '=', tenant)
      .where('messageCid', 'in', cids)
      .executeTakeFirstOrThrow();

    // TODO: numDeletedRows is a bigint. need to decide what to return here
    return result.numDeletedRows as any;
  }

  async clear(): Promise<void> {
    throw new Error('Method not implemented.');
  }
}