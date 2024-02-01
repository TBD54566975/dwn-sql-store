import { DataStore, DataStream, DataStoreGetResult, DataStorePutResult } from '@tbd54566975/dwn-sdk-js';
import { Kysely } from 'kysely';
import { Readable } from 'readable-stream';
import { Database } from './database.js';
import { Dialect } from './dialect/dialect.js';

export class DataStoreSql implements DataStore {
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

    let table = this.#db.schema
      .createTable('dataStore')
      .ifNotExists()
      .addColumn('tenant', 'text', (col) => col.notNull())
      .addColumn('recordId', 'varchar(60)', (col) => col.notNull())
      .addColumn('dataCid', 'varchar(60)', (col) => col.notNull());

    // Add columns that have dialect-specific constraints
    table = this.#dialect.addAutoIncrementingColumn(table, 'id', (col) => col.primaryKey());
    table = this.#dialect.addBlobColumn(table, 'data', (col) => col.notNull());
    await table.execute();

    // Add index for efficient lookups.
    this.#db.schema.createIndex('tenant_recordId_dataCid')
      .on('dataStore')
      .columns(['tenant', 'recordId', 'dataCid'])
      .ifNotExists()
      .execute();
  }

  async close(): Promise<void> {
    await this.#db?.destroy();
    this.#db = null;
  }

  async get(
    tenant: string,
    recordId: string,
    dataCid: string
  ): Promise<DataStoreGetResult | undefined> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `get`.'
      );
    }

    const result = await this.#db
      .selectFrom('dataStore')
      .selectAll()
      .where('tenant', '=', tenant)
      .where('recordId', '=', recordId)
      .where('dataCid', '=', dataCid)
      .executeTakeFirst();

    if (!result) {
      return undefined;
    }

    return {
      dataSize   : result.data.length,
      dataStream : new Readable({
        read() {
          this.push(Buffer.from(result.data));
          this.push(null);
        }
      }),
    };
  }

  async put(
    tenant: string,
    recordId: string,
    dataCid: string,
    dataStream: Readable
  ): Promise<DataStorePutResult> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `put`.'
      );
    }

    const bytes = await DataStream.toBytes(dataStream);
    const data = Buffer.from(bytes);

    await this.#db
      .insertInto('dataStore')
      .values({ tenant, recordId, dataCid, data })
      .executeTakeFirstOrThrow();

    return {
      dataSize: bytes.length
    };
  }

  async delete(
    tenant: string,
    recordId: string,
    dataCid: string
  ): Promise<void> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `delete`.'
      );
    }

    // Delete the data from the dataStore, no other messages reference it
    await this.#db
      .deleteFrom('dataStore')
      .where('tenant', '=', tenant)
      .where('recordId', '=', recordId)
      .where('dataCid', '=', dataCid)
      .execute();
  }

  async clear(): Promise<void> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `clear`.'
      );
    }

    await this.#db
      .deleteFrom('dataStore')
      .execute();
  }

}