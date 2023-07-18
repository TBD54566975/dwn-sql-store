import { AssociateResult, DataStore, DataStream, GetResult, PutResult } from '@tbd54566975/dwn-sdk-js';
import { Kysely } from 'kysely';
import { Readable } from 'readable-stream';
import { Database } from './database.js';
import { Cid } from '@tbd54566975/dwn-sdk-js';
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

    let createTable = this.#db.schema
      .createTable('dataStore')
      .ifNotExists()
      .addColumn('tenant', 'text', (col) => col.notNull())
      .addColumn('dataCid', 'varchar(60)', (col) => col.notNull());

    let createReferenceTable = this.#db.schema
      .createTable('dataStoreReferences')
      .ifNotExists()
      .addColumn('tenant', 'text', (col) => col.notNull())
      .addColumn('dataCid', 'varchar(60)', (col) => col.notNull())
      .addColumn('messageCid', 'varchar(60)', (col) => col.notNull());

    // Add columns that have dialect-specific constraints
    createTable = this.#dialect.addAutoIncrementingColumn(createTable, 'id', (col) => col.primaryKey());
    createTable = this.#dialect.addBlobColumn(createTable, 'data', (col) => col.notNull());
    createReferenceTable = this.#dialect.addAutoIncrementingColumn(createReferenceTable, 'id', (col) => col.primaryKey());

    await createTable.execute();
    await createReferenceTable.execute();
  }

  async close(): Promise<void> {
    await this.#db?.destroy();
    this.#db = null;
  }

  async get(
    tenant: string,
    messageCid: string,
    dataCid: string
  ): Promise<GetResult | undefined> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `get`.'
      );
    }

    const hasReferenceToData = await this.#db
      .selectFrom('dataStoreReferences')
      .selectAll()
      .where('tenant', '=', tenant)
      .where('messageCid', '=', messageCid)
      .where('dataCid', '=', dataCid)
      .executeTakeFirst()
      .then((result) => result !== undefined);

    if (!hasReferenceToData) {
      return undefined;
    }

    const result = await this.#db
      .selectFrom('dataStore')
      .selectAll()
      .where('tenant', '=', tenant)
      .where('dataCid', '=', dataCid)
      .executeTakeFirst();

    if (!result) {
      return undefined;
    }

    return {
      dataCid    : result.dataCid,
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
    messageCid: string,
    dataCid: string,
    dataStream: Readable
  ): Promise<PutResult> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `put`.'
      );
    }

    const bytes = await DataStream.toBytes(dataStream);
    const data = Buffer.from(bytes);

    await this.#db
      .insertInto('dataStore')
      .values({ tenant, dataCid, data })
      .executeTakeFirstOrThrow();

    await this.#db
      .insertInto('dataStoreReferences')
      .values({ tenant, messageCid, dataCid })
      .executeTakeFirstOrThrow();

    return {
      dataCid  : await Cid.computeDagPbCidFromBytes(bytes),
      dataSize : bytes.length
    };
  }

  async associate(
    tenant: string,
    messageCid: string,
    dataCid: string
  ): Promise<AssociateResult | undefined> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `associate`.'
      );
    }

    const dataRecord = await this.#db
      .selectFrom('dataStore')
      .selectAll()
      .where('tenant', '=', tenant)
      .where('dataCid', '=', dataCid)
      .executeTakeFirst();

    if (!dataRecord) {
      return undefined;
    }

    const hasExistingReference = await this.#db
      .selectFrom('dataStoreReferences')
      .selectAll()
      .where('tenant', '=', tenant)
      .where('messageCid', '=', messageCid)
      .where('dataCid', '=', dataCid)
      .execute()
      .then((results) => results.length !== 0);

    if (!hasExistingReference) {
      // This message doesn't have a reference to the data. Make one!
      await this.#db
        .insertInto('dataStoreReferences')
        .values({ tenant, messageCid, dataCid })
        .executeTakeFirstOrThrow();
    }

    return {
      dataCid  : dataCid,
      dataSize : dataRecord.data.length
    };
  }

  async delete(
    tenant: string,
    messageCid: string,
    dataCid: string
  ): Promise<void> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `delete`.'
      );
    }

    // Delete the message's reference to the data
    await this.#db
      .deleteFrom('dataStoreReferences')
      .where('tenant', '=', tenant)
      .where('messageCid', '=', messageCid)
      .where('dataCid', '=', dataCid)
      .execute();

    const wasLastReferenceToData = await this.#db
      .selectFrom('dataStoreReferences')
      .selectAll()
      .where('tenant', '=', tenant)
      .where('dataCid', '=', dataCid)
      .execute()
      .then((results) => results.length === 0);

    if (wasLastReferenceToData) {
      // Delete the data from the dataStore, no other messages reference it
      await this.#db
        .deleteFrom('dataStore')
        .where('tenant', '=', tenant)
        .where('dataCid', '=', dataCid)
        .execute();
    }
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

    await this.#db
      .deleteFrom('dataStoreReferences')
      .execute();
  }

}