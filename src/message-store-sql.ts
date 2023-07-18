import { executeUnlessAborted, Filter, GenericMessage, MessageStore, MessageStoreOptions } from '@tbd54566975/dwn-sdk-js';
import { Kysely } from 'kysely';
import { Database } from './database.js';
import * as block from 'multiformats/block';
import * as cbor from '@ipld/dag-cbor';
import { sha256 } from 'multiformats/hashes/sha2';
import { Dialect } from './dialect/dialect.js';
import { filterSelectQuery } from './utils/filter.js';
import { sanitizeRecords } from './utils/sanitize.js';


export class MessageStoreSql implements MessageStore {
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
      .createTable('messageStore')
      .ifNotExists()
      .addColumn('tenant', 'text', (col) => col.notNull())
      .addColumn('messageCid', 'varchar(60)', (col) => col.notNull())
      // "indexes" start
      .addColumn('interface', 'text')
      .addColumn('method', 'text')
      .addColumn('schema', 'text')
      .addColumn('dataCid', 'text')
      .addColumn('dataSize', 'text')
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
    createTable = this.#dialect.addAutoIncrementingColumn(createTable, 'id', (col) => col.primaryKey());
    createTable = this.#dialect.addBlobColumn(createTable, 'encodedMessageBytes', (col) => col.notNull());

    await createTable.execute();
  }

  async close(): Promise<void> {
    await this.#db?.destroy();
    this.#db = null;
  }

  async put(
    tenant: string,
    message: GenericMessage,
    indexes: Record<string, string>,
    options?: MessageStoreOptions
  ): Promise<void> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `put`.'
      );
    }

    options?.signal?.throwIfAborted();

    const encodedMessageBlock = await executeUnlessAborted(
      block.encode({ value: message, codec: cbor, hasher: sha256}),
      options?.signal
    );

    const messageCid = encodedMessageBlock.cid.toString();
    const encodedMessageBytes = Buffer.from(encodedMessageBlock.bytes);
    sanitizeRecords(indexes);

    await executeUnlessAborted(
      this.#db
        .insertInto('messageStore')
        .values({
          tenant,
          messageCid,
          encodedMessageBytes,
          ...indexes,
        })
        .executeTakeFirstOrThrow(),
      options?.signal
    );
  }

  async get(
    tenant: string,
    cid: string,
    options?: MessageStoreOptions
  ): Promise<GenericMessage | undefined> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `get`.'
      );
    }

    options?.signal?.throwIfAborted();

    const result = await executeUnlessAborted(
      this.#db
        .selectFrom('messageStore')
        .selectAll()
        .where('tenant', '=', tenant)
        .where('messageCid', '=', cid)
        .executeTakeFirst(),
      options?.signal
    );

    if (!result) {
      return undefined;
    }

    return this.parseEncodedMessage(result.encodedMessageBytes, options);
  }

  async query(
    tenant: string,
    filter: Filter,
    options?: MessageStoreOptions
  ): Promise<GenericMessage[]> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `query`.'
      );
    }

    options?.signal?.throwIfAborted();

    let query = this.#db
      .selectFrom('messageStore')
      .selectAll()
      .where('tenant', '=', tenant);

    query = filterSelectQuery(filter, query);

    const results = await executeUnlessAborted(
      query.execute(),
      options?.signal
    );

    const messages = results.map(async (result) => {
      return this.parseEncodedMessage(result.encodedMessageBytes, options);
    });

    return await Promise.all(messages);
  }

  async delete(
    tenant: string,
    cid: string,
    options?: MessageStoreOptions
  ): Promise<void> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `delete`.'
      );
    }

    options?.signal?.throwIfAborted();

    await executeUnlessAborted(
      this.#db
        .deleteFrom('messageStore')
        .where('tenant', '=', tenant)
        .where('messageCid', '=', cid)
        .execute(),
      options?.signal
    );
  }

  async clear(): Promise<void> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `clear`.'
      );
    }

    await this.#db
      .deleteFrom('messageStore')
      .execute();
  }

  private async parseEncodedMessage(
    encodedMessageBytes: Uint8Array,
    options?: MessageStoreOptions
  ): Promise<GenericMessage> {
    options?.signal?.throwIfAborted();

    const decodedBlock = await block.decode({
      bytes  : encodedMessageBytes,
      codec  : cbor,
      hasher : sha256
    });

    const message = decodedBlock.value as GenericMessage;
    return message;
  }

}