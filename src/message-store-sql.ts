import {
  DwnInterfaceName,
  DwnMethodName,
  executeUnlessAborted,
  Filter,
  GenericMessage,
  Message,
  MessageStore,
  MessageStoreOptions,
  MessageSort,
  Pagination,
  SortOrder
} from '@tbd54566975/dwn-sdk-js';
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
      .addColumn('encodedData', 'text') // we optionally store encoded data if it is below a threshold
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

    // gets the encoded data and removes it from the message
    const getEncodedData = (message: GenericMessage): { message: GenericMessage, encodedData: string|null} => {
      let encodedData: string|null = null;
      if (message.descriptor.interface === DwnInterfaceName.Records && message.descriptor.method === DwnMethodName.Write) {
        const data = (message as any).encodedData as string|undefined;
        if(data) {
          delete (message as any).encodedData;
          encodedData = data;
        }
      }
      return { message, encodedData };
    };

    const { message: messageToProcess, encodedData} = getEncodedData(message);

    const encodedMessageBlock = await executeUnlessAborted(
      block.encode({ value: messageToProcess, codec: cbor, hasher: sha256}),
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
          encodedData,
          ...indexes
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

    return this.parseEncodedMessage(result.encodedMessageBytes, result.encodedData, options);
  }

  async query(
    tenant: string,
    filters: Filter[],
    messageSort?: MessageSort,
    pagination?: Pagination,
    options?: MessageStoreOptions
  ): Promise<{ messages: GenericMessage[], paginationMessageCid?: string }> {
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

    // if query is sorted by date published, only show records which are published
    if(messageSort?.datePublished !== undefined) {
      query = query.where('published', '=', 'true');
    }

    // add filters to query
    query = filterSelectQuery(filters, query);

    // extract sort property and direction from the supplied messageSort
    const { property: sortProperty, direction: sortDirection } = this.getOrderBy(messageSort);

    if(pagination?.messageCid !== undefined) {
      const messageCid = pagination.messageCid;
      query = query.where(({ eb, selectFrom, refTuple }) => {
        const direction = sortDirection === SortOrder.Ascending ? '>' : '<';

        // fetches the cursor as a sort property tuple from the database based on the messageCid.
        const cursor = selectFrom('messageStore')
          .select([sortProperty, 'messageCid'])
          .where('tenant', '=', tenant)
          .where('messageCid', '=', messageCid)
          .limit(1).$asTuple(sortProperty, 'messageCid');

        // https://kysely-org.github.io/kysely-apidoc/interfaces/ExpressionBuilder.html#refTuple
        return eb(refTuple(sortProperty, 'messageCid'), direction, cursor);
      });
    }

    // sorting by the provided sort property, the tiebreak is always in ascending order regardless of sort
    query =  query
      .orderBy(sortProperty, sortDirection === SortOrder.Ascending ? 'asc' : 'desc')
      .orderBy('messageCid', 'asc');

    if (pagination?.limit !== undefined && pagination?.limit > 0) {
      // we query for one additional record to decide if we return a pagination cursor or not.
      query = query.limit(pagination.limit + 1);
    }

    const results = await executeUnlessAborted(
      query.execute(),
      options?.signal
    );

    // extracts the full encoded message from the stored blob for each result item.
    const messages: Promise<GenericMessage>[] = results.map((r:any) => this.parseEncodedMessage(r.encodedMessageBytes, r.encodedData, options));

    // returns the pruned the messages, since we have and additional record from above, and a potential paginationMessageCid
    return this.getPaginationResults(messages,  pagination?.limit);
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
    encodedData: string | null | undefined,
    options?: MessageStoreOptions
  ): Promise<GenericMessage> {
    options?.signal?.throwIfAborted();

    const decodedBlock = await block.decode({
      bytes  : encodedMessageBytes,
      codec  : cbor,
      hasher : sha256
    });

    const message = decodedBlock.value as GenericMessage;
    // If encodedData is stored within the MessageStore we include it in the response.
    // We store encodedData when the data is below a certain threshold.
    // https://github.com/TBD54566975/dwn-sdk-js/pull/456
    if (message !== undefined && encodedData !== undefined && encodedData !== null) {
      (message as any).encodedData = encodedData;
    }
    return message;
  }

  /**
   * Gets the pagination Message Cid if there are additional messages to paginate.
   * Accepts more messages than the limit, as we query for additional records to check if we should paginate.
   *
   * @param messages a list of messages, potentially larger than the provided limit.
   * @param limit the maximum number of messages to be returned
   *
   * @returns the pruned message results and an optional paginationMessageCid
   */
  private async getPaginationResults(
    messages: Promise<GenericMessage>[], limit?: number
  ): Promise<{ messages: GenericMessage[], paginationMessageCid?: string }>{
    if (limit !== undefined && messages.length > limit) {
      messages = messages.slice(0, limit);
      const lastMessage = messages.at(-1);
      return {
        messages             : await Promise.all(messages),
        paginationMessageCid : lastMessage ? await Message.getCid(await lastMessage) : undefined
      };
    }

    return { messages: await Promise.all(messages) };
  }

  private getOrderBy(
    messageSort?: MessageSort
  ):{ property: 'dateCreated' | 'datePublished' | 'messageTimestamp', direction: SortOrder } {
    if(messageSort?.dateCreated !== undefined)  {
      return  { property: 'dateCreated', direction: messageSort.dateCreated };
    } else if(messageSort?.datePublished !== undefined) {
      return  { property: 'datePublished', direction: messageSort.datePublished };
    } else if (messageSort?.messageTimestamp !== undefined) {
      return  { property: 'messageTimestamp', direction: messageSort.messageTimestamp };
    } else {
      return  { property: 'messageTimestamp', direction: SortOrder.Ascending };
    }
  }
}