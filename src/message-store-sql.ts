import {
  DwnInterfaceName,
  DwnMethodName,
  executeUnlessAborted,
  Filter,
  GenericMessage,
  MessageStore,
  MessageStoreOptions,
  MessageSort,
  Pagination,
  SortDirection,
  PaginationCursor,
} from '@tbd54566975/dwn-sdk-js';

import { Kysely, Transaction } from 'kysely';
import { DwnDatabaseType, KeyValues } from './types.js';
import * as block from 'multiformats/block';
import * as cbor from '@ipld/dag-cbor';
import { sha256 } from 'multiformats/hashes/sha2';
import { Dialect } from './dialect/dialect.js';
import { filterSelectQuery } from './utils/filter.js';
import { extractTagsAndSanitizeIndexes } from './utils/sanitize.js';
import { TagTables } from './utils/tags.js';


export class MessageStoreSql implements MessageStore {
  #dialect: Dialect;
  #tags: TagTables;
  #db: Kysely<DwnDatabaseType> | null = null;

  constructor(dialect: Dialect) {
    this.#dialect = dialect;
    this.#tags = new TagTables(dialect, 'messageStoreMessages');
  }

  async open(): Promise<void> {
    if (this.#db) {
      return;
    }

    this.#db = new Kysely<DwnDatabaseType>({ dialect: this.#dialect });
    let createTable = this.#db.schema
      .createTable('messageStoreMessages')
      .ifNotExists()
      .addColumn('tenant', 'varchar(255)', (col) => col.notNull())
      .addColumn('messageCid', 'varchar(60)', (col) => col.notNull())
      .addColumn('encodedData', 'text') // we optionally store encoded data if it is below a threshold
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
      .addColumn('recordId', 'varchar(60)')
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
      .createTable('messageStoreRecordsTags')
      .ifNotExists()
      .addColumn('tag', 'text', (col) => col.notNull())
      .addColumn('valueString', 'text')
      .addColumn('valueNumber', 'integer');

    // Add columns that have dialect-specific constraints
    createTable = this.#dialect.addAutoIncrementingColumn(createTable, 'id', (col) => col.primaryKey());
    createTable = this.#dialect.addBlobColumn(createTable, 'encodedMessageBytes', (col) => col.notNull());
    createRecordsTagsTable = this.#dialect.addAutoIncrementingColumn(createRecordsTagsTable, 'id', (col) => col.primaryKey());
    createRecordsTagsTable = this.#dialect.addReferencedColumn(createRecordsTagsTable, 'messageStoreRecordsTags', 'messageInsertId', 'integer', 'messageStoreMessages', 'id', 'cascade');

    await createTable.execute();
    await createRecordsTagsTable.execute();
  }

  async close(): Promise<void> {
    await this.#db?.destroy();
    this.#db = null;
  }

  async put(
    tenant: string,
    message: GenericMessage,
    indexes: KeyValues,
    options?: MessageStoreOptions
  ): Promise<void> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `put`.'
      );
    }

    options?.signal?.throwIfAborted();

    // gets the encoded data and removes it from the message
    // we remove it from the message as it would cause the `encodedMessageBytes` to be greater than the
    // maximum bytes allowed by SQL
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

    // we execute the insert in a transaction as we are making multiple inserts into multiple tables.
    // if any of these inserts would throw, the whole transaction would be rolled back.
    // otherwise it is committed.
    await this.#db.transaction().execute(this.executePutTransaction({
      tenant, messageCid, encodedMessageBytes, encodedData, indexes
    }));
  }

  private executePutTransaction(queryOptions: {
    tenant: string;
    messageCid: string;
    encodedMessageBytes: Buffer;
    encodedData: string | null;
    indexes: KeyValues;
  }): (tx: Transaction<DwnDatabaseType>) => Promise<void> {
    const { tenant, messageCid, encodedMessageBytes, encodedData, indexes } = queryOptions;

    // we extract the tag indexes into their own object to be inserted separately.
    // we also sanitize the indexes to convert any `boolean` values to `text` representations.
    const { indexes: putIndexes, tags } = extractTagsAndSanitizeIndexes(indexes);

    return async (tx) => {

      const messageIndexValues = {
        tenant,
        messageCid,
        encodedMessageBytes,
        encodedData,
        ...putIndexes
      };

      // we use the dialect-specific `insertThenReturnId` in order to be able to extract the `insertId`
      const result = await this.#dialect
        .insertThenReturnId(tx, 'messageStoreMessages', messageIndexValues, 'id as insertId')
        .executeTakeFirstOrThrow();

      // if tags exist, we execute those within the transaction associating them with the `insertId`.
      if (Object.keys(tags).length > 0) {
        await this.#tags.executeTagsInsert(result.insertId, tags, tx);
      }

    };
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
        .selectFrom('messageStoreMessages')
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
  ): Promise<{ messages: GenericMessage[], cursor?: PaginationCursor}> {
    if (!this.#db) {
      throw new Error(
        'Connection to database not open. Call `open` before using `query`.'
      );
    }

    options?.signal?.throwIfAborted();

    // extract sort property and direction from the supplied messageSort
    const { property: sortProperty, direction: sortDirection } = this.extractSortProperties(messageSort);

    let query = this.#db
      .selectFrom('messageStoreMessages')
      .leftJoin('messageStoreRecordsTags', 'messageStoreRecordsTags.messageInsertId', 'messageStoreMessages.id')
      .select('messageCid')
      .distinct()
      .select([
        'encodedMessageBytes',
        'encodedData',
        sortProperty,
      ])
      .where('tenant', '=', tenant);

    // filter sanitization takes place within `filterSelectQuery`
    query = filterSelectQuery(filters, query);

    if(pagination?.cursor !== undefined) {
      // currently the sort property is explicitly either `dateCreated` | `messageTimestamp` | `datePublished` which are all strings
      // TODO: https://github.com/TBD54566975/dwn-sdk-js/issues/664 to handle the edge case
      const cursorValue = pagination.cursor.value as string;
      const cursorMessageId = pagination.cursor.messageCid;

      query = query.where(({ eb, refTuple, tuple }) => {
        const direction = sortDirection === SortDirection.Ascending ? '>' : '<';
        // https://kysely-org.github.io/kysely-apidoc/interfaces/ExpressionBuilder.html#refTuple
        return eb(refTuple(sortProperty, 'messageCid'), direction, tuple(cursorValue, cursorMessageId));
      });
    }

    const orderDirection = sortDirection === SortDirection.Ascending ? 'asc' : 'desc';
    // sorting by the provided sort property, the tiebreak is always in ascending order regardless of sort
    query =  query
      .orderBy(sortProperty, orderDirection)
      .orderBy('messageCid', orderDirection);

    if (pagination?.limit !== undefined && pagination?.limit > 0) {
      // we query for one additional record to decide if we return a pagination cursor or not.
      query = query.limit(pagination.limit + 1);
    }

    const results = await executeUnlessAborted(
      query.execute(),
      options?.signal
    );

    // prunes the additional requested message, if it exists, and adds a cursor to the results.
    // also parses the encoded message for each of the returned results.
    return this.processPaginationResults(results, sortProperty, pagination?.limit, options);
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
        .deleteFrom('messageStoreMessages')
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
      .deleteFrom('messageStoreMessages')
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
   * Processes the paginated query results.
   * Builds a pagination cursor if there are additional messages to paginate.
   * Accepts more messages than the limit, as we query for additional records to check if we should paginate.
   *
   * @param messages a list of messages, potentially larger than the provided limit.
   * @param limit the maximum number of messages to be returned
   *
   * @returns the pruned message results and an optional pagination cursor
   */
  private async processPaginationResults(
    results: any[],
    sortProperty: string,
    limit?: number,
    options?: MessageStoreOptions,
  ): Promise<{ messages: GenericMessage[], cursor?: PaginationCursor}> {
    // we queried for one additional message to determine if there are any additional messages beyond the limit
    // we now check if the returned results are greater than the limit, if so we pluck the last item out of the result set
    // the cursor is always the last item in the *returned* result so we use the last item in the remaining result set to build a cursor
    let cursor: PaginationCursor | undefined;
    if (limit !== undefined && results.length > limit) {
      results = results.slice(0, limit);
      const lastMessage = results.at(-1);
      const cursorValue = lastMessage[sortProperty];
      cursor = { messageCid: lastMessage.messageCid, value: cursorValue };
    }

    // extracts the full encoded message from the stored blob for each result item.
    const messages: Promise<GenericMessage>[] = results.map(r => this.parseEncodedMessage(r.encodedMessageBytes, r.encodedData, options));
    return { messages: await Promise.all(messages), cursor };
  }

  /**
   * Extracts the appropriate sort property and direction given a MessageSort object.
   */
  private extractSortProperties(
    messageSort?: MessageSort
  ):{ property: 'dateCreated' | 'datePublished' | 'messageTimestamp', direction: SortDirection } {
    if(messageSort?.dateCreated !== undefined)  {
      return  { property: 'dateCreated', direction: messageSort.dateCreated };
    } else if(messageSort?.datePublished !== undefined) {
      return  { property: 'datePublished', direction: messageSort.datePublished };
    } else if (messageSort?.messageTimestamp !== undefined) {
      return  { property: 'messageTimestamp', direction: messageSort.messageTimestamp };
    } else {
      return  { property: 'messageTimestamp', direction: SortDirection.Ascending };
    }
  }
}