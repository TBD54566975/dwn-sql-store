import { InsertResult, Transaction } from 'kysely';
import { DwnDatabaseType, KeyValues } from '../types.js';
import { sanitizedValue } from './sanitize.js';

export async function executeTagsInsert(options: {
  store: 'messageStore' | 'eventLog'
  id: number;
  tags: KeyValues;
  tx: Transaction<DwnDatabaseType>;
}):Promise<void> {
  const { store, id, tags, tx } = options;

  if (Object.keys(tags).length > 0) {
    for (const tag in tags) {
      const tagInsert = await insertTag({ store, id, tag, tx });
      const tagId = Number(tagInsert.insertId);
      const tagValue = tags[tag];
      if (Array.isArray(tagValue)) {
        for (const value of tagValue) {
          await insertTagValue({ store, tx, tagId, value });
        }
      } else {
        await insertTagValue({ store, tx, tagId, value: tagValue });
      }
    }
  }
}

export async function insertTag(options: {
  store: 'messageStore' | 'eventLog'
  id: number;
  tag: string;
  tx: Transaction<DwnDatabaseType>;
}): Promise<InsertResult> {
  const { store, id, tx, tag } = options;
  if (store === 'messageStore') {
    return  tx
      .insertInto('messageStoreRecordsTags')
      .values({ messageStoreId: id, tag })
      .executeTakeFirstOrThrow();
  } else {
    return  tx
      .insertInto('eventLogRecordsTags')
      .values({ eventLogWatermark: id, tag })
      .executeTakeFirstOrThrow();
  }
}

export async function insertTagValue(options: {
  store: 'messageStore' | 'eventLog'
  tx: Transaction<DwnDatabaseType>;
  tagId: number;
  value: string | number | boolean
}): Promise<void> {
  const { store, tx, tagId, value } = options;
  const sanitizedTagValue = sanitizedValue(value);

  const values = {
    tagId,
    valueNumber : typeof sanitizedTagValue === 'number' ? sanitizedTagValue : null,
    valueString : typeof sanitizedTagValue === 'string' ? sanitizedTagValue : null,
  };

  if (store === 'messageStore') {
    await tx.insertInto('messageStoreRecordsTagValues')
      .values({ ...values })
      .execute();
  } else {
    await tx.insertInto('eventLogRecordsTagValues')
      .values({ ...values })
      .execute();
  }
}