import { Dialect } from '../dialect/dialect.js';
import { Transaction } from 'kysely';
import { DwnDatabaseType, KeyValues } from '../types.js';
import { sanitizedValue } from './sanitize.js';

export async function executeTagsInsert(options: {
  dialect: Dialect,
  store: 'messageStore' | 'eventLog'
  id: number;
  tags: KeyValues;
  tx: Transaction<DwnDatabaseType>;
}):Promise<void> {
  const { dialect, store, id, tags, tx } = options;
  if (Object.keys(tags).length > 0) {
    for (const tag in tags) {
      const tagInsert = await insertTag({ dialect, store, id, tag, tx });
      const tagId = Number(tagInsert.insertId);
      const tagValue = tags[tag];
      await insertTagValues({
        store,
        tx,
        tagId,
        values: Array.isArray(tagValue) ? tagValue : [ tagValue ]
      });
    }
  }
}

export async function insertTag(options: {
  dialect: Dialect,
  store: 'messageStore' | 'eventLog'
  id: number;
  tag: string;
  tx: Transaction<DwnDatabaseType>;
}): Promise<{ insertId: number }> {
  const { store, id, tx, tag, dialect } = options;
  if (store === 'messageStore') {
    return dialect.insertIntoAndReturning(
      tx,
      'messageStoreRecordsTags',
      { messageStoreId: id, tag },
      'id as insertId'
    ).executeTakeFirstOrThrow();
  } else {
    return await dialect.insertIntoAndReturning(
      tx,
      'eventLogRecordsTags',
      { eventLogWatermark: id, tag },
      'id as insertId'
    ).executeTakeFirstOrThrow();
  }
}

export async function insertTagValues(options: {
  store: 'messageStore' | 'eventLog'
  tx: Transaction<DwnDatabaseType>;
  tagId: number;
  values: Array<string | number | boolean>
}): Promise<void> {
  const { store, tx, tagId, values } = options;

  const insertValues = values.map(value => {
    const insertValue = sanitizedValue(value);
    return {
      tagId,
      valueNumber : typeof insertValue === 'number' ? insertValue : null,
      valueString : typeof insertValue === 'string' ? insertValue : null,
    };
  });

  if (store === 'messageStore') {
    await tx.insertInto('messageStoreRecordsTagValues')
      .values(insertValues)
      .execute();
  } else {
    await tx.insertInto('eventLogRecordsTagValues')
      .values(insertValues)
      .execute();
  }
}