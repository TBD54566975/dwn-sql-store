import { Transaction } from 'kysely';

import type { DwnDatabaseType, KeyValues } from '../types.js';

import { Dialect } from '../dialect/dialect.js';
import { sanitizedValue } from './sanitize.js';

/**
 * Helper class to manage adding indexes for `RecordsWrite` messages which contain `tags`.
 */
export class TagTables {

  /**
   * @param dialect the target dialect, necessary for returning the `insertId`
   * @param table the DB Table in order to index the tags and values in the correct tables. Choice between `messageStore` and `eventLog`
   */
  constructor(private dialect: Dialect, private table: 'messageStore' | 'eventLog'){}

  /**
   * Inserts the given tag and value associates to the parent Id.
   */
  async executeTagsInsert(
    id: number,
    tags: KeyValues,
    tx: Transaction<DwnDatabaseType>,
  ):Promise<void> {
    const tagTable = this.table === 'messageStore' ? 'messageStoreRecordsTags' : 'eventLogRecordsTags';
    const tagValue = tagTable === 'messageStoreRecordsTags' ? { messageStoreId: id } : { eventLogWatermark: id };

    for (const tag in tags) {
      const { insertId } = await this.dialect.insertIntoReturning(tx, tagTable, { ...tagValue, tag }, 'id as insertId').executeTakeFirstOrThrow();
      const tagId = Number(insertId);
      const tagValues = tags[tag];
      const values = Array.isArray(tagValues) ? tagValues : [ tagValues ];
      await this.insertTagValues(tagId, values, tx);
    }
  }

  /**
   * Inserts the tag values for a given tag id.
   */
  private async insertTagValues(
    tagId: number,
    values: Array<string | number | boolean>,
    tx: Transaction<DwnDatabaseType>,
  ): Promise<void> {
    const tagValueTable = this.table === 'messageStore' ? 'messageStoreRecordsTagValues' : 'eventLogRecordsTagValues';

    const formatValue = (value: string | number | boolean) =>  {
      const insertValue = sanitizedValue(value);
      return {
        tagId,
        valueNumber : typeof insertValue === 'number' ? insertValue : null,
        valueString : typeof insertValue === 'string' ? insertValue : null,
      };
    };

    const insertValues = values.map(formatValue);
    await tx.insertInto(tagValueTable).values(insertValues).execute();
  }
}