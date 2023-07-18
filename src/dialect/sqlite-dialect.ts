import { Dialect } from './dialect.js';
import {
  ColumnBuilderCallback,
  CreateTableBuilder,
  SqliteDialect as KyselySqliteDialect,
} from 'kysely';

export class SqliteDialect extends KyselySqliteDialect implements Dialect {
  isStreamingSupported = false;

  addAutoIncrementingColumn<TB extends string>(
    builder: CreateTableBuilder<TB>,
    columnName: string,
    callback?: ColumnBuilderCallback
  ): CreateTableBuilder<TB> {
    return builder.addColumn(columnName, 'integer', (col) => {
      col = col.autoIncrement();
      if (callback) {
        col = callback(col);
      }
      return col;
    });
  }

  addBlobColumn<TB extends string>(
    builder: CreateTableBuilder<TB>,
    columnName: string,
    callback?: ColumnBuilderCallback
  ): CreateTableBuilder<TB> {
    return builder.addColumn(columnName, 'blob', callback);
  }
}