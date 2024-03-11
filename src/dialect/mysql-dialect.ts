import { Dialect } from './dialect.js';
import {
  CreateTableBuilder,
  ColumnBuilderCallback,
  MysqlDialect as KyselyMysqlDialect,
  ColumnDefinitionBuilder
} from 'kysely';

export class MysqlDialect extends KyselyMysqlDialect implements Dialect {
  isStreamingSupported = true;

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

  addReferencedColumn(
    builder: ColumnDefinitionBuilder,
    referenceTable: string,
    referenceColumnName: string,
    onDeleteAction: 'cascade' | 'no action' | 'restrict' | 'set null' | 'set default' = 'cascade',
  ): ColumnDefinitionBuilder {
    return builder.references(`${referenceTable}.${referenceColumnName}`).onDelete(onDeleteAction);
  }
}