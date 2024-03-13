import { Dialect } from './dialect.js';
import {
  ColumnBuilderCallback,
  ColumnDefinitionBuilder,
  CreateTableBuilder,
  Kysely,
  InsertObject,
  InsertQueryBuilder,
  SelectExpression,
  Selection,
  SqliteDialect as KyselySqliteDialect,
  Transaction,
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

  addReferencedColumn(
    builder: ColumnDefinitionBuilder,
    referenceTable: string,
    referenceColumnName: string,
    onDeleteAction: 'cascade' | 'no action' | 'restrict' | 'set null' | 'set default' = 'cascade',
  ): ColumnDefinitionBuilder {
    return builder.references(`${referenceTable}.${referenceColumnName}`).onDelete(onDeleteAction);
  }

  insertIntoReturning<DB, TB extends keyof DB = keyof DB, SE extends SelectExpression<DB, TB & string> = any>(
    db: Transaction<DB> | Kysely<DB>,
    table: TB & string,
    values: InsertObject<DB, TB & string>,
    returning: SE & `${string} as insertId`,
  ): InsertQueryBuilder<DB, TB & string, Selection<DB, TB & string, SE & `${string} as insertId`>> {
    return db.insertInto(table).values(values).returning(returning);
  }
}