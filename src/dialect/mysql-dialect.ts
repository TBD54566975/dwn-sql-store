import { Dialect } from './dialect.js';
import {
  AnyColumn,
  CreateTableBuilder,
  ColumnBuilderCallback,
  ColumnDefinitionBuilder,
  InsertObject,
  InsertQueryBuilder,
  SelectExpression,
  Selection,
  Transaction,
  MysqlDialect as KyselyMysqlDialect,
  Kysely,
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

  insertIntoAndReturning<DB, TB extends keyof DB = keyof DB, SE extends SelectExpression<DB, TB & string> = AnyColumn<DB, TB>>(
    db: Transaction<DB> | Kysely<DB>,
    table: TB & string,
    values: InsertObject<DB, TB & string>,
    _returning: SE,
  ): InsertQueryBuilder<DB, TB & string, Selection<DB, TB & string, SE>> {
    return db.insertInto(table).values(values);
  }
}