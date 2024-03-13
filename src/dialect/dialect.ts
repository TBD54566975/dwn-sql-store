import {
  ColumnBuilderCallback,
  ColumnDefinitionBuilder,
  CreateTableBuilder,
  Dialect as KyselyDialect,
  Kysely,
  InsertObject,
  InsertQueryBuilder,
  Selection,
  SelectExpression,
  Transaction
} from 'kysely';

export interface Dialect extends KyselyDialect {
  readonly isStreamingSupported: boolean;

  addAutoIncrementingColumn<TB extends string>(
    builder: CreateTableBuilder<TB>,
    columnName: string,
    callback?: ColumnBuilderCallback
  ): CreateTableBuilder<TB>;

  addBlobColumn<TB extends string>(
    builder: CreateTableBuilder<TB>,
    columnName: string,
    callback?: ColumnBuilderCallback
  ): CreateTableBuilder<TB>;

  addReferencedColumn(
    builder: ColumnDefinitionBuilder,
    referenceTable: string,
    referenceColumnName: string,
    onDeleteAction?: 'cascade' | 'no action' | 'restrict' | 'set null' | 'set default',
  ): ColumnDefinitionBuilder;

  insertIntoReturning<DB, TB extends keyof DB = keyof DB, SE extends SelectExpression<DB, TB & string> = any>(
    db: Transaction<DB> | Kysely<DB>,
    table: TB & string,
    values: InsertObject<DB, TB & string>,
    returning: SE,
  ): InsertQueryBuilder<DB, TB & string, Selection<DB, TB & string, SE>>;
}