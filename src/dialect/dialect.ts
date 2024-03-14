import {
  ColumnBuilderCallback,
  ColumnDataType,
  CreateTableBuilder,
  Dialect as KyselyDialect,
  Kysely,
  InsertObject,
  InsertQueryBuilder,
  Selection,
  SelectExpression,
  Transaction,
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

  addReferencedColumn<TB extends string>(
    builder: CreateTableBuilder<TB & string>,
    tableName: TB,
    columnName: string,
    targetType: ColumnDataType,
    referenceTable: string,
    referenceColumnName: string,
    onDeleteAction: 'cascade' | 'no action' | 'restrict' | 'set null' | 'set default',
  ): CreateTableBuilder<TB & string>;

  /**
   * This is a helper method to return an `insertId` for each dialect.
   * We need this method because Postgres does not return an `insertId` with insert an insert.
   *
   * Since `returning` is supported on both `sqlite` and `postgres`, it will be used on those and ignored on `mysql`.
   *
   * @param db the Kysely DB object or a DB Transaction.
   * @param table the table to insert into.
   * @param values the values to insert.
   * @param returning a string representing the generated key you'd like returned as an insertId.
   *
   *  NOTE: the `returning` value must be formatted to return an insertId value.
   *  ex. if the generated key is `id` the string should be `id as insertId`.
   *      if the generated key is `watermark` the string should be `watermark as insertId`.
   *
   * @returns {InsertQueryBuilder} object to further modify the query or execute it.
   */
  insertIntoReturning<DB, TB extends keyof DB = keyof DB, SE extends SelectExpression<DB, TB & string> = any>(
    db: Transaction<DB> | Kysely<DB>,
    table: TB & string,
    values: InsertObject<DB, TB & string>,
    returning: SE & `${string} as insertId`,
  ): InsertQueryBuilder<DB, TB & string, Selection<DB, TB & string, SE & `${string} as insertId`>>;
}