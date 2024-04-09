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

  /**
   * This is a helper method to add a column with foreign key constraints.
   * This is primarily useful because the `mySQL` dialect adds the constraints in a different way than `sqlite` and `postgres`.
   *
   * @param builder the CreateTableBuilder to add the column to.
   * @param tableName the name of the table to add the column to.
   * @param columnName the name of the column to add.
   * @param targetType the type of the column to add.
   * @param referenceTable the foreign table to reference.
   * @param referenceColumnName the foreign column to reference.
   * @param onDeleteAction the action to take when the referenced row is deleted.
   *
   * The ForeignKey name it creates in `mysql` will be in the following format:
   * `${referenceTable}${referenceColumnName}_${tableName}${columnName}`
   * ex: if the reference table is `users` and the reference column is `id` and the table is `profiles` and the column is `userId`,
   * the resulting name for the foreign key is: `usersid_profilesuserId`
   *
   * @returns {CreateTableBuilder} the CreateTableBuilder with the added column.
   */
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