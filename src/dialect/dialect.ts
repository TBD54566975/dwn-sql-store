import {
  ColumnBuilderCallback,
  CreateTableBuilder,
  Dialect as KyselyDialect
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

}