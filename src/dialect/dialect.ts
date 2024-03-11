import {
  ColumnBuilderCallback,
  ColumnDefinitionBuilder,
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

  addReferencedColumn(
    builder: ColumnDefinitionBuilder,
    referenceTable: string,
    referenceColumnName: string,
    onDeleteAction?: 'cascade' | 'no action' | 'restrict' | 'set null' | 'set default',
  ): ColumnDefinitionBuilder;

}