import { Dialect } from './dialect.js';
import {
  CreateTableBuilder,
  ColumnBuilderCallback,
  PostgresDialect as KyselyPostgresDialect,
  ColumnDefinitionBuilder
} from 'kysely';

export class PostgresDialect extends KyselyPostgresDialect implements Dialect {
  isStreamingSupported = true;

  addAutoIncrementingColumn<TB extends string>(
    builder: CreateTableBuilder<TB>,
    columnName: string,
    callback?: ColumnBuilderCallback
  ): CreateTableBuilder<TB> {
    return builder.addColumn(columnName, 'serial', callback);
  }

  addBlobColumn<TB extends string>(
    builder: CreateTableBuilder<TB>,
    columnName: string,
    callback?: ColumnBuilderCallback
  ): CreateTableBuilder<TB> {
    return builder.addColumn(columnName, 'bytea', callback);
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