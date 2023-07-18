import { Dialect } from './dialect.js';
import {
  CreateTableBuilder,
  ColumnBuilderCallback,
  PostgresDialect as KyselyPostgresDialect
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
}