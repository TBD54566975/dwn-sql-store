import { Filter } from '@tbd54566975/dwn-sdk-js';
import { DynamicModule, SelectQueryBuilder } from 'kysely';
import { sanitizedString } from './sanitize.js';

export function filterSelectQuery<DB = unknown, TB extends keyof DB = keyof DB, O = unknown>(
  filters: Filter[],
  query: SelectQueryBuilder<DB, TB, O>
): SelectQueryBuilder<DB, TB, O> {
  if (filters.length === 0) {
    return query;
  }
  const filter = filters[0];
  // for (let filter of filters) {
  for (let property in filter) {
    const value = filter[property];
    const column = new DynamicModule().ref(property);
    if (Array.isArray(value)) { // OneOfFilter
      query = query.where(column, 'in', value);
    } else if (typeof value === 'object') { // RangeFilter
      if (value.gt) {
        query = query.where(column, '>', sanitizedString(value.gt));
      }
      if (value.gte) {
        query = query.where(column, '>=', sanitizedString(value.gte));
      }
      if (value.lt) {
        query = query.where(column, '<', sanitizedString(value.lt));
      }
      if (value.lte) {
        query = query.where(column, '<=', sanitizedString(value.lte));
      }
    } else { // EqualFilter
      query = query.where(column, '=', sanitizedString(value));
    }
  }
  // }

  return query;
}