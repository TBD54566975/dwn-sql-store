import { Filter } from '@tbd54566975/dwn-sdk-js';

export function sanitizeIndexes(records: Record<string, string | number | boolean>) {
  for (let key in records) {
    let value = records[key];
    records[key] = sanitizedValue(value);
  }
}

// we sanitize the incoming value into a string or number
// sqlite3 and the driver we use does not support booleans, so we convert them to strings
export function sanitizedValue(value: string | number | boolean): string | number {
  switch (typeof value) {
  case 'boolean':
    return String(value);
  default:
    return value;
  }
}

// we sanitize the filter value for a string representation of the boolean
// TODO: export filter types from `dwn-sdk-js`
export function sanitizeFilterValue(value: any): any {
  switch (typeof value) {
  case 'boolean':
    return String(value);
  default:
    return value;
  }
}

export function sanitizeFilters(filters: Filter[]) {
  for (let key in filters) {
    let filter = filters[key];
    filters[key] = sanitizeFilter(filter);
  }
}

export function sanitizeFilter(filter: Filter): Filter {
  for (let key in filter) {
    let value = filter[key];
    filter[key] = sanitizeFilterValue(value);
  }
  return filter;
}