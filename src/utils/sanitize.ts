import { Filter } from '@tbd54566975/dwn-sdk-js';

export function sanitizeIndexes(records: Record<string, string | number | boolean>) {
  for (let key in records) {
    let value = records[key];
    records[key] = sanitizedValue(value);
  }
}

export function sanitizedValue(value: any): string | number {
  if (typeof value === 'string') {
    return value;
  } else if (typeof value === 'number') {
    return value;
  } else {
    return JSON.stringify(value);
  }
}

export function sanitizeFilterValue(value: any): any {
  switch (typeof value) {
  case 'number':
  case 'string':
  case 'object':
    return  value;
  default:
    return JSON.stringify(value);
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