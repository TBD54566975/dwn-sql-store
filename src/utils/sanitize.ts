export function sanitizeRecords(records: Record<string, string | number>) {
  for (let key in records) {
    let value = records[key];
    records[key] = sanitizedValue(value);
  }
}

// we only use strings and numbers (integers) within this store.
// anything that isn't explicitly a string or a number, becomes a json string.
export function sanitizedValue(value: any): string | number {
  if (typeof value === 'string') {
    return value;
  } else if (typeof value === 'number') {
    return value;
  } else {
    return JSON.stringify(value);
  }
}