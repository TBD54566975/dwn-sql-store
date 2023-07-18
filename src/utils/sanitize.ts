export function sanitizeRecords(records: Record<string, string>) {
  for (let key in records) {
    let value = records[key];
    records[key] = sanitizedString(value);
  }
}

export function sanitizedString(value: any): string {
  if (typeof value === 'string') {
    return value;
  } else {
    return JSON.stringify(value);
  }
}