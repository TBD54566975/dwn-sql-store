import type { Generated } from 'kysely';

export interface EventLogTable {
  id: Generated<number>;
  tenant: string;
  messageCid: string;
}

export interface MessageStoreTable {
  id: Generated<number>;
  tenant: string;
  messageCid: string;
  encodedMessageBytes: Uint8Array;
  encodedData: string | null;
  // "indexes" start
  interface: string | null;
  method: string | null;
  schema: string | null;
  dataCid: string | null;
  dataSize: number | null;
  dateCreated: string | null;
  messageTimestamp: string | null;
  dataFormat: string | null;
  isLatestBaseState: string | null;
  published: string | null;
  author: string | null;
  recordId: string | null;
  entryId: string | null;
  datePublished: string | null;
  latest: string | null;
  protocol: string | null;
  dateExpires: string | null;
  description: string | null;
  grantedTo: string | null;
  grantedBy: string | null;
  grantedFor: string | null;
  permissionsRequestId: string | null;
  attester: string | null;
  protocolPath: string | null;
  recipient: string | null;
  contextId: string | null;
  parentId: string | null;
  permissionsGrantId: string | null;
  // "indexes" end
}

export interface DataStoreTable {
  id: Generated<number>;
  tenant: string;
  dataCid: string;
  data: Uint8Array;
}

export interface DataStoreReferencesTable {
  id: Generated<number>;
  tenant: string;
  dataCid: string;
  messageCid: string;
}

export interface Database {
  eventLog: EventLogTable;
  messageStore: MessageStoreTable;
  dataStore: DataStoreTable;
  dataStoreReferences: DataStoreReferencesTable;
}