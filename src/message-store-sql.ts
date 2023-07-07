import type { GenericMessage, MessageStore, MessageStoreOptions } from '@tbd54566975/dwn-sdk-js';

// TODO: All of types below should be provided by dwn-sdk-js
// Update that repo to expose these.
export type EqualFilter = string | number | boolean;

export type OneOfFilter = EqualFilter[];

export type Filter = {
  [property: string]: EqualFilter | OneOfFilter | RangeFilter
};

export type GT = ({ gt: string } & { gte?: never }) | ({ gt?: never } & { gte: string });

export type LT = ({ lt: string } & { lte?: never }) | ({ lt?: never } & { lte: string });

export type RangeFilter = (GT | LT) & Partial<GT> & Partial<LT>;

// TODO: remove this once functions have been implemented
/* eslint-disable @typescript-eslint/no-unused-vars */

export class MessageStoreSql implements MessageStore {

  open(): Promise<void> {
    throw new Error('Method not implemented.');
  }

  close(): Promise<void> {
    throw new Error('Method not implemented.');
  }

  put(tenant: string, messageJson: GenericMessage, indexes: Record<string, string>, options?: MessageStoreOptions): Promise<void> {
    throw new Error('Method not implemented.');
  }

  get(tenant: string, cid: string, options?: MessageStoreOptions): Promise<GenericMessage | undefined> {
    throw new Error('Method not implemented.');
  }

  query(tenant: string, filter: Filter, options?: MessageStoreOptions ): Promise<GenericMessage[]> {
    throw new Error('Method not implemented.');
  }

  delete(tenant: string, cid: string, options?: MessageStoreOptions): Promise<void> {
    throw new Error('Method not implemented.');
  }

  clear(): Promise<void> {
    throw new Error('Method not implemented.');
  }

}