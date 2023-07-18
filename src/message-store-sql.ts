import type { Filter, GenericMessage, MessageStore, MessageStoreOptions } from '@tbd54566975/dwn-sdk-js';

// TODO: remove this once functions have been implemented
/* eslint-disable @typescript-eslint/no-unused-vars */


export class MessageStoreSql implements MessageStore {

  async open(): Promise<void> {
    throw new Error('Method not implemented.');
  }

  async close(): Promise<void> {
    throw new Error('Method not implemented.');
  }

  async put(
    tenant: string,
    messageJson: GenericMessage,
    indexes: Record<string, string>,
    options?: MessageStoreOptions
  ): Promise<void> {
    throw new Error('Method not implemented.');
  }

  async get(
    tenant: string,
    cid: string,
    options?: MessageStoreOptions
  ): Promise<GenericMessage | undefined> {
    throw new Error('Method not implemented.');
  }

  async query(
    tenant: string,
    filter: Filter,
    options?: MessageStoreOptions
  ): Promise<GenericMessage[]> {
    throw new Error('Method not implemented.');
  }

  async delete(
    tenant: string,
    cid: string,
    options?: MessageStoreOptions
  ): Promise<void> {
    throw new Error('Method not implemented.');
  }

  async clear(): Promise<void> {
    throw new Error('Method not implemented.');
  }

}