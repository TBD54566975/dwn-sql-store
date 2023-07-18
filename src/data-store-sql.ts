import { AssociateResult, DataStore, GetResult, PutResult } from '@tbd54566975/dwn-sdk-js';
import { Readable } from 'readable-stream';

// TODO: remove this once functions have been implemented
/* eslint-disable @typescript-eslint/no-unused-vars */

export class DataStoreSql implements DataStore {

  async open(): Promise<void> {
    throw new Error('Method not implemented.');
  }

  async close(): Promise<void> {
    throw new Error('Method not implemented.');
  }

  async get(
    tenant: string,
    messageCid: string,
    dataCid: string
  ): Promise<GetResult | undefined> {
    throw new Error('Method not implemented.');
  }

  async put(
    tenant: string,
    messageCid: string,
    dataCid: string,
    dataStream: Readable
  ): Promise<PutResult> {
    throw new Error('Method not implemented.');
  }

  async associate(
    tenant: string,
    messageCid: string,
    dataCid: string
  ): Promise<AssociateResult | undefined> {
    throw new Error('Method not implemented.');
  }

  async delete(
    tenant: string,
    messageCid: string,
    dataCid: string
  ): Promise<void> {
    throw new Error('Method not implemented.');
  }

  async clear(): Promise<void> {
    throw new Error('Method not implemented.');
  }

}