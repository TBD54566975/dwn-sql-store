import { DataStore } from '@tbd54566975/dwn-sdk-js';
import { Readable } from 'readable-stream';

// TODO: All of types below should be provided by dwn-sdk-js
// Update that repo to expose these.

export type PutResult = {
  dataCid: string;
  dataSize: number;
};

export type GetResult = {
  dataCid: string;
  dataSize: number;
  dataStream: Readable;
};

export type AssociateResult = {
  dataCid: string;
  dataSize: number;
};

// TODO: remove this once functions have been implemented
/* eslint-disable @typescript-eslint/no-unused-vars */

export class DataStoreSql implements DataStore {

  open(): Promise<void> {
    throw new Error('Method not implemented.');
  }

  close(): Promise<void> {
    throw new Error('Method not implemented.');
  }

  get(tenant: string, messageCid: string, dataCid: string): Promise<GetResult | undefined> {
    throw new Error('Method not implemented.');
  }

  put(tenant: string, messageCid: string, dataCid: string, dataStream: Readable): Promise<PutResult> {
    throw new Error('Method not implemented.');
  }

  associate(tenant: string, messageCid: string, dataCid: string): Promise<AssociateResult | undefined> {
    throw new Error('Method not implemented.');
  }

  delete(tenant: string, messageCid: string, dataCid: string): Promise<void> {
    throw new Error('Method not implemented.');
  }

  clear(): Promise<void> {
    throw new Error('Method not implemented.');
  }

}