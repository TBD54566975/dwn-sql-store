import type { DidState } from '@tbd54566975/dids';
import type {
  SignatureInput,
  PrivateJwk as DwnPrivateKeyJwk,
  RecordsWriteOptions
} from '@tbd54566975/dwn-sdk-js';


import { Readable } from 'readable-stream';
import { DidIonApi } from '@tbd54566975/dids';
import { RecordsWrite, DataStream, Encoder } from '@tbd54566975/dwn-sdk-js';

const DidIon = new DidIonApi();

export type Identity = {
  did: string;
  didState: DidState
  dwnSignatureInput: SignatureInput
}

/**
 * creates a DID and associated private keys that can be used to create / sign dweb messages
 */
export async function createIdentity() {
  const didState = await DidIon.create();

  const { keys } = didState;
  const [ key ] = keys;
  const { privateKeyJwk } = key;

  // TODO: make far less naive
  const kidFragment = privateKeyJwk.kid || key.id;
  const kid = `${didState.id}#${kidFragment}`;

  const dwnSignatureInput: SignatureInput = {
    privateJwk      : <DwnPrivateKeyJwk>privateKeyJwk,
    protectedHeader : { alg: privateKeyJwk.crv, kid }
  };

  return {
    did: didState.id,
    didState,
    dwnSignatureInput
  };
}

export type RecordsWriteRequest = {
  data?: unknown;
  message?: Omit<Partial<RecordsWriteOptions>, 'authorizationSignatureInput'>;
}

/**
 * creates a signed DWN RecordsWrite message
 * @param signer - identity used to sign resulting message
 * @param options - optional overrides
 * @returns - the signed message and a datastream that includes data associated to the message
 */
export async function createRecordsWriteMessage(signer: Identity, options: RecordsWriteRequest = {}) {
  options.data ??= randomBytes(30);
  const { bytes, dataFormat } = dataToBytes(options.data, options?.message?.dataFormat);

  const recordsWrite = await RecordsWrite.create({
    ...options.message,
    dataFormat,
    data                        : bytes,
    authorizationSignatureInput : signer.dwnSignatureInput,
  });

  let dataStream: Readable = DataStream.fromBytes(bytes);

  return {
    recordsWrite,
    dataStream
  };
}

/**
 * generates and returns _n_ bytes
 * @param length - # of bytes to generate
 * @returns a Uint8Array
 */
export function randomBytes(length: number): Uint8Array {
  const randomBytes = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    randomBytes[i] = Math.floor(Math.random() * 256);
  }

  return randomBytes;
}


/**
 * marshals provided data into bytes
 * @param data - data to convert
 * @param dataFormat - mime type. optional. function will attempt to derive format if not provided
 * @returns data as bytes and assumed data format
 */
const dataToBytes = (data: any, dataFormat?: string) => {
  let bytes: Uint8Array;

  // Check for Object or String, and if neither, assume bytes.
  const detectedType = toType(data);
  if (dataFormat === 'text/plain' || detectedType === 'string') {
    bytes = Encoder.stringToBytes(data);
    dataFormat = 'text/plain';
  } else if (dataFormat === 'application/json' || detectedType === 'object') {
    bytes = Encoder.objectToBytes(data);
    dataFormat = 'application/json';
  } else if (data instanceof Uint8Array) {
    bytes = data;
    dataFormat = 'application/octet-stream';
  } else {
    throw new Error('data type not supported.');
  }

  return { bytes, dataFormat };
};

const toType = (obj) => {
  return ({}).toString.call(obj).match(/\s([a-zA-Z]+)/)[1].toLowerCase();
};

