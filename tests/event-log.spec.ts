import { Dwn } from '@tbd54566975/dwn-sdk-js';
import { EventLogSql } from '../src/event-log-sql.js';
import { createIdentity, createRecordsWriteMessage } from './utils.js';

// TODO: switch to using mocha for tests.

const eventLog = new EventLogSql();
const dwn = await Dwn.create({ eventLog });

const alice = await createIdentity();
const { recordsWrite, dataStream } = await createRecordsWriteMessage(alice);

const result = await dwn.processMessage(alice.did, recordsWrite.toJSON(), dataStream);
console.log(JSON.stringify(result, null, 2));