SQL backed implementations of DWN `MessageStore`, `DataStore`, and `EventLog`

> ⚠️ WIP ⚠️. I just started working on this. it's very early stages. I only have a _part_ of `EventLog` working as a janky prototype. Haven't started on `MessageStore` or `DataStore` yet


Instructions:
* start dockerized mysql using `./scripts/start-mysql`
* run `npm run compile` to compile typescript
* run `node dist/tests/event-log.spec.js`. You should see a `202: Accepted` message