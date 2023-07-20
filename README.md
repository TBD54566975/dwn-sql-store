SQL backed implementations of DWN `MessageStore`, `DataStore`, and `EventLog`. 

- [Supported DBs](#supported-dbs)
- [Installation](#installation)
- [Usage](#usage)
  - [SQLite](#sqlite)
  - [MySQL](#mysql)
  - [PostgreSQL](#postgresql)
- [Development](#development)
  - [Prerequisites](#prerequisites)
    - [`node` and `npm`](#node-and-npm)
    - [Docker](#docker)
  - [Running Tests](#running-tests)
  - [`npm` scripts](#npm-scripts)


# Supported DBs
* SQLite ✔️
* MySQL ✔️
* PostgreSQL ✔️

# Installation

```bash
npm install @tbd54566975/dwn-sql-store
```

# Usage

## SQLite

```typescript
import Database from 'better-sqlite3';

import { Dwn } from '@tbd54566975/dwn-sdk-js'
import { SqliteDialect, MessageStoreSql, DataStoreSql, EventLogSql } from '@tbd54566975/dwn-sql-store';

const sqliteDialect = new SqliteDialect({
  database: async () => new Database('dwn.sqlite', {
    fileMustExist: true,
  })
});

const messageStore = new MessageStoreSql(sqliteDialect);
const dataStore = new DataStoreSql(sqliteDialect);
const eventLog = new DataStoreSql(sqliteDialect);

const dwn = await Dwn.create({ messageStore, dataStore, eventLog });
```

## MySQL

```typescript
import { createPool } from 'mysql2';
import { Dwn } from '@tbd54566975/dwn-sdk-js'
import { MysqlDialect, MessageStoreSql, DataStoreSql, EventLogSql } from '@tbd54566975/dwn-sql-store';

const mysqlDialect = new MysqlDialect({
  pool: async () => createPool({
    host     : 'localhost',
    port     : 3306,
    database : 'dwn',
    user     : 'root',
    password : 'dwn'
  })
});

const messageStore = new MessageStoreSql(mysqlDialect);
const dataStore = new DataStoreSql(mysqlDialect);
const eventLog = new DataStoreSql(mysqlDialect);

const dwn = await Dwn.create({ messageStore, dataStore, eventLog });
```

## PostgreSQL

```typescript
import pg from 'pg';
import Cursor from 'pg-cursor';

import { Dwn } from '@tbd54566975/dwn-sdk-js'
import { PostgresDialect, MessageStoreSql, DataStoreSql, EventLogSql } from '@tbd54566975/dwn-sql-store';

const postgresDialect = new PostgresDialect({
  pool: async () => new pg.Pool({
    host     : 'localhost',
    port     : 5432,
    database : 'dwn',
    user     : 'root',
    password : 'dwn'
  }),
  cursor: Cursor
});

const messageStore = new MessageStoreSql(postgresDialect);
const dataStore = new DataStoreSql(postgresDialect);
const eventLog = new DataStoreSql(postgresDialect);

const dwn = await Dwn.create({ messageStore, dataStore, eventLog });
```

# Development

## Prerequisites
### `node` and `npm`
This project is using `node v20.3.0` and `npm v9.6.7`. You can verify your `node` and `npm` installation via the terminal:

```
$ node --version
v20.3.0
$ npm --version
9.6.7
```

If you don't have `node` installed. Feel free to choose whichever approach you feel the most comfortable with. If you don't have a preferred installation method, i'd recommend using `nvm` (aka node version manager). `nvm` allows you to install and use different versions of node. It can be installed by running `brew install nvm` (assuming that you have homebrew)

Once you have installed `nvm`, install the desired node version with `nvm install vX.Y.Z`.

### Docker
Docker is used to spin up a local containerized DBs for testing purposes. Docker from [here](https://docs.docker.com/engine/install/)

## Running Tests
> 💡 Make sure you have all the [prerequisites](#prerequisites)

0. clone the repo and `cd` into the project directory
1. Install all project dependencies by running `npm install`
2. start the test databases using `./scripts/start-databases` (requires Docker)
3. run tests using `npm run test`

## `npm` scripts

| Script                  | Description                                 |
| ----------------------- | ------------------------------------------- |
| `npm run compile`       | compiles typescript into ESM JS             |
| `npm run clean`         | deletes compiled JS                         |
| `npm run test`          | runs tests.                                 |
| `npm run test-coverage` | runs tests and includes coverage            |
| `npm run lint`          | runs linter                                 |
| `npm run lint:fix`      | runs linter and fixes auto-fixable problems |
