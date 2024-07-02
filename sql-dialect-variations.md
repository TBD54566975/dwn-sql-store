# SQL Dialect Variations

We use `Kysely` to help us abstract differences between SQL variants in this codebase. Unfortunately many differences are not fully abstracted away. This document outlines the specific handling required in this codebase to support variations between MySQL, PostgreSQL, and SQLite. Its purpose is to help future repository maintainers avoid known pitfalls. Note that this may not be an exhaustive list of all special handling within the codebase.

## MySQL

- Does not support adding a reference column directly, requires adding a foreign key constraint instead.

- When inserting values into a table, the inserted values are returned by default, unlike PostgreSQL and SQLite.

- Does not support "if not exists" syntax when creating indexes, workaround is to create index only on a newly created table.

## PostgreSQL

- Uses a special type: `serial` for auto-increment columns.

- Uses a special type: `bytea` for blob columns.

- `bigint` column type gets returned as `string` in `pg` library.

