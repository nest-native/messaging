import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import { getTableConfig as getSqliteConfig } from 'drizzle-orm/sqlite-core';
import { getTableConfig as getPgConfig } from 'drizzle-orm/pg-core';
import { getTableConfig as getMysqlConfig } from 'drizzle-orm/mysql-core';
import * as sqlite from '../dialects/sqlite';
import * as postgres from '../dialects/postgres';
import * as mysql from '../dialects/mysql';

// getTableConfig builds the table's columns + indexes, which executes the
// `(table) => [...]` index-definition callbacks — validating the schema and the
// dedup indexes that the stores rely on, for both dialects.

describe('sqlite schema', () => {
  test('outbox_events: columns + idempotency-key partial unique + status index', () => {
    const cfg = getSqliteConfig(sqlite.outboxEvents);
    assert.equal(cfg.name, 'outbox_events');
    const names = cfg.indexes.map((i) => i.config.name).sort();
    assert.deepEqual(names, [
      'outbox_events_idempotency_key_unique',
      'outbox_events_status_available_idx',
    ]);
    const unique = cfg.indexes.find(
      (i) => i.config.name === 'outbox_events_idempotency_key_unique',
    );
    assert.equal(unique?.config.unique, true);
  });

  test('inbox_events: composite (source, message_key) unique index', () => {
    const cfg = getSqliteConfig(sqlite.inboxEvents);
    assert.equal(cfg.name, 'inbox_events');
    const unique = cfg.indexes.find(
      (i) => i.config.name === 'inbox_events_source_message_key_unique',
    );
    assert.equal(unique?.config.unique, true);
    assert.equal(unique?.config.columns.length, 2);
  });
});

describe('postgres schema', () => {
  test('outbox_events: columns + indexes', () => {
    const cfg = getPgConfig(postgres.outboxEvents);
    assert.equal(cfg.name, 'outbox_events');
    const names = cfg.indexes.map((i) => i.config.name).sort();
    assert.deepEqual(names, [
      'outbox_events_idempotency_key_unique',
      'outbox_events_status_available_idx',
    ]);
  });

  test('inbox_events: composite unique index', () => {
    const cfg = getPgConfig(postgres.inboxEvents);
    assert.equal(cfg.name, 'inbox_events');
    const unique = cfg.indexes.find(
      (i) => i.config.name === 'inbox_events_source_message_key_unique',
    );
    assert.equal(unique?.config.unique, true);
    assert.equal(unique?.config.columns.length, 2);
  });
});

describe('mysql schema', () => {
  test('outbox_events: columns + full idempotency-key unique + status index', () => {
    const cfg = getMysqlConfig(mysql.outboxEvents);
    assert.equal(cfg.name, 'outbox_events');
    const names = cfg.indexes.map((i) => i.config.name).sort();
    assert.deepEqual(names, [
      'outbox_events_idempotency_key_unique',
      'outbox_events_status_available_idx',
    ]);
    // A *full* unique index (MySQL has no partial indexes) — a UNIQUE index on
    // the nullable column still permits multiple NULLs, matching the semantics.
    const unique = cfg.indexes.find(
      (i) => i.config.name === 'outbox_events_idempotency_key_unique',
    );
    assert.equal(unique?.config.unique, true);
    assert.equal(unique?.config.columns.length, 1);
  });

  test('inbox_events: composite (source, message_key) unique index', () => {
    const cfg = getMysqlConfig(mysql.inboxEvents);
    assert.equal(cfg.name, 'inbox_events');
    const unique = cfg.indexes.find(
      (i) => i.config.name === 'inbox_events_source_message_key_unique',
    );
    assert.equal(unique?.config.unique, true);
    assert.equal(unique?.config.columns.length, 2);
  });
});
