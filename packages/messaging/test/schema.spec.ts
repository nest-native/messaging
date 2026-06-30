import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import { getTableConfig as getSqliteConfig } from 'drizzle-orm/sqlite-core';
import { getTableConfig as getPgConfig } from 'drizzle-orm/pg-core';
import * as sqlite from '../dialects/sqlite';
import * as postgres from '../dialects/postgres';

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
