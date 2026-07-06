import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import {
  getTableConfig as getSqliteConfig,
  SQLiteSyncDialect,
} from 'drizzle-orm/sqlite-core';
import { getTableConfig as getPgConfig, PgDialect } from 'drizzle-orm/pg-core';
import { getTableConfig as getMysqlConfig } from 'drizzle-orm/mysql-core';
import * as sqlite from '../dialects/sqlite';
import * as postgres from '../dialects/postgres';
import * as mysql from '../dialects/mysql';

// getTableConfig builds the table's columns + indexes, which executes the
// `(table) => [...]` index-definition callbacks — validating the schema and the
// dedup indexes that the stores rely on, for both dialects.
//
// The column tables below pin the *db-facing* metadata (column name, NOT NULL,
// defaults, varchar widths) — the part drizzle-kit turns into DDL. A mutation
// there silently changes consumers' generated migrations, so every column is
// asserted explicitly.

type ColumnSpec = {
  notNull: boolean;
  default?: unknown;
  length?: number;
};

function columnTable(cfg: { columns: unknown[] }): Record<string, ColumnSpec> {
  const out: Record<string, ColumnSpec> = {};
  for (const raw of cfg.columns) {
    const c = raw as {
      name: string;
      keyAsName: boolean;
      notNull: boolean;
      default?: unknown;
      length?: number;
    };
    // Every column pins an EXPLICIT db name (drizzle's `keyAsName` is false).
    // Dropping a column's name string makes drizzle fall back to the JS property
    // key; for columns whose key already equals the snake_case name that leaves
    // `name` unchanged, so this flag is the only signal that catches it — and it
    // guards the real contract that the DDL column name must never silently
    // track a JS-property rename.
    assert.equal(c.keyAsName, false, `column ${c.name} must set an explicit db name`);
    const spec: ColumnSpec = { notNull: c.notNull };
    if (c.default !== undefined) spec.default = c.default;
    if (c.length !== undefined) spec.length = c.length;
    out[c.name] = spec;
  }
  return out;
}

const OUTBOX_TEXT_COLUMNS = {
  id: { notNull: true },
  topic: { notNull: true },
  payload: { notNull: true },
  status: { notNull: true },
  attempts: { notNull: true, default: 0 },
  max_attempts: { notNull: true, default: 10 },
  idempotency_key: { notNull: false },
  available_at: { notNull: true },
  claimed_at: { notNull: false },
  claimed_by: { notNull: false },
  processed_at: { notNull: false },
  last_error: { notNull: false },
  created_at: { notNull: true },
};

const INBOX_TEXT_COLUMNS = {
  id: { notNull: true },
  message_key: { notNull: true },
  source: { notNull: true },
  status: { notNull: true },
  processed_at: { notNull: true },
  last_error: { notNull: false },
  created_at: { notNull: true },
};

describe('sqlite schema', () => {
  test('outbox_events: columns + idempotency-key partial unique + status index', () => {
    const cfg = getSqliteConfig(sqlite.outboxEvents);
    assert.equal(cfg.name, 'outbox_events');
    assert.deepEqual(columnTable(cfg), OUTBOX_TEXT_COLUMNS);
    const names = cfg.indexes.map((i) => i.config.name).sort();
    assert.deepEqual(names, [
      'outbox_events_idempotency_key_unique',
      'outbox_events_status_available_idx',
    ]);
    const unique = cfg.indexes.find(
      (i) => i.config.name === 'outbox_events_idempotency_key_unique',
    );
    assert.equal(unique?.config.unique, true);
    // The partial-index predicate is part of the dedup contract.
    const where = new SQLiteSyncDialect().sqlToQuery(unique!.config.where!);
    assert.match(where.sql, /idempotency_key.+is not null/i);
    const statusIdx = cfg.indexes.find(
      (i) => i.config.name === 'outbox_events_status_available_idx',
    );
    assert.deepEqual(
      statusIdx?.config.columns.map((c) => (c as { name: string }).name),
      ['status', 'available_at'],
    );
  });

  test('inbox_events: composite (source, message_key) unique index', () => {
    const cfg = getSqliteConfig(sqlite.inboxEvents);
    assert.equal(cfg.name, 'inbox_events');
    assert.deepEqual(columnTable(cfg), INBOX_TEXT_COLUMNS);
    const unique = cfg.indexes.find(
      (i) => i.config.name === 'inbox_events_source_message_key_unique',
    );
    assert.equal(unique?.config.unique, true);
    assert.deepEqual(
      unique?.config.columns.map((c) => (c as { name: string }).name),
      ['source', 'message_key'],
    );
  });
});

describe('postgres schema', () => {
  test('outbox_events: columns + indexes', () => {
    const cfg = getPgConfig(postgres.outboxEvents);
    assert.equal(cfg.name, 'outbox_events');
    assert.deepEqual(columnTable(cfg), OUTBOX_TEXT_COLUMNS);
    const names = cfg.indexes.map((i) => i.config.name).sort();
    assert.deepEqual(names, [
      'outbox_events_idempotency_key_unique',
      'outbox_events_status_available_idx',
    ]);
    const unique = cfg.indexes.find(
      (i) => i.config.name === 'outbox_events_idempotency_key_unique',
    );
    assert.equal(unique?.config.unique, true);
    const where = new PgDialect().sqlToQuery(unique!.config.where!);
    assert.match(where.sql, /idempotency_key.+is not null/i);
    const statusIdx = cfg.indexes.find(
      (i) => i.config.name === 'outbox_events_status_available_idx',
    );
    assert.deepEqual(
      statusIdx?.config.columns.map((c) => (c as { name: string }).name),
      ['status', 'available_at'],
    );
  });

  test('inbox_events: composite unique index', () => {
    const cfg = getPgConfig(postgres.inboxEvents);
    assert.equal(cfg.name, 'inbox_events');
    assert.deepEqual(columnTable(cfg), INBOX_TEXT_COLUMNS);
    const unique = cfg.indexes.find(
      (i) => i.config.name === 'inbox_events_source_message_key_unique',
    );
    assert.equal(unique?.config.unique, true);
    assert.deepEqual(
      unique?.config.columns.map((c) => (c as { name: string }).name),
      ['source', 'message_key'],
    );
  });
});

describe('mysql schema', () => {
  test('outbox_events: columns + full idempotency-key unique + status index', () => {
    const cfg = getMysqlConfig(mysql.outboxEvents);
    assert.equal(cfg.name, 'outbox_events');
    // varchar widths are deliberate (191 = utf8mb4-safe index width; 32 fits
    // ISO-8601) — see the schema file's rationale comment.
    assert.deepEqual(columnTable(cfg), {
      id: { notNull: true, length: 191 },
      topic: { notNull: true, length: 255 },
      payload: { notNull: true },
      status: { notNull: true, length: 32 },
      attempts: { notNull: true, default: 0 },
      max_attempts: { notNull: true, default: 10 },
      idempotency_key: { notNull: false, length: 191 },
      available_at: { notNull: true, length: 32 },
      claimed_at: { notNull: false, length: 32 },
      claimed_by: { notNull: false, length: 191 },
      processed_at: { notNull: false, length: 32 },
      last_error: { notNull: false },
      created_at: { notNull: true, length: 32 },
    });
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
    assert.deepEqual(
      unique?.config.columns.map((c) => (c as { name: string }).name),
      ['idempotency_key'],
    );
    const statusIdx = cfg.indexes.find(
      (i) => i.config.name === 'outbox_events_status_available_idx',
    );
    assert.deepEqual(
      statusIdx?.config.columns.map((c) => (c as { name: string }).name),
      ['status', 'available_at'],
    );
  });

  test('inbox_events: composite (source, message_key) unique index', () => {
    const cfg = getMysqlConfig(mysql.inboxEvents);
    assert.equal(cfg.name, 'inbox_events');
    assert.deepEqual(columnTable(cfg), {
      id: { notNull: true, length: 191 },
      message_key: { notNull: true, length: 191 },
      source: { notNull: true, length: 191 },
      status: { notNull: true, length: 32 },
      processed_at: { notNull: true, length: 32 },
      last_error: { notNull: false },
      created_at: { notNull: true, length: 32 },
    });
    const unique = cfg.indexes.find(
      (i) => i.config.name === 'inbox_events_source_message_key_unique',
    );
    assert.equal(unique?.config.unique, true);
    assert.deepEqual(
      unique?.config.columns.map((c) => (c as { name: string }).name),
      ['source', 'message_key'],
    );
  });
});
