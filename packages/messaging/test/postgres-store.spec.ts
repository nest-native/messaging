import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { before, beforeEach, describe, test } from 'node:test';
import { PGlite } from '@electric-sql/pglite';
import { drizzle, type PgliteDatabase } from 'drizzle-orm/pglite';
import { eq } from 'drizzle-orm';
import { DEFAULT_CLAIMER_CONFIG } from '../outbox-claimer.service';
import {
  inboxEvents,
  isPgUniqueViolation,
  outboxEvents,
  PostgresInboxStore,
  PostgresOutboxStore,
} from '../dialects/postgres';

// The stores cast `db as NodePgDatabase` at runtime; pglite's PgliteDatabase
// runs the same pg-core SQL in-process, so it exercises the real Postgres paths
// (jsonb, 23505 unique violations, async transactions) without a service.
const DDL = `
CREATE TABLE outbox_events (
  id TEXT PRIMARY KEY, topic TEXT NOT NULL, payload JSONB NOT NULL, status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 10,
  idempotency_key TEXT, available_at TEXT NOT NULL, claimed_at TEXT, claimed_by TEXT,
  processed_at TEXT, last_error TEXT, created_at TEXT NOT NULL);
CREATE UNIQUE INDEX outbox_idem ON outbox_events (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE TABLE inbox_events (
  id TEXT PRIMARY KEY, message_key TEXT NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL,
  processed_at TEXT NOT NULL, last_error TEXT, created_at TEXT NOT NULL);
CREATE UNIQUE INDEX inbox_src_key ON inbox_events (source, message_key);
`;

let db: PgliteDatabase<Record<string, never>>;
const cfg = { ...DEFAULT_CLAIMER_CONFIG, batchSize: 10, stuckTimeoutMs: 1_000 };

before(() => {
  // Surface a clear message if the optional native dep failed to load.
  assert.ok(PGlite, 'pglite must be installed for the postgres store suite');
});

beforeEach(async () => {
  const client = new PGlite();
  db = drizzle(client);
  for (const stmt of DDL.split(';')) {
    const trimmed = stmt.trim();
    if (trimmed) await db.execute(trimmed);
  }
});

describe('PostgresOutboxStore', () => {
  const store = new PostgresOutboxStore();

  test('enqueue inserts a pending row (await) and stores jsonb payload', async () => {
    const row = await store.enqueue(db, {
      topic: 't',
      payload: { a: 1 },
      idempotencyKey: 'k1',
    });
    assert.equal(row.status, 'pending');
    assert.deepEqual(row.payload, { a: 1 });
    assert.equal(row.idempotencyKey, 'k1');
    assert.equal(row.maxAttempts, 10);
  });

  test('enqueue defaults: null idempotency key, maxAttempts override', async () => {
    const row = await store.enqueue(db, { topic: 't', payload: {}, maxAttempts: 2 });
    assert.equal(row.idempotencyKey, null);
    assert.equal(row.maxAttempts, 2);
  });

  test('claimBatch claims due rows; empty when none due; reclaims stuck', async () => {
    await store.enqueue(db, { topic: 't', payload: {} });
    const claimed = await store.claimBatch(db, cfg);
    assert.equal(claimed.length, 1);
    assert.equal(claimed[0]?.status, 'processing');

    assert.deepEqual(await store.claimBatch(db, cfg), []);

    const stale = new Date(Date.now() - 10_000).toISOString();
    await db
      .update(outboxEvents)
      .set({ status: 'processing', claimedAt: stale, claimedBy: 'dead' })
      .where(eq(outboxEvents.id, claimed[0]!.id));
    const reclaimed = await store.claimBatch(db, cfg);
    assert.equal(reclaimed.length, 1);
    assert.equal(reclaimed[0]?.claimedBy, cfg.workerInstanceId);
  });

  test('markCompleted / retry / markFailed transition the row', async () => {
    const row = await store.enqueue(db, { topic: 't', payload: {} });
    await store.markCompleted(db, row.id);
    assert.equal((await fetch(row.id))?.status, 'completed');

    await store.retry(db, row.id, 5_000, 'boom');
    let after = await fetch(row.id);
    assert.equal(after?.status, 'pending');
    assert.equal(after?.attempts, 1);
    assert.equal(after?.lastError, 'boom');

    await store.retry(db, row.id, 1_000);
    after = await fetch(row.id);
    assert.equal(after?.attempts, 2);
    assert.equal(after?.lastError, null);

    await store.markFailed(db, row.id, 'dead');
    after = await fetch(row.id);
    assert.equal(after?.status, 'failed');
    assert.equal(after?.attempts, 3);
  });

  async function fetch(id: string) {
    const rows = await db.select().from(outboxEvents).where(eq(outboxEvents.id, id));
    return rows[0];
  }
});

describe('PostgresInboxStore', () => {
  const store = new PostgresInboxStore();

  test('runOnce processes a fresh key (async side effect)', async () => {
    let ran = 0;
    const outcome = await store.runOnce(db, 'k1', 'src', async () => {
      ran += 1;
    });
    assert.equal(outcome, 'processed');
    assert.equal(ran, 1);
    assert.equal((await db.select().from(inboxEvents)).length, 1);
  });

  test('runOnce returns duplicate on a repeated key, skips side effect', async () => {
    await store.runOnce(db, 'k1', 'src', () => {});
    let ran = 0;
    const outcome = await store.runOnce(db, 'k1', 'src', () => {
      ran += 1;
    });
    assert.equal(outcome, 'duplicate');
    assert.equal(ran, 0);
  });

  test('a side-effect error propagates', async () => {
    await assert.rejects(
      () =>
        store.runOnce(db, 'k1', 'src', () => {
          throw new Error('side effect failed');
        }),
      /side effect failed/,
    );
  });

  test('a non-unique INSERT error is rethrown (not treated as duplicate)', async () => {
    const failingDb = {
      insert: () => ({ values: () => Promise.reject(new Error('connection lost')) }),
    };
    await assert.rejects(
      () => store.runOnce(failingDb, 'k', 'src', () => {}),
      /connection lost/,
    );
  });
});

describe('isPgUniqueViolation', () => {
  test('matches 23505 (direct or wrapped in cause), rejects others', () => {
    assert.equal(isPgUniqueViolation({ code: '23505' }), true);
    assert.equal(isPgUniqueViolation({ cause: { code: '23505' } }), true);
    assert.equal(isPgUniqueViolation({ code: '23503' }), false);
    assert.equal(isPgUniqueViolation(new Error('x')), false);
    assert.equal(isPgUniqueViolation(null), false);
    assert.equal(isPgUniqueViolation(42), false);
  });
});
