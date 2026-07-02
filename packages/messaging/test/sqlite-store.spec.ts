import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { beforeEach, describe, test } from 'node:test';
import Database from 'better-sqlite3';
import { drizzle, type BetterSQLite3Database } from 'drizzle-orm/better-sqlite3';
import { eq } from 'drizzle-orm';
import { DEFAULT_CLAIMER_CONFIG } from '../outbox-claimer.service';
import {
  inboxEvents,
  isSqliteUniqueViolation,
  outboxEvents,
  SqliteInboxStore,
  SqliteOutboxStore,
} from '../dialects/sqlite';

const DDL = `
CREATE TABLE outbox_events (
  id TEXT PRIMARY KEY, topic TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 10,
  idempotency_key TEXT, available_at TEXT NOT NULL, claimed_at TEXT, claimed_by TEXT,
  processed_at TEXT, last_error TEXT, created_at TEXT NOT NULL);
CREATE UNIQUE INDEX outbox_idem ON outbox_events (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE TABLE inbox_events (
  id TEXT PRIMARY KEY, message_key TEXT NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL,
  processed_at TEXT NOT NULL, last_error TEXT, created_at TEXT NOT NULL);
CREATE UNIQUE INDEX inbox_src_key ON inbox_events (source, message_key);
`;

let db: BetterSQLite3Database<Record<string, never>>;
const cfg = { ...DEFAULT_CLAIMER_CONFIG, batchSize: 10, stuckTimeoutMs: 1_000 };

beforeEach(() => {
  const sqlite = new Database(':memory:');
  sqlite.exec(DDL);
  db = drizzle(sqlite);
});

describe('SqliteOutboxStore', () => {
  const store = new SqliteOutboxStore();

  test('enqueue inserts a pending row and returns it synchronously', () => {
    const row = store.enqueue(db, {
      topic: 't',
      payload: { a: 1 },
      idempotencyKey: 'k1',
    });
    assert.equal(row.status, 'pending');
    assert.equal(row.topic, 't');
    assert.equal(row.idempotencyKey, 'k1');
    assert.deepEqual(row.payload, { a: 1 });
    assert.equal(row.attempts, 0);
    assert.equal(row.maxAttempts, 10);
  });

  test('enqueue accepts a payload typed as a plain interface — no cast', () => {
    // Compile-level regression: an interface has no index signature, so it is
    // not assignable to Record<string, unknown>; the store seam takes
    // EnqueueInput<object> and widens the stored payload internally.
    interface OrderPlaced {
      orderId: string;
      qty: number;
    }
    const payload: OrderPlaced = { orderId: 'o-1', qty: 2 };
    const row = store.enqueue(db, { topic: 'order.placed', payload });
    assert.deepEqual(row.payload, { orderId: 'o-1', qty: 2 });
  });

  test('enqueue honours availableAt and maxAttempts; null idempotency key', () => {
    const at = new Date(Date.now() + 60_000);
    const row = store.enqueue(db, {
      topic: 't',
      payload: {},
      availableAt: at,
      maxAttempts: 3,
    });
    assert.equal(row.availableAt, at.toISOString());
    assert.equal(row.maxAttempts, 3);
    assert.equal(row.idempotencyKey, null);
  });

  test('claimBatch claims due pending rows and marks them processing', async () => {
    store.enqueue(db, { topic: 't', payload: {} });
    store.enqueue(db, {
      topic: 't',
      payload: {},
      availableAt: new Date(Date.now() + 60_000),
    });
    const claimed = await store.claimBatch(db, cfg);
    assert.equal(claimed.length, 1);
    assert.equal(claimed[0]?.status, 'processing');
    assert.equal(claimed[0]?.claimedBy, cfg.workerInstanceId);
  });

  test('claimBatch returns empty when nothing is due', async () => {
    store.enqueue(db, {
      topic: 't',
      payload: {},
      availableAt: new Date(Date.now() + 60_000),
    });
    assert.deepEqual(await store.claimBatch(db, cfg), []);
  });

  test('claimBatch reclaims a stuck processing row past the timeout', async () => {
    const row = store.enqueue(db, { topic: 't', payload: {} });
    const stale = new Date(Date.now() - 10_000).toISOString();
    db.update(outboxEvents)
      .set({ status: 'processing', claimedAt: stale, claimedBy: 'dead' })
      .where(eq(outboxEvents.id, row.id))
      .run();
    const claimed = await store.claimBatch(db, cfg);
    assert.equal(claimed.length, 1);
    assert.equal(claimed[0]?.claimedBy, cfg.workerInstanceId);
  });

  test('markCompleted, retry, markFailed transition the row', async () => {
    const row = store.enqueue(db, { topic: 't', payload: {} });
    await store.markCompleted(db, row.id);
    let after = db.select().from(outboxEvents).where(eq(outboxEvents.id, row.id)).get();
    assert.equal(after?.status, 'completed');
    assert.ok(after?.processedAt);

    await store.retry(db, row.id, 5_000, 'boom');
    after = db.select().from(outboxEvents).where(eq(outboxEvents.id, row.id)).get();
    assert.equal(after?.status, 'pending');
    assert.equal(after?.attempts, 1);
    assert.equal(after?.lastError, 'boom');

    await store.retry(db, row.id, 1_000);
    after = db.select().from(outboxEvents).where(eq(outboxEvents.id, row.id)).get();
    assert.equal(after?.attempts, 2);
    assert.equal(after?.lastError, null);

    await store.markFailed(db, row.id, 'dead');
    after = db.select().from(outboxEvents).where(eq(outboxEvents.id, row.id)).get();
    assert.equal(after?.status, 'failed');
    assert.equal(after?.lastError, 'dead');
    assert.equal(after?.attempts, 3);
  });
});

describe('SqliteInboxStore', () => {
  const store = new SqliteInboxStore();

  test('runOnce processes a fresh key and runs the side effect', () => {
    let ran = 0;
    const outcome = store.runOnce(db, 'k1', 'src', () => {
      ran += 1;
    });
    assert.equal(outcome, 'processed');
    assert.equal(ran, 1);
    assert.equal(db.select().from(inboxEvents).all().length, 1);
  });

  test('runOnce returns duplicate on a repeated key and skips the side effect', () => {
    store.runOnce(db, 'k1', 'src', () => {});
    let ran = 0;
    const outcome = store.runOnce(db, 'k1', 'src', () => {
      ran += 1;
    });
    assert.equal(outcome, 'duplicate');
    assert.equal(ran, 0);
    assert.equal(db.select().from(inboxEvents).all().length, 1);
  });

  test('same key under a different source is processed independently', () => {
    assert.equal(store.runOnce(db, 'k1', 'a', () => {}), 'processed');
    assert.equal(store.runOnce(db, 'k1', 'b', () => {}), 'processed');
  });

  test('a side-effect error propagates (not swallowed as duplicate)', () => {
    assert.throws(
      () =>
        store.runOnce(db, 'k1', 'src', () => {
          throw new Error('side effect failed');
        }),
      /side effect failed/,
    );
  });

  test('a non-unique INSERT error is rethrown (not treated as duplicate)', () => {
    // A db whose insert fails with a non-unique error exercises the rethrow path.
    const failingDb = {
      insert: () => ({ values: () => ({ run: () => { throw new Error('disk full'); } }) }),
    };
    assert.throws(
      () => store.runOnce(failingDb, 'k', 'src', () => {}),
      /disk full/,
    );
  });
});

describe('isSqliteUniqueViolation', () => {
  test('matches the unique code (direct or wrapped in cause), rejects others', () => {
    assert.equal(isSqliteUniqueViolation({ code: 'SQLITE_CONSTRAINT_UNIQUE' }), true);
    assert.equal(
      isSqliteUniqueViolation({ cause: { code: 'SQLITE_CONSTRAINT_UNIQUE' } }),
      true,
    );
    assert.equal(isSqliteUniqueViolation({ code: 'SQLITE_BUSY' }), false);
    assert.equal(isSqliteUniqueViolation(new Error('x')), false);
    assert.equal(isSqliteUniqueViolation(null), false);
    assert.equal(isSqliteUniqueViolation('nope'), false);
  });
});
