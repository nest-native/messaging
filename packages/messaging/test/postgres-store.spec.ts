import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { before, beforeEach, describe, test } from 'node:test';
import { PGlite } from '@electric-sql/pglite';
import { drizzle, type PgliteDatabase } from 'drizzle-orm/pglite';
import { eq } from 'drizzle-orm';
import { DEFAULT_CLAIMER_CONFIG } from '../outbox-claimer.service';
import type { OutboxClaim, OutboxEventRow } from '../interfaces';
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

  test('#62: the claim runs at READ COMMITTED whatever the database default', async () => {
    // Under REPEATABLE READ or SERIALIZABLE, a row another worker claims while
    // the scan is running fails the claim (40001) instead of being skipped.
    const configs: unknown[] = [];
    const recording = {
      transaction: (...args: Parameters<typeof db.transaction>) => {
        configs.push(args[1]);
        return db.transaction(...args);
      },
    };
    await store.enqueue(db, { topic: 't', payload: {} });
    assert.equal((await store.claimBatch(recording, cfg)).length, 1);
    assert.deepEqual(configs, [{ isolationLevel: 'read committed' }]);
  });

  // The claim the claimer hands back: the row's id plus the stamp claimBatch wrote.
  const claimOf = (row: OutboxEventRow): OutboxClaim => ({
    id: row.id,
    claimedBy: row.claimedBy!,
    claimedAt: row.claimedAt!,
  });
  // Ages a claim past stuckTimeoutMs, as if its worker had stalled.
  const backdate = (id: string) =>
    db
      .update(outboxEvents)
      .set({ claimedAt: new Date(Date.now() - 10_000).toISOString() })
      .where(eq(outboxEvents.id, id));

  test('markCompleted transitions the row its claim still holds', async () => {
    await store.enqueue(db, { topic: 't', payload: {} });
    const [claimed] = await store.claimBatch(db, cfg);
    assert.equal(await store.markCompleted(db, claimOf(claimed!)), true);
    assert.equal((await fetch(claimed!.id))?.status, 'completed');
  });

  test('retry re-arms the row and releases the claim', async () => {
    await store.enqueue(db, { topic: 't', payload: {} });
    const [claimed] = await store.claimBatch(db, cfg);
    const before = Date.now();
    assert.equal(await store.retry(db, claimOf(claimed!), 5_000, 'boom'), true);
    const after = await fetch(claimed!.id);
    assert.equal(after?.status, 'pending');
    assert.equal(after?.attempts, 1);
    assert.equal(after?.lastError, 'boom');
    // The retry delay pushes availableAt INTO THE FUTURE by delayMs.
    assert.ok(new Date(after!.availableAt).getTime() >= before + 5_000);
    assert.equal(after?.claimedBy, null);
    assert.equal(after?.claimedAt, null);
  });

  test('retry without lastError clears the previous one; attempts accumulate', async () => {
    await store.enqueue(db, { topic: 't', payload: {} });
    const [first] = await store.claimBatch(db, cfg);
    // Due again at once, so the next claim picks it up with 'boom' still set.
    await store.retry(db, claimOf(first!), 0, 'boom');
    const [second] = await store.claimBatch(db, cfg);
    await store.retry(db, claimOf(second!), 1_000);
    const after = await fetch(first!.id);
    assert.equal(after?.lastError, null);
    assert.equal(after?.attempts, 2);
  });

  test('markFailed records the reason', async () => {
    await store.enqueue(db, { topic: 't', payload: {} });
    const [claimed] = await store.claimBatch(db, cfg);
    assert.equal(await store.markFailed(db, claimOf(claimed!), 'dead'), true);
    const after = await fetch(claimed!.id);
    assert.equal(after?.status, 'failed');
    assert.equal(after?.lastError, 'dead');
    assert.equal(after?.attempts, 1);
  });

  test('#62: a worker whose stuck claim was taken over cannot move the row', async () => {
    const row = await store.enqueue(db, { topic: 't', payload: {} });
    const [mine] = await store.claimBatch(db, { ...cfg, workerInstanceId: 'worker-A' });
    await backdate(row.id);
    const [theirs] = await store.claimBatch(db, { ...cfg, workerInstanceId: 'worker-B' });
    const owned = await fetch(row.id);
    assert.equal(await store.markCompleted(db, claimOf(mine!)), false);
    assert.equal(await store.retry(db, claimOf(mine!), 0, 'late'), false);
    assert.equal(await store.markFailed(db, claimOf(mine!), 'late'), false);
    assert.deepEqual(await fetch(row.id), owned);
    assert.equal(await store.markCompleted(db, claimOf(theirs!)), true);
  });

  test('an earlier claim under the same worker id cannot move the row', async () => {
    // Two loops in one process share the default host-pid id, so the id alone
    // cannot tell the stale claim from the live one; the claim stamp does.
    const row = await store.enqueue(db, { topic: 't', payload: {} });
    await store.claimBatch(db, cfg);
    await backdate(row.id);
    const stale = claimOf((await fetch(row.id)) as OutboxEventRow);
    const [live] = await store.claimBatch(db, cfg);
    assert.equal(live?.claimedBy, stale.claimedBy);
    const owned = await fetch(row.id);
    assert.equal(await store.retry(db, stale, 0, 'late'), false);
    assert.deepEqual(await fetch(row.id), owned);
    assert.equal(await store.markCompleted(db, claimOf(live!)), true);
  });

  test('a transition that hits a serialization failure reports the claim lost', async () => {
    // What a REPEATABLE READ or SERIALIZABLE server raises (40001) when the
    // row changed under the UPDATE; drizzle may wrap it as the `cause`.
    const failing = (error: unknown) => ({
      update: () => ({
        set: () => ({ where: () => ({ returning: () => Promise.reject(error) }) }),
      }),
    });
    const serialization = Object.assign(new Error('could not serialize access'), { code: '40001' });
    const claim: OutboxClaim = { id: 'x', claimedBy: 'w', claimedAt: new Date().toISOString() };
    for (const error of [serialization, Object.assign(new Error('Failed query'), { cause: serialization })]) {
      assert.equal(await store.markCompleted(failing(error), claim), false);
      assert.equal(await store.retry(failing(error), claim, 0, 'x'), false);
      assert.equal(await store.markFailed(failing(error), claim, 'x'), false);
    }
    // Anything else still throws.
    await assert.rejects(store.markCompleted(failing(new Error('connection lost')), claim), /connection lost/);
    await assert.rejects(store.markCompleted(failing(null), claim));
  });

  // The fence has four conditions; each case below breaks exactly one, so
  // dropping any of them from the store fails a case.
  const transitions: [string, (claim: OutboxClaim) => Promise<boolean>][] = [
    ['markCompleted', (claim) => store.markCompleted(db, claim)],
    ['retry', (claim) => store.retry(db, claim, 0, 'stale')],
    ['markFailed', (claim) => store.markFailed(db, claim, 'stale')],
  ];
  for (const [name, transition] of transitions) {
    test(`${name} with another claimedBy writes nothing`, async () => {
      await store.enqueue(db, { topic: 't', payload: {} });
      const [claimed] = await store.claimBatch(db, cfg);
      const before = await fetch(claimed!.id);
      assert.equal(await transition({ ...claimOf(claimed!), claimedBy: 'another-worker' }), false);
      assert.deepEqual(await fetch(claimed!.id), before);
    });

    test(`${name} with another claimedAt writes nothing`, async () => {
      await store.enqueue(db, { topic: 't', payload: {} });
      const [claimed] = await store.claimBatch(db, cfg);
      const before = await fetch(claimed!.id);
      const earlier = new Date(Date.parse(claimed!.claimedAt!) - 60_000).toISOString();
      assert.equal(await transition({ ...claimOf(claimed!), claimedAt: earlier }), false);
      assert.deepEqual(await fetch(claimed!.id), before);
    });

    test(`${name} on a row that is no longer processing writes nothing`, async () => {
      await store.enqueue(db, { topic: 't', payload: {} });
      const [claimed] = await store.claimBatch(db, cfg);
      // markCompleted keeps the claim stamp, so only the status tells.
      await store.markCompleted(db, claimOf(claimed!));
      const before = await fetch(claimed!.id);
      assert.equal(await transition(claimOf(claimed!)), false);
      assert.deepEqual(await fetch(claimed!.id), before);
    });

    test(`${name} moves only its own row, not the rest of its batch`, async () => {
      await store.enqueue(db, { topic: 't', payload: {} });
      await store.enqueue(db, { topic: 't', payload: {} });
      const [mine, other] = await store.claimBatch(db, cfg);
      // One claim stamps the whole batch, so only the id tells the rows apart.
      assert.equal(mine!.claimedAt, other!.claimedAt);
      const before = await fetch(other!.id);
      assert.equal(await transition(claimOf(mine!)), true);
      assert.deepEqual(await fetch(other!.id), before);
    });
  }

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
    const rows = await db.select().from(inboxEvents);
    assert.equal(rows.length, 1);
    // The dedup row records the full inbox shape.
    assert.equal(rows[0]?.messageKey, 'k1');
    assert.equal(rows[0]?.source, 'src');
    assert.equal(rows[0]?.status, 'processed');
    assert.ok(rows[0]?.processedAt);
    assert.ok(rows[0]?.createdAt);
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

describe('PostgresOutboxStore wakeChannel', () => {
  test('enqueue fires pg_notify on the configured channel (observed via LISTEN)', async () => {
    // A dedicated PGlite so we can reach its listen() API directly — the shared
    // beforeEach db hides the raw client behind drizzle.
    const raw = new PGlite();
    const wakeDb = drizzle(raw);
    for (const stmt of DDL.split(';')) {
      const trimmed = stmt.trim();
      if (trimmed) await wakeDb.execute(trimmed);
    }
    let wakes = 0;
    await raw.listen('outbox_wake_test', () => {
      wakes += 1;
    });

    const store = new PostgresOutboxStore({ wakeChannel: 'outbox_wake_test' });
    const row = await store.enqueue(wakeDb, { topic: 't', payload: { a: 1 } });
    assert.equal(row.status, 'pending'); // the insert itself is unchanged

    const deadline = Date.now() + 5_000;
    while (wakes === 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    assert.equal(wakes, 1, 'pg_notify must reach the listener');
    await raw.close();
  });

  test('rejects an unsafe wakeChannel at construction', () => {
    assert.throws(
      () => new PostgresOutboxStore({ wakeChannel: 'x"; DROP TABLE y' }),
      /invalid wake channel/,
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
