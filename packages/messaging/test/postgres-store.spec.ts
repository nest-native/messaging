import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { EventEmitter } from 'node:events';
import { before, beforeEach, describe, test } from 'node:test';
import { Logger } from '@nestjs/common';
import { PGlite } from '@electric-sql/pglite';
import { drizzle, type PgliteDatabase } from 'drizzle-orm/pglite';
import { drizzle as drizzleNodePg } from 'drizzle-orm/node-postgres';
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

let pglite: PGlite;
let db: PgliteDatabase<Record<string, never>>;
const cfg = { ...DEFAULT_CLAIMER_CONFIG, batchSize: 10, stuckTimeoutMs: 1_000 };

/**
 * A node-postgres Pool as the store sees it (`connect()` and `totalCount`),
 * whose one client runs every statement on the in-process PGlite and records
 * the statements and how it was released. `failOn` makes a statement fail.
 */
function pglitePool(connectDelayMs = 0) {
  const statements: string[] = [];
  const releases: (Error | undefined)[] = [];
  let failing: (text: string) => Error | undefined = () => undefined;
  const client = Object.assign(new EventEmitter(), {
    async query(config: string | { text: string; rowMode?: 'array' }, values?: unknown[]) {
      const text = typeof config === 'string' ? config : config.text;
      statements.push(text);
      const failure = failing(text);
      if (failure) throw failure;
      const rowMode = typeof config === 'string' ? undefined : config.rowMode;
      const result = await pglite.query(text, values, { rowMode: rowMode ?? 'object' });
      return { rows: result.rows, rowCount: result.affectedRows ?? 0, fields: result.fields };
    },
    release(error?: Error) {
      releases.push(error);
    },
  });
  const pool = {
    totalCount: 1,
    connect: () => new Promise((resolve) => setTimeout(() => resolve(client), connectDelayMs)),
  };
  return {
    db: drizzleNodePg(pool as never),
    pool,
    client,
    statements,
    releases,
    transactionStatements: () => statements.filter((t) => /^(begin|commit|rollback)\b/.test(t)),
    failOn(predicate: (text: string) => Error | undefined) {
      failing = predicate;
    },
  };
}

before(() => {
  // Surface a clear message if the optional native dep failed to load.
  assert.ok(PGlite, 'pglite must be installed for the postgres store suite');
});

beforeEach(async () => {
  pglite = new PGlite();
  db = drizzle(pglite);
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

  test('release hands a held row back pending and unclaimed, attempts untouched', async () => {
    const row = await store.enqueue(db, { topic: 't', payload: {} });
    const [mine] = await store.claimBatch(db, cfg);
    assert.equal(await store.release(db, claimOf(mine!)), true);
    const after = await fetch(row.id);
    assert.equal(after?.status, 'pending');
    assert.equal(after?.claimedBy, null);
    assert.equal(after?.claimedAt, null);
    assert.equal(after?.attempts, 0);
    assert.equal(after?.availableAt, row.availableAt);
    assert.equal(await store.release(db, claimOf(mine!)), false);
  });

  test('#62: a worker whose stuck claim was taken over cannot move the row', async () => {
    const row = await store.enqueue(db, { topic: 't', payload: {} });
    const [mine] = await store.claimBatch(db, { ...cfg, workerInstanceId: 'worker-A' });
    await backdate(row.id);
    const [theirs] = await store.claimBatch(db, { ...cfg, workerInstanceId: 'worker-B' });
    const owned = await fetch(row.id);
    assert.equal(await store.release(db, claimOf(mine!)), false);
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

  test('#62: each transition runs in its own READ COMMITTED transaction whatever the database default', async () => {
    // Under SERIALIZABLE, two workers' fenced UPDATEs abort each other (40001)
    // while each claim still holds its row; under READ COMMITTED a real
    // takeover simply matches nothing.
    const configs: unknown[] = [];
    // Neither a missing client, a null one, nor a single node-postgres Client
    // (connect() but no pool counters) is a pool the store manages itself.
    const recording = ($client?: unknown) => ({
      $client,
      transaction: (...args: Parameters<typeof db.transaction>) => {
        configs.push(args[1]);
        return db.transaction(...args);
      },
    });
    for (const [transition, handle] of [
      [(claim: OutboxClaim, h: unknown) => store.markCompleted(h, claim), recording()],
      [(claim: OutboxClaim, h: unknown) => store.retry(h, claim, 60_000, 'later'), recording(null)],
      [(claim: OutboxClaim, h: unknown) => store.markFailed(h, claim, 'dead'), recording({ connect: () => undefined })],
    ] as const) {
      const row = await store.enqueue(db, { topic: 't', payload: {} });
      const claimed = await store.claimBatch(db, cfg);
      assert.deepEqual(claimed.map((r) => r.id), [row.id]);
      assert.equal(await transition(claimOf(claimed[0]!), handle), true);
    }
    assert.deepEqual(configs, Array(3).fill({ isolationLevel: 'read committed' }));
  });

  test('a database error in a transition propagates instead of reading as a lost claim', async () => {
    // A serialization failure included: it does not say another claim took
    // the row over.
    const failing = (error: unknown) => ({ transaction: () => Promise.reject(error) });
    // The transaction opens, and the fenced UPDATE itself fails.
    const failingUpdate = (error: unknown) => ({
      transaction: (work: (tx: unknown) => Promise<unknown>) =>
        work({ update: () => ({ set: () => ({ where: () => ({ returning: () => Promise.reject(error) }) }) }) }),
    });
    const serialization = Object.assign(new Error('could not serialize access'), { code: '40001' });
    const claim: OutboxClaim = { id: 'x', claimedBy: 'w', claimedAt: new Date().toISOString() };
    await assert.rejects(store.markCompleted(failing(serialization), claim), /could not serialize access/);
    await assert.rejects(store.retry(failing(serialization), claim, 0, 'x'), /could not serialize access/);
    await assert.rejects(store.markFailed(failing(new Error('connection lost')), claim, 'x'), /connection lost/);
    await assert.rejects(store.markCompleted(failingUpdate(serialization), claim), /could not serialize access/);
    await assert.rejects(store.retry(failingUpdate(serialization), claim, 0, 'x'), /could not serialize access/);
    await assert.rejects(store.markFailed(failingUpdate(serialization), claim, 'x'), /could not serialize access/);
  });

  test('on a node-postgres pool, claims and transitions run on a client the store checks out itself', async () => {
    // drizzle's transaction() leaves the checked-out client without an `error`
    // listener and sends BEGIN outside its cleanup.
    const pg = pglitePool();
    const row = await store.enqueue(db, { topic: 't', payload: {} });
    const [claimed] = await store.claimBatch(pg.db, cfg);
    assert.equal(claimed?.id, row.id);
    assert.equal(await store.markCompleted(pg.db, claimOf(claimed!)), true);
    assert.equal((await fetch(row.id))?.status, 'completed');
    assert.deepEqual(pg.transactionStatements(), [
      'begin isolation level read committed',
      'commit',
      'begin isolation level read committed',
      'commit',
    ]);
    assert.deepEqual(pg.releases, [undefined, undefined]);
    assert.equal(pg.client.listenerCount('error'), 0);
  });

  test('on a node-postgres pool, a claim is stamped once the connection is checked out', async () => {
    // A stamp taken before a slow checkout makes the claim look older than it
    // is, so another worker would treat its rows as stuck that much sooner.
    const pg = pglitePool(80);
    await store.enqueue(db, { topic: 't', payload: {} });
    const asked = Date.now();
    const [claimed] = await store.claimBatch(pg.db, cfg);
    assert.ok(Date.parse(claimed!.claimedAt!) >= asked + 70, `${claimed!.claimedAt} vs ${new Date(asked).toISOString()}`);
  });

  test('on a node-postgres pool, a failed statement rolls back and the client goes back for reuse', async () => {
    const pg = pglitePool();
    await store.enqueue(db, { topic: 't', payload: {} });
    const [claimed] = await store.claimBatch(pg.db, cfg);
    const failure = new Error('statement failed');
    pg.failOn((text) => (text.startsWith('update') ? failure : undefined));
    // drizzle reports a failed statement as "Failed query: …", with the error as its cause.
    await assert.rejects(
      store.markCompleted(pg.db, claimOf(claimed!)),
      (error: Error & { cause?: unknown }) => error.cause === failure,
    );
    assert.deepEqual(pg.transactionStatements().slice(-2), ['begin isolation level read committed', 'rollback']);
    assert.deepEqual(pg.releases, [undefined, undefined]);
  });

  test('on a node-postgres pool, a connection lost mid-transaction rejects and the client is discarded', async () => {
    // The server drops the connection while the transaction is open: the client
    // emits `error` between statements, and every later statement fails. Without
    // a listener that `error` crashes the process.
    const pg = pglitePool();
    await store.enqueue(db, { topic: 't', payload: {} });
    const [claimed] = await store.claimBatch(pg.db, cfg);
    const lost = new Error('Connection terminated unexpectedly');
    const notQueryable = new Error('Client has encountered a connection error and is not queryable');
    const rollbackFailed = new Error('rollback failed too');
    pg.failOn((text) => {
      if (text.startsWith('update')) {
        pg.client.emit('error', lost);
        return undefined;
      }
      if (text === 'commit') return notQueryable;
      return text === 'rollback' ? rollbackFailed : undefined;
    });
    // The statement's error reaches the caller, not the failed ROLLBACK's.
    await assert.rejects(store.markCompleted(pg.db, claimOf(claimed!)), (error) => error === notQueryable);
    // Released with the connection's error, so the pool discards the client.
    assert.equal(pg.releases[pg.releases.length - 1], lost);
    assert.equal(pg.client.listenerCount('error'), 0);
  });

  test('on a node-postgres pool, a BEGIN that fails still returns the client', async () => {
    const pg = pglitePool();
    const reset = new Error('Connection reset');
    const rollbackFailed = new Error('rollback failed too');
    pg.failOn((text) => {
      if (text.startsWith('begin')) return reset;
      return text === 'rollback' ? rollbackFailed : undefined;
    });
    await assert.rejects(store.claimBatch(pg.db, cfg), (error) => error === reset);
    // Released with the failed ROLLBACK's error: the connection is unusable.
    assert.deepEqual(pg.releases, [rollbackFailed]);
  });

  test("on a node-postgres pool, the store's statements go through the application's drizzle logger", async () => {
    const pg = pglitePool();
    const logged: string[] = [];
    const loggedDb = drizzleNodePg(pg.pool as never, { logger: { logQuery: (query: string) => logged.push(query) } });
    await store.enqueue(db, { topic: 't', payload: {} });
    const [claimed] = await store.claimBatch(loggedDb, cfg);
    assert.equal(await store.markCompleted(loggedDb, claimOf(claimed!)), true);
    assert.ok(logged.some((q) => q.includes('for update skip locked')), logged.join('\n'));
    assert.ok(logged.some((q) => q.startsWith('update "outbox_events" set "status"')), logged.join('\n'));
  });

  test("on a node-postgres pool without an 'error' listener, the store warns once", async () => {
    // node-postgres reports a connection an idle client loses as the pool's
    // `error` event, which crashes the process when nothing listens.
    const warns: string[] = [];
    Logger.overrideLogger({
      log: () => {},
      error: () => {},
      warn: (message: unknown) => warns.push(String(message)),
      debug: () => {},
      verbose: () => {},
    });
    try {
      const unguarded = Object.assign(new EventEmitter(), pglitePool().pool);
      const guarded = Object.assign(new EventEmitter(), pglitePool().pool).on('error', () => undefined);
      for (const pool of [unguarded, unguarded, guarded]) {
        const row = await store.enqueue(db, { topic: 't', payload: {} });
        const [claimed] = await store.claimBatch(drizzleNodePg(pool as never), cfg);
        assert.equal(claimed?.id, row.id);
      }
    } finally {
      Logger.overrideLogger(false);
    }
    assert.equal(warns.length, 1, warns.join('\n'));
    assert.match(warns[0] ?? '', /no 'error' listener/);
  });

  test('on a node-postgres pool, a handle without drizzle internals still works', async () => {
    // Only `$client` is read off the handle; the logger is a convenience.
    const pg = pglitePool();
    const row = await store.enqueue(db, { topic: 't', payload: {} });
    const [claimed] = await store.claimBatch({ $client: pg.pool }, cfg);
    assert.equal(claimed?.id, row.id);
    assert.equal(await store.markCompleted({ $client: pg.pool }, claimOf(claimed!)), true);
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
