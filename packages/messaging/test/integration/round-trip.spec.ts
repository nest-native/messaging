import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { after, before, describe, test } from 'node:test';
import { eq, sql } from 'drizzle-orm';
import type { OutboxClaim, OutboxEventRow } from '../../interfaces';
import { DEFAULT_CLAIMER_CONFIG } from '../../outbox-claimer.service';
import {
  inboxEvents as mysqlInboxEvents,
  MysqlInboxStore,
  MysqlOutboxStore,
  outboxEvents as mysqlOutboxEvents,
} from '../../dialects/mysql';
import {
  inboxEvents as pgInboxEvents,
  outboxEvents as pgOutboxEvents,
  PostgresInboxStore,
  PostgresOutboxStore,
  PostgresWakeListener,
} from '../../dialects/postgres';

// Gated end-to-end tests against a REAL database. They skip unless the matching
// URL env var is set, so `npm test` / `test:cov` stay hermetic and 100%. CI's
// `integration` job runs them through `test:integration:strict`, which fails if
// any of them skipped; locally, `npm run infra:up && npm run test:full` (see the
// "Local full-mode verification" section in GUIDELINES_NEST_MESSAGING.md).
// The stores are driven directly (no Nest) — a genuine produce -> claim ->
// complete -> inbox-dedup round-trip that exercises the real driver: JSON
// payloads, errno 1062 unique violations, and async transactions in `claimBatch`.

const MYSQL_URL = process.env.MESSAGING_MYSQL_URL;
const POSTGRES_URL = process.env.MESSAGING_POSTGRES_URL;
const cfg = { ...DEFAULT_CLAIMER_CONFIG, batchSize: 50, stuckTimeoutMs: 1_000 };

// The claim the claimer hands back: the row's id plus the stamp claimBatch wrote.
const claimOf = (row: OutboxEventRow): OutboxClaim => ({
  id: row.id,
  claimedBy: row.claimedBy!,
  claimedAt: row.claimedAt!,
});
const sortedIds = (rows: { id: string }[]): string[] => rows.map((r) => r.id).sort();
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const backdated = () => new Date(Date.now() - 10_000).toISOString();

/** `promise`'s value, or 'blocked' when it has not settled within `ms`. */
async function settleWithin<T>(promise: Promise<T>, ms: number): Promise<T | 'blocked'> {
  let timer: NodeJS.Timeout | undefined;
  const blocked = new Promise<'blocked'>((resolve) => {
    timer = setTimeout(() => resolve('blocked'), ms);
  });
  try {
    return await Promise.race([promise, blocked]);
  } finally {
    clearTimeout(timer);
  }
}

const MYSQL_DDL = [
  'DROP TABLE IF EXISTS outbox_events',
  'DROP TABLE IF EXISTS inbox_events',
  'DROP TABLE IF EXISTS integration_side_effects',
  `CREATE TABLE outbox_events (
     id VARCHAR(191) PRIMARY KEY, topic VARCHAR(255) NOT NULL, payload JSON NOT NULL,
     status VARCHAR(32) NOT NULL, attempts INT NOT NULL DEFAULT 0, max_attempts INT NOT NULL DEFAULT 10,
     idempotency_key VARCHAR(191), available_at VARCHAR(32) NOT NULL, claimed_at VARCHAR(32),
     claimed_by VARCHAR(191), processed_at VARCHAR(32), last_error TEXT, created_at VARCHAR(32) NOT NULL,
     UNIQUE KEY outbox_events_idempotency_key_unique (idempotency_key),
     KEY outbox_events_status_available_idx (status, available_at))`,
  `CREATE TABLE inbox_events (
     id VARCHAR(191) PRIMARY KEY, message_key VARCHAR(191) NOT NULL, source VARCHAR(191) NOT NULL,
     status VARCHAR(32) NOT NULL, processed_at VARCHAR(32) NOT NULL, last_error TEXT, created_at VARCHAR(32) NOT NULL,
     UNIQUE KEY inbox_events_source_message_key_unique (source, message_key))`,
  `CREATE TABLE integration_side_effects (dedup_key VARCHAR(191) PRIMARY KEY, note VARCHAR(255) NOT NULL)`,
];

const PG_DDL = [
  'DROP TABLE IF EXISTS outbox_events',
  'DROP TABLE IF EXISTS inbox_events',
  'DROP TABLE IF EXISTS integration_side_effects',
  `CREATE TABLE outbox_events (
     id TEXT PRIMARY KEY, topic TEXT NOT NULL, payload JSONB NOT NULL, status TEXT NOT NULL,
     attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 10,
     idempotency_key TEXT, available_at TEXT NOT NULL, claimed_at TEXT, claimed_by TEXT,
     processed_at TEXT, last_error TEXT, created_at TEXT NOT NULL)`,
  'CREATE UNIQUE INDEX outbox_events_idempotency_key_unique ON outbox_events (idempotency_key) WHERE idempotency_key IS NOT NULL',
  // The shipped schema's index. The plan decides which rows a statement reads
  // and locks: with it, #73's first cut failed half the transitions in the
  // SERIALIZABLE spec below; without it, one or two.
  'CREATE INDEX outbox_events_status_available_idx ON outbox_events (status, available_at)',
  `CREATE TABLE inbox_events (
     id TEXT PRIMARY KEY, message_key TEXT NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL,
     processed_at TEXT NOT NULL, last_error TEXT, created_at TEXT NOT NULL)`,
  'CREATE UNIQUE INDEX inbox_events_source_message_key_unique ON inbox_events (source, message_key)',
  'CREATE TABLE integration_side_effects (dedup_key TEXT PRIMARY KEY, note TEXT NOT NULL)',
];

describe('MySQL round-trip (real service)', { skip: !MYSQL_URL }, () => {
  let connection: Awaited<ReturnType<typeof import('mysql2/promise').createConnection>>;
  let db: Awaited<ReturnType<typeof buildMysqlDb>>;
  // The concurrency specs need several connections: on the single one above,
  // two claims would simply run one after the other.
  let pool: import('mysql2/promise').Pool;
  let poolDb: Awaited<ReturnType<typeof buildMysqlDb>>;
  const outbox = new MysqlOutboxStore();
  const inbox = new MysqlInboxStore();

  async function buildMysqlDb(conn: unknown) {
    const { drizzle } = await import('drizzle-orm/mysql2');
    return drizzle(conn as never, { mode: 'default' });
  }

  before(async () => {
    const mysql = await import('mysql2/promise');
    connection = await mysql.createConnection(MYSQL_URL as string);
    for (const stmt of MYSQL_DDL) await connection.query(stmt);
    db = await buildMysqlDb(connection);
    pool = mysql.createPool({ uri: MYSQL_URL as string, connectionLimit: 4 });
    poolDb = await buildMysqlDb(pool);
  });

  after(async () => {
    await connection?.end();
    await pool?.end();
  });

  test('produce -> claim -> complete, with JSON payload + idempotency dedup', async () => {
    const enqueued = await outbox.enqueue(db, {
      topic: 'order.placed',
      payload: { id: 'o-1', item: 'widget', qty: 2 },
      idempotencyKey: 'order:o-1',
    });
    assert.equal(enqueued.status, 'pending');
    assert.deepEqual(enqueued.payload, { id: 'o-1', item: 'widget', qty: 2 });

    // Enqueues without an idempotency key never collide (a UNIQUE index on the
    // nullable column permits multiple NULLs); a duplicate key does collide.
    await outbox.enqueue(db, { topic: 'noop', payload: {} });
    await outbox.enqueue(db, { topic: 'noop', payload: {} });
    await assert.rejects(() =>
      db.insert(mysqlOutboxEvents).values({
        id: 'dup', topic: 'order.placed', payload: {}, status: 'pending',
        idempotencyKey: 'order:o-1', availableAt: new Date().toISOString(),
        createdAt: new Date().toISOString(),
      }),
    );

    const claimed = await outbox.claimBatch(db, cfg);
    assert.equal(claimed.length, 3);
    assert.ok(claimed.every((row) => row.status === 'processing'));

    const mine = claimed.find((row) => row.id === enqueued.id)!;
    assert.equal(await outbox.markCompleted(db, claimOf(mine)), true);
    const [completed] = await db
      .select()
      .from(mysqlOutboxEvents)
      .where(eq(mysqlOutboxEvents.id, enqueued.id));
    assert.equal(completed.status, 'completed');
  });

  test('inbox dedups a redelivery (errno 1062); side effect runs once', async () => {
    const key = 'order.placed:o-1';
    const source = 'orders-service';
    const sideEffect = async () => {
      await connection.query(
        'INSERT INTO integration_side_effects (dedup_key, note) VALUES (?, ?)',
        [key, 'processed'],
      );
    };

    const first = await inbox.runOnce(db, key, source, sideEffect);
    const second = await inbox.runOnce(db, key, source, sideEffect);
    assert.equal(first, 'processed');
    assert.equal(second, 'duplicate');

    // The dedup row also lands in inbox_events under the same (source, key).
    const dedupRows = await db
      .select()
      .from(mysqlInboxEvents)
      .where(eq(mysqlInboxEvents.messageKey, key));
    assert.equal(dedupRows.length, 1);

    const [rows] = await connection.query(
      'SELECT COUNT(*) AS c FROM integration_side_effects WHERE dedup_key = ?',
      [key],
    );
    assert.equal((rows as { c: number }[])[0].c, 1);
  });

  async function seed(count: number): Promise<OutboxEventRow[]> {
    await pool.query('DELETE FROM outbox_events');
    const rows: OutboxEventRow[] = [];
    for (let i = 0; i < count; i += 1) {
      rows.push(await outbox.enqueue(poolDb, { topic: 'claims', payload: { i } }));
    }
    return rows;
  }
  // Opens two pooled connections up front, so two claims really overlap
  // instead of the second one starting after the first has committed.
  const warm = () => Promise.all([pool.query('SELECT 1'), pool.query('SELECT 1')]);

  test('#62: a claim skips rows another claim holds instead of taking them too', async () => {
    const rows = await seed(10);
    // Another worker's claim in flight. It runs at READ COMMITTED like ours,
    // so it holds just the rows it locked; under REPEATABLE READ this tiny
    // table is scanned in full and every row would end up locked.
    const holder = await pool.getConnection();
    await holder.query('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');
    await holder.query('START TRANSACTION');
    await holder.query('SELECT id FROM outbox_events WHERE id IN (?) FOR UPDATE', [
      rows.slice(0, 5).map((r) => r.id),
    ]);
    const claim = outbox.claimBatch(poolDb, cfg);
    try {
      const claimed = await settleWithin(claim, 2_000);
      assert.notEqual(claimed, 'blocked', 'the claim waited on rows another claim holds');
      assert.deepEqual(sortedIds(claimed as OutboxEventRow[]), sortedIds(rows.slice(5)));
    } finally {
      await holder.query('ROLLBACK');
      holder.release();
      await claim;
    }
  });

  test('#62: two concurrent claimers never claim the same row', async () => {
    for (let round = 0; round < 5; round += 1) {
      await seed(10);
      await warm();
      const [a, b] = await Promise.all([
        outbox.claimBatch(poolDb, { ...cfg, workerInstanceId: 'worker-A' }),
        outbox.claimBatch(poolDb, { ...cfg, workerInstanceId: 'worker-B' }),
      ]);
      const ids = [...a, ...b].map((r) => r.id);
      assert.equal(ids.length, 10, `round ${round}: every row claimed`);
      assert.equal(new Set(ids).size, 10, `round ${round}: a row was claimed twice`);
    }
  });

  test('#62: a worker whose stuck claim was taken over cannot move the row', async () => {
    const [row] = await seed(1);
    const [mine] = await outbox.claimBatch(poolDb, { ...cfg, workerInstanceId: 'worker-A' });
    await pool.query('UPDATE outbox_events SET claimed_at = ? WHERE id = ?', [backdated(), row!.id]);
    const [theirs] = await outbox.claimBatch(poolDb, { ...cfg, workerInstanceId: 'worker-B' });
    assert.equal(await outbox.markCompleted(poolDb, claimOf(mine!)), false);
    assert.equal(await outbox.retry(poolDb, claimOf(mine!), 0, 'late'), false);
    assert.equal(await outbox.markFailed(poolDb, claimOf(mine!), 'late'), false);
    const [owned] = await poolDb
      .select()
      .from(mysqlOutboxEvents)
      .where(eq(mysqlOutboxEvents.id, row!.id));
    assert.equal(owned?.status, 'processing');
    assert.equal(owned?.claimedBy, 'worker-B');
    assert.equal(await outbox.markCompleted(poolDb, claimOf(theirs!)), true);
  });

  // The MySQL connection running `pattern`: blocked behind the holder, it is
  // the only one with that statement in flight. (A row-lock wait reports its
  // state as "updating", a table-lock wait as "Waiting for table ... lock".)
  async function mysqlLockWaiter(pattern: string): Promise<number> {
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const [rows] = await connection.query(
        "SELECT ID AS id FROM information_schema.PROCESSLIST WHERE DB = DATABASE() AND COMMAND = 'Query' AND INFO LIKE ?",
        [pattern],
      );
      const waiting = rows as { id: number }[];
      if (waiting.length > 0) return waiting[0]!.id;
      await sleep(20);
    }
    throw new Error(`no connection waiting on a lock for ${pattern}`);
  }

  // A failover or KILL while the store's statement waits on a lock. mysql2's
  // pooled connection listens for its own errors, so the call must reject
  // without crashing the process, and the pool must still serve the next call.
  async function survivesConnectionLoss(
    block: (holder: import('mysql2/promise').PoolConnection) => Promise<unknown>,
    run: (target: typeof poolDb) => Promise<unknown>,
    waiting: string,
  ): Promise<void> {
    const mysql = await import('mysql2/promise');
    const doomed = mysql.createPool({ uri: MYSQL_URL as string, connectionLimit: 2 });
    const doomedDb = await buildMysqlDb(doomed);
    const holder = await pool.getConnection();
    try {
      await holder.query('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');
      await holder.query('BEGIN');
      await block(holder);
      const pending = run(doomedDb);
      pending.catch(() => undefined);
      await connection.query(`KILL ${await mysqlLockWaiter(waiting)}`);
      await assert.rejects(pending);
      await holder.query('UNLOCK TABLES');
      await holder.query('ROLLBACK');
      const [rows] = await doomed.query('SELECT 1 AS ok');
      assert.deepEqual(rows, [{ ok: 1 }], 'the pool replaced the dead connection');
    } finally {
      holder.release();
      await doomed.end();
    }
  }

  test('a connection lost mid-claim rejects instead of crashing the process', async () => {
    await seed(1);
    await survivesConnectionLoss(
      // SKIP LOCKED never waits on a row lock, so hold the whole table instead.
      (holder) => holder.query('LOCK TABLES outbox_events WRITE'),
      (target) => outbox.claimBatch(target, cfg),
      'select `id` from `outbox_events`%',
    );
  });

  test('a connection lost mid-transition rejects instead of crashing the process', async () => {
    const [row] = await seed(1);
    const [mine] = await outbox.claimBatch(poolDb, cfg);
    await survivesConnectionLoss(
      (holder) => holder.query('SELECT id FROM outbox_events WHERE id = ? FOR UPDATE', [row!.id]),
      (target) => outbox.markCompleted(target, claimOf(mine!)),
      'update `outbox_events`%',
    );
  });

  // As InboxService runs it: the dedup row and the side effect in one transaction.
  const deliver = (key: string, effect: () => Promise<void> = async () => {}) =>
    poolDb.transaction((tx) =>
      inbox.runOnce(tx, key, 'payments', async () => {
        await effect();
        await tx.execute(
          sql`INSERT INTO integration_side_effects (dedup_key, note) VALUES (${key}, 'processed')`,
        );
      }),
    );
  const sideEffects = async (key: string) => {
    const [rows] = await pool.query(
      'SELECT COUNT(*) AS c FROM integration_side_effects WHERE dedup_key = ?',
      [key],
    );
    return (rows as { c: number }[])[0].c;
  };

  test('two concurrent deliveries of one message run the side effect once', async () => {
    await warm();
    const outcomes = await Promise.all([deliver('order.paid:o-9'), deliver('order.paid:o-9')]);
    assert.deepEqual(outcomes.sort(), ['duplicate', 'processed']);
    assert.equal(await sideEffects('order.paid:o-9'), 1);
  });

  test('a delivery that fails does not swallow a concurrent redelivery', async () => {
    let entered!: () => void;
    const firstEntered = new Promise<void>((resolve) => (entered = resolve));
    let fail!: () => void;
    const firstFails = new Promise<void>((resolve) => (fail = resolve));
    const first = deliver('order.paid:o-10', async () => {
      entered();
      await firstFails;
      throw new Error('first delivery failed');
    });
    // The first delivery's dedup row is written but not committed yet.
    await firstEntered;
    let secondSettled = false;
    const second = deliver('order.paid:o-10').finally(() => (secondSettled = true));
    await sleep(200);
    assert.equal(secondSettled, false, "the redelivery waits for the first delivery's outcome");
    fail();
    await assert.rejects(first, /first delivery failed/);
    // The rollback freed the key, so the redelivery processes it after all.
    assert.equal(await second, 'processed');
    assert.equal(await sideEffects('order.paid:o-10'), 1);
  });
});

describe('Postgres round-trip (real service)', { skip: !POSTGRES_URL }, () => {
  let pool: import('pg').Pool;
  let db: Awaited<ReturnType<typeof buildPgDb>>;
  const outbox = new PostgresOutboxStore();
  const inbox = new PostgresInboxStore();

  async function buildPgDb(client: unknown) {
    const { drizzle } = await import('drizzle-orm/node-postgres');
    return drizzle(client as never);
  }

  before(async () => {
    const pg = await import('pg');
    pool = new pg.Pool({ connectionString: POSTGRES_URL });
    // node-postgres requires an `error` listener on every pool (the store warns without one).
    pool.on('error', () => undefined);
    for (const stmt of PG_DDL) await pool.query(stmt);
    db = await buildPgDb(pool);
  });

  after(async () => {
    await pool?.end();
  });

  test('produce -> claim -> complete -> inbox dedup', async () => {
    const enqueued = await outbox.enqueue(db, {
      topic: 'order.placed',
      payload: { id: 'o-1', item: 'widget' },
      idempotencyKey: 'order:o-1',
    });
    assert.equal(enqueued.status, 'pending');
    assert.deepEqual(enqueued.payload, { id: 'o-1', item: 'widget' });

    const claimed = await outbox.claimBatch(db, cfg);
    assert.equal(claimed.length, 1);
    assert.equal(claimed[0]?.status, 'processing');
    assert.equal(await outbox.markCompleted(db, claimOf(claimed[0]!)), true);
    const [completed] = await db
      .select()
      .from(pgOutboxEvents)
      .where(eq(pgOutboxEvents.id, enqueued.id));
    assert.equal(completed.status, 'completed');

    const key = 'order.placed:o-1';
    const source = 'orders-service';
    const sideEffect = async () => {
      await pool.query('INSERT INTO integration_side_effects (dedup_key, note) VALUES ($1, $2)', [key, 'processed']);
    };
    assert.equal(await inbox.runOnce(db, key, source, sideEffect), 'processed');
    assert.equal(await inbox.runOnce(db, key, source, sideEffect), 'duplicate');
    const seen = await pool.query('SELECT count(*)::int AS c FROM integration_side_effects WHERE dedup_key = $1', [key]);
    assert.equal((seen.rows as { c: number }[])[0].c, 1);
    void pgInboxEvents;
  });

  async function seed(count: number): Promise<OutboxEventRow[]> {
    await pool.query('DELETE FROM outbox_events');
    const rows: OutboxEventRow[] = [];
    for (let i = 0; i < count; i += 1) {
      rows.push(await outbox.enqueue(db, { topic: 'claims', payload: { i } }));
    }
    return rows;
  }
  // Opens two pooled connections up front, so two claims really overlap
  // instead of the second one starting after the first has committed.
  const warm = (target: import('pg').Pool) =>
    Promise.all([target.query('SELECT 1'), target.query('SELECT 1')]);

  test('#62: a claim skips rows another claim holds instead of taking them too', async () => {
    const rows = await seed(10);
    const holder = await pool.connect();
    await holder.query('BEGIN');
    await holder.query('SELECT id FROM outbox_events WHERE id = ANY($1) FOR UPDATE', [
      rows.slice(0, 5).map((r) => r.id),
    ]);
    const claim = outbox.claimBatch(db, cfg);
    try {
      const claimed = await settleWithin(claim, 2_000);
      assert.notEqual(claimed, 'blocked', 'the claim waited on rows another claim holds');
      assert.deepEqual(sortedIds(claimed as OutboxEventRow[]), sortedIds(rows.slice(5)));
    } finally {
      await holder.query('ROLLBACK');
      holder.release();
      await claim;
    }
  });

  test('#62: two concurrent claimers never claim the same row', async () => {
    for (let round = 0; round < 5; round += 1) {
      await seed(10);
      await warm(pool);
      const [a, b] = await Promise.all([
        outbox.claimBatch(db, { ...cfg, workerInstanceId: 'worker-A' }),
        outbox.claimBatch(db, { ...cfg, workerInstanceId: 'worker-B' }),
      ]);
      const ids = [...a, ...b].map((r) => r.id);
      assert.equal(ids.length, 10, `round ${round}: every row claimed`);
      assert.equal(new Set(ids).size, 10, `round ${round}: a row was claimed twice`);
    }
  });

  test('#62: a worker whose stuck claim was taken over cannot move the row', async () => {
    const [row] = await seed(1);
    const [mine] = await outbox.claimBatch(db, { ...cfg, workerInstanceId: 'worker-A' });
    await pool.query('UPDATE outbox_events SET claimed_at = $1 WHERE id = $2', [backdated(), row!.id]);
    const [theirs] = await outbox.claimBatch(db, { ...cfg, workerInstanceId: 'worker-B' });
    assert.equal(await outbox.markCompleted(db, claimOf(mine!)), false);
    assert.equal(await outbox.retry(db, claimOf(mine!), 0, 'late'), false);
    assert.equal(await outbox.markFailed(db, claimOf(mine!), 'late'), false);
    const [owned] = await db.select().from(pgOutboxEvents).where(eq(pgOutboxEvents.id, row!.id));
    assert.equal(owned?.status, 'processing');
    assert.equal(owned?.claimedBy, 'worker-B');
    assert.equal(await outbox.markCompleted(db, claimOf(theirs!)), true);
  });

  test('a stale transition racing a reclaim reports the claim lost under a SERIALIZABLE default', async () => {
    // The stale UPDATE waits on the row the new owner is claiming. In its own
    // READ COMMITTED transaction it then re-checks the fence against the
    // committed row and matches nothing, whatever the server default.
    const pg = await import('pg');
    const strict = new pg.Pool({
      connectionString: POSTGRES_URL,
      options: '-c default_transaction_isolation=serializable',
    }).on('error', () => undefined);
    const strictDb = await buildPgDb(strict);
    const reclaim = await pool.connect();
    try {
      const [row] = await seed(1);
      const [mine] = await outbox.claimBatch(db, { ...cfg, workerInstanceId: 'worker-A' });
      await reclaim.query('BEGIN');
      await reclaim.query(
        "UPDATE outbox_events SET claimed_by = 'worker-B', claimed_at = $1 WHERE id = $2",
        [new Date().toISOString(), row!.id],
      );
      const stale = outbox.markCompleted(strictDb, claimOf(mine!));
      await lockWaiter('update "outbox_events"%');
      await reclaim.query('COMMIT');
      assert.equal(await stale, false);
      const [owned] = await db.select().from(pgOutboxEvents).where(eq(pgOutboxEvents.id, row!.id));
      assert.equal(owned?.status, 'processing');
      assert.equal(owned?.claimedBy, 'worker-B');
    } finally {
      reclaim.release();
      await strict.end();
    }
  });

  test('workers under a SERIALIZABLE default never report a claim they still hold as lost', async () => {
    // Nothing can be taken over here (the stuck timeout outlasts the test), so
    // every transition must apply. Run as serializable statements, the fenced
    // UPDATEs scan the status index and abort each other (40001) while each
    // claim still holds its row; their own READ COMMITTED transaction prevents
    // that. ANALYZE gives the planner the steady state, few rows `processing`,
    // in which it picks that index.
    const pg = await import('pg');
    const workers = Array.from({ length: 8 }, () =>
      new pg.Pool({
        connectionString: POSTGRES_URL,
        options: '-c default_transaction_isolation=serializable',
        max: 2,
      }).on('error', () => undefined),
    );
    try {
      await seed(256);
      await pool.query('ANALYZE outbox_events');
      await Promise.all(workers.map(warm));
      const outcomes = await Promise.all(
        workers.map(async (worker, index) => {
          const workerDb = await buildPgDb(worker);
          const workerCfg = {
            ...cfg,
            workerInstanceId: `worker-${index}`,
            batchSize: 32,
            stuckTimeoutMs: 600_000,
          };
          const applied: boolean[] = [];
          for (;;) {
            const claimed = await outbox.claimBatch(workerDb, workerCfg);
            if (claimed.length === 0) return applied;
            for (const row of claimed) applied.push(await outbox.markCompleted(workerDb, claimOf(row)));
          }
        }),
      );
      const all = outcomes.flat();
      assert.equal(all.length, 256, 'every row claimed exactly once');
      assert.equal(all.filter((applied) => !applied).length, 0, 'a held claim was reported lost');
      const { rows } = await pool.query(
        "SELECT count(*)::int AS c FROM outbox_events WHERE status = 'completed'",
      );
      assert.equal((rows as { c: number }[])[0].c, 256);
    } finally {
      await Promise.all(workers.map((worker) => worker.end()));
    }
  });

  // The backend running `pattern` once it is waiting on a lock.
  async function lockWaiter(pattern: string): Promise<number> {
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const { rows } = await pool.query(
        "SELECT pid FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database() AND query LIKE $1",
        [pattern],
      );
      if (rows.length > 0) return (rows as { pid: number }[])[0].pid;
      await sleep(20);
    }
    throw new Error(`no backend waiting on a lock for ${pattern}`);
  }

  test('a concurrent write to a claimed row does not lose its outcome under a REPEATABLE READ default', async () => {
    // An operator's UPDATE of the row commits while the transition waits on it.
    // Run at the server's REPEATABLE READ, the transition would fail with 40001
    // although its claim still holds the row; at READ COMMITTED it re-checks
    // the fence against the committed row and applies.
    const pg = await import('pg');
    const repeatable = new pg.Pool({
      connectionString: POSTGRES_URL,
      options: '-c default_transaction_isolation=repeatable\\ read',
    }).on('error', () => undefined);
    const repeatableDb = await buildPgDb(repeatable);
    const operator = await pool.connect();
    try {
      const [row] = await seed(1);
      const [mine] = await outbox.claimBatch(db, cfg);
      await operator.query('BEGIN');
      await operator.query('UPDATE outbox_events SET max_attempts = max_attempts + 5 WHERE id = $1', [row!.id]);
      const completing = outbox.markCompleted(repeatableDb, claimOf(mine!));
      await lockWaiter('update "outbox_events"%');
      await operator.query('COMMIT');
      assert.equal(await completing, true);
      const [after] = await db.select().from(pgOutboxEvents).where(eq(pgOutboxEvents.id, row!.id));
      assert.equal(after?.status, 'completed');
      assert.equal(after?.maxAttempts, row!.maxAttempts + 5);
    } finally {
      await operator.query('ROLLBACK');
      operator.release();
      await repeatable.end();
    }
  });

  // A failover or pg_terminate_backend while the store's statement waits on a
  // lock. drizzle's transaction() left that client without an `error` listener,
  // which crashed the process; the call must reject instead, and the pool must
  // drop the broken client. The pool has an `error` listener, as node-postgres
  // requires of every pool.
  async function survivesConnectionLoss(
    block: (holder: import('pg').PoolClient) => Promise<unknown>,
    run: (target: typeof db) => Promise<unknown>,
    waiting: string,
  ): Promise<void> {
    const pg = await import('pg');
    const doomed = new pg.Pool({ connectionString: POSTGRES_URL });
    doomed.on('error', () => undefined);
    const doomedDb = await buildPgDb(doomed);
    const holder = await pool.connect();
    try {
      await holder.query('BEGIN');
      await block(holder);
      const pending = run(doomedDb);
      pending.catch(() => undefined);
      const pid = await lockWaiter(waiting);
      await pool.query('SELECT pg_terminate_backend($1)', [pid]);
      await assert.rejects(pending);
      assert.equal(doomed.totalCount, 0, 'the broken client was discarded');
    } finally {
      await holder.query('ROLLBACK');
      holder.release();
      await doomed.end();
    }
  }

  test('a connection lost mid-transition rejects instead of crashing the process', async () => {
    const [row] = await seed(1);
    const [mine] = await outbox.claimBatch(db, cfg);
    await survivesConnectionLoss(
      (holder) => holder.query('SELECT id FROM outbox_events WHERE id = $1 FOR UPDATE', [row!.id]),
      (target) => outbox.markCompleted(target, claimOf(mine!)),
      'update "outbox_events"%',
    );
  });

  test('a connection lost mid-claim rejects instead of crashing the process', async () => {
    await seed(1);
    await survivesConnectionLoss(
      // The claim's locking scan takes ROW SHARE on the table, which EXCLUSIVE blocks.
      (holder) => holder.query('LOCK TABLE outbox_events IN EXCLUSIVE MODE'),
      (target) => outbox.claimBatch(target, cfg),
      'select "id" from "outbox_events"%',
    );
  });

  // As InboxService runs it: the dedup row and the side effect in one transaction.
  const deliver = (key: string, effect: () => Promise<void> = async () => {}) =>
    db.transaction((tx) =>
      inbox.runOnce(tx, key, 'payments', async () => {
        await effect();
        await tx.execute(
          sql`INSERT INTO integration_side_effects (dedup_key, note) VALUES (${key}, 'processed')`,
        );
      }),
    );
  const sideEffects = async (key: string) => {
    const seen = await pool.query(
      'SELECT count(*)::int AS c FROM integration_side_effects WHERE dedup_key = $1',
      [key],
    );
    return (seen.rows as { c: number }[])[0].c;
  };

  test('two concurrent deliveries of one message run the side effect once', async () => {
    await warm(pool);
    const outcomes = await Promise.all([deliver('order.paid:o-9'), deliver('order.paid:o-9')]);
    assert.deepEqual(outcomes.sort(), ['duplicate', 'processed']);
    assert.equal(await sideEffects('order.paid:o-9'), 1);
  });

  test('a delivery that fails does not swallow a concurrent redelivery', async () => {
    let entered!: () => void;
    const firstEntered = new Promise<void>((resolve) => (entered = resolve));
    let fail!: () => void;
    const firstFails = new Promise<void>((resolve) => (fail = resolve));
    const first = deliver('order.paid:o-10', async () => {
      entered();
      await firstFails;
      throw new Error('first delivery failed');
    });
    // The first delivery's dedup row is written but not committed yet.
    await firstEntered;
    let secondSettled = false;
    const second = deliver('order.paid:o-10').finally(() => (secondSettled = true));
    await sleep(200);
    assert.equal(secondSettled, false, "the redelivery waits for the first delivery's outcome");
    fail();
    await assert.rejects(first, /first delivery failed/);
    // The rollback freed the key, so the redelivery processes it after all.
    assert.equal(await second, 'processed');
    assert.equal(await sideEffects('order.paid:o-10'), 1);
  });

  test('LISTEN/NOTIFY wake: delivered on commit, dropped on rollback', async () => {
    const pg = await import('pg');
    let wakes = 0;
    const listener = new PostgresWakeListener({
      // A dedicated client, NOT the pool: notifications arrive only on the
      // exact connection that issued LISTEN.
      connect: () => new pg.Client({ connectionString: POSTGRES_URL, keepAlive: true }),
      channel: 'outbox_wake_it',
      waker: { notify: () => (wakes += 1) },
    });
    listener.start();
    try {
      const wakeStore = new PostgresOutboxStore({ wakeChannel: 'outbox_wake_it' });

      // Give the listener a beat to establish LISTEN before the first NOTIFY.
      await new Promise((resolve) => setTimeout(resolve, 300));

      // Committed enqueue → exactly one wake arrives.
      await db.transaction(async (tx) => {
        await wakeStore.enqueue(tx, { topic: 'wake.commit', payload: { n: 1 } });
      });
      const deadline = Date.now() + 5_000;
      while (wakes === 0 && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      assert.equal(wakes, 1, 'a committed enqueue must wake the listener');

      // Rolled-back enqueue → Postgres drops the notification with the tx.
      await assert.rejects(
        db.transaction(async (tx) => {
          await wakeStore.enqueue(tx, { topic: 'wake.rollback', payload: { n: 2 } });
          throw new Error('force rollback');
        }),
        /force rollback/,
      );
      await new Promise((resolve) => setTimeout(resolve, 500));
      assert.equal(wakes, 1, 'a rolled-back enqueue must NOT wake the listener');
    } finally {
      await listener.stop();
    }
  });
});
