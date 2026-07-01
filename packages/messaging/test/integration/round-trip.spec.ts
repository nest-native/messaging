import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { after, before, describe, test } from 'node:test';
import { eq } from 'drizzle-orm';
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
} from '../../dialects/postgres';

// Gated end-to-end tests against a REAL database. They skip unless the matching
// URL env var is set, so `npm test` / `test:cov` stay hermetic and 100%. CI runs
// them in a dedicated job with a `mysql:8.4` service (see .github/workflows). The
// stores are driven directly (no Nest) — a genuine produce -> claim -> complete
// -> inbox-dedup round-trip that exercises the real driver: JSON payloads, errno
// 1062 unique violations, and async transactions in `claimBatch`.

const MYSQL_URL = process.env.MESSAGING_MYSQL_URL;
const POSTGRES_URL = process.env.MESSAGING_POSTGRES_URL;
const cfg = { ...DEFAULT_CLAIMER_CONFIG, batchSize: 50, stuckTimeoutMs: 1_000 };

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
  `CREATE TABLE inbox_events (
     id TEXT PRIMARY KEY, message_key TEXT NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL,
     processed_at TEXT NOT NULL, last_error TEXT, created_at TEXT NOT NULL)`,
  'CREATE UNIQUE INDEX inbox_events_source_message_key_unique ON inbox_events (source, message_key)',
  'CREATE TABLE integration_side_effects (dedup_key TEXT PRIMARY KEY, note TEXT NOT NULL)',
];

describe('MySQL round-trip (real service)', { skip: !MYSQL_URL }, () => {
  let connection: Awaited<ReturnType<typeof import('mysql2/promise').createConnection>>;
  let db: Awaited<ReturnType<typeof buildMysqlDb>>;
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
  });

  after(async () => {
    await connection?.end();
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

    await outbox.markCompleted(db, enqueued.id);
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
    await outbox.markCompleted(db, enqueued.id);
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
});
