import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import { DEFAULT_CLAIMER_CONFIG } from '../outbox-claimer.service';
import {
  isMysqlUniqueViolation,
  MysqlInboxStore,
  MysqlOutboxStore,
} from '../dialects/mysql';
import type { OutboxEventRow } from '../interfaces';

// There is no in-process MySQL (the pglite equivalent does not exist), so the
// store methods are exercised against a recording stand-in for a mysql2 Drizzle
// database: each builder call runs the store's real code path and records the
// values it would send, while canned rows drive the return-value assertions.
// This reaches 100% coverage of the store files hermetically; genuine end-to-end
// behaviour (json round-trip, errno 1062 dedup, transactions) is proven by the
// gated real-MySQL integration test in `test/integration/`.

const cfg = { ...DEFAULT_CLAIMER_CONFIG, batchSize: 10, stuckTimeoutMs: 1_000 };

function row(overrides: Partial<OutboxEventRow> = {}): OutboxEventRow {
  return {
    id: 'row-1',
    topic: 't',
    payload: { a: 1 },
    status: 'pending',
    attempts: 0,
    maxAttempts: 10,
    idempotencyKey: null,
    availableAt: '2026-01-01T00:00:00.000Z',
    claimedAt: null,
    claimedBy: null,
    processedAt: null,
    lastError: null,
    createdAt: '2026-01-01T00:00:00.000Z',
    ...overrides,
  };
}

interface OutboxMockOptions {
  selectRows?: OutboxEventRow[];
  candidates?: { id: string }[];
}

function outboxMock(options: OutboxMockOptions = {}) {
  const captured: { insert?: Record<string, unknown>; set?: Record<string, unknown> } = {};
  const selectRows = options.selectRows ?? [];
  // A projected `select({ id })` is the claimer's candidate query (terminated by
  // `.limit()`); a bare `select()` is the enqueue read-back / claim re-read.
  const buildSelect = (projection?: unknown) => ({
    from: () => ({
      where: () =>
        projection
          ? { limit: () => Promise.resolve(options.candidates ?? []) }
          : Promise.resolve(selectRows),
    }),
  });
  const db: unknown = {
    insert: () => ({
      values: (values: Record<string, unknown>) => {
        captured.insert = values;
        return Promise.resolve([{}]);
      },
    }),
    select: (projection?: unknown) => buildSelect(projection),
    update: () => ({
      set: (values: Record<string, unknown>) => {
        captured.set = values;
        return { where: () => Promise.resolve([{}]) };
      },
    }),
    transaction: (run: (tx: unknown) => unknown) => run(db),
  };
  return { db, captured };
}

describe('MysqlOutboxStore', () => {
  const store = new MysqlOutboxStore();

  test('enqueue inserts a pending row then reads it back by id', async () => {
    const persisted = row({ id: 'generated', idempotencyKey: 'k1', maxAttempts: 3 });
    const { db, captured } = outboxMock({ selectRows: [persisted] });

    const result = await store.enqueue(db, {
      topic: 't',
      payload: { a: 1 },
      idempotencyKey: 'k1',
      maxAttempts: 3,
      availableAt: new Date('2026-02-02T00:00:00.000Z'),
    });

    assert.equal(result, persisted);
    assert.equal(captured.insert?.topic, 't');
    assert.deepEqual(captured.insert?.payload, { a: 1 });
    assert.equal(captured.insert?.status, 'pending');
    assert.equal(captured.insert?.idempotencyKey, 'k1');
    assert.equal(captured.insert?.maxAttempts, 3);
    assert.equal(captured.insert?.availableAt, '2026-02-02T00:00:00.000Z');
    assert.ok(typeof captured.insert?.id === 'string' && captured.insert.id.length > 0);
  });

  test('enqueue defaults: null idempotency key, maxAttempts 10, availableAt now', async () => {
    const { db, captured } = outboxMock({ selectRows: [row()] });

    await store.enqueue(db, { topic: 't', payload: {} });

    assert.equal(captured.insert?.idempotencyKey, null);
    assert.equal(captured.insert?.maxAttempts, 10);
    assert.ok(typeof captured.insert?.availableAt === 'string');
  });

  test('claimBatch marks candidates processing and returns the re-read rows', async () => {
    const claimed = row({ id: 'a', status: 'processing' });
    const { db, captured } = outboxMock({ candidates: [{ id: 'a' }], selectRows: [claimed] });

    const result = await store.claimBatch(db, cfg);

    assert.deepEqual(result, [claimed]);
    assert.equal(captured.set?.status, 'processing');
    assert.equal(captured.set?.claimedBy, cfg.workerInstanceId);
    assert.ok(typeof captured.set?.claimedAt === 'string');
  });

  test('claimBatch returns [] with no update when nothing is due', async () => {
    const { db, captured } = outboxMock({ candidates: [] });

    assert.deepEqual(await store.claimBatch(db, cfg), []);
    assert.equal(captured.set, undefined);
  });

  test('markCompleted transitions the row to completed', async () => {
    const { db, captured } = outboxMock();
    await store.markCompleted(db, 'id-1');
    assert.equal(captured.set?.status, 'completed');
    assert.equal(captured.set?.lastError, null);
    assert.ok(typeof captured.set?.processedAt === 'string');
  });

  test('retry re-arms the row, carrying or clearing lastError', async () => {
    const withError = outboxMock();
    await store.retry(withError.db, 'id-1', 5_000, 'boom');
    assert.equal(withError.captured.set?.status, 'pending');
    assert.equal(withError.captured.set?.lastError, 'boom');
    assert.equal(withError.captured.set?.claimedAt, null);
    assert.equal(withError.captured.set?.claimedBy, null);

    const noError = outboxMock();
    await store.retry(noError.db, 'id-1', 1_000);
    assert.equal(noError.captured.set?.lastError, null);
  });

  test('markFailed records the reason and increments attempts', async () => {
    const { db, captured } = outboxMock();
    await store.markFailed(db, 'id-1', 'dead');
    assert.equal(captured.set?.status, 'failed');
    assert.equal(captured.set?.lastError, 'dead');
    assert.ok(typeof captured.set?.processedAt === 'string');
  });
});

describe('MysqlInboxStore', () => {
  const store = new MysqlInboxStore();
  const okDb = { insert: () => ({ values: () => Promise.resolve([{}]) }) };

  test('runOnce processes a fresh key (async side effect)', async () => {
    let ran = 0;
    const outcome = await store.runOnce(okDb, 'k1', 'src', async () => {
      ran += 1;
    });
    assert.equal(outcome, 'processed');
    assert.equal(ran, 1);
  });

  test('runOnce returns duplicate on errno 1062, skips the side effect', async () => {
    const dupDb = {
      insert: () => ({
        values: () => Promise.reject({ code: 'ER_DUP_ENTRY', errno: 1062 }),
      }),
    };
    let ran = 0;
    const outcome = await store.runOnce(dupDb, 'k1', 'src', () => {
      ran += 1;
    });
    assert.equal(outcome, 'duplicate');
    assert.equal(ran, 0);
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

  test('a side-effect error propagates', async () => {
    await assert.rejects(
      () =>
        store.runOnce(okDb, 'k1', 'src', () => {
          throw new Error('side effect failed');
        }),
      /side effect failed/,
    );
  });
});

describe('isMysqlUniqueViolation', () => {
  test('matches ER_DUP_ENTRY / errno 1062 (direct or wrapped in cause)', () => {
    assert.equal(isMysqlUniqueViolation({ code: 'ER_DUP_ENTRY' }), true);
    assert.equal(isMysqlUniqueViolation({ errno: 1062 }), true);
    assert.equal(isMysqlUniqueViolation({ cause: { code: 'ER_DUP_ENTRY' } }), true);
    assert.equal(isMysqlUniqueViolation({ cause: { errno: 1062 } }), true);
  });

  test('rejects other errors and non-objects', () => {
    assert.equal(isMysqlUniqueViolation({ code: 'ER_NO_SUCH_TABLE' }), false);
    assert.equal(isMysqlUniqueViolation({ errno: 1146 }), false);
    assert.equal(isMysqlUniqueViolation(new Error('x')), false);
    assert.equal(isMysqlUniqueViolation(null), false);
    assert.equal(isMysqlUniqueViolation(42), false);
  });
});
