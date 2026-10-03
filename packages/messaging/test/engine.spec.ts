import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { hostname } from 'node:os';
import { afterEach, beforeEach, describe, test } from 'node:test';
import { Injectable, Logger, Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { DynamicModule, INestApplicationContext } from '@nestjs/common';
import {
  ClsPluginTransactional,
  InjectTransaction,
  Transactional,
} from '@nestjs-cls/transactional';
import { TransactionalAdapterDrizzleOrm } from '@nestjs-cls/transactional-adapter-drizzle-orm';
import { ClsModule } from 'nestjs-cls';
import Database from 'better-sqlite3';
import { drizzle, type BetterSQLite3Database } from 'drizzle-orm/better-sqlite3';
import { eq, sql } from 'drizzle-orm';
import {
  InboxService,
  MessagingModule,
  OutboxClaimer,
  OutboxProducer,
  type OutboxEventRow,
  type OutboxStore,
  PermanentError,
  RetryableError,
} from '../index';
import { DEFAULT_CLAIMER_CONFIG, resolveClaimerConfig } from '../outbox-claimer.service';
import {
  inboxEvents,
  outboxEvents,
  SqliteInboxStore,
  SqliteOutboxStore,
} from '../dialects/sqlite';
import { InMemoryOutboxTransport } from '../testing';

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
CREATE TABLE widgets (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);
CREATE TABLE deliveries (key TEXT PRIMARY KEY);
`;

const DRIZZLE = Symbol('test-drizzle');
type Db = BetterSQLite3Database<Record<string, never>>;

// Compile-level regression for EnqueueInput<TPayload>: a payload typed as a
// plain interface (NO index signature, so NOT assignable to
// Record<string, unknown>) must be accepted by enqueue without any cast.
interface WidgetCreated {
  name: string;
}

@Injectable()
class WidgetService {
  constructor(
    @InjectTransaction() private readonly db: Db,
    private readonly producer: OutboxProducer<SqliteOutboxStore>,
  ) {}

  // Synchronous @Transactional body (better-sqlite3): enqueue + business write
  // commit atomically; a throw rolls both back.
  @Transactional()
  create(name: string, fail = false): Promise<OutboxEventRow> {
    const payload: WidgetCreated = { name };
    const row = this.producer.enqueue({
      topic: 'widget.created',
      payload,
      idempotencyKey: `widget:${name}`,
    });
    this.db.run(sql`INSERT INTO widgets (name) VALUES (${name})`);
    if (fail) throw new Error('rollback');
    return row as unknown as Promise<OutboxEventRow>;
  }
}

// The Drizzle instance is provided by a global module, mirroring how a real app
// registers it (e.g. @nest-native/drizzle is global) — so both the CLS adapter
// and MessagingModule resolve the token without an explicit import.
@Module({})
class DbModule {}
const dbImport = (db: Db): DynamicModule => ({
  module: DbModule,
  global: true,
  providers: [{ provide: DRIZZLE, useValue: db }],
  exports: [DRIZZLE],
});

@Module({})
class FixtureModule {
  static register(
    db: Db,
    transport: InMemoryOutboxTransport,
    withInbox: boolean,
  ): DynamicModule {
    return {
      module: FixtureModule,
      imports: [
        dbImport(db),
        ClsModule.forRoot({
          global: true,
          plugins: [
            new ClsPluginTransactional({
              adapter: new TransactionalAdapterDrizzleOrm({
                drizzleInstanceToken: DRIZZLE,
              }),
              enableTransactionProxy: true,
            }),
          ],
        }),
        MessagingModule.forRoot({
          drizzleInstanceToken: DRIZZLE,
          outboxStore: new SqliteOutboxStore(),
          inboxStore: withInbox ? new SqliteInboxStore() : undefined,
          transport,
        }),
      ],
      providers: [WidgetService],
      exports: [WidgetService],
    };
  }
}

let app: INestApplicationContext;
let db: Db;
let raw: Database.Database;
let transport: InMemoryOutboxTransport;

const count = (table: string): number =>
  (raw.prepare(`SELECT count(*) c FROM ${table}`).get() as { c: number }).c;

const fetchRow = (id: string) =>
  db.select().from(outboxEvents).where(eq(outboxEvents.id, id)).get();

/** Records the claimer's Logger warn/error output until `restore()`. */
function captureLogs(): { warns: string[]; errors: string[]; restore: () => void } {
  const warns: string[] = [];
  const errors: string[] = [];
  Logger.overrideLogger({
    log: () => {},
    error: (message: unknown) => errors.push(String(message)),
    warn: (message: unknown) => warns.push(String(message)),
    debug: () => {},
    verbose: () => {},
  });
  return { warns, errors, restore: () => Logger.overrideLogger(false) };
}

async function boot(withInbox = true) {
  raw = new Database(':memory:');
  raw.exec(DDL);
  db = drizzle(raw);
  transport = new InMemoryOutboxTransport();
  app = await NestFactory.createApplicationContext(
    FixtureModule.register(db, transport, withInbox),
    { logger: false, abortOnError: false },
  );
}

afterEach(async () => {
  await app?.close();
  raw?.close();
});

describe('OutboxProducer (atomic enqueue)', () => {
  beforeEach(() => boot());

  test('enqueue commits the outbox row with the business write', async () => {
    const svc = app.get(WidgetService);
    const row = await svc.create('alpha');
    assert.equal(row.topic, 'widget.created');
    const stored = db.select().from(outboxEvents).where(eq(outboxEvents.id, row.id)).get();
    assert.equal(stored?.status, 'pending');
    assert.equal(count('widgets'), 1);
  });

  test('a throw rolls back BOTH the outbox row and the business write', async () => {
    const svc = app.get(WidgetService);
    await assert.rejects(() => svc.create('beta', true), /rollback/);
    assert.equal(db.select().from(outboxEvents).all().length, 0);
    assert.equal(count('widgets'), 0);
  });
});

describe('OutboxClaimer (publish outcomes)', () => {
  beforeEach(() => boot());

  test('tick publishes a pending row and marks it completed', async () => {
    const svc = app.get(WidgetService);
    const row = await svc.create('gamma');
    const claimer = app.get(OutboxClaimer);
    const report = await claimer.tick();
    assert.deepEqual(report, { claimed: 1, completed: 1, retried: 0, failed: 0, lost: 0 });
    assert.equal(transport.list().length, 1);
    assert.equal(transport.list()[0]?.idempotencyKey, 'widget:gamma');
    const after = db.select().from(outboxEvents).where(eq(outboxEvents.id, row.id)).get();
    assert.equal(after?.status, 'completed');
  });

  test('a PermanentError fails the row immediately (and warns with the reason)', async () => {
    await app.get(WidgetService).create('p');
    transport.failWith(new PermanentError('no handler'));
    // Capture the claimer's Logger output for this tick: the failure warn must
    // name the event and carry the reason.
    const warns: unknown[] = [];
    Logger.overrideLogger({
      log: () => {},
      error: () => {},
      warn: (message: unknown) => warns.push(message),
      debug: () => {},
      verbose: () => {},
    });
    try {
      const report = await app.get(OutboxClaimer).tick();
      assert.equal(report.failed, 1);
    } finally {
      Logger.overrideLogger(false);
    }
    const failed = db.select().from(outboxEvents).all()[0];
    assert.equal(failed?.status, 'failed');
    assert.equal(warns.length, 1);
    assert.match(String(warns[0]), new RegExp(`outbox event ${failed?.id} failed: no handler`));
  });

  test('a RetryableError reschedules (honouring delayMs)', async () => {
    await app.get(WidgetService).create('r');
    transport.failWith(new RetryableError('later', 5_000));
    const before = Date.now();
    const report = await app.get(OutboxClaimer).tick();
    assert.equal(report.retried, 1);
    const row = db.select().from(outboxEvents).all()[0];
    assert.equal(row?.status, 'pending');
    assert.equal(row?.attempts, 1);
    // The transport-supplied delay wins over the exponential backoff (which at
    // attempts=0 would be at most baseBackoffMs * 2 = 2s < 5s).
    assert.ok(new Date(row!.availableAt).getTime() >= before + 5_000);
  });

  test('generic-error backoff is jittered-exponential and capped', async () => {
    const store = new SqliteOutboxStore();
    // maxAttempts far above the sampled attempt counts so every tick retries.
    const row = store.enqueue(db, { topic: 't', payload: {}, maxAttempts: 100 });
    transport.failWith(new Error('flaky'));
    const claimer = app.get(OutboxClaimer);

    // Re-arm the row as due with a fixed attempts count, tick, and measure the
    // scheduled delay (availableAt - now). attempts=2 → base 1000 * 2^2 = 4000.
    const sample = async (attempts: number): Promise<number> => {
      db.update(outboxEvents)
        .set({
          status: 'pending',
          attempts,
          availableAt: new Date(0).toISOString(),
          claimedAt: null,
          claimedBy: null,
        })
        .where(eq(outboxEvents.id, row.id))
        .run();
      const before = Date.now();
      const report = await claimer.tick();
      assert.equal(report.retried, 1);
      const after = db.select().from(outboxEvents).where(eq(outboxEvents.id, row.id)).get();
      return new Date(after!.availableAt).getTime() - before;
    };

    const delays: number[] = [];
    for (let i = 0; i < 12; i += 1) delays.push(await sample(2));
    for (const d of delays) {
      // Bounds: capped-base + jitter ∈ [4000, 5000) (small slop for the clock).
      assert.ok(d >= 4_000 - 50, `delay below backoff base: ${d}`);
      assert.ok(d < 5_000 + 250, `delay above base + jitter: ${d}`);
    }
    // Jitter is real: across 12 samples at least one lands well above the base
    // (P(all uniform jitters < 300ms) ≈ 0.3^12 ≈ 5e-7).
    assert.ok(
      delays.some((d) => d >= 4_300),
      `expected jitter above the base, got ${delays.join(',')}`,
    );

    // Deep attempt counts are capped at maxBackoffMs (60s) + jitter.
    const capped = await sample(20);
    assert.ok(capped >= 60_000 - 50, `cap not applied: ${capped}`);
    assert.ok(capped < 61_000 + 250, `cap exceeded: ${capped}`);
  });

  test('a RetryableError without delay reschedules with backoff', async () => {
    await app.get(WidgetService).create('r2');
    transport.failWith(new RetryableError('later'));
    assert.equal((await app.get(OutboxClaimer).tick()).retried, 1);
  });

  test('a generic error retries while attempts remain', async () => {
    await app.get(WidgetService).create('g');
    transport.failWith(new Error('flaky'));
    assert.equal((await app.get(OutboxClaimer).tick()).retried, 1);
  });

  test('a generic error fails once attempts are exhausted (maxAttempts=1)', async () => {
    // enqueue directly with maxAttempts 1 so the first generic failure fails it.
    const store = new SqliteOutboxStore();
    store.enqueue(db, { topic: 't', payload: {}, maxAttempts: 1 });
    transport.failWith(new Error('flaky'));
    const report = await app.get(OutboxClaimer).tick();
    assert.equal(report.failed, 1);
    assert.equal(db.select().from(outboxEvents).all()[0]?.status, 'failed');
  });

  test('DEFAULT_CLAIMER_CONFIG identifies the worker as host-pid', () => {
    // The default claim owner ties a processing row to a live process — losing
    // either half makes stuck-claim forensics impossible.
    assert.equal(
      DEFAULT_CLAIMER_CONFIG.workerInstanceId,
      `${hostname()}-${process.pid}`,
    );
  });

  test('a non-Error rejection is stringified into lastError', async () => {
    await app.get(WidgetService).create('s');
    // failWith stores any thrown value; reject a plain string to exercise the
    // String(error) branch of the claimer's error mapping.
    transport.failWith('plain string failure' as unknown as Error);
    assert.equal((await app.get(OutboxClaimer).tick()).retried, 1);
    assert.equal(
      db.select().from(outboxEvents).all()[0]?.lastError,
      'plain string failure',
    );
  });
});

describe('OutboxClaimer (claims)', () => {
  beforeEach(() => boot());

  // Every transition the claimer makes must carry the claim it holds, so each
  // outcome is driven under a non-default worker id.
  for (const [name, failure, status] of [
    ['publishes', undefined, 'completed'],
    ['retries a RetryableError', new RetryableError('later'), 'pending'],
    ['retries a generic error', new Error('flaky'), 'pending'],
    ['fails a PermanentError', new PermanentError('no handler'), 'failed'],
  ] as const) {
    test(`a tick under its own workerInstanceId ${name}`, async () => {
      const row = await app.get(WidgetService).create(name);
      if (failure) transport.failWith(failure);
      const report = await app.get(OutboxClaimer).tick({ workerInstanceId: 'pod-7' });
      assert.equal(report.lost, 0);
      assert.equal(fetchRow(row.id)?.status, status);
    });
  }

  test('an undefined workerInstanceId override keeps the default instead of stranding the row', async () => {
    // e.g. `{ workerInstanceId: process.env.WORKER_ID }` with the variable
    // unset. Claiming under no owner left the row processing forever, so it
    // was republished after every stuck timeout.
    const row = await app.get(WidgetService).create('unset');
    const claimer = app.get(OutboxClaimer);
    const report = await claimer.tick({ workerInstanceId: undefined });
    assert.deepEqual(report, { claimed: 1, completed: 1, retried: 0, failed: 0, lost: 0 });
    assert.equal(fetchRow(row.id)?.claimedBy, DEFAULT_CLAIMER_CONFIG.workerInstanceId);
    assert.equal((await claimer.tick({ workerInstanceId: undefined })).claimed, 0);
    assert.equal(transport.list().length, 1);
  });

  // Another worker reclaims the row while this one is publishing it (its claim
  // outlived stuckTimeoutMs), then the publish settles with `failure`.
  const reclaimDuringPublish = (failure?: Error) => {
    transport.publish = (message) => {
      db.update(outboxEvents)
        .set({ claimedBy: 'other-worker', claimedAt: new Date(Date.now() + 1).toISOString() })
        .where(eq(outboxEvents.id, message.id))
        .run();
      return failure ? Promise.reject(failure) : Promise.resolve();
    };
  };

  for (const [outcome, failure, warning] of [
    ['completed', undefined, 'was published, but another claim took it over before it was marked completed; the new owner will publish it again'],
    ['retried', new RetryableError('later'), 'failed to publish (later), but another claim took it over before it was marked retried; leaving it to the new owner'],
    ['failed', new PermanentError('no handler'), 'failed to publish (no handler), but another claim took it over before it was marked failed; leaving it to the new owner'],
  ] as const) {
    test(`a claim taken over mid-publish is reported lost, not ${outcome}`, async () => {
      const row = await app.get(WidgetService).create(outcome);
      reclaimDuringPublish(failure);
      const logs = captureLogs();
      try {
        const report = await app.get(OutboxClaimer).tick();
        assert.deepEqual(report, { claimed: 1, completed: 0, retried: 0, failed: 0, lost: 1 });
      } finally {
        logs.restore();
      }
      // The new owner's claim is untouched, and nothing claims a failure it did not record.
      const after = fetchRow(row.id);
      assert.equal(after?.status, 'processing');
      assert.equal(after?.claimedBy, 'other-worker');
      // The publish error is kept even though no row recorded it.
      assert.deepEqual(logs.warns, [`outbox event ${row.id} ${warning}`]);
    });
  }

  test('an event whose claim expired while its batch was publishing is skipped, then delivered once', async () => {
    const rows = [
      await app.get(WidgetService).create('slow'),
      await app.get(WidgetService).create('late'),
    ];
    const publish = transport.publish.bind(transport);
    transport.publish = async (message) => {
      await publish(message);
      // Each publish outlasts stuckTimeoutMs, so the next event's claim is
      // already reclaimable by another worker when its turn comes.
      await new Promise((resolve) => setTimeout(resolve, 80));
    };
    const claimer = app.get(OutboxClaimer);
    const logs = captureLogs();
    try {
      const report = await claimer.tick({ stuckTimeoutMs: 50 });
      assert.deepEqual(report, { claimed: 2, completed: 1, retried: 0, failed: 0, lost: 1 });
    } finally {
      logs.restore();
    }
    const [published] = transport.list();
    const skipped = rows.find((r) => r.id !== published?.id)!;
    assert.equal(fetchRow(published!.id)?.status, 'completed');
    assert.equal(fetchRow(skipped.id)?.status, 'processing');
    assert.deepEqual(logs.warns, [
      `skipped 1 claimed outbox event(s) [${skipped.id}]: the batch was held longer than stuckTimeoutMs (50 ms), so another worker may own them now; raise stuckTimeoutMs or lower batchSize`,
    ]);

    // Once stuck, the skipped event is reclaimed and published, each event once.
    await new Promise((resolve) => setTimeout(resolve, 60));
    assert.equal((await claimer.tick({ stuckTimeoutMs: 50 })).completed, 1);
    assert.deepEqual(
      transport.list().map((m) => m.id).sort(),
      rows.map((r) => r.id).sort(),
    );
  });

  test('a claim slower than stuckTimeoutMs still delivers the first event of its batch', async () => {
    // Ages count from the batch's arrival: measured from the claim stamp, a
    // slow claim would skip every event and the worker would never progress.
    await app.get(WidgetService).create('slow-claim');
    const real = new SqliteOutboxStore();
    const store: OutboxStore = {
      enqueue: (handle, input) => real.enqueue(handle, input),
      claimBatch: async (handle, cfg) => {
        const rows = await real.claimBatch(handle, cfg);
        await new Promise((resolve) => setTimeout(resolve, 30));
        return rows;
      },
      markCompleted: (handle, claim) => real.markCompleted(handle, claim),
      retry: (handle, claim, delayMs, lastError) => real.retry(handle, claim, delayMs, lastError),
      markFailed: (handle, claim, reason) => real.markFailed(handle, claim, reason),
    };
    const report = await new OutboxClaimer(db, store, transport).tick({ stuckTimeoutMs: 10 });
    assert.equal(report.completed, 1);
    assert.equal(transport.list().length, 1);
  });

  test('failing to record a delivery throws instead of retrying the published event', async () => {
    // Mapping it to a publish failure burned an attempt, and on the last one
    // marked a delivered event failed.
    const row = await app.get(WidgetService).create('unrecorded');
    const real = new SqliteOutboxStore();
    const store = {
      claimBatch: (handle: unknown, cfg: Parameters<OutboxStore['claimBatch']>[1]) =>
        real.claimBatch(handle, cfg),
      markCompleted: () => Promise.reject(new Error('database went away')),
    } as unknown as OutboxStore;
    await assert.rejects(new OutboxClaimer(db, store, transport).tick(), /database went away/);
    const after = fetchRow(row.id);
    assert.equal(after?.status, 'processing');
    assert.equal(after?.attempts, 0);
    assert.equal(transport.list().length, 1);
  });

  test('a row claimBatch returns without its claim stamp is neither published nor transitioned', async () => {
    const row = new SqliteOutboxStore().enqueue(db, { topic: 't', payload: {} });
    for (const missing of [{ claimedBy: null }, { claimedAt: null }]) {
      const unstamped: OutboxEventRow = {
        ...row,
        status: 'processing',
        claimedBy: 'custom-store',
        claimedAt: new Date().toISOString(),
        ...missing,
      };
      // A store that forgets to stamp: only claimBatch exists, so any
      // transition call would throw and fail the test.
      const store = { claimBatch: () => Promise.resolve([unstamped]) } as unknown as OutboxStore;
      const logs = captureLogs();
      try {
        const report = await new OutboxClaimer(db, store, transport).tick();
        assert.deepEqual(report, { claimed: 1, completed: 0, retried: 0, failed: 0, lost: 1 });
      } finally {
        logs.restore();
      }
      assert.match(logs.errors[0] ?? '', /without claimedBy\/claimedAt/);
    }
    assert.equal(transport.list().length, 0);
  });
});

describe('resolveClaimerConfig', () => {
  test('applies overrides over the defaults; an undefined override keeps the default', () => {
    assert.deepEqual(resolveClaimerConfig(), DEFAULT_CLAIMER_CONFIG);
    assert.deepEqual(resolveClaimerConfig({ batchSize: 5, workerInstanceId: undefined }), {
      ...DEFAULT_CLAIMER_CONFIG,
      batchSize: 5,
    });
    // A plain JavaScript caller may pass null for "no overrides".
    assert.deepEqual(resolveClaimerConfig(null), DEFAULT_CLAIMER_CONFIG);
  });

  test('names a string value as a string, as an env-backed config hands it over', () => {
    assert.throws(
      () => resolveClaimerConfig({ stuckTimeoutMs: '60000' as unknown as number }),
      /stuckTimeoutMs must be a positive number of milliseconds within Date's range, got "60000" \(string\)/,
    );
  });

  for (const [name, overrides] of [
    ['an empty workerInstanceId', { workerInstanceId: '' }],
    ['a blank workerInstanceId', { workerInstanceId: '  ' }],
    ['a non-string workerInstanceId', { workerInstanceId: 7 as unknown as string }],
    ['a zero batchSize', { batchSize: 0 }],
    ['a fractional batchSize', { batchSize: 1.5 }],
    ['a zero stuckTimeoutMs', { stuckTimeoutMs: 0 }],
    ['an infinite stuckTimeoutMs', { stuckTimeoutMs: Number.POSITIVE_INFINITY }],
    ['a stuckTimeoutMs past Date range', { stuckTimeoutMs: Number.MAX_SAFE_INTEGER }],
    ['a negative baseBackoffMs', { baseBackoffMs: -1 }],
    ['a NaN maxBackoffMs', { maxBackoffMs: Number.NaN }],
  ] as const) {
    test(`rejects ${name}`, () => {
      assert.throws(() => resolveClaimerConfig(overrides), /^\w+Error: claimer \w+ must be/);
    });
  }
});

describe('InboxService (dedup via the app)', () => {
  beforeEach(() => boot());

  test('runOnce processes once, dedups a redelivery, and rolls back on throw', async () => {
    const inbox = app.get(InboxService);
    const writeDelivery = (key: string): void => {
      db.run(sql`INSERT INTO deliveries (key) VALUES (${key})`);
    };

    assert.equal(await inbox.runOnce('k1', 'src', () => writeDelivery('k1')), 'processed');
    assert.equal(await inbox.runOnce('k1', 'src', () => writeDelivery('k1-dup')), 'duplicate');
    assert.equal(count('deliveries'), 1);

    await assert.rejects(
      () =>
        inbox.runOnce('k2', 'src', () => {
          writeDelivery('k2');
          throw new Error('handler boom');
        }),
      /handler boom/,
    );
    // The dedup row rolled back with the side effect → k2 reprocesses cleanly.
    assert.equal(db.select().from(inboxEvents).all().length, 1);
    assert.equal(await inbox.runOnce('k2', 'src', () => writeDelivery('k2-retry')), 'processed');
  });
});

describe('MessagingModule.forRootAsync / no inbox', () => {
  test('forRootAsync wires the transport via a factory and omits the inbox', async () => {
    raw = new Database(':memory:');
    raw.exec(DDL);
    db = drizzle(raw);
    transport = new InMemoryOutboxTransport();

    const TRANSPORT_CONFIG = Symbol('transport-config');

    @Module({})
    class AsyncFixture {}
    @Module({})
    class TransportConfigModule {}
    const transportConfig: DynamicModule = {
      module: TransportConfigModule,
      providers: [{ provide: TRANSPORT_CONFIG, useValue: transport }],
      exports: [TRANSPORT_CONFIG],
    };

    app = await NestFactory.createApplicationContext(
      {
        module: AsyncFixture,
        imports: [
          dbImport(db),
          ClsModule.forRoot({
            global: true,
            plugins: [
              new ClsPluginTransactional({
                adapter: new TransactionalAdapterDrizzleOrm({ drizzleInstanceToken: DRIZZLE }),
                enableTransactionProxy: true,
              }),
            ],
          }),
          MessagingModule.forRootAsync({
            isGlobal: false,
            drizzleInstanceToken: DRIZZLE,
            outboxStore: new SqliteOutboxStore(),
            imports: [transportConfig],
            inject: [TRANSPORT_CONFIG],
            // Idiomatic typed factory — assignable now that useTransport mirrors
            // Nest's `(...args: any[]) => T` (the dogfood-surfaced ergonomic fix).
            useTransport: (t: InMemoryOutboxTransport) => t,
          }),
        ],
      },
      { logger: false, abortOnError: false },
    );

    // The outbox half works...
    const store = new SqliteOutboxStore();
    store.enqueue(db, { topic: 't', payload: { n: 1 } });
    assert.equal((await app.get(OutboxClaimer).tick()).completed, 1);
    assert.equal(transport.list().length, 1);
    // ...and the inbox half is absent (no inboxStore provided).
    assert.throws(() => app.get(InboxService));
  });

  test('forRoot applies defaults when isGlobal/imports are omitted', () => {
    const mod = MessagingModule.forRoot({
      drizzleInstanceToken: DRIZZLE,
      outboxStore: new SqliteOutboxStore(),
      transport: new InMemoryOutboxTransport(),
    });
    assert.equal(mod.global, true);
    assert.deepEqual(mod.imports, []);
    assert.ok(mod.exports?.includes(OutboxClaimer));
  });

  test('forRootAsync applies defaults when isGlobal/imports/inject are omitted', () => {
    // Calling the factory evaluates the `?? true` / `?? []` fallbacks; inspect the
    // returned DynamicModule directly (no DI bootstrap needed for the defaults).
    const mod = MessagingModule.forRootAsync({
      drizzleInstanceToken: DRIZZLE,
      outboxStore: new SqliteOutboxStore(),
      useTransport: () => new InMemoryOutboxTransport(),
    });
    assert.equal(mod.global, true);
    assert.deepEqual(mod.imports, []);
    assert.ok(mod.exports?.includes(OutboxClaimer));
    // No inboxStore → InboxService is not provided.
    assert.equal(mod.exports?.includes(InboxService), false);
    // The transport factory provider defaults to an empty inject list.
    const transportProvider = (mod.providers ?? []).find(
      (p) => typeof p === 'object' && p !== null && 'useFactory' in p,
    ) as { inject?: unknown[] } | undefined;
    assert.deepEqual(transportProvider?.inject, []);
  });
});
