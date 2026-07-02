import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { afterEach, beforeEach, describe, test } from 'node:test';
import { Injectable, Module } from '@nestjs/common';
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
  PermanentError,
  RetryableError,
} from '../index';
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
    assert.deepEqual(report, { claimed: 1, completed: 1, retried: 0, failed: 0 });
    assert.equal(transport.list().length, 1);
    assert.equal(transport.list()[0]?.idempotencyKey, 'widget:gamma');
    const after = db.select().from(outboxEvents).where(eq(outboxEvents.id, row.id)).get();
    assert.equal(after?.status, 'completed');
  });

  test('a PermanentError fails the row immediately', async () => {
    await app.get(WidgetService).create('p');
    transport.failWith(new PermanentError('no handler'));
    const report = await app.get(OutboxClaimer).tick();
    assert.equal(report.failed, 1);
    assert.equal(db.select().from(outboxEvents).all()[0]?.status, 'failed');
  });

  test('a RetryableError reschedules (honouring delayMs)', async () => {
    await app.get(WidgetService).create('r');
    transport.failWith(new RetryableError('later', 5_000));
    const report = await app.get(OutboxClaimer).tick();
    assert.equal(report.retried, 1);
    const row = db.select().from(outboxEvents).all()[0];
    assert.equal(row?.status, 'pending');
    assert.equal(row?.attempts, 1);
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
