---
title: Migrate to @nestjs/outbox
description: Move from @nest-native/messaging to NestJS's official transactional outbox.
---

# Migrate to `@nestjs/outbox`

NestJS now ships an official transactional outbox,
[`@nestjs/outbox`](https://docs.nestjs.com/reliability/outbox). It covers what
this package was for: messages written in your business transaction, a relay
that publishes them after the commit with leases, retries and a dead-letter
table, and an inbox that deduplicates redeliveries. So `@nest-native/messaging`
is **deprecated**: 0.8.4 is its last release, and the repository is archived.
Existing installs keep working, but there will be no further fixes. Move to the
official module.

Most of the move is mechanical. `MessagingModule` becomes `OutboxModule`,
`enqueue()` becomes `Outbox.add(tx, message)` with the transaction passed
explicitly, the worker loop becomes the module's relay, and
`InboxService.runOnce()` becomes `OutboxInbox.processInTransaction()`. Four
things this package shipped have no counterpart in the official one, and the
sections below show how to get each back:

- **The SQLite store.** The module ships stores for PostgreSQL and MySQL only.
- **The cross-process wakes.** `notify()` wakes the relay in its own process
  only; `WakeSocketServer`/`WakeSocketClient` and the Postgres
  `LISTEN`/`NOTIFY` listener have no counterpart.
- **The Kafka and RabbitMQ transports and inbox consumers.** The module
  publishes through `@nestjs/microservices` clients, not over
  `@nest-native/kafka` or amqplib.
- **Status rows.** A published message is deleted, so the outbox is no longer a
  delivery history, and the unique index on `idempotency_key` goes with it.

The [reference app](https://github.com/nest-native/reference-app) made this
move on SQLite, with Kafka and RabbitMQ profiles, and it is the worked example
throughout: its SQLite store, its two broker transports and its inbox consumers
are application code you can copy (MIT).

## Before you start

- **Node.js.** `@nestjs/outbox` is ESM-only. A CommonJS application loads it
  through Node's `require(esm)`, which is unflagged from Node 20.19 and 22.12.
  On NestJS 12 this package needed 22.12 already; on NestJS 11 it ran on any
  Node 22.
- **TypeScript.** Its typings come through its `exports` map. TypeScript 6's
  default resolution for `module: commonjs` (`bundler`) finds them, and so do
  `module: nodenext` from TypeScript 5.8 and `node20` from 5.9. `node16` and
  `node18` find them but refuse to `require()` an ES module (TS1479), and
  `node10` finds the root entry point but not `@nestjs/outbox/postgres`,
  `/mysql` or `/testing`.
- **NestJS.** It supports NestJS 11 and 12, like this package.
- **Dependencies.** This package had none; `@nestjs/outbox` has one,
  `@nestjs/store-kit`, whose peers (`drizzle-orm`, `kysely`, `mysql2`, `pg`) are
  all optional.
- **Databases.** PostgreSQL, with READ COMMITTED as the default isolation
  level of the store's connections (`default_transaction_isolation`, which the
  server, the database or the role can set), and MySQL 8.4 LTS or 9.x with a
  strict `sql_mode`. SQLite needs a store of your own ([SQLite](#sqlite)).
- **Transactions.** The module needs no transaction library: you pass it your
  ORM's transaction. `@nestjs-cls/transactional` keeps working, and the
  examples below use it.

```bash
npm install @nestjs/outbox
# once the old outbox_events and inbox_events tables are drained and dropped:
npm uninstall @nest-native/messaging
```

## What changes

| `@nest-native/messaging` | `@nestjs/outbox` |
| --- | --- |
| `MessagingModule.forRoot({ drizzleInstanceToken, outboxStore, inboxStore, transport })` | `OutboxModule.forRoot({ transports, route, relay, retry })` plus one store provider ([Register the module](#register-the-module)) |
| `MessagingModule.forRootAsync({ inject, useTransport })` | `OutboxModule.forRootAsync({ inject, useFactory })`; the factory returns every option, transport instances included |
| `OutboxProducer.enqueue(input)` | `Outbox.add(tx, message)`, then `Outbox.notify()` after the commit ([Add messages](#add-messages-in-your-transaction)) |
| `EnqueueInput`: `idempotencyKey`, `availableAt`, `maxAttempts` | `NewOutboxMessage`: `headers`, `key`, `id`, `availableAt`, `delay`; no per-message budget |
| `OutboxClaimer.tick()`, `TickReport` | `OutboxRelay.runOnce()`, `OutboxRelayRunResult` ([Run the relay](#run-the-relay)) |
| `runWorkerLoop()`, `WorkerLoopOptions` | the relay's own loop: `relay.enabled`, `start()`, `stop()`, shutdown hooks |
| `ClaimerConfig`, `DEFAULT_CLAIMER_CONFIG`, `resolveClaimerConfig()` | the `relay` and `retry` options, checked at startup |
| `OutboxWaker`, `WakeSignal` | `Outbox.notify()` and `OutboxRelay.notify()` ([Wake the relay](#wake-the-relay)) |
| `WakeSocketServer`, `WakeSocketClient` | none ([Wake the relay](#wake-the-relay)) |
| `PostgresWakeListener`, `wakeChannel`, `assertValidWakeChannel()` | none ([Wake the relay](#wake-the-relay)) |
| `RetryableError` | none: every failure spends the budget; a retry-after goes through `retry.backoff` ([Retries](#retries-and-dead-letters)) |
| `PermanentError` | `NonRetryableMessageError`, or `retry.retryIf` returning `false` |
| `OutboxStatus`, `OUTBOX_STATUSES` | none: `relay.stats()`, `OutboxDeadLetters` and `OutboxEvents` |
| `OutboxRegistry.register()`, `OutboxHandler`, `OutboxHandlerResult` | `@OnOutboxMessage(topic, { consumer })` ([Consume in-process](#consume-in-process)) |
| `InProcessOutboxTransport` | the built-in `local` transport |
| `InboxService.runOnce(messageKey, source, handler)`, `RunOnceOutcome` | `OutboxInbox.processInTransaction(tx, consumer, messageId, work)` or `process()`, `OutboxInboxResult` ([The inbox](#the-inbox)) |
| `InboxStatus`, `INBOX_STATUSES` | none: an inbox record exists or it doesn't |
| `OutboxTransport`, `OUTBOX_TRANSPORT`, `OutboxMessage` | `OutboxTransport` (`publish(message, { signal })`), registered by name in `transports`; `OutboxMessage` |
| `OutboxStore`, `OutboxClaim`, `OutboxEventRow`, `InboxStore`, `InboxSideEffect` | `OutboxStore` and `OutboxInboxStore`, registered with `OutboxStorage.registerSource()` ([A store of your own](#a-store-of-your-own)) |
| `OUTBOX_STORE`, `INBOX_STORE`, `MESSAGING_DRIZZLE`, `MESSAGING_OPTIONS` | `OutboxStorage` (its `messages` and `inbox` are the stores in use), `OUTBOX_MODULE_OPTIONS` |
| `/postgres`: `PostgresOutboxStore`, `PostgresInboxStore` | `PostgresOutboxStore` from `@nestjs/outbox/postgres` ([PostgreSQL](#postgresql)) |
| `/mysql`: `MysqlOutboxStore`, `MysqlInboxStore` | `MySqlOutboxStore` from `@nestjs/outbox/mysql` ([MySQL](#mysql)) |
| `/sqlite`: `SqliteOutboxStore`, `SqliteInboxStore` | none ([SQLite](#sqlite)) |
| `outboxEvents`, `inboxEvents` table factories | none: the stores own their tables and migrations |
| `isPgUniqueViolation()`, `isMysqlUniqueViolation()`, `isSqliteUniqueViolation()` | none; copy the one you use |
| `/kafka`: `KafkaOutboxTransport`, `KafkaInboxConsumer` | none over `@nest-native/kafka` ([Kafka](#kafka)) |
| `/rabbitmq`: `RabbitOutboxTransport`, `RabbitInboxConsumer`, `X_DEAD_LETTER_ID` | none over amqplib ([RabbitMQ](#rabbitmq)) |
| `X_EVENT_ID`, `X_IDEMPOTENCY_KEY`, `X_ERROR`, `headerToString()`, `encodeWireValue()`, `decodeWireValue()` | none: headers are free-form strings; copy the wire contract ([Kafka](#kafka)) |
| `deriveDedupKey()`, `ConsumerAction`, `actionForOutcome()`, `actionForError()` | none; the reference app keeps `deriveDedupKey()` in its wire contract |
| `/testing`: `InMemoryOutboxTransport` | `relay.runOnce()` and a transport of your own; `InMemoryOutboxStore`, from the root entry point; contract suites in `@nestjs/outbox/testing` ([Testing](#testing)) |
| `onTick`, `onError` | `OutboxEvents.events$`, the `nestjs:outbox:*` diagnostics channels, `relay.stats()` ([Observability](#observability)) |

## Register the module

```ts
// Before
import { MessagingModule } from '@nest-native/messaging';
import { InProcessOutboxTransport, OutboxRegistry } from '@nest-native/messaging/in-process';
import { PostgresInboxStore, PostgresOutboxStore } from '@nest-native/messaging/postgres';

const registry = new OutboxRegistry();

@Module({
  imports: [
    DrizzleModule.forRoot({ db }),
    ClsModule.forRoot({ /* ClsPluginTransactional on getDrizzleToken() */ }),
    MessagingModule.forRoot({
      drizzleInstanceToken: getDrizzleToken(),
      outboxStore: new PostgresOutboxStore(),
      inboxStore: new PostgresInboxStore(),
      transport: new InProcessOutboxTransport(registry),
    }),
  ],
  providers: [{ provide: OutboxRegistry, useValue: registry }],
})
export class AppModule {}
```

```ts
// After
import { DrizzleModule, getDrizzleToken } from '@nestjs/drizzle';
import { OutboxModule, OutboxStorage } from '@nestjs/outbox';
import { fromDrizzle, PostgresOutboxStore } from '@nestjs/outbox/postgres';

@Module({
  imports: [
    DrizzleModule.forRoot({ db }),
    ClsModule.forRoot({ /* unchanged */ }),
    OutboxModule.forRoot({
      // This package's defaults: a 60 s claim and 10 attempts, at most 1 min apart.
      relay: { lease: '1m' },
      retry: { attempts: 10, backoff: { delay: '1s', maxDelay: '1m' } },
    }),
  ],
  providers: [
    {
      // The messages, the dead letters and the consumers' inbox, in one store
      provide: PostgresOutboxStore,
      inject: [getDrizzleToken(), OutboxStorage],
      useFactory: (db: AppDatabase, storage: OutboxStorage) =>
        new PostgresOutboxStore({ executor: fromDrizzle(db) }, storage),
    },
  ],
})
export class AppModule {}
```

- **The store is a provider, not an option.** It registers itself with
  `OutboxStorage` in its constructor. A leftover `store` option fails the
  startup. Register the store once, in the module that imports `OutboxModule`:
  the registry locks when `OutboxModule` initializes, so a store registered
  from a lifecycle hook, a request-scoped provider or a lazy-loaded module
  throws.
- **Without a registered store, the module runs in memory.** The in-memory
  store loses its messages on restart and can't join your transactions, so a
  rolled-back message is still published. With `NODE_ENV=production`, startup
  fails instead, unless you set `allowInMemoryStorage: true`.
- **`drizzleInstanceToken` goes.** The store's factory injects the database.
- **The in-process transport is built in.** `@OnOutboxMessage()` handlers run on
  the `local` transport: there is no registry or transport to construct.
- **It is global by default**, as before. `Outbox`, `OutboxRelay`,
  `OutboxInbox`, `OutboxDeadLetters`, `OutboxEvents` and `OutboxStorage` can be
  injected anywhere, and the options token is `OUTBOX_MODULE_OPTIONS`. Where
  you injected `OUTBOX_STORE` or `INBOX_STORE`, inject `OutboxStorage` and read
  its `messages` or `inbox` in a method or a lifecycle hook, never in a
  constructor: the first read locks the registry, and a store whose
  constructor runs after it fails to register, which fails the startup.

### With a broker transport

```ts
// Before
MessagingModule.forRootAsync({
  drizzleInstanceToken: getDrizzleToken(),
  outboxStore: new PostgresOutboxStore(),
  inboxStore: new PostgresInboxStore(),
  inject: [KafkaProducerService],
  useTransport: (producer: KafkaProducerService) => new KafkaOutboxTransport(producer),
});

// After: KafkaOutboxTransport is now your application's class (see Kafka)
OutboxModule.forRootAsync({
  inject: [KafkaProducerService],
  useFactory: (producer: KafkaProducerService) => ({
    transports: { kafka: new KafkaOutboxTransport(producer) },
    relay: { lease: '1m' },
    retry: { attempts: 10, backoff: { delay: '1s', maxDelay: '1m' } },
  }),
});
```

This package's `forRootAsync()` made only the transport asynchronous; the
module's factory returns every option, and `useClass` or `useExisting` (an
`OutboxOptionsFactory`) work too. The factory returns transport *instances*. A
transport *class* for Nest to instantiate inside `OutboxModule` (an
`@Injectable()` one) goes at the top level instead, next to `useFactory`, with
`imports` for the modules its dependencies come from. A class in the factory's
result, or a name set in both places, fails the startup.

A transport implements `publish(message, { signal })`: extend `OutboxTransport`,
or pass any object with a `publish()` method. The message it gets carries more
than this package's did: `headers` in place of `idempotencyKey` (now the
`x-idempotency-key` header), an ordering `key`, `createdAt` and `availableAt`
in epoch milliseconds (the outbox rows kept ISO-8601 text), `attempts`, the
failed attempts so far (0 on the first delivery), and `lastError`, the error
of the last one (`null` before it). After `publishTimeout` the relay aborts
`signal` and counts the attempt as failed; a transport that ignores the signal
keeps running, and its retry may publish the message a second time.

This package had exactly one transport. The module has any number, by name, and
publishes each message to exactly one of them: `local` (the
`@OnOutboxMessage()` handlers, a reserved name) or one of yours. With several
transports, or with transports and handlers in the same application, `route`
is required, and its absence fails the startup:

```ts
route: (message) => (message.topic.startsWith('billing.') ? 'kafka' : 'local'),
```

## Add messages in your transaction

```ts
// Before
@Injectable()
export class OrderService {
  constructor(
    @InjectTransaction() private readonly db: AppDatabase,
    private readonly producer: OutboxProducer<PostgresOutboxStore>,
  ) {}

  @Transactional()
  async placeOrder(id: string, item: string): Promise<void> {
    await this.db.insert(orders).values({ id, item });
    await this.producer.enqueue({
      topic: 'order.placed',
      payload: { id, item },
      idempotencyKey: `order:${id}`,
    });
  }
}
```

```ts
// After
import { Transactional, TransactionHost } from '@nestjs-cls/transactional';
import type { TransactionalAdapterDrizzleOrm } from '@nestjs-cls/transactional-adapter-drizzle-orm';
import { Outbox } from '@nestjs/outbox';

@Injectable()
export class OrderService {
  constructor(
    private readonly txHost: TransactionHost<TransactionalAdapterDrizzleOrm<AppDatabase>>,
    private readonly outbox: Outbox<AppDatabase>,
  ) {}

  async placeOrder(id: string, item: string): Promise<void> {
    await this.save(id, item);
    this.outbox.notify(); // committed: publish now instead of at the next poll
  }

  @Transactional()
  private async save(id: string, item: string): Promise<void> {
    const tx = this.txHost.tx;
    await tx.insert(orders).values({ id, item });
    await this.outbox.add(tx, {
      topic: 'order.placed',
      payload: { id, item },
      headers: { 'x-idempotency-key': `order:${id}` },
    });
  }
}
```

- **Pass the transaction, never the database.** `txHost.tx` is the open
  transaction, and so is the `@InjectTransaction()` proxy inside a
  `@Transactional()` method. Without nestjs-cls, pass the `tx` your
  `db.transaction()` callback receives. `add()` refuses a missing handle, and
  the shipped stores refuse the database itself, with
  `OutboxTransactionRequiredError`. This package's `enqueue()` called outside a
  transaction committed the event on its own; that mistake now fails.
- **Await `add()`** on PostgreSQL and MySQL. Pass an array to add a batch.
- **`notify()` after the commit.** Nothing is published inside the
  transaction; without `notify()`, the message goes out at the next poll.
- **`idempotencyKey` becomes a header.** Transports put headers on the wire, and
  the reference app's consumers fall back to `x-idempotency-key` as this
  package's did. There is no producer-side dedup any more: the unique index on
  `idempotency_key` (partial on PostgreSQL and SQLite) failed a second
  `enqueue()` with the same key for as long as the first row existed,
  completed rows included. An `id` you pass is unique only while its message
  is pending, since a published message is deleted. If you relied on the
  index to refuse a repeated business event, enforce that in your own table,
  in the same transaction.
- **`maxAttempts` has no per-message equivalent.** The budget is module-wide;
  see [Retries and dead letters](#retries-and-dead-letters).
- **`availableAt` stays**, and takes epoch milliseconds as well as a `Date`;
  or pass `delay: '10m'`, not both.
- **`key` is new: an ordering key.** Messages that share a key are published one
  at a time, in the order their transactions committed, and a retrying message
  holds back the rest of its key. This package gave no order at all. Opt in per
  stream (`` key: `order:${id}` ``), and don't share a key between transports.
- **The payload is a JSON snapshot.** `add()` serializes it at once, so it
  throws on a `BigInt` or a cycle, and any JSON value is accepted, `null`
  included (this package required an object). `add()` refuses U+0000 and lone
  UTF-16 surrogates in the topic, key, id, headers and payload with a
  `TypeError`.
- **Ids are UUIDv7** (time-ordered) unless you pass `id`; this package used
  random v4 UUIDs. Order within a key comes from the store's numbering, in
  commit order, never from the id.
- **The return value** is the built `OutboxMessage`, typed `Awaitable`. A
  synchronous store returns it synchronously, so inside a synchronous
  better-sqlite3 transaction cast it to read the id at once:
  `outbox.add(tx, input) as OutboxMessage<OrderPlaced>`.

## Run the relay

```ts
// Before
const claimer = app.get(OutboxClaimer);
const waker = new OutboxWaker();
const controller = new AbortController();
runWorkerLoop(claimer, {
  pollIntervalMs: 1_000,
  claimer: { batchSize: 32, stuckTimeoutMs: 60_000 },
  signal: controller.signal,
  waker,
  onTick: (report) => metrics.record(report),
  onError: (error) => logger.error(error),
}).catch((error) => logger.error(error));
// on shutdown: controller.abort();
```

The relay is part of the module. By default it runs in every process that boots
`OutboxModule`, starting at bootstrap. Delete the loop and enable shutdown
hooks, so a deploy drains the relay:

```ts
// After: main.ts
async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  // On SIGTERM the relay stops claiming, lets in-flight publishes settle,
  // and releases the leases on messages it hasn't started.
  app.enableShutdownHooks();
  await app.listen(3000);
}
void bootstrap();
```

To keep publishing in a separate worker, turn the relay off in the API
instances and boot the same module without an HTTP server in the worker. The
relay's poll timer keeps that process alive until a shutdown hook stops it:

```ts
OutboxModule.forRoot({
  relay: { enabled: process.env.OUTBOX_RELAY !== 'off', lease: '1m' }, // a boolean: a string fails the startup
});

// worker.ts
async function bootstrap() {
  const app = await NestFactory.createApplicationContext(AppModule);
  app.enableShutdownHooks();
}
void bootstrap();
```

The reference app keeps `enabled: false` in `AppModule` and calls
`relay.start()` from its worker, next to its jobs loop
([`scripts/start-worker.ts`](https://github.com/nest-native/reference-app/blob/main/scripts/start-worker.ts)).

| This package | `@nestjs/outbox` |
| --- | --- |
| `pollIntervalMs` (2 s) | `relay.pollInterval` (1 s; milliseconds or a string such as `'2s'`) |
| `batchSize` (32) | `relay.batchSize` (100) |
| `stuckTimeoutMs` (60 s) | `relay.lease` (30 s) |
| `baseBackoffMs`, `maxBackoffMs` (1 s, 60 s) | `retry.backoff.delay`, `retry.backoff.maxDelay` (1 s, 5 min) |
| `maxAttempts` per message (10) | `retry.attempts` for every message (20) |
| `workerInstanceId` | none: each claim gets a random owner token |
| none | `relay.concurrency` (10 key groups at a time), `relay.publishTimeout` (a third of `lease`) |
| `signal` | `relay.stop()`, `app.close()` or the shutdown hooks |
| `waker` | `outbox.notify()` |
| `onTick` | `OutboxEvents.events$` ([Observability](#observability)) |
| `onError` | the relay's own log lines, through Nest's `Logger` |

- **Leases replace stuck-claim recovery, with the same guarantees.** A claim is
  exclusive for `lease`; every later write is fenced by the claim's random
  owner token, so a relay that stalled past its lease can't overwrite the work
  of the one that took over. Set `lease` to your old `stuckTimeoutMs`, and keep
  the hosts' clocks NTP-synced: leases use the relay's clock.
- **Publishes run in parallel.** Messages are grouped by key, and up to
  `relay.concurrency` groups publish at once; this package published one at a
  time.
- **A publish has a deadline.** After `publishTimeout`, the relay aborts the
  publish's `signal` and counts the attempt as failed. `publishTimeout` must be
  shorter than `lease`, and the relay never starts a publish with less than
  `publishTimeout` of lease left; it releases the rest of the batch instead.
- **Bad options fail the startup**, a `publishTimeout` that isn't shorter than
  `lease` included; this package's worker loop rejected before its first tick.
- **`tick()` becomes `runOnce()`** in tests and scripts. `completed` is now
  `published`, `failed` is `deadLettered`, and `lost` is `leaseLost`, plus
  `released`. Per-tick overrides have no equivalent: the options are
  module-wide. `runOnce()` logs a store error and returns a partial result,
  where `tick()` threw.

## Retries and dead letters

```ts
// Before
await producer.enqueue({ topic: 'invoice.issued', payload, maxAttempts: 3 });
throw new RetryableError('rate limited', 30_000); // retried after 30 s, without limit
throw new PermanentError('invoice has no customer'); // the row fails at once
```

```ts
// After
export class RetryAfterError extends Error {
  constructor(message: string, readonly delayMs: number) {
    super(message);
  }
}

OutboxModule.forRoot({
  retry: {
    attempts: 10,
    // A transport's retry-after wins; otherwise 1 s, doubling, at most 1 min.
    backoff: (attempt, error) =>
      error instanceof RetryAfterError ? error.delayMs : Math.min(1_000 * 2 ** (attempt - 1), 60_000),
    // A smaller budget for one topic: invoices stop after their third attempt.
    retryIf: (_error, attempt, message) => !(message.topic === 'invoice.issued' && attempt >= 3),
  },
});

throw new RetryAfterError('rate limited', 30_000); // counts toward the 10 attempts
throw new NonRetryableMessageError('invoice has no customer'); // dead-lettered at once
```

- **Every failure spends the budget.** `RetryableError` retried without limit;
  there is no such retry now. After `retry.attempts` (counting the first) the
  message is dead-lettered with the reason `exhausted`.
- **`NonRetryableMessageError`**, or `retryIf` returning `false`, dead-letters
  the message at once with the reason `rejected`.
- **The defaults retry for much longer.** The official docs: "The package
  default is 20 attempts, about 30 to 60 minutes in total." This package's 10
  attempts, from 1 s doubling to 1 min, gave up after about 4 minutes. The
  `retry` option in [Register the module](#register-the-module) keeps that
  budget; with the module's equal jitter (each wait between half and all of its
  value) a message is dead-lettered 2 to 4 minutes after its first failure.
- **A topic with no handler is retried.** The in-process transport failed such
  an event at once; the `local` transport throws `OutboxNoHandlerError`, which
  is retried, so a rolling deploy can't dead-letter a topic only the new
  version handles. To fail fast as before:
  `retryIf: (error) => !(error instanceof OutboxNoHandlerError)`.
- **A retrying keyed message holds back its key** until it is published or
  dead-lettered; dead-lettering unblocks the key.

Failed events used to stay in `outbox_events` as `failed` rows, with no API.
They now move to a dead-letter table, with the error of every attempt,
and `OutboxDeadLetters` lists, inspects, requeues and purges them. The module
mounts no routes; expose it behind your own guard, since dead letters hold full
payloads:

```ts
@Controller('admin/outbox')
@UseGuards(AdminGuard)
export class OutboxAdminController {
  constructor(private readonly deadLetters: OutboxDeadLetters) {}

  @Get('dead-letters')
  list(@Query('topic') topic?: string) {
    return this.deadLetters.list({ topic });
  }

  @Post('dead-letters/:id/requeue')
  async requeue(@Param('id') id: string) {
    return { requeued: await this.deadLetters.requeue(id) };
  }
}
```

A requeued message keeps its id, so consumers that already processed it skip
it, and its place in its key, with a fresh budget. Purge the ones you have
dealt with on a schedule: `deadLetters.purge({ failedBefore })`.

There are no statuses to query. Where you read `outbox_events.status`:

| `status` | Now |
| --- | --- |
| `pending`, `processing` | `relay.stats()`: `pending`, `ready`, `leased`, plus `oldestDueAt`, `lagMs` and `inFlight` |
| `failed` | `OutboxDeadLetters.list()`, and `deadLetters` in `relay.stats()` |
| `completed` | nothing is kept; subscribe to `published` events to record them ([Observability](#observability)) |

`inbox_events.status` was only ever written as `processed` (`dead_lettered` was
declared, never written), so nothing is lost there: an inbox record exists or
it doesn't.

## Consume in-process

```ts
// Before
@Injectable()
export class OrderPlacedHandler implements OnModuleInit {
  constructor(
    @InjectTransaction() private readonly db: AppDatabase,
    private readonly registry: OutboxRegistry,
    private readonly inbox: InboxService,
  ) {}

  onModuleInit(): void {
    this.registry.register('order.placed', (payload, message) => this.handle(payload, message));
  }

  private async handle(payload: Record<string, unknown>, message: OutboxMessage): Promise<OutboxHandlerResult> {
    if (!isOrderPlaced(payload)) {
      throw new PermanentError('order.placed: malformed payload');
    }
    const key = message.idempotencyKey ?? message.id;
    await this.inbox.runOnce(key, 'order.placed:audit', async () => {
      await this.db.insert(orderAudit).values({ key, item: payload.item });
    });
    return 'completed';
  }
}
```

```ts
// After
@Injectable()
export class OrderPlacedHandler {
  constructor(private readonly txHost: TransactionHost<TransactionalAdapterDrizzleOrm<AppDatabase>>) {}

  // The runOnce() source it replaces, so the inbox records carried over still match
  @OnOutboxMessage('order.placed', { consumer: 'order.placed:audit' })
  @Transactional()
  async audit(payload: unknown, ctx: OutboxHandlerContext<AppDatabase>): Promise<void> {
    if (!isOrderPlaced(payload)) {
      throw new NonRetryableMessageError('order.placed: malformed payload');
    }
    const tx = this.txHost.tx;
    // Records the message id through tx, then runs the work only if it is new to this consumer.
    await ctx.processInTransaction(tx, async () => {
      await tx.insert(orderAudit).values({ key: ctx.message.id, item: payload.item });
    });
  }
}
```

- **Handlers are discovered, not registered.** Put `@OnOutboxMessage()` on a
  method of a singleton provider; a request-scoped one fails the startup.
- **Every handler has an inbox**, named by `consumer` (required; keep it
  stable, or every past message looks new). A handler that replaces a
  `runOnce()` takes its source as the name, so the records
  [carried over](#carry-over-the-rows-in-flight) from `inbox_events` still
  match. By default the `local` transport skips a message id the consumer has
  processed, and records it after the handler succeeds. That record is not
  atomic with the handler: if the process dies in between, the handler runs
  again. For database writes, `ctx.processInTransaction(tx, work)` records the
  id in your transaction, which makes them exactly-once. `inbox: false` runs
  every delivery. This package's registry handlers had no inbox unless you
  paired them with `runOnce()`.
- **A topic can have several handlers**, each with its own consumer name and
  inbox, run concurrently. This package allowed one handler per topic.
- **Return values are ignored.** Return to succeed, throw to retry, and throw
  `NonRetryableMessageError` to dead-letter. `{ retryAfterMs }` has no
  equivalent; throw an error that `retry.backoff` reads instead.
- **The context** has the `message` (`id`, `topic`, `key`, `headers`…), the
  `consumer`, the `attempt` (1 on the first delivery) and a `signal` that aborts
  after `publishTimeout`; pass it to the calls the handler makes.

The reference app's in-process handlers are
[`activity.handler.ts`](https://github.com/nest-native/reference-app/blob/main/src/modules/outbox/activity.handler.ts),
which records its inbox entry in the projection's transaction, and
[`user-invited.handler.ts`](https://github.com/nest-native/reference-app/blob/main/src/modules/outbox/user-invited.handler.ts),
an email that relies on the default inbox.

## The inbox

```ts
// Before
const outcome = await this.inbox.runOnce(dedupKey, 'billing', async () => {
  await this.db.insert(invoices).values(invoice);
});
if (outcome === 'duplicate') return;
```

```ts
// After
@Transactional()
private async record(dedupKey: string, invoice: NewInvoice): Promise<boolean> {
  const tx = this.txHost.tx;
  const result = await this.inbox.processInTransaction(tx, 'billing', dedupKey, async () => {
    await tx.insert(invoices).values(invoice);
  });
  return !result.duplicate;
}
```

- **The arguments are in the other order**: the consumer first, then the
  message id. `runOnce()` took the key, then the source.
- **It runs in your transaction.** `runOnce()` opened one, or joined yours;
  `processInTransaction()` needs your transaction handle, and throws
  `OutboxTransactionRequiredError` without one. Await it on an asynchronous
  store. With a synchronous store and synchronous work, as on better-sqlite3,
  it returns synchronously.
- **The result is an object**: `{ duplicate: true }`, or
  `{ duplicate: false, result }` with what the work returned.
- **`process(consumer, messageId, work, { signal })`** checks, runs and records
  without a transaction, for side effects outside your database, such as an
  email. It is not exactly-once across processes or after a crash.
- **Nothing prunes the inbox.** Neither package did. Schedule
  `inbox.prune('30d')`, longer than any redelivery, requeued dead letters
  included; the package's README has a Retention section, and the reference app
  runs it nightly
  ([`inbox-prune.job.ts`](https://github.com/nest-native/reference-app/blob/main/src/modules/outbox/inbox-prune.job.ts)).

## Stores

### PostgreSQL

This package's `PostgresOutboxStore` and `PostgresInboxStore` become the
module's single `PostgresOutboxStore`, registered as in
[Register the module](#register-the-module).

- **It owns its tables**, in a schema of its own, `nest_outbox` (`messages`,
  `dead_letters`, `inbox`, `migrations`), which drizzle-kit leaves alone. Drop
  `outboxEvents` and `inboxEvents` from your Drizzle schema once
  [the rows in flight are carried over](#carry-over-the-rows-in-flight).
- **It migrates itself** at startup, except with `NODE_ENV=production`. In
  production, run `npx nest-outbox migrate` in your deploy step and
  `npx nest-outbox status` in CI (it exits with 1 while the schema is behind).
  To keep the SQL in your own migrations instead:
  `npx drizzle-kit generate --custom --name=outbox`, then fill the file with
  `npx nest-outbox sql --statement-breakpoints`. The migrations only add
  tables, columns and indexes; there are no down migrations.
- **READ COMMITTED must be the default.** At startup the store reads
  `default_transaction_isolation` on its connection and refuses a stricter
  level, whether the server, the database or the role set it. This package
  pinned READ COMMITTED on its own transactions instead, so a SERIALIZABLE
  default worked; set it back to `read committed` for the database, or for the
  store's own connections. Your own transactions may run at any level.
- **`add()` takes a lock per key in your transaction**, so producers of the same
  key take turns. Keep the transactions that add keyed messages short.
- **A connection dropped mid-transaction.** Through `fromDrizzle()`, the store's
  own transactions run in drizzle's `db.transaction()`, which leaves the
  checked-out node-postgres client without an `error` listener
  ([drizzle-orm#6437](https://github.com/drizzle-team/drizzle-orm/issues/6437)):
  a failover or `pg_terminate_backend()` then ends the process. This package's
  stores checked out their own client, with a listener. Register the database
  through `@nestjs/drizzle` 0.0.2 or later, which listens on checked-out
  clients, or give every client a listener yourself:
  `pool.on('connect', (client) => client.on('error', handle))`.
- **`isPgUniqueViolation()`** is gone; copy it from
  [`dialects/postgres/inbox-store.ts`](https://github.com/nest-native/messaging/blob/main/packages/messaging/dialects/postgres/inbox-store.ts)
  if you use it.

### MySQL

`MysqlOutboxStore` and `MysqlInboxStore` become `MySqlOutboxStore` (note the
capital S), from `@nestjs/outbox/mysql`, registered the same way with that entry
point's `fromDrizzle()`.

- **MySQL 8.4 LTS or 9.x, with a strict `sql_mode`.** This package ran on 8.0.1
  and later; a lax `sql_mode`, or MariaDB, fails the startup.
- **Its tables** live in the connection's database: `nest_outbox_messages`,
  `nest_outbox_dead_letters`, `nest_outbox_inbox`, `nest_outbox_migrations` and
  `nest_outbox_locks`, with a binary collation, so keys compare byte for byte.
  Ids, topics, keys and consumer names hold at most 255 characters.
- **Deadlocks can reach your transaction.** `add()` locks one of 16,384 key
  bucket rows in your transaction. When MySQL breaks a deadlock there (error
  1213), it rolls the whole transaction back, so run it again.
- **Migrations:** `npx nest-outbox migrate --url mysql://…`, or
  `npx nest-outbox sql --dialect mysql`, or
  `MySqlOutboxStore.migrationStatements()`, one statement per call.
- **`isMysqlUniqueViolation()`** is gone; copy it from
  [`dialects/mysql/inbox-store.ts`](https://github.com/nest-native/messaging/blob/main/packages/messaging/dialects/mysql/inbox-store.ts)
  if you use it.

### SQLite

Neither `@nestjs/outbox` nor `@nestjs/store-kit`, whose executors it runs on,
ships a SQLite store. The official
[production checklist](https://docs.nestjs.com/reliability/outbox#production-checklist)
argues against one:

> Never keep the outbox on the instance itself, in memory or in a file database
> such as SQLite on its disk: a container's filesystem goes away when the task
> is replaced, and every message that hadn't been published goes with it.

If your SQLite file outlives the instance (one host, a persistent volume), the
store is yours to write, against the module's public contracts
([A store of your own](#a-store-of-your-own)). The reference app did, and its
store is a starting point:

- [`sqlite-outbox.store.ts`](https://github.com/nest-native/reference-app/blob/main/src/modules/outbox/sqlite-outbox.store.ts)
  implements both contracts on the app's Drizzle database. Every method is
  synchronous, like better-sqlite3, so `add()` and `processInTransaction()`
  stay synchronous inside synchronous `@Transactional()` bodies.
- Its tables are part of the app's Drizzle schema
  ([`outbox.ts`](https://github.com/nest-native/reference-app/blob/main/src/database/schema/outbox.ts)),
  migrated by drizzle-kit like every other table
  ([`0007_official_outbox_tables.sql`](https://github.com/nest-native/reference-app/blob/main/src/database/migrations/0007_official_outbox_tables.sql)).
- It numbers messages in commit order without a lock per key, because SQLite
  has a single writer, and its own multi-statement transactions open with
  `BEGIN IMMEDIATE`, as this package's claim did.
- It passes the module's contract suites, except the three cases that need a
  second transaction open on one connection
  ([`sqlite-outbox-store.contract.spec.ts`](https://github.com/nest-native/reference-app/blob/main/test/integration/sqlite-outbox-store.contract.spec.ts));
  a cross-process spec runs two of those against a second process
  ([`sqlite-outbox-store.cross-process.spec.ts`](https://github.com/nest-native/reference-app/blob/main/test/integration/sqlite-outbox-store.cross-process.spec.ts)).

```ts
// Registered once, next to OutboxModule (the reference app's sqliteOutboxStoreProvider)
{
  provide: SqliteOutboxStore,
  inject: [getDrizzleToken(), OutboxStorage],
  useFactory: (db: SqliteDatabase, storage: OutboxStorage) => new SqliteOutboxStore(db, storage),
}

// In a synchronous @Transactional() body: the row is written before add() returns
const message = this.outbox.add(this.db, { topic, payload, headers }) as OutboxMessage<UserInvited>;
```

When the API and the relay are separate processes on one file, open the
producers' transactions that read before they write with `BEGIN IMMEDIATE`:
`@Transactional<TransactionalAdapterDrizzleOrm<AppDatabase>>({ behavior: 'immediate' })`.
A deferred transaction that has read fails at once with `SQLITE_BUSY` when it
needs the write lock the relay holds, instead of waiting out the busy timeout.
That was already true with this package.

### A store of your own

This package's `OutboxStore` (`enqueue`, `claimBatch`, `markCompleted`, `retry`,
`markFailed`, `release`) and `InboxStore` (`runOnce`) become two contracts:

- `OutboxStore<Tx>`: `add`, `claim`, `markPublished`, `reschedule`,
  `deadLetter`, `release`, `stats`, `listDeadLetters`, `getDeadLetter`,
  `requeueDeadLetters` and `purgeDeadLetters`;
- `OutboxInboxStore<Tx>`: `recordInbox`, `hasInbox` and `pruneInbox`. The store
  records; `OutboxInbox` runs the work when the record is new.

Any method may return a value or a promise. The store registers itself in its
constructor with `storage.registerSource({ messages: this, inbox: this })`
(`{ inbox: this }` alone in a service that only consumes). What changes in the
rules: the fence is a fresh random owner per claim instead of `claimedBy` plus
`claimedAt`; times come from the relay's clock (the `now` argument), never the
database's; `markPublished()` deletes the row; rows are numbered in commit
order; a dead letter moves atomically, keeping its place and every attempt's
error. The interfaces' JSDoc spells out each rule and the race it prevents, and
[the store contract](https://docs.nestjs.com/reliability/outbox#the-store-contract)
covers registration.

Prove it with the module's own suites, runner-agnostic cases that throw on
failure, with `concurrent: true` against your production database engine:

```ts
import { outboxInboxStoreContract, outboxStoreContract } from '@nestjs/outbox/testing';

for (const c of outboxStoreContract(freshStore, { concurrent: true })) test(c.name, c.run);
for (const c of outboxInboxStoreContract(freshStore, { concurrent: true })) test(c.name, c.run);
```

`freshStore` returns the store on empty tables, a way to open one of your
transactions, and `notATransaction`, a handle the store must refuse. Pin
`@nestjs/outbox` exactly, as the reference app does: under semver, a 0.x
package may change anything in any release, this contract included.

## Carry over the rows in flight

The old tables hold events that weren't published yet and the consumers' inbox
records. Move both before you drop the tables:

- **Stop the old workers first.** A worker still running can publish a
  carried-over event a second time.
- **Keep the ids.** An event the old worker claimed may already have reached
  its consumers. Under its old id, their carried-over inbox records recognise
  it when the new relay publishes it again. For that, keep the consumer names:
  the inbox records are keyed by them. An in-process handler's `runOnce()` was
  keyed by `idempotencyKey ?? id`, and the `local` transport looks up the id:
  a record kept under a carried event's idempotency key needs a copy under the
  event's id.
- **Decide about failed events.** On PostgreSQL and MySQL, no API writes a dead
  letter: re-add the failed events you still want delivered (they get a fresh
  budget), and export the rest. Completed events are history; keep the table
  if you need it.

On SQLite, where the store's tables are the app's own, the reference app does
it in SQL migrations: 0008 copies the pending and claimed events with their
ids, the failed ones as dead letters and the inbox records with their names,
and 0009 drops `outbox_events` and `inbox_events`
([`0008_carry_over_outbox.sql`](https://github.com/nest-native/reference-app/blob/main/src/database/migrations/0008_carry_over_outbox.sql),
[`0009_drop_legacy_outbox_tables.sql`](https://github.com/nest-native/reference-app/blob/main/src/database/migrations/0009_drop_legacy_outbox_tables.sql),
proven by
[`outbox-carry-over.spec.ts`](https://github.com/nest-native/reference-app/blob/main/test/integration/outbox-carry-over.spec.ts)).
The reference app's old registry handlers kept no inbox, so a carried-over
event they had already handled is handled again there. Handlers that paired
with `runOnce()` need the copy under the event's id that the script below
makes.

On PostgreSQL and MySQL the store's tables are its own, so carry the rows over
through the module's API, from a script that boots only the database and the
outbox. Run it once the store's schema exists (`npx nest-outbox migrate` in
production) and before the new consumers start, then once more after the last
old instance has stopped. It deletes only the events it carried over, so the
second run moves what the old instances added in between.

```ts
import { DrizzleModule } from '@nestjs/drizzle';
import { Outbox, OutboxInbox, OutboxModule } from '@nestjs/outbox';
import { inboxEvents, outboxEvents } from '@nest-native/messaging/postgres'; // or /mysql
import { asc, eq, inArray } from 'drizzle-orm';

@Module({
  imports: [DrizzleModule.forRoot({ db }), OutboxModule.forRoot({ relay: { enabled: false } })],
  providers: [outboxStoreProvider], // the store provider your AppModule registers
})
class CarryOverModule {}

async function main(): Promise<void> {
  const app = await NestFactory.createApplicationContext(CarryOverModule);
  const outbox = app.get(Outbox);
  const inbox = app.get(OutboxInbox);
  const unpublished = inArray(outboxEvents.status, ['pending', 'processing']);

  await db.transaction(async (tx) => {
    // The consumers' records, under the same names.
    for (const record of await tx.select().from(inboxEvents)) {
      await inbox.processInTransaction(tx, record.source, record.messageKey, () => undefined);
    }
    // The unpublished events, with their ids and due times. Their attempts start over.
    const events = await tx.select().from(outboxEvents).where(unpublished).orderBy(asc(outboxEvents.createdAt));
    // In-process handlers keyed runOnce() by idempotencyKey ?? id; the local transport checks the id.
    for (const event of events) {
      if (event.idempotencyKey) {
        const keyed = await tx.select().from(inboxEvents).where(eq(inboxEvents.messageKey, event.idempotencyKey));
        for (const record of keyed) {
          await inbox.processInTransaction(tx, record.source, event.id, () => undefined);
        }
      }
    }
    if (events.length > 0) {
      await outbox.add(
        tx,
        events.map((event) => ({
          id: event.id,
          topic: event.topic,
          payload: event.payload,
          headers: event.idempotencyKey ? { 'x-idempotency-key': event.idempotencyKey } : undefined,
          availableAt: new Date(event.availableAt),
        })),
      );
      // Only the events carried over: one an old instance added since the select waits for the next run.
      await tx.delete(outboxEvents).where(inArray(outboxEvents.id, events.map((event) => event.id)));
    }
  });
  await app.close();
}

void main();
```

For a large inbox, copy only the records younger than your longest redelivery
window, in batches. Once `outbox_events` has nothing left to publish, drop the
two tables from your schema and generate the migration that removes them; then
uninstall this package.

## Wake the relay

**In the same process**, `waker.notify()` after the commit becomes
`outbox.notify()` (or `relay.notify()`), and the `waker` option goes. Like the
waker, it is latched: a `notify()` during a poll runs another one.

**Across processes** there is nothing to swap in. The official docs, on
[running several instances](https://docs.nestjs.com/reliability/outbox#run-several-instances):

> `notify()` wakes only the relay of the instance that added the message, so a
> relay instance picks the messages up within one `pollInterval`.

So a message added by an API process waits up to `relay.pollInterval` (1 s by
default) for a relay in another process. Lower `pollInterval` if that is too
long, or bring a wake back:

- **Same machine (`WakeSocketServer`, `WakeSocketClient`).** Copy
  [`wake-socket.ts`](https://github.com/nest-native/messaging/blob/main/packages/messaging/wake-socket.ts)
  (MIT; it imports only `node:net` and `node:fs`), replacing its `WakeSignal`
  import with `{ notify(): void }`. The relay is a valid waker:

  ```ts
  // In the relay process's bootstrap()
  const wakes = new WakeSocketServer({ path: WAKE_SOCKET, waker: app.get(OutboxRelay) });
  await wakes.listen(); // await wakes.close() on shutdown

  // In the API process, after the commit, instead of outbox.notify()
  wakeClient.notify(); // const wakeClient = new WakeSocketClient({ path: WAKE_SOCKET })
  ```

- **Across machines, on PostgreSQL (`PostgresWakeListener`, `wakeChannel`).**
  Copy
  [`dialects/postgres/wake.ts`](https://github.com/nest-native/messaging/blob/main/packages/messaging/dialects/postgres/wake.ts)
  the same way, and send the notification in the producer's transaction, where
  `PostgresOutboxStore({ wakeChannel })` sent it:

  ```ts
  // In the producer's transaction: delivered on commit, dropped on rollback
  await this.outbox.add(tx, message);
  await tx.execute(sql`select pg_notify('outbox_wake', '')`);

  // In each relay's process: a dedicated, non-pooled connection
  const listener = new PostgresWakeListener({
    connect: () => new pg.Client({ connectionString, keepAlive: true }),
    channel: 'outbox_wake',
    waker: app.get(OutboxRelay),
  });
  listener.start(); // await listener.stop() on shutdown
  ```

Keep the channel name within 63 bytes (`assertValidWakeChannel()` in the same
file checks it): `LISTEN` truncates a longer name, but `pg_notify` raises, and
that fails the business transaction. What you lose is the maintenance. The
copies carry what this package's tests pinned down: the stale socket recovery,
and on PostgreSQL the reconnect loop, a `stop()` that can't hang on a
connection still opening, TCP keepalive against half-open connections, and a
throwing waker kept out of the `notification` handler. They are yours to keep
correct now.

## Kafka

The module ships no transport over `@nest-native/kafka`. Its
`ClientProxyTransport` publishes through a `@nestjs/microservices`
`ClientProxy`; it changes the wire (an envelope inside a Nest packet, unless
its `toPacket` option maps it), and it ignores the relay's abort signal. To keep
this package's wire byte for byte, the reference app carries its own transport,
a small `OutboxTransport`:

```ts
// The reference app's kafka-outbox.transport.ts
import type { KafkaProducerService } from '@nest-native/kafka';
import { type OutboxMessage, OutboxTransport } from '@nestjs/outbox';
import { encodeWireValue, wireHeaders, X_IDEMPOTENCY_KEY } from './wire-contract';

export class KafkaOutboxTransport extends OutboxTransport {
  constructor(
    private readonly producer: KafkaProducerService,
    private readonly topicPrefix = '',
  ) {
    super();
  }

  async publish(message: OutboxMessage, { signal }: { signal: AbortSignal }): Promise<void> {
    // A produce can't be called back: past publishTimeout the attempt counts as
    // failed, and if the send lands anyway, the inbox drops the retry's copy.
    signal.throwIfAborted();
    const headers = wireHeaders(message); // x-event-id, x-idempotency-key, then the rest
    await this.producer.send({
      topic: `${this.topicPrefix}${message.topic}`,
      messages: [{ key: headers[X_IDEMPOTENCY_KEY], value: encodeWireValue(message.payload), headers }],
    });
  }
}
```

The message key and `x-idempotency-key` are the business key, or the id when
the producer set none, as `idempotencyKey ?? id` was; `x-event-id` is the
message id. Copy
[`kafka-outbox.transport.ts`](https://github.com/nest-native/reference-app/blob/main/src/modules/outbox/kafka-outbox.transport.ts)
and the wire contract it shares with the consumers,
[`wire-contract.ts`](https://github.com/nest-native/reference-app/blob/main/src/modules/outbox/wire-contract.ts)
(the header names, `wireHeaders()`, `headerToString()`, `deriveDedupKey()`, now
throwing `NonRetryableMessageError`, and the JSON codec). Register the
transport with `forRootAsync()` as in
[With a broker transport](#with-a-broker-transport).

On the consuming side, `KafkaInboxConsumer` becomes the app's
[`KafkaInbox`](https://github.com/nest-native/reference-app/blob/main/src/modules/inbox/kafka-inbox.ts),
the same engine on `OutboxInbox.processInTransaction()`: a processed or
duplicate delivery returns and the offset commits, poison (no dedup key, or a
payload `validate` rejects) is republished to `dlqTopic` with an `x-error`
header, and anything else throws, so `@nest-native/kafka` redelivers it. Your
`@KafkaConsumer` shells change one option name:

```ts
// Before: KafkaInboxConsumer
await this.inbox.consume({ source: 'order.placed:billing', context, headers, payload, validate, sideEffect, dlqTopic });

// After: the app's KafkaInbox
await this.inbox.consume({ consumer: 'order.placed:billing', context, headers, payload, validate, sideEffect, dlqTopic });
```

Keep the names, so the carried-over inbox records still match. The reference
app's side effects are synchronous because it runs on SQLite; on PostgreSQL or
MySQL, widen `sideEffect` to return a promise, which
`processInTransaction()` awaits inside the transaction. See
[`user-invited.consumer.ts`](https://github.com/nest-native/reference-app/blob/main/src/modules/inbox/user-invited.consumer.ts)
for a consumer shell.

## RabbitMQ

The module ships no transport over amqplib. `ClientProxyTransport` over Nest's
RMQ client only awaits `ClientProxy.emit()`, and the wire becomes Nest's RMQ
packet. That client publishes on a confirm channel without `mandatory` (unless
the `RmqRecord` that `toPacket` returns sets it) and listens for no returned
message, so an unroutable message is lost while the relay counts it as
published. The reference app ported this package's transport and consumer
instead:

- [`rabbit-outbox.transport.ts`](https://github.com/nest-native/reference-app/blob/main/src/modules/rabbitmq/rabbit-outbox.transport.ts)
  publishes `mandatory` on a confirm channel, resolves only when the broker
  acked the message and did not return it, and reopens its channel lazily, with
  the same wire. The relay's signal bounds the wait for the channel and the
  confirm, so `confirmTimeoutMs` becomes `relay.publishTimeout`.
  `routingKeyPrefix` and `close()` were dropped: the relay drains in-flight
  publishes in `onModuleDestroy`, before the app closes its connection in
  `onApplicationShutdown`.
- [`rabbit-inbox.ts`](https://github.com/nest-native/reference-app/blob/main/src/modules/rabbitmq/rabbit-inbox.ts)
  is `RabbitInboxConsumer` on `OutboxInbox.processInTransaction()`, with
  `consumer` in place of `source`, as in [Kafka](#kafka).

```ts
// Before
MessagingModule.forRootAsync({
  drizzleInstanceToken: getDrizzleToken(),
  outboxStore: new PostgresOutboxStore(),
  inboxStore: new PostgresInboxStore(),
  imports: [RabbitModule],
  inject: [RABBITMQ],
  useTransport: (connection: RecoveringChannelModel) =>
    new RabbitOutboxTransport({ connection, exchange: 'events', confirmTimeoutMs: 10_000 }),
});

// After
OutboxModule.forRootAsync({
  imports: [RabbitModule],
  inject: [RABBITMQ],
  useFactory: (connection: RecoveringChannelModel) => ({
    transports: { rabbitmq: new RabbitOutboxTransport({ connection, exchange: 'events' }) },
    relay: { lease: '1m', publishTimeout: '10s' }, // what confirmTimeoutMs bounded
    retry: { attempts: 10, backoff: { delay: '1s', maxDelay: '1m' } },
  }),
});
```

What you lose is an engine maintained and proven against a real RabbitMQ 4
broker in this repository's CI. The port keeps every rule, and they are yours to
keep now: a backoff before each requeue (RabbitMQ doesn't count an explicit
requeue toward a quorum queue's delivery limit, so an immediate one redelivered
about 1,400 times a second), dead-letter copies published `mandatory` on a
confirm channel with `x-dead-letter-id`, the `x-error` reason capped at 1,000
characters (amqplib encodes headers into a 64 KiB buffer), an `error` listener on
the dead-letter channel, no dedup key from an integer header past 2^53, and a
`consume()` that never rejects. The reference app tests them with fake channels
and against a live broker
([`rabbit-inbox.spec.ts`](https://github.com/nest-native/reference-app/blob/main/test/integration/rabbit-inbox.spec.ts),
[`reliable-messaging.rabbitmq.spec.ts`](https://github.com/nest-native/reference-app/blob/main/test/integration/reliable-messaging.rabbitmq.spec.ts)).

## Testing

```ts
// Before
const transport = new InMemoryOutboxTransport(); // MessagingModule.forRoot({ ..., transport })
await orderService.placeOrder('o-1', 'widget');
expect((await claimer.tick()).completed).toBe(1);
expect(transport.listTopic('order.placed')).toHaveLength(1);
transport.failWith(new Error('broker down'));
```

```ts
// After
const published: OutboxMessage[] = [];
let failure: Error | undefined;
// OutboxModule.forRoot({
//   relay: { enabled: false },
//   transports: { test: { publish: (message) => { if (failure) throw failure; published.push(message); } } },
// })
await orderService.placeOrder('o-1', 'widget');
expect((await relay.runOnce()).published).toBe(1);
expect(published.filter((message) => message.topic === 'order.placed')).toHaveLength(1);
failure = new Error('broker down');
```

- **Drive the relay yourself.** Turn its loop off and call `relay.runOnce()`,
  which claims and processes one batch and resolves when the batch is done.
- **A transport is any object with `publish()`**, so a recording one is a
  closure, and `failWith()` becomes a transport that throws.
- **`InMemoryOutboxStore`** is the default store and a test double. Its
  `transaction(work)` gives a handle whose writes apply when the work resolves
  and are dropped when it throws, for rollback tests. A plain instance doesn't
  register itself, and it can't join your database's transactions.
- **Prefer a real database.** The official docs run PostgreSQL in-process with
  PGlite ([Testing](https://docs.nestjs.com/reliability/outbox#testing)); the
  reference app runs its relay specs on a SQLite file
  ([`outbox-relay.spec.ts`](https://github.com/nest-native/reference-app/blob/main/test/integration/outbox-relay.spec.ts)).
  For backoff and lease expiry, move `Date.now()` forward: the relay takes its
  time from it, for itself and for the store.

## Observability

`onTick` and `onError` had a `TickReport` and a logger to offer. The module has
events and stats:

```ts
@Injectable()
export class OutboxMetrics implements OnModuleInit {
  constructor(
    private readonly events: OutboxEvents,
    private readonly relay: OutboxRelay,
  ) {}

  onModuleInit() {
    // published, retry-scheduled, dead-lettered, lease-lost
    this.events.events$.subscribe((event) => metrics.increment(`outbox.${event.type}`));
  }

  snapshot() {
    // pending, ready, leased, deadLetters, oldestDueAt, lagMs, inFlight
    return this.relay.stats();
  }
}
```

Each event is also published on a `node:diagnostics_channel` channel,
`nestjs:outbox:published` and so on, which tracing and metrics libraries can
subscribe to without Nest code. Alert on `lagMs`, which grows while a
downstream is down, and on `deadLetters`. A health check stays in your
application, for example a terminus indicator on `stats().lagMs`.

## Samples

This package's samples (SQLite in-process, Kafka, and RabbitMQ in one and in
two services) have no official counterparts. The official
[`37-outbox` sample](https://github.com/nestjs/nest/tree/master/sample/37-outbox)
runs on PostgreSQL with Drizzle: in-process handlers, an analytics service over
TCP through `ClientProxyTransport`, a dead-letter admin API and tests. The
[reference app](https://github.com/nest-native/reference-app) runs on SQLite in
three profiles (in-process, Kafka and RabbitMQ), with every piece this guide
links to.
