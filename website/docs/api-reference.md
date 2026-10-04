---
sidebar_position: 3
title: API Reference
---

# API Reference

Signatures below are taken from the source. Types are TypeScript; `db` arguments
on the store seams are intentionally `unknown` — the engine never inspects the
Drizzle instance, it hands it to the dialect store.

## `@nest-native/messaging` (core)

### `OutboxProducer<TStore>`

Injectable. Writes events into the outbox inside the caller's transaction.

```ts
class OutboxProducer<TStore extends OutboxStore = OutboxStore> {
  enqueue<TPayload extends object>(
    input: EnqueueInput<TPayload>,
  ): ReturnType<TStore['enqueue']>;
}

interface EnqueueInput<TPayload extends object = Record<string, unknown>> {
  topic: string;
  payload: TPayload;
  idempotencyKey?: string;
  availableAt?: Date;
  maxAttempts?: number;
}
```

The payload input is **structural**: a value typed as a plain interface (which
has no index signature, so it is not assignable to `Record<string, unknown>`)
is accepted as-is — no cast. The stored row shape stays
`Record<string, unknown>`; the dialect stores widen internally.

`enqueue` returns the store's native shape: the **sqlite** store returns the
`OutboxEventRow` synchronously (call it without `await` inside a synchronous
`@Transactional` body); the **postgres** store returns a `Promise`. Parameterize
the producer (`OutboxProducer<SqliteOutboxStore>`) to get the exact return type.
Requires `@nestjs-cls/transactional` configured with `enableTransactionProxy: true`.

### `OutboxClaimer`

Injectable. Drains committed outbox rows to the transport.

```ts
class OutboxClaimer {
  tick(overrides?: ClaimerConfig): Promise<TickReport>;
}

interface ClaimerConfig {        // every field optional
  workerInstanceId?: string;     // claim owner; default `${hostname}-${pid}`
  stuckTimeoutMs?: number;       // a claim older than this is taken again; default 60000
  batchSize?: number;            // rows per claim; default 32
  baseBackoffMs?: number;        // retry backoff base; default 1000
  maxBackoffMs?: number;         // retry backoff cap; default 60000
}

interface TickReport {
  claimed: number;
  completed: number;
  retried: number;
  failed: number;
  lost: number;                  // let go: the claim expired, was taken over, or came back unstamped
}

const DEFAULT_CLAIMER_CONFIG: ResolvedClaimerConfig; // exported
```

`tick()` claims a batch (the store opens its own transaction), publishes each row
through the transport, and records the outcome. A publish that throws is mapped
to a retry/fail decision (see [Transport seam](#transport-seam)). Run it from a
background worker — never inside a business transaction.

An override set to `undefined` keeps its default, so `{ workerInstanceId:
process.env.WORKER_ID }` is safe with the variable unset. An invalid value — a
blank `workerInstanceId`, a `batchSize` below 1, a `stuckTimeoutMs` that is not
positive, a negative backoff — throws. So does a value of the wrong type,
including a numeric string from the environment: pass
`batchSize: env.BATCH_SIZE ? Number(env.BATCH_SIZE) : undefined`.

**Running several workers.** Any number of workers can drain one outbox:

- **Claims are exclusive.** The Postgres and MySQL stores lock the rows they claim
  with `FOR UPDATE SKIP LOCKED`, so concurrent claims split the backlog instead
  of sharing rows; SQLite runs one write transaction at a time.
- **A stalled claim is taken over.** A row still `processing` after
  `stuckTimeoutMs` is claimed again by whichever worker gets to it first.
- **Outcomes are fenced on the claim.** Completing, retrying or failing a row
  applies only while it is still `processing` under that exact claim
  (`claimedBy` + `claimedAt`). A worker that stalled past the timeout cannot
  overwrite the new owner's outcome, even under the same `workerInstanceId`.
  On Postgres each transition runs in its own READ COMMITTED transaction, like
  the claim, so a stricter server default cannot fail a transition whose claim
  still holds the row.
- **A batch held too long is cut short.** Once a batch has been held longer
  than `stuckTimeoutMs`, its remaining events may already belong to another
  worker. The worker skips them instead of publishing them a second time. The
  first event of a batch is always published, so a slow claim still makes
  progress.

A transition that lost its claim and an event skipped from an expired batch
both count as `lost` and log a warning. A steady non-zero `lost` means batches
take longer than `stuckTimeoutMs`: raise it or lower `batchSize`. A row the
store returned without a claim stamp also counts as `lost`, and logs an error:
that is a store bug, which no timeout setting fixes.

Give every worker on one table the same `stuckTimeoutMs`, longer than the
slowest single publish, and keep their clocks in sync. Each worker stamps, ages
and reclaims claims with its own clock and its own value, so the smallest
`stuckTimeoutMs` in the fleet is the one in force.

**Upgrading from 0.7.x.** These guarantees hold once every worker draining a
table runs this version. A 0.7.x worker claims without `SKIP LOCKED`, so it can
take over a row a newer worker has just claimed, and both publish the event;
and its transitions match on the row's id alone, so it can complete, retry or
fail a row another worker holds. Stop the 0.7.x workers before starting the new
ones, or expect duplicates and `lost` warnings while both run.

If recording a delivery fails — the database errors after a successful
publish — `tick()` throws. The event stays claimed and is published again once
its claim goes stale. Retrying it would spend an attempt on an event that was
already delivered.

`resolveClaimerConfig(overrides)`, also exported, applies the same defaults and
validation `tick()` does, so a worker can check its config at startup.

### `runWorkerLoop`

```ts
function runWorkerLoop(
  claimer: OutboxClaimer,
  options?: WorkerLoopOptions,
): Promise<void>;

interface WorkerLoopOptions {
  pollIntervalMs?: number;          // idle delay, default 2000
  claimer?: ClaimerConfig;          // overrides applied to every tick
  signal?: AbortSignal;             // abort to stop the loop
  onTick?: (report: TickReport) => void;
  onError?: (error: unknown) => void;
  waker?: OutboxWaker;              // optional event-driven wake (see below)
}
```

Loops `claimer.tick()`: when a tick claims a batch it loops immediately to drain
the backlog; when it claims nothing it waits `pollIntervalMs`. A throwing tick is
reported via `onError` and the loop continues. An invalid `claimer` config
rejects the returned promise at once, before the first tick, so keep that
promise and handle the rejection: discarded with `void`, it ends the process as
an unhandled rejection, without reaching `onError`. The promise also rejects,
and the loop stops, if `onError` itself throws.

### Wake tiers (cutting the idle latency)

The idle `pollIntervalMs` wait is the worst-case latency for a lone event
landing in an idle outbox. Three opt-in tiers cut it — polling always remains
the delivery backstop, so a missed wake only costs one poll interval:

```ts
// same process: notify() after the enqueueing transaction commits
const waker = new OutboxWaker();
runWorkerLoop(claimer, { waker, signal })
  .catch((error) => console.error('claimer worker stopped', error));
waker.notify();

// separate processes, same machine: a unix-domain-socket bridge
const server = new WakeSocketServer({ path, waker });  // worker side
await server.listen();
new WakeSocketClient({ path }).notify();               // producer side

// separate machines (Postgres only): LISTEN/NOTIFY through the database
new PostgresOutboxStore({ wakeChannel: 'outbox_wake' }) // pg_notify rides the tx
const listener = new PostgresWakeListener({             // worker side
  connect: () => new pg.Client({ connectionString, keepAlive: true }),   // dedicated, non-pooled
  channel: 'outbox_wake',
  waker,
});
listener.start(); // reconnects on drops; await listener.stop() on shutdown
```

Producers can depend on the shared `WakeSignal` shape (`{ notify(): void }`), so
switching deployment topology never touches domain code. With `wakeChannel`,
Postgres delivers the notify **on commit** and drops it on rollback — the wake
is atomic with the event becoming visible.

### `InboxService`

Injectable (only registered when an `inboxStore` is supplied). The idempotent
inbox primitive.

```ts
class InboxService {
  runOnce(
    messageKey: string,
    source: string,
    handler: InboxSideEffect,
  ): Promise<RunOnceOutcome>;
}

type InboxSideEffect = () => void | Promise<void>;
type RunOnceOutcome = 'processed' | 'duplicate';
```

`runOnce` opens a transaction, inserts the `(source, messageKey)` dedup row, and
runs `handler` in the **same** transaction. A duplicate delivery violates the
unique index and returns `'duplicate'` (handler skipped); a handler throw rolls
back the dedup row so the redelivery reprocesses. On the sqlite store `handler`
must be synchronous and DB-only.

### `MessagingModule`

```ts
class MessagingModule {
  static forRoot(options: MessagingModuleOptions): DynamicModule;
  static forRootAsync(options: MessagingModuleAsyncOptions): DynamicModule;
}

interface MessagingModuleOptions {
  drizzleInstanceToken: symbol | string;
  outboxStore: OutboxStore;
  inboxStore?: InboxStore;            // omit to use only the outbox half
  transport: OutboxTransport;
  imports?: ModuleMetadata['imports'];
  isGlobal?: boolean;                 // default true
}

interface MessagingModuleAsyncOptions {
  drizzleInstanceToken: symbol | string;
  outboxStore: OutboxStore;
  inboxStore?: InboxStore;
  imports?: ModuleMetadata['imports'];
  inject?: (InjectionToken | OptionalFactoryDependency)[];
  useTransport: (...args: any[]) => OutboxTransport | Promise<OutboxTransport>;
  isGlobal?: boolean;
}
```

`drizzleInstanceToken` is the **base** (non-transactional) Drizzle instance — the
same one the CLS Drizzle adapter is configured with; the claimer opens its own
transaction on it. The module exports `OutboxProducer`, `OutboxClaimer`,
`OUTBOX_TRANSPORT`, and (when `inboxStore` is set) `InboxService`. Use
`forRootAsync` when the transport must inject runtime providers (e.g. a Kafka
producer).

### Transport seam

The dependency-free seam the claimer publishes through.

```ts
interface OutboxTransport {
  publish(message: OutboxMessage): Promise<void>;
}

interface OutboxMessage {
  id: string;                          // outbox row id; fallback message key
  topic: string;
  payload: Record<string, unknown>;
  idempotencyKey?: string;             // preferred message key when present
}

const OUTBOX_TRANSPORT: symbol;        // DI token for the active transport

class RetryableError extends Error {
  constructor(message: string, readonly delayMs?: number);
}
class PermanentError extends Error {
  constructor(message: string);
}
```

The claimer maps a rejected `publish` as: `RetryableError` → schedule a retry
(honouring `delayMs`); `PermanentError` → mark failed immediately; any other
error → retry with backoff until `maxAttempts`, then fail.

### Store seams

Implement these to support another dialect; the shipped stores implement them for
you.

```ts
interface OutboxStore {
  enqueue(db: unknown, input: EnqueueInput<object>): OutboxEventRow | Promise<OutboxEventRow>;
  claimBatch(db: unknown, cfg: ResolvedClaimerConfig): Promise<OutboxEventRow[]>;
  markCompleted(db: unknown, claim: OutboxClaim): Promise<boolean>;
  retry(db: unknown, claim: OutboxClaim, delayMs: number, lastError?: string): Promise<boolean>;
  markFailed(db: unknown, claim: OutboxClaim, reason: string): Promise<boolean>;
  release?(db: unknown, claim: OutboxClaim): Promise<boolean>; // optional (0.8.2+)
}

interface OutboxClaim {
  id: string;
  claimedBy: string;             // the workerInstanceId the claim wrote
  claimedAt: string;             // the timestamp the claim wrote
}

interface InboxStore {
  runOnce(
    db: unknown,
    messageKey: string,
    source: string,
    handler: InboxSideEffect,
  ): RunOnceOutcome | Promise<RunOnceOutcome>;
}
```

An outbox store must uphold what the claimer relies on:

- **`claimBatch` is exclusive.** It never returns one row to two concurrent
  callers, and it stamps every row it returns with `claimedBy` and `claimedAt`.
  It returns each row as its claiming UPDATE left it. The claimer refuses a row
  without a string stamp, counting it as `lost` and logging an error, but it
  cannot tell a stale stamp from the one the claim wrote: a reclaimed row read
  before that UPDATE carries the previous claim's stamp, and is published under
  a claim no transition matches, again after every stuck timeout.
- **Transitions are fenced.** Each one writes the row only while it is still
  `processing` with exactly the claim's `claimedBy` and `claimedAt`. It resolves
  `true` when it wrote the row and `false` when the claim no longer held it; a
  database error rejects.
- **`release` is optional.** It hands a held row back `pending` and unclaimed,
  with its attempts and due time unchanged, under the same fence. When
  recording an outcome fails (the database went away mid-batch), the claimer
  calls it for the batch's unpublished rows, best effort, so the next claim
  takes them at once; without it they wait for `stuckTimeoutMs`. The row whose
  outcome failed to record is never handed back: it was published, and handing
  it back would publish it again at once.
- **Stamp the claim with the time it ran.** Take `claimedAt` once the claim has
  its connection, not before a pooled checkout: a stamp taken before a slow
  checkout makes the claim look older than it is, and other workers reclaim its
  rows that much sooner.

Also exported: `OutboxEventRow`, `ResolvedClaimerConfig` / `ClaimerConfig`,
`OutboxStatus` / `OUTBOX_STATUSES`, `InboxStatus` / `INBOX_STATUSES`, and the DI
tokens `OUTBOX_STORE`, `INBOX_STORE`, `MESSAGING_DRIZZLE`, `MESSAGING_OPTIONS`.

### Wire contract

A single source of truth shared by the Kafka transport and the inbox consumer so
the two halves never drift.

```ts
const X_EVENT_ID = 'x-event-id';
const X_IDEMPOTENCY_KEY = 'x-idempotency-key';
const X_ERROR = 'x-error';

function headerToString(value: WireHeaderValue): string | undefined;
function deriveDedupKey(
  headers: Record<string, WireHeaderValue> | undefined,
  messageKey: string | undefined,
): string | undefined;
function encodeWireValue(payload: unknown): string;
function decodeWireValue(value: string | Buffer | null): unknown;
```

The dedup-key order is the contract: `x-event-id` → `x-idempotency-key` → broker
message key.

## `@nest-native/messaging/in-process`

The no-broker default transport: the claimer "publishes" a claimed event by
dispatching it to the handler registered for its topic, in the same process.
Depends only on `@nestjs/common` (already a required peer).

### `OutboxRegistry`

Injectable. The topic → handler registry behind the transport. Consumers
register themselves on module init; one handler per topic (a second `register`
for the same topic throws at startup).

```ts
class OutboxRegistry {
  register(topic: string, handler: OutboxHandler): void;
  get(topic: string): OutboxHandler | undefined;
}

type OutboxHandler = (
  payload: Record<string, unknown>,
  message: OutboxMessage,
) => Promise<OutboxHandlerResult> | OutboxHandlerResult;

type OutboxHandlerResult = 'completed' | { retryAfterMs: number };
```

The handler receives the stored payload plus the full `OutboxMessage`, so it can
derive the same dedup key the Kafka consumer would (`idempotencyKey ?? id`) and
pair with `InboxService.runOnce` for exactly-once side effects.

### `InProcessOutboxTransport`

```ts
class InProcessOutboxTransport implements OutboxTransport {
  constructor(registry: OutboxRegistry);
  publish(message: OutboxMessage): Promise<void>;
}
```

`publish` looks up the topic's handler and maps the outcome for the claimer:

- **no handler registered** → `PermanentError` — the event is unroutable and can
  never succeed, so the row fails immediately;
- **`'completed'`** → resolves; the claimer marks the row completed;
- **`{ retryAfterMs }`** → `RetryableError` carrying that delay;
- **a handler throw** → propagates untouched into the claimer's error mapping: a
  thrown `PermanentError` fails the row now (e.g. a malformed payload), a thrown
  `RetryableError` keeps its delay, anything else retries with backoff until
  `maxAttempts`.

Delivery is **at-least-once** via the claimer (it redelivers after a retry, a
crash between handler success and `markCompleted`, or a claim that outlived
`stuckTimeoutMs`), so handlers must be
idempotent — or wrap their side effect in the inbox. The
[`00-showcase` sample](./samples.md) runs this profile end to end.

## `@nest-native/messaging/sqlite`

better-sqlite3 (synchronous) dialect.

| Export | Kind | Notes |
| --- | --- | --- |
| `outboxEvents` | Drizzle table | `outbox_events` factory — partial unique index on `idempotency_key`, plus `(status, available_at)` index for the claimer |
| `inboxEvents` | Drizzle table | `inbox_events` factory — unique index on `(source, message_key)` |
| `SqliteOutboxStore` | class | implements `OutboxStore`; `enqueue` returns synchronously |
| `SqliteInboxStore` | class | implements `InboxStore`; `runOnce` handler must be synchronous + DB-only |
| `isSqliteUniqueViolation` | function | `(error: unknown) => boolean` — the dedup primitive |

## `@nest-native/messaging/postgres`

node-postgres (asynchronous) dialect. Same shape as `/sqlite`. On a
node-postgres `Pool`, the outbox claim and each transition run on a client the
store checks out itself, so a connection the database drops mid-transaction
rejects the call instead of crashing the process. Give the pool an `error`
listener, as node-postgres requires of every pool: without one, a connection an
idle client loses still crashes the process, and the store logs a warning once
when the pool has none. The inbox and your own
`@Transactional` bodies still go through drizzle's `transaction()`, which
leaves its checked-out client without one; a per-client listener
(`pool.on('connect', (client) => client.on('error', handle))`) covers those too.

| Export | Kind | Notes |
| --- | --- | --- |
| `outboxEvents` / `inboxEvents` | Drizzle tables | `pgTable` factories with the matching indexes |
| `PostgresOutboxStore` | class | implements `OutboxStore`; `enqueue` returns a `Promise` |
| `PostgresInboxStore` | class | implements `InboxStore`; an async DB-only `runOnce` handler is allowed |
| `isPgUniqueViolation` | function | `(error: unknown) => boolean` |

## `@nest-native/messaging/mysql`

mysql2 (asynchronous) dialect. Same shape as `/postgres`. Needs MySQL 8.0.1 or
later, for the claim's `FOR UPDATE SKIP LOCKED`. The claim runs at READ
COMMITTED, and with binary logging on InnoDB refuses writes from it under
`binlog_format=STATEMENT`, so `binlog_format` must be ROW (the MySQL 8 default)
or MIXED. A connection the database drops mid-claim or mid-transition rejects
the call: mysql2's pooled connections listen for their own errors, so unlike
node-postgres nothing extra is needed, and the pool replaces the connection.

| Export | Kind | Notes |
| --- | --- | --- |
| `outboxEvents` / `inboxEvents` | Drizzle tables | `mysqlTable` factories with the matching indexes |
| `MysqlOutboxStore` | class | implements `OutboxStore`; `enqueue` returns a `Promise` (no `RETURNING` in MySQL — it inserts, then reads the row back by id) |
| `MysqlInboxStore` | class | implements `InboxStore`; an async DB-only `runOnce` handler is allowed |
| `isMysqlUniqueViolation` | function | `(error: unknown) => boolean` — errno `1062` / `ER_DUP_ENTRY`, unwrapping `DrizzleQueryError.cause` |

## `@nest-native/messaging/kafka`

Requires the optional `@nest-native/kafka` peer.

### `KafkaOutboxTransport`

```ts
class KafkaOutboxTransport implements OutboxTransport {
  constructor(producer: KafkaProducerService, topicPrefix?: string);
  publish(message: OutboxMessage): Promise<void>;
}
```

Publishes a claimed event to Kafka. The message `key` is `idempotencyKey ?? id`;
the `x-event-id` and `x-idempotency-key` headers carry the dedup inputs; the
value is JSON (`encodeWireValue`). A failing `send` propagates so the claimer
retries.

### `KafkaInboxConsumer`

Injectable. The reusable idempotent-consumer engine — inject it into a thin
`@KafkaConsumer` and call `consume` from the `@KafkaHandler`.

```ts
class KafkaInboxConsumer {
  consume<T>(options: ConsumeOptions<T>): Promise<ConsumeResult>;
}

interface ConsumeOptions<T> {
  source: string;                                  // scopes dedup keys
  context: KafkaContext;                           // message key + DLQ republish
  headers: Record<string, WireHeaderValue> | undefined;
  payload: unknown;
  validate: (payload: unknown) => payload is T;    // failure -> dead-letter
  sideEffect: (payload: T, dedupKey: string) => void | Promise<void>;
  dlqTopic: string;
}

interface ConsumeResult {
  outcome: 'processed' | 'duplicate' | 'dead-lettered';
  dedupKey?: string;
}
```

It runs all async broker work outside the dedup transaction and only
`InboxService.runOnce` inside it: happy path / duplicate returns (offset commits);
a `PermanentError` (bad key or invalid payload) is republished to `dlqTopic` then
returns; any other error **throws** so the offset is not committed and the broker
redelivers.

Also exported from `/kafka`: `deriveDedupKey` (throws `PermanentError` when a
message has no usable key), `actionForOutcome`, `actionForError`, and the
`ConsumerAction` type.

## `@nest-native/messaging/rabbitmq`

Requires the optional `amqplib` peer (`^2.0.0`); the entry point imports only
its types. See [RabbitMQ](rabbitmq.md) for wiring and semantics.

### `RabbitOutboxTransport`

```ts
class RabbitOutboxTransport implements OutboxTransport {
  constructor(options: RabbitOutboxTransportOptions);
  publish(message: OutboxMessage): Promise<void>;
  close(): Promise<void>;                  // closes its channel, not the connection
}

interface RabbitOutboxTransportOptions {
  connection: RabbitConfirmChannelSource;  // amqplib ChannelModel or RecoveringChannelModel
  exchange: string;                        // routing key = routingKeyPrefix + topic
  routingKeyPrefix?: string;
  confirmTimeoutMs?: number;               // default 10_000
}
```

Publishes persistent JSON on a confirm channel, `mandatory`, with `messageId`,
`x-event-id` and `x-idempotency-key` set. Resolves only when the broker acked
the message and did not return it; a return, a nack, a closed channel, or a
timeout is thrown as a plain `Error`, so the claimer retries it until
`maxAttempts`.

### `RabbitInboxConsumer`

Injectable. Call `consume` from your `channel.consume` callback (manual acks).

```ts
class RabbitInboxConsumer {
  consume<T>(options: RabbitConsumeOptions<T>): Promise<RabbitConsumeResult>; // never rejects
}

interface RabbitConsumeOptions<T> {
  source: string;                                  // scopes dedup keys (e.g. the queue)
  channel: Channel;                                // ack/nack go here
  message: ConsumeMessage;
  validate: (payload: unknown) => payload is T;    // false or a throw -> dead-letter
  sideEffect: (payload: T, dedupKey: string) => void | Promise<void>;
  deadLetter?: { channel: ConfirmChannel; exchange: string; routingKey: string };
  retry?: RabbitRetryOptions;
}

interface RabbitRetryOptions {
  delayMs?: number;      // wait before the first requeue; doubles per failed attempt. Default 1000
  maxDelayMs?: number;   // cap on that wait. Default 30000
  maxAttempts?: number;  // dead-letter after this many failures (per process). Default: never
}

interface RabbitConsumeResult {
  outcome: 'processed' | 'duplicate' | 'dead-lettered' | 'requeued';
  dedupKey?: string;  // on every outcome once derived; absent only when the message has no key
}

const X_DEAD_LETTER_ID = 'x-dead-letter-id'; // matches a returned dead-letter copy to its publish
```

Processed and duplicate deliveries are acked. A `PermanentError` (no dedup key,
a body that is not JSON, a payload `validate` rejects or throws on) is
republished to `deadLetter` — `mandatory`, with an `x-error` header of at most
1 000 characters — and the original acked once the broker confirms the copy; if
the copy fails or comes back unroutable, the original is requeued instead,
after the same backoff as a transient failure. With no `deadLetter` it is
rejected without requeue. An integer header is a dedup key only while it is
exact: amqplib rounds a 64-bit integer beyond 2^53, so such a value is never a
key. Any other error is `nack`ed with requeue after a
backoff, or dead-lettered once `retry.maxAttempts` is reached. A channel that
closed before the delivery could be settled is logged, not thrown: the broker
redelivers the delivery.

`deriveDedupKey`, `actionForOutcome`, `actionForError` and `ConsumerAction` are
the same helpers `/kafka` exports.

## `@nest-native/messaging/testing`

### `InMemoryOutboxTransport`

```ts
class InMemoryOutboxTransport implements OutboxTransport {
  publish(message: OutboxMessage): Promise<void>;
  list(): readonly OutboxMessage[];
  listTopic(topic: string): readonly OutboxMessage[];
  failWith(error: Error): void;        // make publish reject until cleared
  clearFailure(): void;
  reset(): void;                       // clear messages + injected failure
}
```

A broker-free transport for tests: it records every published message and can be
made to fail on demand to exercise the claimer's retry/fail paths. See
[Testing](./testing.md).
