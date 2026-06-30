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
  enqueue(input: EnqueueInput): ReturnType<TStore['enqueue']>;
}

interface EnqueueInput {
  topic: string;
  payload: Record<string, unknown>;
  idempotencyKey?: string;
  availableAt?: Date;
  maxAttempts?: number;
}
```

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

interface TickReport {
  claimed: number;
  completed: number;
  retried: number;
  failed: number;
}

const DEFAULT_CLAIMER_CONFIG: ResolvedClaimerConfig; // exported
```

`tick()` claims a batch (the store opens its own transaction), publishes each row
through the transport, and records the outcome. A publish that throws is mapped
to a retry/fail decision (see [Transport seam](#transport-seam)). Run it from a
background worker — never inside a business transaction.

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
}
```

Loops `claimer.tick()`: when a tick claims a batch it loops immediately to drain
the backlog; when it claims nothing it waits `pollIntervalMs`. A throwing tick is
reported via `onError` and the loop continues.

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
  enqueue(db: unknown, input: EnqueueInput): OutboxEventRow | Promise<OutboxEventRow>;
  claimBatch(db: unknown, cfg: ResolvedClaimerConfig): Promise<OutboxEventRow[]>;
  markCompleted(db: unknown, id: string): Promise<void>;
  retry(db: unknown, id: string, delayMs: number, lastError?: string): Promise<void>;
  markFailed(db: unknown, id: string, reason: string): Promise<void>;
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

node-postgres (asynchronous) dialect. Same shape as `/sqlite`:

| Export | Kind | Notes |
| --- | --- | --- |
| `outboxEvents` / `inboxEvents` | Drizzle tables | `pgTable` factories with the matching indexes |
| `PostgresOutboxStore` | class | implements `OutboxStore`; `enqueue` returns a `Promise` |
| `PostgresInboxStore` | class | implements `InboxStore`; an async DB-only `runOnce` handler is allowed |
| `isPgUniqueViolation` | function | `(error: unknown) => boolean` |

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
