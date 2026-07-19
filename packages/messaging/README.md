# @nest-native/messaging

<p align="center">Transactional outbox + idempotent inbox for NestJS — persisted with Drizzle ORM (SQLite, Postgres &amp; MySQL), delivered in-process or over Kafka.</p>

<p align="center">
  <a href="https://www.npmjs.com/package/@nest-native/messaging"><img src="https://img.shields.io/npm/v/@nest-native/messaging.svg" alt="NPM Version" /></a>
  <a href="https://www.npmjs.com/package/@nest-native/messaging"><img src="https://img.shields.io/npm/dm/@nest-native/messaging.svg" alt="NPM Downloads" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/license-MIT-green.svg" alt="Package License" /></a>
  <img src="https://img.shields.io/badge/coverage-100%25-brightgreen.svg" alt="Test Coverage" />
</p>

> [!NOTE]
> **v0.x — early but stable.** The producer, claimer, inbox, transport seam, and the Drizzle stores are implemented and tested at 100% coverage. SQLite, Postgres, and MySQL are supported, with in-process (no broker) and Kafka transports.

## The problem it solves

"Write rows **and** publish an event" is a **dual write** — two systems that can't be updated atomically. If the process crashes between the DB commit and the broker publish, the event is lost; publish-then-fail-to-commit emits a phantom event.

`@nest-native/messaging` closes that gap with the two halves of the reliable-messaging pattern:

- **Transactional outbox (producer)** — `enqueue()` writes the event into an `outbox_events` row **inside your business transaction** (via [`@nestjs-cls/transactional`](https://www.npmjs.com/package/@nestjs-cls/transactional)). A background **claimer** then relays committed rows to the broker — at-least-once, with retry/backoff.
- **Idempotent inbox (consumer)** — `runOnce()` deduplicates redeliveries via a unique `(source, message_key)` row written **in the same transaction as the side effect**, yielding **effective exactly-once** processing.

It is **not** a generic multi-broker abstraction — it is the outbox/inbox pattern, done natively for the Drizzle + Kafka + NestJS stack.

## Install

```bash
npm install @nest-native/messaging
# plus your driver + transport (peer dependencies):
npm install drizzle-orm @nestjs-cls/transactional better-sqlite3   # or pg / mysql2
npm install @nest-native/kafka                                     # only for the Kafka transport
```

## Entry points

| Import | Contents |
| --- | --- |
| `@nest-native/messaging` | core engine — `OutboxProducer`, `OutboxClaimer` + worker loop, `InboxService`, the `OutboxTransport`/`OutboxStore`/`InboxStore` seams, the wire contract, `MessagingModule` |
| `@nest-native/messaging/in-process` | the no-broker default transport — `OutboxRegistry` (topic → handler) + `InProcessOutboxTransport` |
| `@nest-native/messaging/sqlite` | better-sqlite3 (synchronous) stores + `outbox_events`/`inbox_events` table factories |
| `@nest-native/messaging/postgres` | node-postgres (async) stores + table factories |
| `@nest-native/messaging/mysql` | mysql2 (async) stores + table factories |
| `@nest-native/messaging/kafka` | `KafkaOutboxTransport` + the idempotent consumer engine, over `@nest-native/kafka` |
| `@nest-native/messaging/testing` | in-memory transport for broker-free tests |

## How it fits together

1. Add the dialect's table factories to your Drizzle schema and generate a migration.
2. Configure `@nestjs-cls/transactional` with the Drizzle adapter, then register `MessagingModule.forRoot({ drizzleInstanceToken, outboxStore, inboxStore, transport })`.
3. Inject `OutboxProducer` into your `@Transactional()` services and `enqueue()` alongside your business writes.
4. Run `OutboxClaimer` in a worker (`runWorkerLoop`) to relay events through the transport — in-process handlers by default, Kafka when a broker enters the picture.
5. Consume in-process by registering a handler per topic on the `OutboxRegistry`, or over Kafka with a thin `@KafkaConsumer` that delegates to the idempotent consumer engine. Delivery is at-least-once either way — make handlers idempotent or pair them with the inbox.

See the [00-showcase sample](https://github.com/nest-native/messaging/tree/main/sample/00-showcase) for a runnable end-to-end example on SQLite.

### Cutting the idle latency (`OutboxWaker`)

`runWorkerLoop` is self-clocking: after a tick that claims a full batch it loops
again immediately to drain the backlog, and it only waits `pollIntervalMs`
(default 2s) when a tick claims **nothing**. So that interval is the worst-case
latency for a *lone* event landing in an otherwise-idle outbox — not a per-event
tax. Under load, throughput is never gated by it.

When even that idle latency matters — a user-facing "we've got it" sitting behind
an outbox event — turning the poll interval down works but has a floor and a
DB-load cost. The better lever is an **in-process wake**: pass an `OutboxWaker`
to the loop and `notify()` it right after the enqueueing transaction commits, so
the worker relays now instead of on the next poll. Polling stays the backstop, so
a missed or absent `notify()` never stalls delivery — it only widens latency back
to one interval.

```ts
import { OutboxWaker, runWorkerLoop } from '@nest-native/messaging';

const waker = new OutboxWaker();

// worker: the idle wait is now woken early by notify()
runWorkerLoop(claimer, { pollIntervalMs: 2_000, waker, signal });

// request path: notify AFTER the transaction commits (before commit the row
// isn't visible to the claimer's own transaction yet)
await this.txHost.withTransaction(async () => {
  await this.outbox.enqueue({ topic: 'order.paid', payload });
  // ...business writes...
});
waker.notify();
```

**Across processes on the same machine** — the classic split where the HTTP app
and the worker (`npm run start:worker`) are separate processes sharing one
database — an in-memory `notify()` can't cross the boundary. The
`WakeSocketServer`/`WakeSocketClient` pair bridges it over a unix domain socket
(a `\\.\pipe\…` name on Windows), built on `node:net` alone:

```ts
// worker process — feed incoming wakes into the loop's waker
const waker = new OutboxWaker();
const wakeServer = new WakeSocketServer({ path: env.outboxWakeSocket, waker });
await wakeServer.listen(); // recovers a stale path left by a crashed worker
runWorkerLoop(claimer, { pollIntervalMs: 2_000, waker, signal });

// app process — same path, fire-and-forget after the commit
const wake = new WakeSocketClient({ path: env.outboxWakeSocket });
wake.notify(); // never throws; a failed wake only costs one poll interval
```

Producers can depend on the shared `WakeSignal` shape (`{ notify(): void }`) so
switching topology — single process (`OutboxWaker`) ↔ app + worker processes
(`WakeSocketClient`) — never touches domain code. Note that for the SQLite store
this covers every supported deployment: processes sharing a SQLite file are by
definition on one machine.

**Across machines** the wake travels through the one thing every worker already
shares — the database. On the **Postgres** dialect, `LISTEN`/`NOTIFY` carries it:

```ts
// producer side — one option on the store; pg_notify rides the enqueue
// transaction, so Postgres delivers the wake ON COMMIT and drops it on
// rollback (the signal is atomic with the event becoming visible)
new PostgresOutboxStore({ wakeChannel: 'outbox_wake' })

// worker side — a DEDICATED (non-pooled) LISTEN connection feeds the waker
import { PostgresWakeListener } from '@nest-native/messaging/postgres';

const listener = new PostgresWakeListener({
  connect: () => new pg.Client({ connectionString, keepAlive: true }), // fresh client per attempt
  channel: 'outbox_wake',
  waker, // the same OutboxWaker passed to runWorkerLoop
});
listener.start();
// on shutdown: await listener.stop();
```

The listener reconnects (default every 5s) when its connection drops;
notifications missed during the gap are not recovered — polling remains the
backstop, exactly as with the other tiers. `LISTEN`/`NOTIFY` is Postgres-only:
SQLite and MySQL deployments use the socket tier above (for SQLite that is the
whole story anyway — its processes share one machine by definition).

## Status & scope

- **Drivers:** SQLite (better-sqlite3, sync), Postgres (`pg`, async), and MySQL (`mysql2`, async) via per-dialect stores.
- **Transports:** in-process (default, `@nest-native/messaging/in-process` — no broker, at-least-once via the claimer) and Kafka (`@nest-native/kafka`).
- **Latency:** the worker drains a backlog immediately and only idles at `pollIntervalMs`; the wake tiers cut that idle wait — `OutboxWaker` in-process, the `WakeSocket` pair across processes on one machine, and Postgres `LISTEN`/`NOTIFY` across machines (see above).
- **Roadmap:** additional transports. CDC (Debezium) is an intentional non-goal — this is the app-level outbox.

Part of the [nest-native](https://github.com/nest-native) family. Not affiliated with the NestJS core team. MIT licensed.
