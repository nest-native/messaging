# @nest-native/messaging

<p align="center">Transactional outbox + idempotent inbox for NestJS — persisted with Drizzle ORM (SQLite &amp; Postgres), delivered over Kafka.</p>

<p align="center">
  <a href="https://www.npmjs.com/package/@nest-native/messaging"><img src="https://img.shields.io/npm/v/@nest-native/messaging.svg" alt="NPM Version" /></a>
  <a href="https://www.npmjs.com/package/@nest-native/messaging"><img src="https://img.shields.io/npm/dm/@nest-native/messaging.svg" alt="NPM Downloads" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/license-MIT-green.svg" alt="Package License" /></a>
  <img src="https://img.shields.io/badge/coverage-100%25-brightgreen.svg" alt="Test Coverage" />
</p>

> [!NOTE]
> **v0.1.x — early but stable.** The producer, claimer, inbox, transport seam, and the Drizzle stores are implemented and tested at 100% coverage. SQLite and Postgres are supported; MySQL and additional transports are on the roadmap.

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
npm install drizzle-orm @nestjs-cls/transactional better-sqlite3   # or pg
npm install @nest-native/kafka                                     # for the Kafka transport
```

## Entry points

| Import | Contents |
| --- | --- |
| `@nest-native/messaging` | core engine — `OutboxProducer`, `OutboxClaimer` + worker loop, `InboxService`, the `OutboxTransport`/`OutboxStore`/`InboxStore` seams, the wire contract, `MessagingModule` |
| `@nest-native/messaging/sqlite` | better-sqlite3 (synchronous) stores + `outbox_events`/`inbox_events` table factories |
| `@nest-native/messaging/postgres` | node-postgres (async) stores + table factories |
| `@nest-native/messaging/kafka` | `KafkaOutboxTransport` + the idempotent consumer engine, over `@nest-native/kafka` |
| `@nest-native/messaging/testing` | in-memory transport for broker-free tests |

## How it fits together

1. Add the dialect's table factories to your Drizzle schema and generate a migration.
2. Configure `@nestjs-cls/transactional` with the Drizzle adapter, then register `MessagingModule.forRoot({ drizzleInstanceToken, outboxStore, inboxStore, transport })`.
3. Inject `OutboxProducer` into your `@Transactional()` services and `enqueue()` alongside your business writes.
4. Run `OutboxClaimer` in a worker (`runWorkerLoop`) to relay events to Kafka.
5. Consume with a thin `@KafkaConsumer` that delegates to the idempotent consumer engine.

See the [00-showcase sample](https://github.com/nest-native/messaging/tree/main/sample/00-showcase) for a runnable end-to-end example on SQLite.

## Status & scope

- **Drivers:** SQLite (better-sqlite3, sync) and Postgres (`pg`, async) via per-dialect stores.
- **Transports:** Kafka (`@nest-native/kafka`) and in-process (default).
- **Roadmap:** MySQL store, additional transports. CDC (Debezium) is an intentional non-goal — this is the app-level outbox.

Part of the [nest-native](https://github.com/nest-native) family. Not affiliated with the NestJS core team. MIT licensed.
