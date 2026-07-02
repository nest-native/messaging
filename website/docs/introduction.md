---
sidebar_position: 1
title: Introduction
---

# @nest-native/messaging

Transactional **outbox** + idempotent **inbox** for NestJS — persisted with
Drizzle ORM (SQLite, Postgres, and MySQL), delivered in-process or over Kafka.

:::note v0.x — early but stable
The producer, claimer, inbox, transport seam, and the Drizzle stores are
implemented and tested at 100% coverage. SQLite, Postgres, and MySQL are
supported, with in-process (no broker) and Kafka transports. This is a community
project in the `nest-native` family and is **not** affiliated with the NestJS
core team.
:::

## The dual-write problem

"Write rows **and** publish an event" is a **dual write** — two systems that
cannot be updated atomically. If the process crashes between the database commit
and the broker publish, the event is lost. If it publishes first and then fails
to commit, you emit a phantom event for work that never happened.

`@nest-native/messaging` closes that gap with the two halves of the
reliable-messaging pattern.

### Transactional outbox (producer)

`enqueue()` writes the event into an `outbox_events` row **inside your business
transaction** (via [`@nestjs-cls/transactional`](https://www.npmjs.com/package/@nestjs-cls/transactional)).
The row commits atomically with your business writes — no event without the work,
no work without the event. A background **claimer** then relays committed rows to
the broker: at-least-once, with retry and backoff.

### Idempotent inbox (consumer)

`runOnce()` deduplicates redeliveries via a unique `(source, message_key)` row
written **in the same transaction as the side effect**. A redelivery hits the
unique index and is skipped; a handler that throws rolls back the dedup row too,
so the next delivery reprocesses cleanly. The result is **effective
exactly-once** processing on top of an at-least-once broker.

It is **not** a generic multi-broker abstraction — it is the outbox/inbox
pattern, done natively for the Drizzle + Kafka + NestJS stack.

## Entry points

| Import | Contents |
| --- | --- |
| `@nest-native/messaging` | core engine — `OutboxProducer`, `OutboxClaimer` + `runWorkerLoop`, `InboxService`, the `OutboxTransport` / `OutboxStore` / `InboxStore` seams, the wire contract, `MessagingModule` |
| `@nest-native/messaging/in-process` | the no-broker default transport — `OutboxRegistry` (topic → handler) + `InProcessOutboxTransport` |
| `@nest-native/messaging/sqlite` | better-sqlite3 (synchronous) stores + `outbox_events` / `inbox_events` table factories |
| `@nest-native/messaging/postgres` | node-postgres (asynchronous) stores + table factories |
| `@nest-native/messaging/mysql` | mysql2 (asynchronous) stores + table factories |
| `@nest-native/messaging/kafka` | `KafkaOutboxTransport` + the idempotent `KafkaInboxConsumer`, over `@nest-native/kafka` |
| `@nest-native/messaging/testing` | in-memory transport for broker-free tests |

## How it fits together

1. Add the dialect's table factories to your Drizzle schema and generate a
   migration with drizzle-kit.
2. Configure `@nestjs-cls/transactional` with the Drizzle adapter, then register
   `MessagingModule.forRoot({ drizzleInstanceToken, outboxStore, inboxStore, transport })`.
3. Inject `OutboxProducer` into your `@Transactional()` services and `enqueue()`
   alongside your business writes.
4. Run `OutboxClaimer` in a worker (`runWorkerLoop`) to relay events through
   the transport.
5. Consume in-process by registering a handler per topic on the
   `OutboxRegistry`, or over Kafka with a thin `@KafkaConsumer` that delegates
   to `KafkaInboxConsumer`. Delivery is at-least-once either way — make handlers
   idempotent or pair them with the inbox.

Continue to the [Quick Start](./quick-start.md) for a runnable end-to-end setup,
or the [API Reference](./api-reference.md) for the full surface.

## Status and scope

- **Drivers:** SQLite (better-sqlite3, synchronous), Postgres (`pg`,
  asynchronous), and MySQL (`mysql2`, asynchronous) via per-dialect stores. You
  may provide your own store.
- **Transports:** in-process (default, `@nest-native/messaging/in-process` — no
  broker, at-least-once via the claimer) and Kafka (`@nest-native/kafka`), plus
  an in-memory one for tests.
- **Roadmap:** additional transports.
- **Out of scope:** CDC (Debezium) log-tailing is an intentional non-goal — this
  is the application-level outbox, written through your ORM transaction. Generic
  multi-broker routing is also out of scope.

Part of the [nest-native](https://github.com/nest-native) family. MIT licensed.
