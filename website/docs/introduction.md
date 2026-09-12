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

## Compatibility

| Runtime | Supported line |
| --- | --- |
| Node.js | `>=22` (`>=22.12` with NestJS 12 — see the note below the table) |
| NestJS | `^11.0.0 \|\| ^12.0.0` |
| Drizzle ORM | `^0.44.0 \|\| ^0.45.0` |
| `@nestjs-cls/transactional` | `^3.0.0` — on NestJS 12, `3.3+` (with `nestjs-cls` `6.3+`): the first releases whose own peer ranges admit 12 |
| `better-sqlite3` | `^11.0.0 \|\| ^12.0.0 \|\| ^13.0.0` |
| `@nest-native/kafka` | `^0.2.0 \|\| ^0.3.0 \|\| ^0.4.0 \|\| ^0.5.0` — on NestJS 12, `0.5.1+`: the first release whose peer range admits 12 |

Both ends of the NestJS range are tested, not assumed: the default lockfile
keeps the suite on an 11.x in the middle of the range, and the `nestjs-compat`
CI matrix resolves the tree against each end in every workspace — `11.0.0`
pinned exactly (nothing this package uses was added by a later 11.x), and
`^12` — proves every workspace resolves exactly that and every peer range in
the NestJS ecosystem is satisfied, and reruns the suite, the package build, and
both samples. NestJS 11 runs on any Node.js `>=22`. NestJS 12 is ESM-only;
loading it from CommonJS (this package, and both samples) goes through Node's
`require(esm)`, which is behind a flag before Node.js 22.12.0, so the 12 end of
the range needs Node.js `>=22.12` — a current Node 22, or 24. `engines` stays
`>=22` because the 11 end does not need more; Node 22.0–22.11 satisfies it and
still cannot load NestJS 12. NestJS 12 also reorders lifecycle hooks
across providers by module-hierarchy level; this package relies on no
cross-provider hook order, so nothing changes for it.

Part of the [nest-native](https://github.com/nest-native) family. MIT licensed.
