---
sidebar_position: 5
title: Samples
---

# Samples

## `00-showcase`

[`sample/00-showcase`](https://github.com/nest-native/messaging/tree/main/sample/00-showcase)
is a runnable, end-to-end demonstration of the whole pattern on **SQLite** and
the **in-process transport** — the no-broker default profile — with no migration
step (it creates the tables inline so it runs as a single script):

- `schema.ts` — the library's `outboxEvents` / `inboxEvents` factories combined
  with the business `orders` table and an `order_audit` table (the consumer's
  observable side effect).
- `app.module.ts` — a global Drizzle module, `ClsModule.forRoot` with the Drizzle
  transactional adapter (`enableTransactionProxy: true`), and
  `MessagingModule.forRoot` with `SqliteOutboxStore`, `SqliteInboxStore`, and an
  `InProcessOutboxTransport` over a shared `OutboxRegistry`.
- `order.service.ts` — `placeOrder` inserts the order row and `enqueue`s the
  `order.placed` event in the **same** `@Transactional()` method. The payload is
  a plain interface — `enqueue` accepts it without casts.
- `order-placed.handler.ts` — the consumer: it registers itself for
  `order.placed` on module init and pairs with `InboxService.runOnce` (keyed on
  `idempotencyKey ?? id`) so the audit row is written exactly once.
- `scripts/smoke.ts` — drives the flow and asserts each guarantee.

## What it proves

The smoke script asserts the properties that make the pattern correct:

1. **Atomic outbox** — after `placeOrder`, both the `orders` row and exactly one
   `outbox_events` row exist. They committed in the same transaction, so there is
   no event without the work and no work without the event.
2. **Claim and dispatch** — one `OutboxClaimer.tick()` delivers the committed
   event to the registered handler and marks the row completed
   (`report.completed === 1`). In production with a broker, the Kafka transport
   publishes instead — same seam, different transport.
3. **Exactly-once inbox** — the handler's first delivery writes one
   `order_audit` row; replaying the same logical event (delivery is
   at-least-once) is deduplicated by the inbox and writes **no** second row.
4. **Unroutable events fail fast** — an event on a topic with no registered
   handler maps to `PermanentError`, so the claimer fails the row immediately
   instead of retrying forever.

On success it prints:

```
Showcase smoke passed: atomic outbox → in-process dispatch → exactly-once inbox.
```

## Running it

From the repository root:

```bash
npm install
npm run showcase
```

The showcase runs the in-process default profile — no Docker, no Kafka. Rebind
`KafkaOutboxTransport` and a thin `@KafkaConsumer` (see the
[Quick Start](./quick-start.md)) to take the same flow to production.

## `01-kafka`

[`sample/01-kafka`](https://github.com/nest-native/messaging/tree/main/sample/01-kafka)
takes the showcase one step further: it drives the whole pair over the **real
Kafka transport** — `KafkaOutboxTransport` on the producer side and an actual
`@KafkaConsumer` delegating to `KafkaInboxConsumer` on the consumer side — using
[`@nest-native/kafka`](https://www.npmjs.com/package/@nest-native/kafka)'s
**in-memory broker** (`KafkaTestModule`), so it still runs with no cluster.

- `app.module.ts` — wires `KafkaTestModule.forRoot()` and
  `MessagingModule.forRootAsync({ ..., useTransport: (producer) => new KafkaOutboxTransport(producer) })`.
- `order.consumer.ts` — a thin `@KafkaConsumer('order.placed')` that delegates to
  the library's `KafkaInboxConsumer.consume(...)`, supplying the payload validator
  and the exactly-once side effect.
- `scripts/smoke.ts` — places an order, runs `OutboxClaimer.tick()` (which
  publishes through Kafka to the consumer), asserts one audit row, then **re-emits
  the same message** to prove the inbox deduplicates the redelivery.

Run it from the repository root with `npm run sample:focused`. This is the closest
you can get to the production path without a broker; point `KafkaTestModule` at a
real cluster (or use `KafkaModule`) and the same code runs unchanged.

## `02-rabbitmq`

[`sample/02-rabbitmq`](https://github.com/nest-native/messaging/tree/main/sample/02-rabbitmq)
runs the pair over a **real RabbitMQ broker**: `RabbitOutboxTransport` on the
producer side and `RabbitInboxConsumer` on billing's queue. It is the single
service to read first — every path the adapter promises is asserted against
the broker, not simulated.

- `rabbitmq.ts` — the application owns its `amqplib` connection
  (`connect(url, { recovery: true })`) and declares its topology at startup;
  the transport and the inbox only open channels on it.
- `topology.ts` — a topic exchange for events, a **quorum** work queue with a
  dead-letter exchange *and* a dead-letter routing key (without it, a message
  over the delivery limit is dead-lettered with its original key, misses the
  dead-letter queue's binding, and is dropped), and the dead-letter queue.
- `billing.consumer.ts` — subscribes with manual acks and a prefetch, hands
  every delivery to `RabbitInboxConsumer`, and dead-letters poison on a confirm
  channel. When a channel closes under it — a dropped connection, a channel the
  broker closed, a cancelled consumer — it subscribes again on new ones.
- `scripts/smoke.ts` — places an order and asserts:
  1. **Confirmed publish** — the claimer's row completes only after the broker
     acked the message and did not return it; billing issues one invoice.
  2. **Duplicate** — the same event published again (what a redelivery after a
     lost ack looks like) is acked as a duplicate and bills nothing.
  3. **Poison** — an invalid payload lands in the dead-letter queue with its
     reason in an `x-error` header, instead of being requeued forever.
  4. **Unroutable** — `order.refunded`, which no queue binds yet, comes back to
     the transport; the attempt fails and the event stays in the outbox for a
     retry rather than being acked into the void.
  5. **Dropped connection** — the broker closes the application's connection
     (through the management API, as a restart would). amqplib reconnects, the
     transport opens a new confirm channel, billing subscribes again on its
     own, and the next order is invoiced once.

## `03-rabbitmq-services`

[`sample/03-rabbitmq-services`](https://github.com/nest-native/messaging/tree/main/sample/03-rabbitmq-services)
is the pattern doing the job it exists for: **two services**, orders and
shipping, each a separate process with its own database file, choreographed
over one broker.

- `orders` takes an order and publishes `order.placed` through its outbox.
- `shipping` consumes it through its inbox and, **in the same transaction**,
  books a shipment and enqueues `shipment.scheduled` in its own outbox — the
  dedup row, the shipment and the outgoing event commit together, so consuming
  once and publishing once is one step.
- `orders` consumes `shipment.scheduled` and marks the order scheduled.
- Each service owns its queue (`shared/topology.ts`) and runs its own outbox
  relay (`runWorkerLoop`), stopped in `beforeApplicationShutdown` so it never
  publishes on a connection that is closing.
- `shared/inbox-subscription.ts` keeps each consumer subscribed: whenever its
  channel closes, it subscribes again on a new one, a second later.

The smoke script forks both services and asserts: three orders cross both
services; while shipping is **down**, orders keeps accepting and publishing
orders and RabbitMQ keeps them in shipping's durable queue; a new shipping
process on the same database works through that backlog; the broker then
**drops both services' connections**, and without a restart both reconnect,
subscribe again and carry the next orders through; an event delivered
**again** books no second shipment and publishes no second
`shipment.scheduled`; and both outboxes drain, one event per order.

Why one process per service: `@nestjs-cls/transactional` keeps its transaction
host in process-global state keyed by connection name, and the outbox and inbox
use the default connection. Two Nest applications started in one process —
in a test, say — share one transaction host, so one of them writes through the
other's database. Give each application its own process.

## Running the RabbitMQ samples

Both need a broker, and its management API for the dropped-connection step.
From the repository root:

```bash
npm run infra:up
RABBITMQ_URL=amqp://messaging:messaging@127.0.0.1:56720 \
RABBITMQ_MANAGEMENT_URL=http://messaging:messaging@127.0.0.1:15670 \
  npm run sample:focused
```

Without `RABBITMQ_URL` they skip locally with a notice, and without
`RABBITMQ_MANAGEMENT_URL` they skip only the dropped-connection step. In CI,
the sample jobs run a RabbitMQ service container, and a missing URL fails the
job instead.
