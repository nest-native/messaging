---
sidebar_position: 5
title: Samples
---

# Samples

## `00-showcase`

[`sample/00-showcase`](https://github.com/nest-native/messaging/tree/main/sample/00-showcase)
is a runnable, end-to-end demonstration of the whole pattern on **SQLite**, with
no broker and no migration step (it creates the tables inline so it runs as a
single script). It is wired exactly like the [Quick Start](./quick-start.md):

- `schema.ts` — the library's `outboxEvents` / `inboxEvents` factories combined
  with the business `orders` table and an `order_audit` table (the consumer's
  observable side effect).
- `app.module.ts` — a global Drizzle module, `ClsModule.forRoot` with the Drizzle
  transactional adapter (`enableTransactionProxy: true`), and
  `MessagingModule.forRoot` with `SqliteOutboxStore`, `SqliteInboxStore`, and an
  `InMemoryOutboxTransport`.
- `order.service.ts` — `placeOrder` inserts the order row and `enqueue`s the
  `order.placed` event in the **same** `@Transactional()` method.
- `scripts/smoke.ts` — drives the flow and asserts each guarantee.

## What it proves

The smoke script asserts the three properties that make the pattern correct:

1. **Atomic outbox** — after `placeOrder`, both the `orders` row and exactly one
   `outbox_events` row exist. They committed in the same transaction, so there is
   no event without the work and no work without the event.
2. **Claim and relay** — one `OutboxClaimer.tick()` publishes the committed event
   to the transport and marks the row completed (`report.completed === 1`). In
   production the Kafka transport publishes to a broker instead; the in-memory
   transport records it.
3. **Exactly-once inbox** — delivering the message to `InboxService.runOnce` the
   first time returns `'processed'` and writes one `order_audit` row; a second,
   identical delivery (brokers are at-least-once) returns `'duplicate'` and writes
   **no** second row. The side effect ran exactly once.

On success it prints:

```
Showcase smoke passed: atomic outbox → claim → exactly-once inbox.
```

## Running it

From the repository root:

```bash
npm install
npm run showcase
```

The showcase deliberately uses the `InMemoryOutboxTransport` rather than a real
broker, so it runs anywhere with no Docker or Kafka. Swap in
`KafkaOutboxTransport` and a thin `@KafkaConsumer` (see the
[Quick Start](./quick-start.md)) to take the same flow to production.
