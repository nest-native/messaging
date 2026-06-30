---
sidebar_position: 4
title: Testing
---

# Testing

The outbox and inbox are designed to be tested **without a broker**. The
`@nest-native/messaging/testing` entrypoint ships an in-memory transport so you
can assert the full atomic-outbox → claim → exactly-once-inbox flow against a
real (in-memory) database and zero network.

## `InMemoryOutboxTransport`

It implements `OutboxTransport`, records every published message, and can be made
to fail on demand to exercise the claimer's retry/fail paths.

```ts
import { InMemoryOutboxTransport } from '@nest-native/messaging/testing';
import { RetryableError } from '@nest-native/messaging';

const transport = new InMemoryOutboxTransport();

// Register it where production would register KafkaOutboxTransport:
// MessagingModule.forRoot({ ..., transport })

// After running the claimer:
expect(transport.list()).toHaveLength(1);
expect(transport.listTopic('order.placed')).toHaveLength(1);

// Force the retry path:
transport.failWith(new RetryableError('broker down'));
// ...the next claimer tick now schedules retries instead of completing...
transport.clearFailure();

transport.reset(); // clear recorded messages + any injected failure
```

It is kept out of the package root on purpose, so test scaffolding never enters
your production import surface.

## A broker-free end-to-end test

This mirrors the [`00-showcase` sample](./samples.md): place an order (atomic
outbox write), drain it with the claimer, then deliver it to the inbox twice and
assert the side effect ran exactly once.

```ts
import { InboxService, OutboxClaimer } from '@nest-native/messaging';
import { InMemoryOutboxTransport } from '@nest-native/messaging/testing';

// 1. Place an order — the order row and the outbox event commit atomically.
await orderService.placeOrder('o-1', 'widget');

// 2. The claimer relays the committed event to the in-memory transport.
const report = await claimer.tick();
expect(report.completed).toBe(1);
expect(transport.list()).toHaveLength(1);

// 3. Deliver to the inbox: dedup + apply the side effect once.
const message = transport.list()[0];
const dedupKey = message.idempotencyKey ?? message.id;
const source = 'order.placed:test';
const applyAudit = () => {
  db.insert(orderAudit).values({ key: dedupKey, item: 'widget' }).run();
};

const first = await inbox.runOnce(dedupKey, source, applyAudit);
expect(first).toBe('processed');

// 4. A redelivery is deduplicated — no second audit row.
const second = await inbox.runOnce(dedupKey, source, applyAudit);
expect(second).toBe('duplicate');
```

## The sync-vs-async side-effect rule

`InboxService.runOnce` runs your side effect **inside the dedup transaction**.
What that handler may do depends on the store's driver:

- **SQLite (`SqliteInboxStore`)** — the handler MUST be **synchronous and
  DB-only**. better-sqlite3 cannot suspend a synchronous transaction, so an
  `await` inside the handler would let the transaction commit out from under you.
  Do your DB writes with `.run()` and return; do not `await` anything.
- **Postgres (`PostgresInboxStore`)** — an **async, DB-only** handler is fine,
  because the Postgres transaction is genuinely asynchronous.

In both cases the handler must stay **DB-only**: it shares the dedup transaction,
so any non-transactional side effect (HTTP calls, sending another message,
writing a file) would not roll back if the transaction aborts, breaking the
exactly-once guarantee. Do that work outside `runOnce` — for instance by
enqueuing another outbox event from within the handler's transaction.

The same rule applies to the `sideEffect` you pass to `KafkaInboxConsumer.consume`,
which is wrapped into `runOnce` for you.
