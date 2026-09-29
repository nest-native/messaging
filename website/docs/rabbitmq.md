---
id: rabbitmq
title: RabbitMQ
---

# RabbitMQ

`@nest-native/messaging/rabbitmq` delivers the outbox over RabbitMQ and runs the
idempotent inbox on RabbitMQ deliveries. It sits behind the same
`OutboxTransport` seam as the Kafka transport, so the producer, the claimer and
the inbox do not change — only the wire does.

```bash
npm install amqplib   # ^2.0.0, an optional peer
```

The adapter builds on your own `amqplib` connection and imports only its types,
so it adds no runtime dependency. It does not declare exchanges or queues
either: topology belongs to the application (or to the broker's definitions).

## Publishing: `RabbitOutboxTransport`

```ts
import { Module } from '@nestjs/common';
import { MessagingModule } from '@nest-native/messaging';
import { PostgresInboxStore, PostgresOutboxStore } from '@nest-native/messaging/postgres';
import { RabbitOutboxTransport } from '@nest-native/messaging/rabbitmq';
import { connect, type RecoveringChannelModel } from 'amqplib';

export const RABBITMQ = Symbol('RABBITMQ');

@Module({
  providers: [
    {
      provide: RABBITMQ,
      // amqplib's built-in recovery reconnects after a broker restart. Listen
      // for 'error': an EventEmitter without a listener throws it, and a lost
      // socket would crash the process before recovery could run.
      useFactory: async () => {
        const connection = await connect(process.env.RABBITMQ_URL!, { recovery: true });
        connection.on('error', (error) => console.warn(`RabbitMQ: ${error.message}`));
        return connection;
      },
    },
  ],
  exports: [RABBITMQ],
})
export class RabbitModule {}

@Module({
  imports: [
    MessagingModule.forRootAsync({
      drizzleInstanceToken: DRIZZLE,
      outboxStore: new PostgresOutboxStore(),
      inboxStore: new PostgresInboxStore(),
      imports: [RabbitModule],
      inject: [RABBITMQ],
      useTransport: (connection: RecoveringChannelModel) =>
        new RabbitOutboxTransport({ connection, exchange: 'events' }),
    }),
  ],
})
export class AppModule {}
```

Each outbox event is published to `exchange` with its topic as the routing key
(prefixed by `routingKeyPrefix`, if set), so a `topic` exchange lets consumers
bind by pattern. The message is persistent JSON, and it carries the same wire
contract as the Kafka transport: `messageId` and `x-event-id` hold the outbox
row id, and `x-idempotency-key` holds the business key (or the id).

### When a publish counts as done

The transport publishes on a **confirm channel** and resolves only when the
broker has **acked** the message and **did not return it**. Every message is
published `mandatory`: without that flag RabbitMQ acks a message no queue is
bound for and drops it, and the outbox row would be marked processed for an
event nobody will ever receive. With it, the broker returns the message before
the ack, and the publish fails.

Every failure is a plain `Error`, so the claimer retries it with backoff until
the row's `maxAttempts` and then marks it failed — the same budget the Kafka
transport gets:

| What happened | Result |
| --- | --- |
| Broker acked, message routed | published |
| Broker returned it (no queue bound for the routing key) | failed attempt, retried |
| Broker nacked it | failed attempt, retried |
| Exchange does not exist (the broker closes the channel) | failed attempt, retried on a fresh channel |
| No channel, or no confirm, within `confirmTimeoutMs` (default 10 s) | failed attempt, retried |

When the broker closes the channel, the failure carries its reason — a missing
exchange, an access error, an oversized message — so the outbox row's last
error says what happened. A failed attempt is not proof the event was not
delivered: a publish that timed out waiting for its confirm may still have
reached the queue, and the retry sends it again. Delivery is at-least-once;
the inbox deduplicates.

An unroutable event is retried rather than failed at once because the usual
cause is deploy order: the producer ran before the consumer declared its queue.
A routing mistake that is never fixed still ends in a failed row, not a retry
loop.

The transport opens one confirm channel on first use and reopens it whenever
the broker closes it. amqplib's recovering connection comes back on its own
after a broker restart, but its channels do not; this is what bridges the two.
`close()` closes the transport's channel, never your connection. It first lets
outstanding confirms arrive (up to `confirmTimeoutMs`) — amqplib ignores acks
that arrive after a channel starts closing, so closing mid-publish would fail
publishes the broker already took — and it never waits for a channel that is
still opening, which on a recovering connection can take the whole outage.

## Consuming: `RabbitInboxConsumer`

Call `consume` for every delivery, with manual acks. It runs the side effect
inside the inbox's dedup transaction and settles the delivery itself:

| Delivery | Result |
| --- | --- |
| New message, side effect succeeded | `ack` — `processed` |
| Already processed (same dedup key) | `ack` — `duplicate`, side effect skipped |
| No dedup key, not JSON, rejected by `validate`, or `validate` threw | dead-lettered (below) |
| Side effect or database failed | `nack` with requeue after a backoff — `requeued`; dead-lettered after `retry.maxAttempts` |

The dedup key follows the wire contract: `x-event-id`, then
`x-idempotency-key`, then the AMQP `messageId`. A header value amqplib decodes
as a number — JVM and Python producers send typed headers — is read as its
string form. The result carries the dedup key on every outcome it could be
derived for, so a dead letter or a requeue can be traced to its event.

`consume` never rejects. If the channel closes before the delivery can be
settled — a broker restart, a lost connection, a consumer timeout — the broker
has already put the delivery back; the consumer logs that and returns the
outcome, and the redelivery is deduplicated if the work was done.

```ts
import { Inject, Injectable, type OnApplicationBootstrap } from '@nestjs/common';
import { RabbitInboxConsumer } from '@nest-native/messaging/rabbitmq';
import type { RecoveringChannelModel } from 'amqplib';

type OrderPlaced = { orderId: string };
const isOrderPlaced = (p: unknown): p is OrderPlaced =>
  typeof p === 'object' && p !== null && typeof (p as OrderPlaced).orderId === 'string';

@Injectable()
export class OrderAuditConsumer implements OnApplicationBootstrap {
  constructor(
    @Inject(RABBITMQ) private readonly rabbit: RecoveringChannelModel,
    private readonly inbox: RabbitInboxConsumer,
    private readonly audit: AuditRepository,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    await this.subscribe();
    // Channels do not survive a reconnect; subscribe again on each new
    // connection. (The first 'connect' fired before bootstrap.)
    this.rabbit.on('connect', () => void this.subscribe());
  }

  private async subscribe(): Promise<void> {
    const channel = await this.rabbit.createChannel();
    // A channel the broker closes emits 'error' first; without a listener the
    // process would crash. The next reconnect subscribes again.
    channel.on('error', (error) => console.warn(`orders.audit channel: ${error.message}`));
    await channel.prefetch(10);
    await channel.consume('orders.audit', (message) => {
      if (!message) return;
      void this.inbox.consume({
        source: 'orders.audit',
        channel,
        message,
        validate: isOrderPlaced,
        sideEffect: (order, eventId) => this.audit.record(order.orderId, eventId),
      });
    });
  }
}
```

Register `RabbitInboxConsumer` as a provider next to your consumer. On the
SQLite inbox store the side effect must be synchronous and database-only; on
Postgres and MySQL it may be async.

### Dead letters

- **With `deadLetter: { channel, exchange, routingKey }`** — pass a confirm
  channel. The poison message is republished there with its reason in an
  `x-error` header, and the original is acked only once the broker confirms the
  copy. The copy is published `mandatory` (and carries an `x-dead-letter-id`
  to match a return to it): if no queue is bound behind the dead-letter
  exchange — a typo, a missing binding, a queue declared later — the broker
  returns it instead of acking it into the void. If the copy fails for any
  reason, the original is requeued, after the same backoff as a transient
  failure, rather than lost.
- **Without it** — the delivery is rejected without requeue. That reaches a
  dead-letter queue only if the queue was declared with an
  `x-dead-letter-exchange`, and the reason survives only in your logs.

### Bounding redelivery

RabbitMQ does not bound a requeue loop for you. A requeued message goes back
to the head of the queue, and an explicit requeue (`nack` with requeue) is
**not** counted toward a quorum queue's delivery limit on RabbitMQ 4 — only a
delivery returned by a channel that closed with it unacked is. Measured on
RabbitMQ 4.3: a consumer that requeues at once sees the same message about
1 400 times a second, forever.

So the consumer waits before every requeue: 1 s, doubling with each failed
attempt of the same message, capped at 30 s. While it waits the delivery stays
unacked and holds one prefetch slot, so a failure that persists slows its
consumer down instead of spinning. Tune it per call, and give up if you want a
bound:

```ts
await this.inbox.consume({
  source: 'orders.audit',
  channel,
  message,
  validate: isOrderPlaced,
  sideEffect: (order, eventId) => this.audit.record(order.orderId, eventId),
  // Retry at 2 s, 4 s, … up to 1 min; after 10 failures, dead-letter the
  // message with the last error as its reason.
  retry: { delayMs: 2_000, maxDelayMs: 60_000, maxAttempts: 10 },
});
```

Attempts are counted per message in the consumer's process, so a restart
starts them over. Without `maxAttempts` the consumer never gives up: a long
outage delays messages rather than dead-lettering them. For backoff that
survives restarts, route failures through a retry queue with a TTL whose
dead-letter exchange points back at the work queue.

A delivery returned by a closed channel — a consumer that crashed with it — is
counted, and a quorum queue with a dead-letter exchange dead-letters it past
its delivery limit (20 by default). Set the queue's
`x-dead-letter-routing-key` too, or the message keeps its original routing key
and may miss the dead-letter queue's binding.

## How it is verified

The adapter's behaviour against a real broker is proven on every pull request:
CI's `integration` job runs the gated specs against a RabbitMQ 4 service
container and fails if any of them skipped. They cover an unroutable publish,
a missing exchange (with the broker's reason), a redelivered duplicate, both
dead-letter paths and an unroutable dead-letter target, a requeue and the pace
of a failure that persists, a throwing `validate`, a numeric `x-event-id`, a
channel that closed before the ack, the broker closing the connection, and the
two RabbitMQ 4 delivery-limit facts above.

To run the same specs locally, `npm run infra:up && npm run test:full` starts
a RabbitMQ 4 broker alongside Postgres and MySQL.
