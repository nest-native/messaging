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
      // amqplib's built-in recovery reconnects after a broker restart.
      useFactory: () => connect(process.env.RABBITMQ_URL!, { recovery: true }),
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

An unroutable event is retried rather than failed at once because the usual
cause is deploy order: the producer ran before the consumer declared its queue.
A routing mistake that is never fixed still ends in a failed row, not a retry
loop.

The transport opens one confirm channel on first use and reopens it whenever
the broker closes it. amqplib's recovering connection comes back on its own
after a broker restart, but its channels do not; this is what bridges the two.
`close()` closes the transport's channel, never your connection.

## Consuming: `RabbitInboxConsumer`

Call `consume` for every delivery, with manual acks. It runs the side effect
inside the inbox's dedup transaction and settles the delivery itself:

| Delivery | Result |
| --- | --- |
| New message, side effect succeeded | `ack` — `processed` |
| Already processed (same dedup key) | `ack` — `duplicate`, side effect skipped |
| No dedup key, not JSON, or rejected by `validate` | dead-lettered (below) |
| Side effect or database failed | `nack` with requeue — `requeued` |

The dedup key follows the wire contract: `x-event-id`, then
`x-idempotency-key`, then the AMQP `messageId`.

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
  copy. If the copy fails, the original is requeued rather than lost.
- **Without it** — the delivery is rejected without requeue. That reaches a
  dead-letter queue only if the queue was declared with an
  `x-dead-letter-exchange`, and the reason survives only in your logs.

### Bounding redelivery

A requeued message comes straight back, so a failure that persists would loop.
Bound it on the queue: RabbitMQ 4 quorum queues stop redelivering after a
delivery limit (20 by default) and dead-letter the message instead.

## How it is verified

The adapter's behaviour against a real broker is proven on every pull request:
CI's `integration` job runs the gated specs against a RabbitMQ 4 service
container and fails if any of them skipped. They cover an unroutable publish,
a missing exchange, a redelivered duplicate, both dead-letter paths, a
requeue, and the broker closing the connection.

To run the same specs locally, `npm run infra:up && npm run test:full` starts
a RabbitMQ 4 broker alongside Postgres and MySQL.
