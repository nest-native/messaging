import type { Channel } from 'amqplib';

/**
 * The broker topology this sample runs on. The application owns its topology —
 * the transport and the inbox never declare anything — so it is declared once at
 * startup, idempotently, the way a migration would create a table.
 */
export const TOPOLOGY = {
  /** Every outbox event is published here, with its topic as the routing key. */
  events: 'shop.events',
  /** The routing key billing binds, i.e. the outbox topic it consumes. */
  orderPlaced: 'order.placed',
  /** Billing's work queue. */
  billingQueue: 'billing.order-placed',
  /** Where dead letters go: poison republished by the inbox, and messages over the queue's delivery limit. */
  deadLetters: 'shop.dead-letters',
  /** Billing's dead-letter queue, for a human (or a replay job) to look at. */
  billingDeadLetterQueue: 'billing.order-placed.dead',
} as const;

export async function declareTopology(channel: Channel): Promise<void> {
  await channel.assertExchange(TOPOLOGY.events, 'topic', { durable: true });
  await channel.assertExchange(TOPOLOGY.deadLetters, 'topic', { durable: true });

  // A quorum queue is replicated and durable. Its delivery limit (20 by default
  // in RabbitMQ 4) counts a message that went back to the queue with a closing
  // channel — one that crashes its consumer every time — and with a dead-letter
  // exchange, a message past the limit is moved there instead of being dropped.
  // The limit does not count the inbox's own requeues of a transient failure;
  // the inbox's backoff paces those. The routing key is set too: without it the
  // broker dead-letters with the message's original key (`order.placed`), which
  // the dead-letter queue's binding does not match, and the message is dropped
  // after all. Shared queues must be durable: RabbitMQ 4 refuses a transient one
  // by closing the whole connection.
  await channel.assertQueue(TOPOLOGY.billingQueue, {
    durable: true,
    arguments: {
      'x-queue-type': 'quorum',
      'x-dead-letter-exchange': TOPOLOGY.deadLetters,
      'x-dead-letter-routing-key': TOPOLOGY.billingQueue,
    },
  });
  await channel.bindQueue(TOPOLOGY.billingQueue, TOPOLOGY.events, TOPOLOGY.orderPlaced);

  await channel.assertQueue(TOPOLOGY.billingDeadLetterQueue, {
    durable: true,
    arguments: { 'x-queue-type': 'quorum' },
  });
  await channel.bindQueue(
    TOPOLOGY.billingDeadLetterQueue,
    TOPOLOGY.deadLetters,
    TOPOLOGY.billingQueue,
  );
}
