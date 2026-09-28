import type { Channel } from 'amqplib';

/** Both services publish to this exchange, with the event's topic as the routing key. */
export const EXCHANGE = 'commerce.events';

/** Messages a service's inbox gives up on land here, in the service's own dead-letter queue. */
export const DEAD_LETTERS = 'commerce.dead-letters';

/** A queue a service consumes, and the event it binds. */
export interface Subscription {
  queue: string;
  event: string;
}

/**
 * Declares the queue a service consumes. Each service owns its queue — it
 * declares it at startup, the way it migrates its own tables — so the other
 * service only ever knows the exchange and the event names. What the inbox
 * rejects as poison goes to the service's dead-letter queue, and so does a
 * message that went back to the queue with a closing channel more often than
 * the quorum queue's delivery limit (20 by default in RabbitMQ 4) — one that
 * crashes its consumer every time. The limit does not count the inbox's own
 * requeues of a transient failure; its backoff paces those.
 */
export async function declareSubscription(
  channel: Channel,
  { queue, event }: Subscription,
): Promise<void> {
  const deadLetterQueue = `${queue}.dead`;
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  await channel.assertExchange(DEAD_LETTERS, 'topic', { durable: true });
  await channel.assertQueue(queue, {
    durable: true,
    arguments: {
      'x-queue-type': 'quorum',
      'x-dead-letter-exchange': DEAD_LETTERS,
      'x-dead-letter-routing-key': queue,
    },
  });
  await channel.bindQueue(queue, EXCHANGE, event);
  await channel.assertQueue(deadLetterQueue, {
    durable: true,
    arguments: { 'x-queue-type': 'quorum' },
  });
  await channel.bindQueue(deadLetterQueue, DEAD_LETTERS, queue);
}
