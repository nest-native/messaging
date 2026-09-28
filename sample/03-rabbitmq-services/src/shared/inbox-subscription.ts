import type { Logger } from '@nestjs/common';
import type {
  RabbitConsumeResult,
  RabbitInboxConsumer,
} from '@nest-native/messaging/rabbitmq';
import type { Channel, RecoveringChannelModel } from 'amqplib';

export interface InboxSubscriptionOptions<T> {
  queue: string;
  validate: (payload: unknown) => payload is T;
  /** Runs inside the inbox's dedup transaction: synchronous and database-only on SQLite. */
  sideEffect: (payload: T, eventId: string) => void;
  onSettled: (result: RabbitConsumeResult) => void;
  logger: Logger;
}

/** A queue the service keeps consuming until it stops. */
export interface InboxSubscription {
  /**
   * Stops subscribing again. Call it before the connection closes (in
   * `beforeApplicationShutdown`), or the channel closing with it looks like a
   * failure to recover from.
   */
  stop(): void;
}

/** The pause before subscribing again, so a queue that is gone is not a hot loop. */
const RESUBSCRIBE_DELAY_MS = 1_000;

/**
 * Consumes a queue through the idempotent inbox for as long as the service runs.
 *
 * The channel can go away under the consumer: the connection drops (amqplib's
 * recovering connection comes back, its channels do not), the broker closes
 * the channel (an ack timeout, an access error), or it cancels the consumer
 * (the queue was deleted). Every one of those ends with the channel closing,
 * so this subscribes again, on a new channel, whenever it closes —
 * `createChannel()` waits while the connection is recovering. Poison is
 * rejected without requeue, so the queue's dead-letter exchange takes it.
 */
export async function consumeWithInbox<T>(
  rabbit: RecoveringChannelModel,
  inbox: RabbitInboxConsumer,
  options: InboxSubscriptionOptions<T>,
): Promise<InboxSubscription> {
  let stopped = false;
  let retry: NodeJS.Timeout | undefined;

  const consume = (channel: Channel) => {
    return channel.consume(options.queue, message => {
      if (!message) {
        // The broker cancelled the consumer; closing the channel starts over.
        channel.close().catch(() => undefined);
        return;
      }
      // `consume` never rejects — it settles the delivery itself — but the
      // report of it can fail.
      inbox
        .consume<T>({
          source: options.queue,
          channel,
          message,
          validate: options.validate,
          sideEffect: options.sideEffect,
        })
        .then(options.onSettled)
        .catch((error: unknown) =>
          options.logger.error(`${options.queue}: could not report a delivery: ${String(error)}`),
        );
    });
  };

  const subscribe = async (): Promise<void> => {
    const channel = await rabbit.createChannel();
    // The broker's reason for closing a channel arrives as 'error', and an
    // EventEmitter with no 'error' listener throws it, crashing the service.
    channel.on('error', (error: Error) =>
      options.logger.warn(`${options.queue}: channel closed by the broker: ${error.message}`),
    );
    channel.once('close', resubscribe);
    await channel.prefetch(10);
    await consume(channel);
  };

  const resubscribe = (): void => {
    if (stopped || retry) {
      return;
    }
    retry = setTimeout(() => {
      retry = undefined;
      if (stopped) {
        return;
      }
      subscribe().catch((error: unknown) => {
        options.logger.error(`${options.queue}: could not subscribe: ${String(error)}`);
        resubscribe();
      });
    }, RESUBSCRIBE_DELAY_MS);
  };

  const stop = (): void => {
    stopped = true;
    clearTimeout(retry);
  };

  try {
    await subscribe();
  } catch (error) {
    stop(); // a queue that cannot be consumed at startup fails the boot instead
    throw error;
  }
  return { stop };
}
