import { Inject, Injectable, Logger } from '@nestjs/common';
import type { Channel, ConfirmChannel, ConsumeMessage } from 'amqplib';
import { actionForError, deriveDedupKey } from '../../idempotent-consumer';
import { InboxService } from '../../inbox.service';
import type { InboxSideEffect } from '../../interfaces';
import { PermanentError } from '../../transport';
import { decodeWireValue, X_ERROR, type WireHeaderValue } from '../../wire-contract';

/**
 * Where poison messages go when the consumer republishes them itself, with the
 * reason in an `x-error` header. Without it the consumer rejects them instead
 * (`nack` without requeue), which only reaches a dead-letter queue if the queue
 * was declared with an `x-dead-letter-exchange` — and loses the reason.
 */
export interface RabbitDeadLetterTarget {
  /** A confirm channel, so the poison message is durably stored before the original is acked. */
  channel: ConfirmChannel;
  exchange: string;
  routingKey: string;
}

/**
 * What a single delivery resolved to — returned for logging/metrics. The
 * consumer has already acked or rejected the delivery when it resolves.
 */
export interface RabbitConsumeResult {
  outcome: 'processed' | 'duplicate' | 'dead-lettered' | 'requeued';
  dedupKey?: string;
}

/** Per-delivery options for {@link RabbitInboxConsumer.consume}. */
export interface RabbitConsumeOptions<T> {
  /** Scopes dedup keys to this consumer (typically the queue name). */
  source: string;
  /** The channel the message was delivered on — the ack or nack goes there. */
  channel: Channel;
  /** The delivery, as `channel.consume` hands it over (`noAck: false`). */
  message: ConsumeMessage;
  /** Type guard; a payload that fails is a permanent error → dead-letter. */
  validate: (payload: unknown) => payload is T;
  /**
   * The exactly-once side effect, run inside the dedup transaction. Receives the
   * validated payload and the derived dedup key. On a sqlite inbox store it MUST
   * be synchronous + DB-only; on postgres or mysql it may be async.
   */
  sideEffect: (payload: T, dedupKey: string) => void | Promise<void>;
  /** Republish poison messages here with their reason; omit to reject them instead. */
  deadLetter?: RabbitDeadLetterTarget;
}

/**
 * The idempotent-consumer engine for RabbitMQ. Call {@link consume} from your
 * `channel.consume` callback (manual acks) for every delivery; it runs only the
 * `InboxService.runOnce` primitive inside the dedup transaction and settles the
 * delivery itself:
 *
 *   - processed or duplicate → `ack`
 *   - no dedup key, not JSON, or rejected by `validate` (a PermanentError) →
 *     republished to `deadLetter` with an `x-error` header and acked, or
 *     rejected without requeue when no `deadLetter` is given
 *   - anything else (the side effect threw, the database is down) → `nack`
 *     with requeue, so the broker redelivers it
 *
 * The dedup key follows the shared wire contract: `x-event-id`, then
 * `x-idempotency-key`, then the AMQP `messageId` — what the RabbitMQ outbox
 * transport sets on every message.
 *
 * A requeued message comes straight back, so a failure that persists would
 * loop. Bound it on the queue: RabbitMQ 4 quorum queues stop redelivering after
 * a delivery limit (20 by default) and dead-letter the message instead.
 */
@Injectable()
export class RabbitInboxConsumer {
  private readonly logger = new Logger(RabbitInboxConsumer.name);

  constructor(@Inject(InboxService) private readonly inbox: InboxService) {}

  async consume<T>(options: RabbitConsumeOptions<T>): Promise<RabbitConsumeResult> {
    const { channel, message } = options;
    try {
      const dedupKey = deriveDedupKey(
        message.properties.headers as Record<string, WireHeaderValue> | undefined,
        message.properties.messageId as string | undefined,
      );
      const payload = decodePayload(message);
      if (!options.validate(payload)) {
        throw new PermanentError('payload failed validation');
      }
      const sideEffect: InboxSideEffect = () => options.sideEffect(payload, dedupKey);
      const outcome = await this.inbox.runOnce(dedupKey, options.source, sideEffect);
      if (outcome === 'duplicate') {
        this.logger.debug(`duplicate skipped: ${dedupKey}`);
      }
      channel.ack(message);
      return { outcome, dedupKey };
    } catch (error) {
      if (actionForError(error) === 'dead-letter') {
        return this.deadLetter(options, error as PermanentError);
      }
      this.logger.warn(`requeued for redelivery: ${describe(error)}`);
      channel.nack(message, false, true);
      return { outcome: 'requeued' };
    }
  }

  private async deadLetter<T>(
    options: RabbitConsumeOptions<T>,
    error: PermanentError,
  ): Promise<RabbitConsumeResult> {
    const { channel, message, deadLetter } = options;
    if (!deadLetter) {
      this.logger.warn(`rejected without requeue: ${error.message}`);
      channel.nack(message, false, false);
      return { outcome: 'dead-lettered' };
    }
    try {
      await publishConfirmed(deadLetter, message, error.message);
    } catch (publishError) {
      // The poison message is not stored anywhere yet, so the original must not
      // be dropped: hand it back and let the next delivery try again.
      this.logger.warn(
        `could not dead-letter (${describe(publishError)}); requeued: ${error.message}`,
      );
      channel.nack(message, false, true);
      return { outcome: 'requeued' };
    }
    this.logger.warn(`dead-lettered to ${deadLetter.exchange}: ${error.message}`);
    channel.ack(message);
    return { outcome: 'dead-lettered' };
  }
}

function decodePayload(message: ConsumeMessage): unknown {
  try {
    return decodeWireValue(message.content);
  } catch (error) {
    throw new PermanentError(`payload is not valid JSON: ${describe(error)}`);
  }
}

function publishConfirmed(
  target: RabbitDeadLetterTarget,
  message: ConsumeMessage,
  reason: string,
): Promise<void> {
  // Only the descriptive properties carry over. `expiration` would let the
  // dead-letter copy expire, and `userId` must match the publishing connection's
  // user or the broker closes the channel.
  const { contentType, contentEncoding, correlationId, messageId, timestamp, type, appId } =
    message.properties;
  return new Promise((resolve, reject) => {
    target.channel.publish(
      target.exchange,
      target.routingKey,
      message.content,
      {
        contentType,
        contentEncoding,
        correlationId,
        messageId,
        timestamp,
        type,
        appId,
        persistent: true,
        headers: { ...message.properties.headers, [X_ERROR]: reason },
      },
      (error: unknown) => (error ? reject(error) : resolve()),
    );
  });
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
