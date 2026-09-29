import { randomUUID } from 'node:crypto';
import { Inject, Injectable, Logger } from '@nestjs/common';
import type { Channel, ConfirmChannel, ConsumeMessage, Message } from 'amqplib';
import { actionForError, deriveDedupKey } from '../../idempotent-consumer';
import { InboxService } from '../../inbox.service';
import type { InboxSideEffect } from '../../interfaces';
import { PermanentError } from '../../transport';
import {
  decodeWireValue,
  headerToString,
  X_ERROR,
  type WireHeaderValue,
} from '../../wire-contract';

/**
 * Header stamped on a dead-letter copy so a `basic.return` for it can be matched
 * to its publish. The copy is published `mandatory`: without that, RabbitMQ acks
 * a copy no queue is bound for and drops it, and the original — acked right
 * after — would exist nowhere.
 */
export const X_DEAD_LETTER_ID = 'x-dead-letter-id' as const;

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
 * How a delivery that failed transiently — anything but a {@link PermanentError} —
 * is retried. RabbitMQ does not bound this for you: an explicit requeue is not
 * counted toward a quorum queue's delivery limit, and it puts the message back
 * at the head of the queue, so a failure that persists would redeliver the same
 * message as fast as the broker can send it. The consumer therefore waits before
 * each requeue, and can give up.
 */
export interface RabbitRetryOptions {
  /** Wait before the first requeue; doubles with every failed attempt of the same message. Default: 1 000 ms. */
  delayMs?: number;
  /** Upper bound on that wait. Default: 30 000 ms. */
  maxDelayMs?: number;
  /**
   * Dead-letter the message (with the last error as its reason) once it has
   * failed this many times in this process. Default: never — keep retrying at
   * the capped delay, so a long outage delays messages rather than
   * dead-lettering them.
   */
  maxAttempts?: number;
}

/**
 * What a single delivery resolved to — returned for logging/metrics. The
 * consumer has already acked or rejected the delivery when it resolves, and it
 * never rejects: a channel that closed before the delivery could be settled
 * leaves the delivery with the broker, which redelivers it.
 */
export interface RabbitConsumeResult {
  outcome: 'processed' | 'duplicate' | 'dead-lettered' | 'requeued';
  /**
   * The message's dedup key, whenever one could be derived — on every outcome,
   * so a dead letter or a requeue can be traced back to its event. Absent only
   * for a message that carries no key at all (which is dead-lettered).
   */
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
  /** Type guard; a payload it rejects — or throws on — is a permanent error → dead-letter. */
  validate: (payload: unknown) => payload is T;
  /**
   * The exactly-once side effect, run inside the dedup transaction. Receives the
   * validated payload and the derived dedup key. On a sqlite inbox store it MUST
   * be synchronous + DB-only; on postgres or mysql it may be async.
   */
  sideEffect: (payload: T, dedupKey: string) => void | Promise<void>;
  /** Republish poison messages here with their reason; omit to reject them instead. */
  deadLetter?: RabbitDeadLetterTarget;
  /** Backoff (and an optional attempt limit) for transient failures. */
  retry?: RabbitRetryOptions;
}

const DEFAULT_RETRY_DELAY_MS = 1_000;
const DEFAULT_RETRY_MAX_DELAY_MS = 30_000;
/** Attempt counts are kept for at most this many failing messages, oldest dropped first. */
const MAX_TRACKED_ATTEMPTS = 10_000;

type Settlement = 'ack' | 'requeue' | 'reject';

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
 *     with requeue after a backoff, or dead-lettered once `retry.maxAttempts`
 *     is reached
 *
 * The dedup key follows the shared wire contract: `x-event-id`, then
 * `x-idempotency-key`, then the AMQP `messageId` — what the RabbitMQ outbox
 * transport sets on every message. Header values amqplib decodes as numbers
 * (common from JVM and Python producers) are read as their string form.
 */
@Injectable()
export class RabbitInboxConsumer {
  private readonly logger = new Logger(RabbitInboxConsumer.name);
  private readonly attempts = new Map<string, number>();
  private readonly deadLetterReturns = new WeakMap<ConfirmChannel, Set<string>>();

  constructor(@Inject(InboxService) private readonly inbox: InboxService) {}

  async consume<T>(options: RabbitConsumeOptions<T>): Promise<RabbitConsumeResult> {
    const { channel, message, source } = options;
    let dedupKey: string | undefined;
    try {
      dedupKey = deriveDedupKey(
        normalizeHeaders(message.properties.headers),
        headerText(message.properties.messageId),
      );
      const payload = decodePayload(message);
      if (!isValid(options.validate, payload)) {
        throw new PermanentError('payload failed validation');
      }
      const key = dedupKey;
      const sideEffect: InboxSideEffect = () => options.sideEffect(payload, key);
      const outcome = await this.inbox.runOnce(key, source, sideEffect);
      if (outcome === 'duplicate') {
        this.logger.debug(`duplicate skipped: ${key}`);
      }
      this.attempts.delete(attemptKey(source, key));
      this.settle(channel, message, 'ack');
      return { outcome, dedupKey: key };
    } catch (error) {
      return this.onFailure(options, error, dedupKey);
    }
  }

  private async onFailure<T>(
    options: RabbitConsumeOptions<T>,
    error: unknown,
    dedupKey: string | undefined,
  ): Promise<RabbitConsumeResult> {
    const retry = options.retry ?? {};
    if (actionForError(error) === 'dead-letter') {
      this.forget(options.source, dedupKey);
      return this.deadLetter(options, describe(error), dedupKey, retry);
    }
    // Only work that already derived a key can fail transiently: the key, the
    // body and the payload are all checked (as PermanentErrors) before it.
    const attempt = this.countAttempt(options.source, dedupKey);
    if (attempt >= (retry.maxAttempts ?? Infinity)) {
      this.forget(options.source, dedupKey);
      return this.deadLetter(
        options,
        `gave up after ${attempt} attempts: ${describe(error)}`,
        dedupKey,
        retry,
      );
    }
    const delay = backoff(attempt, retry);
    this.logger.warn(
      `requeued for redelivery in ${delay} ms (attempt ${attempt}): ${describe(error)}`,
    );
    return this.requeueAfter(options, delay, dedupKey);
  }

  private async deadLetter<T>(
    options: RabbitConsumeOptions<T>,
    reason: string,
    dedupKey: string | undefined,
    retry: RabbitRetryOptions,
  ): Promise<RabbitConsumeResult> {
    const { channel, message, deadLetter } = options;
    if (!deadLetter) {
      this.logger.warn(`rejected without requeue: ${reason}`);
      this.settle(channel, message, 'reject');
      return result('dead-lettered', dedupKey);
    }
    try {
      await this.publishDeadLetter(deadLetter, message, reason);
    } catch (publishError) {
      // The poison message is not stored anywhere yet, so the original must not
      // be dropped: hand it back — after the same wait a transient failure gets,
      // so a broken dead-letter target cannot spin.
      const attempt = this.countAttempt(options.source, dedupKey);
      const delay = backoff(attempt, retry);
      this.logger.warn(
        `could not dead-letter (${describe(publishError)}); requeued in ${delay} ms: ${reason}`,
      );
      return this.requeueAfter(options, delay, dedupKey);
    }
    this.logger.warn(`dead-lettered to ${deadLetter.exchange}: ${reason}`);
    this.settle(channel, message, 'ack');
    return result('dead-lettered', dedupKey);
  }

  private async requeueAfter<T>(
    options: RabbitConsumeOptions<T>,
    delay: number,
    dedupKey: string | undefined,
  ): Promise<RabbitConsumeResult> {
    // The delivery stays unacked meanwhile, holding one prefetch slot: a failure
    // that persists slows its consumer down instead of spinning.
    await sleep(delay);
    this.settle(options.channel, options.message, 'requeue');
    return result('requeued', dedupKey);
  }

  /**
   * Acks or nacks the delivery. amqplib throws once the channel has closed —
   * a broker restart, a lost connection, a consumer timeout — and by then the
   * broker has already put the unacked delivery back, so it will come again
   * (and a duplicate of processed work is deduplicated). That is logged, never
   * thrown: `consume` must not reject.
   */
  private settle(channel: Channel, message: ConsumeMessage, how: Settlement): void {
    try {
      if (how === 'ack') {
        channel.ack(message);
      } else {
        channel.nack(message, false, how === 'requeue');
      }
    } catch (error) {
      this.logger.warn(
        `could not ${how} the delivery (${describe(error)}); the broker will redeliver it`,
      );
    }
  }

  private publishDeadLetter(
    target: RabbitDeadLetterTarget,
    message: ConsumeMessage,
    reason: string,
  ): Promise<void> {
    const returned = this.returnsFor(target.channel);
    const id = randomUUID();
    // Only the descriptive properties carry over. `expiration` would let the
    // dead-letter copy expire, and `userId` must match the publishing
    // connection's user or the broker closes the channel.
    const { contentType, contentEncoding, correlationId, messageId, timestamp, type, appId } =
      message.properties;
    return new Promise((resolve, reject) => {
      const confirmed = (error: unknown): void => {
        // The broker returns an unroutable message before it acks it.
        const wasReturned = returned.delete(id);
        if (error) {
          reject(error instanceof Error ? error : new Error(String(error)));
        } else if (wasReturned) {
          reject(
            new Error(
              `no queue is bound to exchange "${target.exchange}" for routing key "${target.routingKey}"`,
            ),
          );
        } else {
          resolve();
        }
      };
      try {
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
            mandatory: true,
            headers: {
              ...message.properties.headers,
              [X_ERROR]: reason,
              [X_DEAD_LETTER_ID]: id,
            },
          },
          confirmed,
        );
      } catch (error) {
        // A closed channel throws synchronously instead of calling back.
        returned.delete(id);
        reject(error);
      }
    });
  }

  /** The dead-letter copies a channel handed back, tracked once per channel. */
  private returnsFor(channel: ConfirmChannel): Set<string> {
    const tracked = this.deadLetterReturns.get(channel);
    if (tracked) {
      return tracked;
    }
    const returned = new Set<string>();
    channel.on('return', (message: Message) => {
      const id = headerToString(
        message.properties.headers?.[X_DEAD_LETTER_ID] as WireHeaderValue,
      );
      if (id) {
        returned.add(id);
      }
    });
    this.deadLetterReturns.set(channel, returned);
    return returned;
  }

  /** Counts a failed attempt of one message, keeping at most MAX_TRACKED_ATTEMPTS counts. */
  private countAttempt(source: string, dedupKey: string | undefined): number {
    if (dedupKey === undefined) {
      return 1;
    }
    const key = attemptKey(source, dedupKey);
    const attempt = (this.attempts.get(key) ?? 0) + 1;
    this.attempts.delete(key); // re-insert at the end: the Map is the LRU order
    this.attempts.set(key, attempt);
    if (this.attempts.size > MAX_TRACKED_ATTEMPTS) {
      const oldest = this.attempts.keys().next().value as string;
      this.attempts.delete(oldest);
    }
    return attempt;
  }

  private forget(source: string, dedupKey: string | undefined): void {
    if (dedupKey !== undefined) {
      this.attempts.delete(attemptKey(source, dedupKey));
    }
  }
}

function attemptKey(source: string, dedupKey: string): string {
  return `${source}\u0000${dedupKey}`;
}

function backoff(attempt: number, retry: RabbitRetryOptions): number {
  const initial = retry.delayMs ?? DEFAULT_RETRY_DELAY_MS;
  const cap = retry.maxDelayMs ?? DEFAULT_RETRY_MAX_DELAY_MS;
  return Math.min(cap, initial * 2 ** (attempt - 1));
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function result(
  outcome: RabbitConsumeResult['outcome'],
  dedupKey: string | undefined,
): RabbitConsumeResult {
  return dedupKey === undefined ? { outcome } : { outcome, dedupKey };
}

/**
 * amqplib decodes typed AMQP header values: numbers, booleans, timestamps,
 * tables. The wire contract speaks strings and Buffers, so a number is read as
 * its string form and anything else is not a usable key.
 */
function normalizeHeaders(
  headers: Record<string, unknown> | undefined,
): Record<string, WireHeaderValue> | undefined {
  if (!headers) {
    return undefined;
  }
  const normalized: Record<string, WireHeaderValue> = {};
  for (const [name, value] of Object.entries(headers)) {
    const text = headerText(value);
    if (text !== undefined) {
      normalized[name] = text;
    }
  }
  return normalized;
}

function headerText(value: unknown): string | undefined {
  if (typeof value === 'string') {
    return value;
  }
  if (Buffer.isBuffer(value)) {
    return value.toString('utf8');
  }
  if (typeof value === 'number' || typeof value === 'bigint') {
    return String(value);
  }
  return undefined;
}

/** A guard that throws has judged the same bytes the same way every time: that is permanent. */
function isValid<T>(validate: (payload: unknown) => payload is T, payload: unknown): payload is T {
  try {
    return validate(payload);
  } catch (error) {
    throw new PermanentError(`validate threw: ${describe(error)}`);
  }
}

function decodePayload(message: ConsumeMessage): unknown {
  try {
    return decodeWireValue(message.content);
  } catch (error) {
    throw new PermanentError(`payload is not valid JSON: ${describe(error)}`);
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
