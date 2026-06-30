import { Inject, Injectable, Logger } from '@nestjs/common';
import { KafkaContext, KafkaProducerService } from '@nest-native/kafka';
import { InboxService } from '../../inbox.service';
import type { InboxSideEffect } from '../../interfaces';
import { PermanentError } from '../../transport';
import { X_ERROR, type WireHeaderValue } from '../../wire-contract';
import {
  actionForError,
  deriveDedupKey,
} from './idempotent-consumer';

/**
 * What a single delivery resolved to — returned for logging/metrics. `'ack'`
 * (incl. duplicates) and `'dead-lettered'` both leave the offset committed; a
 * redeliver throws instead of returning, so it is never a result value.
 */
export interface ConsumeResult {
  outcome: 'processed' | 'duplicate' | 'dead-lettered';
  dedupKey?: string;
}

/** Per-message options for {@link KafkaInboxConsumer.consume}. */
export interface ConsumeOptions<T> {
  /** Scopes dedup keys to this consumer (typically `topic:groupId`). */
  source: string;
  /** The Kafka execution context (for the message key + DLQ republish). */
  context: KafkaContext;
  /** The delivered headers. */
  headers: Record<string, WireHeaderValue> | undefined;
  /** The deserialized payload, narrowed by {@link validate}. */
  payload: unknown;
  /** Type guard; a payload that fails is a permanent error → dead-letter. */
  validate: (payload: unknown) => payload is T;
  /**
   * The exactly-once side effect, run inside the dedup transaction. Receives the
   * validated payload and the derived dedup key (handy for stamping the key into
   * the side effect's own record). On a sqlite inbox store it MUST be synchronous
   * + DB-only; on postgres it may be async.
   */
  sideEffect: (payload: T, dedupKey: string) => void | Promise<void>;
  /** Topic poison messages are republished to before acking. */
  dlqTopic: string;
}

/**
 * The reusable idempotent-consumer engine. Inject it into a thin `@KafkaConsumer`
 * shell (which owns the static topic + group) and call {@link consume} from the
 * `@KafkaHandler`. It runs all async broker work OUTSIDE the dedup transaction
 * and only the `InboxService.runOnce` primitive inside it:
 *
 *   - happy path / duplicate → returns (the caller returns → offset commits)
 *   - bad key / invalid payload (PermanentError) → republished to `dlqTopic`,
 *     then returns so the offset commits (no endless redelivery of poison)
 *   - transient failure (anything else) → THROWS so the offset is not committed
 *     and the broker redelivers
 */
@Injectable()
export class KafkaInboxConsumer {
  private readonly logger = new Logger(KafkaInboxConsumer.name);

  constructor(
    @Inject(InboxService) private readonly inbox: InboxService,
    @Inject(KafkaProducerService)
    private readonly producer: KafkaProducerService,
  ) {}

  async consume<T>(options: ConsumeOptions<T>): Promise<ConsumeResult> {
    const messageKey = this.readKey(options.context);
    try {
      const dedupKey = deriveDedupKey(options.headers, messageKey);
      const payload = options.payload;
      if (!options.validate(payload)) {
        throw new PermanentError('payload failed validation');
      }
      const sideEffect: InboxSideEffect = () => options.sideEffect(payload, dedupKey);
      const outcome = await this.inbox.runOnce(dedupKey, options.source, sideEffect);
      if (outcome === 'duplicate') {
        this.logger.debug(`duplicate skipped: ${dedupKey}`);
      }
      return { outcome, dedupKey };
    } catch (error) {
      if (actionForError(error) === 'dead-letter') {
        await this.deadLetter(options.context, options.dlqTopic, error as PermanentError);
        return { outcome: 'dead-lettered' };
      }
      throw error;
    }
  }

  private readKey(context: KafkaContext): string | undefined {
    const key = context.getMessage().key;
    if (key === null || key === undefined) return undefined;
    return Buffer.isBuffer(key) ? key.toString('utf8') : key;
  }

  private async deadLetter(
    context: KafkaContext,
    dlqTopic: string,
    error: PermanentError,
  ): Promise<void> {
    this.logger.warn(`dead-lettered to ${dlqTopic}: ${error.message}`);
    const original = context.getMessage();
    await this.producer.send({
      topic: dlqTopic,
      messages: [
        {
          key: original.key ?? null,
          value: original.value,
          headers: { [X_ERROR]: error.message },
        },
      ],
    });
  }
}
