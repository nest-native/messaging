import type { KafkaProducerService } from '@nest-native/kafka';
import type { OutboxMessage, OutboxTransport } from '../../transport';
import { encodeWireValue, X_EVENT_ID, X_IDEMPOTENCY_KEY } from '../../wire-contract';

/**
 * The Kafka {@link OutboxTransport}: publishes a claimed outbox event to Kafka
 * via `@nest-native/kafka`. This is the only producer-side file that imports the
 * broker client, so the rest of the engine stays broker-agnostic.
 *
 * The transactional-outbox guarantee is upstream: the row committed in the same
 * DB transaction as the business write, and the claimer publishes it here once.
 * A `send` that throws (broker down, timeout) propagates so the claimer retries
 * the row — at-least-once delivery, which the consumer-side inbox deduplicates.
 *
 * The message `key` is `idempotencyKey ?? id` (per-entity ordering + the value
 * the inbox dedups on); `x-event-id` and `x-idempotency-key` headers carry the
 * dedup inputs; the value is JSON (the Kafka producer does not auto-encode).
 */
export class KafkaOutboxTransport implements OutboxTransport {
  constructor(
    private readonly producer: KafkaProducerService,
    private readonly topicPrefix = '',
  ) {}

  async publish(message: OutboxMessage): Promise<void> {
    const key = message.idempotencyKey ?? message.id;
    await this.producer.send({
      topic: `${this.topicPrefix}${message.topic}`,
      messages: [
        {
          key,
          value: encodeWireValue(message.payload),
          headers: {
            [X_EVENT_ID]: message.id,
            [X_IDEMPOTENCY_KEY]: key,
          },
        },
      ],
    });
  }
}
