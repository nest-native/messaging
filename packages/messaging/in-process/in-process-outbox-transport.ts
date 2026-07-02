import { Inject, Injectable } from '@nestjs/common';
import {
  type OutboxMessage,
  type OutboxTransport,
  PermanentError,
  RetryableError,
} from '../transport';
import { OutboxRegistry } from './outbox-registry';

/**
 * The no-broker {@link OutboxTransport}: "publishing" dispatches the claimed
 * event to the {@link OutboxHandler} registered for its topic — same process,
 * no network. This is the default profile of the pattern: you keep the atomic
 * enqueue + the claimer's retry machinery from day one and rebind the transport
 * to Kafka only when a real broker enters the picture.
 *
 * Outcome mapping (what the claimer sees):
 *
 *   - no handler registered      → {@link PermanentError} — the event is
 *     unroutable and can never succeed, so the row fails immediately;
 *   - handler `'completed'`      → resolves; the claimer marks the row completed;
 *   - handler `{ retryAfterMs }` → {@link RetryableError} carrying that delay;
 *   - handler **throws**         → the error propagates untouched into the
 *     claimer's mapping: a thrown {@link PermanentError} fails the row now, a
 *     thrown {@link RetryableError} keeps its delay, anything else retries
 *     with backoff until `maxAttempts`.
 *
 * Delivery is **at-least-once** via the claimer, so handlers must be idempotent
 * — or pair with `InboxService.runOnce` keyed on
 * `message.idempotencyKey ?? message.id` for exactly-once side effects.
 */
@Injectable()
export class InProcessOutboxTransport implements OutboxTransport {
  constructor(
    @Inject(OutboxRegistry) private readonly registry: OutboxRegistry,
  ) {}

  async publish(message: OutboxMessage): Promise<void> {
    const handler = this.registry.get(message.topic);
    if (!handler) {
      // An unroutable event can never succeed — fail it now instead of retrying.
      throw new PermanentError(
        `no handler registered for topic "${message.topic}"`,
      );
    }

    const result = await handler(message.payload, message);
    if (result === 'completed') return;
    throw new RetryableError(
      `handler for "${message.topic}" requested retry`,
      result.retryAfterMs,
    );
  }
}
