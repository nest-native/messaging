import { Injectable } from '@nestjs/common';
import type { OutboxMessage } from '../transport';

/**
 * What an in-process handler reports back through the transport:
 *
 *   - `'completed'`        → the claimer marks the row completed;
 *   - `{ retryAfterMs }`   → the claimer reschedules the row after that delay
 *                            (a handler-supplied retry-after);
 *   - a **throw**          → propagates untouched into the claimer's error
 *                            mapping: a thrown `PermanentError` fails the row
 *                            immediately (e.g. a malformed payload), a thrown
 *                            `RetryableError` keeps its delay, and any other
 *                            error retries with backoff until `maxAttempts`.
 */
export type OutboxHandlerResult = 'completed' | { retryAfterMs: number };

/**
 * An in-process consumer for one topic. It receives the stored payload plus the
 * full {@link OutboxMessage}, so it can derive the same dedup key the Kafka
 * consumer would (`idempotencyKey ?? id`) and pair with `InboxService.runOnce`
 * for exactly-once side effects.
 *
 * Delivery is **at-least-once**: the claimer redelivers after a retry or a crash
 * between handler success and the row's `markCompleted`. Handlers must be
 * idempotent, or wrap their side effect in the inbox.
 */
export type OutboxHandler = (
  payload: Record<string, unknown>,
  message: OutboxMessage,
) => Promise<OutboxHandlerResult> | OutboxHandlerResult;

/**
 * The topic → handler registry behind {@link InProcessOutboxTransport}. Provide
 * it (it is `@Injectable()`) and let each consumer register itself on module
 * init:
 *
 * ```ts
 * @Injectable()
 * class UserInvitedHandler implements OnModuleInit {
 *   constructor(@Inject(OutboxRegistry) private readonly registry: OutboxRegistry) {}
 *
 *   onModuleInit(): void {
 *     this.registry.register('user.invited', (payload) => this.handle(payload));
 *   }
 * }
 * ```
 *
 * One handler per topic: a second `register` for the same topic throws at
 * startup, surfacing the wiring bug immediately instead of silently replacing
 * an existing consumer.
 */
@Injectable()
export class OutboxRegistry {
  private readonly handlers = new Map<string, OutboxHandler>();

  /** Register the handler for `topic`; throws if the topic already has one. */
  register(topic: string, handler: OutboxHandler): void {
    if (this.handlers.has(topic)) {
      throw new Error(`outbox handler already registered for topic "${topic}"`);
    }
    this.handlers.set(topic, handler);
  }

  /** The handler registered for `topic`, or `undefined` when there is none. */
  get(topic: string): OutboxHandler | undefined {
    return this.handlers.get(topic);
  }
}
