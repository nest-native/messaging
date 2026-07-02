// The transport seam the outbox claimer publishes through. It is deliberately
// dependency-free (no Nest, no broker client, no Drizzle) so both the in-process
// default and the opt-in Kafka transport can implement it, and so the claimer
// depends only on this contract — never on a concrete transport.

/**
 * Injection token for the active {@link OutboxTransport}. `MessagingModule`
 * binds it to the transport you supply — the in-process transport
 * (`@nest-native/messaging/in-process`) for the no-broker default profile, the
 * Kafka transport (`@nest-native/messaging/kafka`) for a real broker, or the
 * in-memory one (`@nest-native/messaging/testing`) in tests.
 */
export const OUTBOX_TRANSPORT = Symbol.for('@nest-native/messaging:outbox-transport');

/**
 * One claimed outbox row, reduced to the fields a transport needs to publish.
 */
export interface OutboxMessage {
  /** The outbox row id — used as the event id and as the fallback message key. */
  id: string;
  /** The logical topic the event was enqueued on. */
  topic: string;
  /** The event body. */
  payload: Record<string, unknown>;
  /** Optional business idempotency key; preferred over `id` as the message key. */
  idempotencyKey?: string;
}

/**
 * The publish side of the outbox seam. `publish` resolves when the message has
 * been handed off durably (delivered in-process, or accepted by the broker) and
 * rejects otherwise. The claimer maps the rejection to a retry/fail decision:
 *
 *   - {@link RetryableError}  → schedule a retry (honour `delayMs` if given)
 *   - {@link PermanentError}  → mark failed immediately (no point retrying)
 *   - any other error         → retry with backoff until maxAttempts, then fail
 */
export interface OutboxTransport {
  publish(message: OutboxMessage): Promise<void>;
}

/**
 * Signals a transient failure: the message could not be published now but a
 * later attempt may succeed. `delayMs`, when set, overrides the claimer's
 * exponential backoff for the next attempt (e.g. a handler-supplied retry-after).
 */
export class RetryableError extends Error {
  constructor(
    message: string,
    readonly delayMs?: number,
  ) {
    super(message);
    this.name = 'RetryableError';
  }
}

/**
 * Signals a non-recoverable failure: retrying will never succeed (e.g. no
 * handler registered for the topic, or a malformed payload). The claimer marks
 * the row failed immediately rather than burning retry attempts.
 */
export class PermanentError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PermanentError';
  }
}
