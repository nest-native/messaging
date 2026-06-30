import type { RunOnceOutcome } from '../../interfaces';
import { PermanentError } from '../../transport';
import {
  deriveDedupKey as deriveWireKey,
  type WireHeaderValue,
} from '../../wire-contract';

// Pure consumer-side helpers — no broker import, so they unit-test without Kafka.
// The consumer reuses the producer/inbox error vocabulary: a PermanentError is
// unrecoverable (dead-letter and ack so it stops redelivering); anything else is
// retryable (throw so the broker redelivers).

/**
 * Derive the dedup key for an incoming message, enforcing the inbox's contract
 * that a message MUST be keyable. Applies the shared wire-contract order
 * (`x-event-id` → `x-idempotency-key` → broker key); a message with none of them
 * cannot be deduplicated, so it is a {@link PermanentError} → dead-letter rather
 * than an endless redelivery.
 */
export function deriveDedupKey(
  headers: Record<string, WireHeaderValue> | undefined,
  messageKey: string | undefined,
): string {
  const key = deriveWireKey(headers, messageKey);
  if (!key) {
    throw new PermanentError(
      'message has no x-event-id, x-idempotency-key, or key — cannot deduplicate',
    );
  }
  return key;
}

/**
 * What the consumer wrapper should do with a message after `runOnce`. Mapped to
 * the broker's at-least-once primitives: `ack` commits the offset (done),
 * `redeliver` leaves it uncommitted (the broker retries), `dead-letter`
 * publishes to the DLQ topic then acks.
 */
export type ConsumerAction = 'ack' | 'redeliver' | 'dead-letter';

/**
 * Map a successful {@link RunOnceOutcome} to a consumer action. Both a freshly
 * processed message and a duplicate are acked — the work is durably done (or was
 * already done), so the offset should advance in both cases.
 */
export function actionForOutcome(_outcome: RunOnceOutcome): ConsumerAction {
  return 'ack';
}

/**
 * Map a thrown error to a consumer action: a {@link PermanentError} is
 * dead-lettered (retrying can never succeed), anything else is redelivered.
 */
export function actionForError(error: unknown): ConsumerAction {
  return error instanceof PermanentError ? 'dead-letter' : 'redeliver';
}
