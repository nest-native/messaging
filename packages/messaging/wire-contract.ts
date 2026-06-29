// Single source of truth for the on-the-wire contract shared by the producer
// transport (the Kafka adapter) and the consumer inbox. It pins the header
// names, the dedup-key derivation order, and the JSON body codec so the two
// halves of the reliable-messaging pair can never drift apart.

/**
 * Header carrying the producer-assigned event id (the outbox row id). It is the
 * first-choice dedup key because it is stable across redeliveries of the same
 * logical event.
 */
export const X_EVENT_ID = 'x-event-id' as const;

/**
 * Header carrying the business idempotency key (e.g. `user.invited:org:user`).
 * Used as the dedup key when no event id is present.
 */
export const X_IDEMPOTENCY_KEY = 'x-idempotency-key' as const;

/**
 * Header attached to a message routed to a dead-letter topic, describing why it
 * could not be processed.
 */
export const X_ERROR = 'x-error' as const;

/**
 * A message header value as delivered by a broker client: a string, a Buffer, an
 * array of either, or undefined when the header is absent.
 */
export type WireHeaderValue =
  | string
  | Buffer
  | (string | Buffer)[]
  | undefined;

/**
 * Coerce a raw header value to a string. Brokers deliver header values as
 * Buffers (or arrays of them); the dedup key derivation needs a plain string.
 */
export function headerToString(value: WireHeaderValue): string | undefined {
  if (value === undefined) return undefined;
  if (Array.isArray(value)) return headerToString(value[0]);
  if (Buffer.isBuffer(value)) return value.toString('utf8');
  return value;
}

/**
 * Derive the dedup key for an incoming message. The order is the contract:
 *
 *   1. `x-event-id`        (producer event id — most stable)
 *   2. `x-idempotency-key` (business key — stable across event ids)
 *   3. the broker message key (last resort)
 *
 * Returns `undefined` when none are present; the caller decides how to treat a
 * keyless message (the inbox routes it to the DLQ).
 */
export function deriveDedupKey(
  headers: Record<string, WireHeaderValue> | undefined,
  messageKey: string | undefined,
): string | undefined {
  const eventId = headerToString(headers?.[X_EVENT_ID]);
  if (eventId) return eventId;
  const idempotencyKey = headerToString(headers?.[X_IDEMPOTENCY_KEY]);
  if (idempotencyKey) return idempotencyKey;
  return messageKey && messageKey.length > 0 ? messageKey : undefined;
}

/**
 * Encode a payload to a JSON string for the message `value`. Brokers do not
 * auto-encode values, so the producer transport owns the encoding and the
 * consumer owns the matching decode.
 */
export function encodeWireValue(payload: unknown): string {
  return JSON.stringify(payload);
}

/**
 * Decode a message `value` back to an object. Accepts the string/Buffer shapes a
 * client may deliver; returns the parsed value (callers narrow it with a type
 * guard).
 */
export function decodeWireValue(value: string | Buffer | null): unknown {
  if (value === null) return null;
  const text = Buffer.isBuffer(value) ? value.toString('utf8') : value;
  return JSON.parse(text);
}
