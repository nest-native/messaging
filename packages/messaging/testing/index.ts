// @nest-native/messaging/testing — broker-free helpers for testing outbox/inbox
// flows. Kept out of the package root so test scaffolding never enters a
// consumer's production import surface.
import type { OutboxMessage, OutboxTransport } from '../transport';

/**
 * An in-memory {@link OutboxTransport} for tests: it records every published
 * message and can be made to fail on demand to exercise the claimer's
 * retry/fail paths — no broker, no network.
 *
 * ```ts
 * const transport = new InMemoryOutboxTransport();
 * // ...run the claimer...
 * expect(transport.list()).toHaveLength(1);
 *
 * transport.failWith(new RetryableError('broker down'));
 * // ...the claimer now schedules retries...
 * ```
 */
export class InMemoryOutboxTransport implements OutboxTransport {
  private readonly published: OutboxMessage[] = [];
  private failure: Error | undefined;

  /** Make every subsequent `publish` reject with `error` until cleared. */
  failWith(error: Error): void {
    this.failure = error;
  }

  /** Stop failing; subsequent publishes record normally again. */
  clearFailure(): void {
    this.failure = undefined;
  }

  publish(message: OutboxMessage): Promise<void> {
    if (this.failure) {
      return Promise.reject(this.failure);
    }
    this.published.push(message);
    return Promise.resolve();
  }

  /** Every message published so far, in order. */
  list(): readonly OutboxMessage[] {
    return this.published;
  }

  /** Messages published to a specific topic. */
  listTopic(topic: string): readonly OutboxMessage[] {
    return this.published.filter((m) => m.topic === topic);
  }

  /** Clear recorded messages and any injected failure. */
  reset(): void {
    this.published.length = 0;
    this.failure = undefined;
  }
}
