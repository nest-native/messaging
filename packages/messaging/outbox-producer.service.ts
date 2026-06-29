import { Inject, Injectable } from '@nestjs/common';
import { InjectTransaction } from '@nestjs-cls/transactional';
import type { EnqueueInput, OutboxStore } from './interfaces';
import { OUTBOX_STORE } from './tokens';

/**
 * Writes events into the outbox **inside the caller's business transaction** —
 * the dual-write guarantee. Inject it into a `@Transactional()` service and call
 * `enqueue` alongside your business writes; the row commits atomically with them.
 *
 * `enqueue` returns the store's native shape: the **sqlite** store returns a
 * synchronous `OutboxEventRow` (call it without `await` inside a synchronous
 * `@Transactional` body); the **postgres** store returns a `Promise` (await it).
 * Type the producer as `OutboxProducer<typeof yourStore>` to get the exact shape.
 *
 * Requires the host app to configure `@nestjs-cls/transactional` with the Drizzle
 * adapter (`enableTransactionProxy: true`) — `@InjectTransaction()` resolves the
 * transaction-scoped Drizzle instance from it.
 */
@Injectable()
export class OutboxProducer<TStore extends OutboxStore = OutboxStore> {
  constructor(
    @InjectTransaction() private readonly db: unknown,
    @Inject(OUTBOX_STORE) private readonly store: TStore,
  ) {}

  enqueue(input: EnqueueInput): ReturnType<TStore['enqueue']> {
    return this.store.enqueue(this.db, input) as ReturnType<TStore['enqueue']>;
  }
}
