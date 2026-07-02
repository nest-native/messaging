import { Injectable } from '@nestjs/common';
import { InjectTransaction, Transactional } from '@nestjs-cls/transactional';
import { OutboxProducer } from '@nest-native/messaging';
import type { SqliteOutboxStore } from '@nest-native/messaging/sqlite';
import type { AppDatabase } from './database';
import { orders } from './schema';

// A plain interface (no index signature) — enqueue accepts it directly, no
// `as unknown as Record<string, unknown>` cast.
export interface OrderPlacedPayload {
  id: string;
  item: string;
}

/**
 * Places an order and enqueues the `order.placed` event in the SAME transaction
 * — the dual-write guarantee. The body is synchronous (better-sqlite3), so
 * `enqueue` returns the row directly; a throw would roll back both writes.
 */
@Injectable()
export class OrderService {
  constructor(
    @InjectTransaction() private readonly db: AppDatabase,
    private readonly producer: OutboxProducer<SqliteOutboxStore>,
  ) {}

  @Transactional()
  placeOrder(id: string, item: string): Promise<void> {
    this.db.insert(orders).values({ id, item }).run();
    const payload: OrderPlacedPayload = { id, item };
    this.producer.enqueue({
      topic: 'order.placed',
      payload,
      idempotencyKey: `order:${id}`,
    });
    return undefined as unknown as Promise<void>;
  }
}
