import { Injectable } from '@nestjs/common';
import { InjectTransaction, Transactional } from '@nestjs-cls/transactional';
import { OutboxProducer } from '@nest-native/messaging';
import type { SqliteOutboxStore } from '@nest-native/messaging/sqlite';
import type { AppDatabase } from './database';
import { orders } from './schema';

export interface OrderPlaced {
  orderId: string;
  item: string;
  amountCents: number;
}

/**
 * Writes the business row and the outbox event in the SAME transaction — the
 * dual-write guarantee. The body is synchronous (better-sqlite3), so `enqueue`
 * returns the row directly; a throw would roll back both writes.
 */
@Injectable()
export class OrderService {
  constructor(
    @InjectTransaction() private readonly db: AppDatabase,
    private readonly producer: OutboxProducer<SqliteOutboxStore>,
  ) {}

  @Transactional()
  placeOrder(orderId: string, item: string, amountCents: number): Promise<void> {
    this.db.insert(orders).values({ id: orderId, item, amountCents }).run();
    this.producer.enqueue<OrderPlaced>({
      topic: 'order.placed',
      payload: { orderId, item, amountCents },
      idempotencyKey: `order-placed:${orderId}`,
    });
    return undefined as unknown as Promise<void>;
  }

  /**
   * Refunds are published before anything consumes them — the usual deploy-order
   * gap. No queue is bound to `order.refunded` yet, so the broker returns the
   * message instead of acking it into the void, and the outbox keeps the event.
   */
  @Transactional()
  refundOrder(orderId: string): Promise<void> {
    this.producer.enqueue({
      topic: 'order.refunded',
      payload: { orderId },
      idempotencyKey: `order-refunded:${orderId}`,
    });
    return undefined as unknown as Promise<void>;
  }
}
