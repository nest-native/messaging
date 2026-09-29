import { Injectable } from '@nestjs/common';
import { InjectTransaction, Transactional } from '@nestjs-cls/transactional';
import { OutboxProducer } from '@nest-native/messaging';
import type { SqliteOutboxStore } from '@nest-native/messaging/sqlite';
import { EVENTS, type OrderPlaced } from '../shared/contracts';
import { orders, type OrdersDatabase } from './schema';

/**
 * Takes an order and announces it, in one transaction: the order row and the
 * `order.placed` event commit together, so the order can never exist without
 * the event, or the event without the order — whether or not shipping, or the
 * broker, is up right now.
 */
@Injectable()
export class OrderService {
  constructor(
    @InjectTransaction() private readonly db: OrdersDatabase,
    private readonly producer: OutboxProducer<SqliteOutboxStore>,
  ) {}

  @Transactional()
  placeOrder(orderId: string, address: string): Promise<void> {
    this.db.insert(orders).values({ id: orderId, address, status: 'placed' }).run();
    this.producer.enqueue<OrderPlaced>({
      topic: EVENTS.orderPlaced,
      payload: { orderId, address },
      idempotencyKey: `order-placed:${orderId}`,
    });
    return undefined as unknown as Promise<void>;
  }
}
