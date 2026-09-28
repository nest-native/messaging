import { Injectable } from '@nestjs/common';
import { InjectTransaction } from '@nestjs-cls/transactional';
import type { AppDatabase } from './database';
import type { OrderPlaced } from './order.service';
import { invoices } from './schema';

/**
 * Billing's side effect. It injects the **transaction-scoped** Drizzle instance,
 * so when the inbox runs it inside `InboxService.runOnce`, the invoice and the
 * dedup row commit in the SAME transaction: a throw rolls back both, and a
 * duplicate delivery writes neither. On the SQLite store it must stay
 * synchronous and database-only.
 */
@Injectable()
export class InvoiceService {
  constructor(@InjectTransaction() private readonly db: AppDatabase) {}

  issue(order: OrderPlaced, eventId: string): void {
    this.db
      .insert(invoices)
      .values({ orderId: order.orderId, eventId, amountCents: order.amountCents })
      .run();
  }
}
