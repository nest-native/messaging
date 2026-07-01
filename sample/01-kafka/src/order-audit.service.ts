import { Injectable } from '@nestjs/common';
import { InjectTransaction } from '@nestjs-cls/transactional';
import type { AppDatabase } from './database';
import { orderAudit } from './schema';

/**
 * Writes the delivery audit row. It injects the **transaction-scoped** Drizzle
 * instance, so when the consumer runs this inside `InboxService.runOnce`, the
 * audit write and the dedup row commit in the SAME transaction — a handler throw
 * would roll back both, and a duplicate delivery writes neither.
 */
@Injectable()
export class OrderAuditService {
  constructor(@InjectTransaction() private readonly db: AppDatabase) {}

  record(key: string, item: string): void {
    this.db.insert(orderAudit).values({ key, item }).run();
  }
}
