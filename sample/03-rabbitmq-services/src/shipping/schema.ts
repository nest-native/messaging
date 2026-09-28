import { sqliteTable, text } from 'drizzle-orm/sqlite-core';
import type { BetterSQLite3Database } from 'drizzle-orm/better-sqlite3';
import { inboxEvents, outboxEvents } from '@nest-native/messaging/sqlite';
import { createServiceDatabase } from '../shared/database';

// `order_id` is deliberately not unique: an order scheduled twice would show up
// as two shipments, so the smoke test can see that it never happens.
export const shipments = sqliteTable('shipments', {
  id: text('id').primaryKey(),
  orderId: text('order_id').notNull(),
  carrier: text('carrier').notNull(),
});

export const shippingSchema = { outboxEvents, inboxEvents, shipments };
export type ShippingDatabase = BetterSQLite3Database<typeof shippingSchema>;

export function createShippingDatabase(file: string) {
  return createServiceDatabase(
    shippingSchema,
    `CREATE TABLE IF NOT EXISTS shipments (
       id TEXT PRIMARY KEY, order_id TEXT NOT NULL, carrier TEXT NOT NULL);`,
    file,
  );
}
