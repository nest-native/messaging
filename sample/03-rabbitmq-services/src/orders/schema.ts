import { sqliteTable, text } from 'drizzle-orm/sqlite-core';
import type { BetterSQLite3Database } from 'drizzle-orm/better-sqlite3';
import { inboxEvents, outboxEvents } from '@nest-native/messaging/sqlite';
import { createServiceDatabase } from '../shared/database';

export const orders = sqliteTable('orders', {
  id: text('id').primaryKey(),
  address: text('address').notNull(),
  status: text('status', { enum: ['placed', 'scheduled'] }).notNull(),
  shipmentId: text('shipment_id'),
  carrier: text('carrier'),
});

export const ordersSchema = { outboxEvents, inboxEvents, orders };
export type OrdersDatabase = BetterSQLite3Database<typeof ordersSchema>;

export function createOrdersDatabase(file: string) {
  return createServiceDatabase(
    ordersSchema,
    `CREATE TABLE IF NOT EXISTS orders (
       id TEXT PRIMARY KEY, address TEXT NOT NULL, status TEXT NOT NULL,
       shipment_id TEXT, carrier TEXT);`,
    file,
  );
}
