import { sqliteTable, text } from 'drizzle-orm/sqlite-core';
import { inboxEvents, outboxEvents } from '@nest-native/messaging/sqlite';

// The app's schema combines the library's outbox/inbox tables (imported from the
// dialect entrypoint) with the business tables. `order_audit` is the consumer's
// exactly-once side effect — a real DB row, so dedup is observable.
export const orders = sqliteTable('orders', {
  id: text('id').primaryKey(),
  item: text('item').notNull(),
});

export const orderAudit = sqliteTable('order_audit', {
  key: text('key').primaryKey(),
  item: text('item').notNull(),
});

export const schema = { outboxEvents, inboxEvents, orders, orderAudit };
