import { integer, sqliteTable, text } from 'drizzle-orm/sqlite-core';
import { inboxEvents, outboxEvents } from '@nest-native/messaging/sqlite';

// The app's schema combines the library's outbox/inbox tables (imported from the
// dialect entrypoint) with the business tables. `invoices` is billing's
// exactly-once side effect: its id is not the event id, so a side effect that
// ran twice would show up as a second invoice for the same order.
export const orders = sqliteTable('orders', {
  id: text('id').primaryKey(),
  item: text('item').notNull(),
  amountCents: integer('amount_cents').notNull(),
});

export const invoices = sqliteTable('invoices', {
  id: integer('id').primaryKey({ autoIncrement: true }),
  orderId: text('order_id').notNull(),
  eventId: text('event_id').notNull(),
  amountCents: integer('amount_cents').notNull(),
});

export const schema = { outboxEvents, inboxEvents, orders, invoices };
