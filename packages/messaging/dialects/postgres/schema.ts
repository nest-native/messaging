import { sql } from 'drizzle-orm';
import {
  index,
  integer,
  jsonb,
  pgTable,
  text,
  uniqueIndex,
} from 'drizzle-orm/pg-core';
import type { InboxStatus, OutboxStatus } from '../../interfaces';

/**
 * Postgres `outbox_events` table. Add it to your Drizzle schema and generate a
 * migration with drizzle-kit. Timestamps are stored as ISO-8601 `text` so the
 * row shape is identical across dialects (ISO-8601 compares lexicographically,
 * which the claimer's `available_at` range query relies on).
 */
export const outboxEvents = pgTable(
  'outbox_events',
  {
    id: text('id').primaryKey(),
    topic: text('topic').notNull(),
    payload: jsonb('payload').$type<Record<string, unknown>>().notNull(),
    status: text('status').$type<OutboxStatus>().notNull(),
    attempts: integer('attempts').notNull().default(0),
    maxAttempts: integer('max_attempts').notNull().default(10),
    idempotencyKey: text('idempotency_key'),
    availableAt: text('available_at').notNull(),
    claimedAt: text('claimed_at'),
    claimedBy: text('claimed_by'),
    processedAt: text('processed_at'),
    lastError: text('last_error'),
    createdAt: text('created_at').notNull(),
  },
  (table) => [
    uniqueIndex('outbox_events_idempotency_key_unique')
      .on(table.idempotencyKey)
      .where(sql`${table.idempotencyKey} IS NOT NULL`),
    index('outbox_events_status_available_idx').on(
      table.status,
      table.availableAt,
    ),
  ],
);

/**
 * Postgres `inbox_events` table. The composite unique index on
 * `(source, message_key)` is the dedup primitive.
 */
export const inboxEvents = pgTable(
  'inbox_events',
  {
    id: text('id').primaryKey(),
    messageKey: text('message_key').notNull(),
    source: text('source').notNull(),
    status: text('status').$type<InboxStatus>().notNull(),
    processedAt: text('processed_at').notNull(),
    lastError: text('last_error'),
    createdAt: text('created_at').notNull(),
  },
  (table) => [
    uniqueIndex('inbox_events_source_message_key_unique').on(
      table.source,
      table.messageKey,
    ),
  ],
);
