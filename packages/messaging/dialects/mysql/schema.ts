import {
  index,
  int,
  json,
  mysqlTable,
  text,
  uniqueIndex,
  varchar,
} from 'drizzle-orm/mysql-core';
import type { InboxStatus, OutboxStatus } from '../../interfaces';

// MySQL-specific schema choices (documented so they are deliberate, not
// accidental divergence from the Postgres/SQLite factories):
//
// - **`varchar` on every indexed column.** MySQL cannot index a `TEXT`/`BLOB`
//   column without a prefix length, so `id`, `status`, `idempotency_key`,
//   `available_at`, `source`, and `message_key` are `varchar(n)`. 191 is the
//   classic utf8mb4-safe single-column index width; the ISO-8601 timestamp
//   columns fit comfortably in `varchar(32)`. Free-form `last_error` stays `text`.
// - **A *full* unique index on the nullable `idempotency_key`** (not the partial
//   index the Postgres/SQLite factories use). MySQL does not support partial /
//   filtered indexes, but a UNIQUE index over a nullable column already permits
//   multiple `NULL`s (SQL treats `NULL` as distinct), so enqueues *without* an
//   idempotency key never collide while duplicate keys still do — the same dedup
//   semantics as the partial index, expressed the MySQL-native way.
// - **`json` payload.** MySQL's native JSON type; mysql2 returns it already
//   parsed, so the row shape matches `OutboxEventRow`.

/**
 * MySQL `outbox_events` table. Add it to your Drizzle schema and generate a
 * migration with drizzle-kit. Timestamps are stored as ISO-8601 `varchar` so the
 * row shape is identical across dialects (ISO-8601 compares lexicographically,
 * which the claimer's `available_at` range query relies on).
 */
export const outboxEvents = mysqlTable(
  'outbox_events',
  {
    id: varchar('id', { length: 191 }).primaryKey(),
    topic: varchar('topic', { length: 255 }).notNull(),
    payload: json('payload').$type<Record<string, unknown>>().notNull(),
    status: varchar('status', { length: 32 }).$type<OutboxStatus>().notNull(),
    attempts: int('attempts').notNull().default(0),
    maxAttempts: int('max_attempts').notNull().default(10),
    idempotencyKey: varchar('idempotency_key', { length: 191 }),
    availableAt: varchar('available_at', { length: 32 }).notNull(),
    claimedAt: varchar('claimed_at', { length: 32 }),
    claimedBy: varchar('claimed_by', { length: 191 }),
    processedAt: varchar('processed_at', { length: 32 }),
    lastError: text('last_error'),
    createdAt: varchar('created_at', { length: 32 }).notNull(),
  },
  (table) => [
    uniqueIndex('outbox_events_idempotency_key_unique').on(table.idempotencyKey),
    index('outbox_events_status_available_idx').on(
      table.status,
      table.availableAt,
    ),
  ],
);

/**
 * MySQL `inbox_events` table. The composite unique index on
 * `(source, message_key)` is the dedup primitive — a redelivery violates it and
 * the inbox treats the violation (errno `1062` / `ER_DUP_ENTRY`) as "already
 * processed".
 */
export const inboxEvents = mysqlTable(
  'inbox_events',
  {
    id: varchar('id', { length: 191 }).primaryKey(),
    messageKey: varchar('message_key', { length: 191 }).notNull(),
    source: varchar('source', { length: 191 }).notNull(),
    status: varchar('status', { length: 32 }).$type<InboxStatus>().notNull(),
    processedAt: varchar('processed_at', { length: 32 }).notNull(),
    lastError: text('last_error'),
    createdAt: varchar('created_at', { length: 32 }).notNull(),
  },
  (table) => [
    uniqueIndex('inbox_events_source_message_key_unique').on(
      table.source,
      table.messageKey,
    ),
  ],
);
