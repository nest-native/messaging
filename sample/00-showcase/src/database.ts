import Database from 'better-sqlite3';
import { drizzle, type BetterSQLite3Database } from 'drizzle-orm/better-sqlite3';
import { schema } from './schema';

export type AppDatabase = BetterSQLite3Database<typeof schema>;

/** The DI token the CLS adapter and MessagingModule resolve the Drizzle db by. */
export const DRIZZLE = Symbol('showcase-drizzle');

// In a real app these tables come from `drizzle-kit generate` after adding the
// library's outbox/inbox factories to your schema. The showcase creates them
// inline so it runs with no migration step.
const DDL = `
CREATE TABLE outbox_events (
  id TEXT PRIMARY KEY, topic TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 10,
  idempotency_key TEXT, available_at TEXT NOT NULL, claimed_at TEXT, claimed_by TEXT,
  processed_at TEXT, last_error TEXT, created_at TEXT NOT NULL);
CREATE UNIQUE INDEX outbox_events_idempotency_key_unique ON outbox_events (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX outbox_events_status_available_idx ON outbox_events (status, available_at);
CREATE TABLE inbox_events (
  id TEXT PRIMARY KEY, message_key TEXT NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL,
  processed_at TEXT NOT NULL, last_error TEXT, created_at TEXT NOT NULL);
CREATE UNIQUE INDEX inbox_events_source_message_key_unique ON inbox_events (source, message_key);
CREATE TABLE orders (id TEXT PRIMARY KEY, item TEXT NOT NULL);
CREATE TABLE order_audit (key TEXT PRIMARY KEY, item TEXT NOT NULL);
`;

export function createDatabase(): { sqlite: Database.Database; db: AppDatabase } {
  const sqlite = new Database(':memory:');
  sqlite.exec(DDL);
  return { sqlite, db: drizzle(sqlite, { schema }) };
}
