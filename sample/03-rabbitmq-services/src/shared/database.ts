import Database from 'better-sqlite3';
import { drizzle, type BetterSQLite3Database } from 'drizzle-orm/better-sqlite3';

/** The DI token each service's CLS adapter and MessagingModule resolve its Drizzle db by. */
export const DRIZZLE = Symbol('service-drizzle');

// In a real service these tables come from `drizzle-kit generate` after adding
// the library's outbox/inbox factories to the schema. The sample creates them
// inline so it runs with no migration step.
const MESSAGING_DDL = `
CREATE TABLE IF NOT EXISTS outbox_events (
  id TEXT PRIMARY KEY, topic TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 10,
  idempotency_key TEXT, available_at TEXT NOT NULL, claimed_at TEXT, claimed_by TEXT,
  processed_at TEXT, last_error TEXT, created_at TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS outbox_events_idempotency_key_unique ON outbox_events (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS outbox_events_status_available_idx ON outbox_events (status, available_at);
CREATE TABLE IF NOT EXISTS inbox_events (
  id TEXT PRIMARY KEY, message_key TEXT NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL,
  processed_at TEXT NOT NULL, last_error TEXT, created_at TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS inbox_events_source_message_key_unique ON inbox_events (source, message_key);
`;

/**
 * One service's own database: the library's outbox and inbox tables next to
 * its business tables. The services share no database — only the broker —
 * which is the whole reason they need an outbox and an inbox.
 *
 * It lives in a file, so a restarted service picks up exactly where it
 * stopped: its inbox still knows every event it processed, and its outbox
 * still holds every event it has not published. WAL mode lets the smoke test
 * read it while the service writes.
 */
export function createServiceDatabase<TSchema extends Record<string, unknown>>(
  schema: TSchema,
  businessDdl: string,
  file: string,
): { sqlite: Database.Database; db: BetterSQLite3Database<TSchema> } {
  const sqlite = new Database(file);
  sqlite.pragma('journal_mode = WAL');
  sqlite.exec(MESSAGING_DDL + businessDdl);
  return { sqlite, db: drizzle(sqlite, { schema }) };
}
