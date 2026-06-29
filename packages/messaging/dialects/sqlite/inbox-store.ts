import { randomUUID } from 'node:crypto';
import type { BetterSQLite3Database } from 'drizzle-orm/better-sqlite3';
import type {
  InboxSideEffect,
  InboxStore,
  RunOnceOutcome,
} from '../../interfaces';
import { inboxEvents } from './schema';

type Db = BetterSQLite3Database<Record<string, never>>;

/**
 * better-sqlite3 surfaces a unique-constraint violation as a `SqliteError` with
 * `code === 'SQLITE_CONSTRAINT_UNIQUE'`. Match on the code (stable across driver
 * versions), not the message.
 */
export function isSqliteUniqueViolation(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    (error as { code?: unknown }).code === 'SQLITE_CONSTRAINT_UNIQUE'
  );
}

/**
 * SQLite (better-sqlite3) inbox store. `runOnce` runs **synchronously**: it
 * inserts the `(source, messageKey)` dedup row and, on a fresh key, runs the
 * synchronous side effect in the same transaction. The handler MUST be
 * synchronous + DB-only (better-sqlite3 cannot suspend a synchronous tx).
 */
export class SqliteInboxStore implements InboxStore {
  runOnce(
    db: unknown,
    messageKey: string,
    source: string,
    handler: InboxSideEffect,
  ): RunOnceOutcome {
    const now = new Date().toISOString();
    try {
      (db as Db)
        .insert(inboxEvents)
        .values({
          id: randomUUID(),
          messageKey,
          source,
          status: 'processed',
          processedAt: now,
          createdAt: now,
        })
        .run();
    } catch (error) {
      if (isSqliteUniqueViolation(error)) return 'duplicate';
      throw error;
    }
    void handler();
    return 'processed';
  }
}
