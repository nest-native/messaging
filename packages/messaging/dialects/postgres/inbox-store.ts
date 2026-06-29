import { randomUUID } from 'node:crypto';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import type {
  InboxSideEffect,
  InboxStore,
  RunOnceOutcome,
} from '../../interfaces';
import { inboxEvents } from './schema';

type Db = NodePgDatabase<Record<string, never>>;

/**
 * Postgres surfaces a unique-constraint violation with SQLSTATE `23505`
 * (`unique_violation`). node-postgres exposes it as `error.code === '23505'`.
 */
export function isPgUniqueViolation(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    (error as { code?: unknown }).code === '23505'
  );
}

/**
 * Postgres (node-postgres) inbox store. `runOnce` is **asynchronous**: it awaits
 * the dedup-row insert and, on a fresh key, awaits the side effect in the same
 * transaction. The handler may be async (DB-only).
 */
export class PostgresInboxStore implements InboxStore {
  async runOnce(
    db: unknown,
    messageKey: string,
    source: string,
    handler: InboxSideEffect,
  ): Promise<RunOnceOutcome> {
    const now = new Date().toISOString();
    try {
      await (db as Db).insert(inboxEvents).values({
        id: randomUUID(),
        messageKey,
        source,
        status: 'processed',
        processedAt: now,
        createdAt: now,
      });
    } catch (error) {
      if (isPgUniqueViolation(error)) return 'duplicate';
      throw error;
    }
    await handler();
    return 'processed';
  }
}
