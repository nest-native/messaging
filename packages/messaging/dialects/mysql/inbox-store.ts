import { randomUUID } from 'node:crypto';
import type { MySql2Database } from 'drizzle-orm/mysql2';
import type {
  InboxSideEffect,
  InboxStore,
  RunOnceOutcome,
} from '../../interfaces';
import { inboxEvents } from './schema';

type Db = MySql2Database<Record<string, never>>;

/**
 * MySQL surfaces a unique-constraint violation as error code `ER_DUP_ENTRY`
 * (errno `1062`). mysql2 sets both `error.code === 'ER_DUP_ENTRY'` and
 * `error.errno === 1062`; Drizzle wraps driver errors in a `DrizzleQueryError`,
 * so the code/errno may instead sit on `error.cause`. Check the code **and** the
 * errno, on the error and on its `cause`, so the predicate is robust to both the
 * driver's shape and Drizzle's wrapping.
 */
export function isMysqlUniqueViolation(error: unknown): boolean {
  return isDuplicate(error) || isDuplicate((error as { cause?: unknown })?.cause);
}

function isDuplicate(error: unknown): boolean {
  return hasProp(error, 'code', 'ER_DUP_ENTRY') || hasProp(error, 'errno', 1062);
}

function hasProp(error: unknown, key: string, value: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    key in error &&
    (error as Record<string, unknown>)[key] === value
  );
}

/**
 * MySQL (mysql2) inbox store. `runOnce` is **asynchronous**: it awaits the
 * dedup-row insert and, on a fresh key, awaits the side effect in the same
 * transaction. The handler may be async (DB-only).
 */
export class MysqlInboxStore implements InboxStore {
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
      if (isMysqlUniqueViolation(error)) return 'duplicate';
      throw error;
    }
    await handler();
    return 'processed';
  }
}
