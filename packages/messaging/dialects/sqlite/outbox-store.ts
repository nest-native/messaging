import { randomUUID } from 'node:crypto';
import { and, eq, inArray, lte, or, sql } from 'drizzle-orm';
import type { BetterSQLite3Database } from 'drizzle-orm/better-sqlite3';
import type {
  EnqueueInput,
  OutboxEventRow,
  OutboxStore,
  ResolvedClaimerConfig,
} from '../../interfaces';
import { outboxEvents } from './schema';

type Db = BetterSQLite3Database<Record<string, never>>;

/**
 * SQLite (better-sqlite3) outbox store. Every method runs **synchronously** —
 * `enqueue` returns the row directly so it composes inside a synchronous
 * `@Transactional` body, and the rest wrap their synchronous result in a
 * resolved Promise for the engine to await from outside the transaction.
 */
export class SqliteOutboxStore implements OutboxStore {
  enqueue(db: unknown, input: EnqueueInput): OutboxEventRow {
    const now = new Date().toISOString();
    return (db as Db)
      .insert(outboxEvents)
      .values({
        id: randomUUID(),
        topic: input.topic,
        payload: input.payload,
        status: 'pending',
        maxAttempts: input.maxAttempts ?? 10,
        idempotencyKey: input.idempotencyKey ?? null,
        availableAt: (input.availableAt ?? new Date()).toISOString(),
        createdAt: now,
      })
      .returning()
      .get();
  }

  claimBatch(
    db: unknown,
    cfg: ResolvedClaimerConfig,
  ): Promise<OutboxEventRow[]> {
    const now = new Date();
    const nowIso = now.toISOString();
    const stuckCutoff = new Date(now.getTime() - cfg.stuckTimeoutMs).toISOString();
    const rows = (db as Db).transaction((tx) => {
      const candidates = tx
        .select({ id: outboxEvents.id })
        .from(outboxEvents)
        .where(
          or(
            and(
              eq(outboxEvents.status, 'pending'),
              lte(outboxEvents.availableAt, nowIso),
            ),
            and(
              eq(outboxEvents.status, 'processing'),
              lte(outboxEvents.claimedAt, stuckCutoff),
            ),
          ),
        )
        .limit(cfg.batchSize)
        .all();
      if (candidates.length === 0) return [];
      const ids = candidates.map((c) => c.id);
      tx.update(outboxEvents)
        .set({ status: 'processing', claimedAt: nowIso, claimedBy: cfg.workerInstanceId })
        .where(inArray(outboxEvents.id, ids))
        .run();
      return tx.select().from(outboxEvents).where(inArray(outboxEvents.id, ids)).all();
    });
    return Promise.resolve(rows);
  }

  markCompleted(db: unknown, id: string): Promise<void> {
    (db as Db)
      .update(outboxEvents)
      .set({ status: 'completed', processedAt: new Date().toISOString(), lastError: null })
      .where(eq(outboxEvents.id, id))
      .run();
    return Promise.resolve();
  }

  retry(db: unknown, id: string, delayMs: number, lastError?: string): Promise<void> {
    const nextAvailable = new Date(Date.now() + delayMs).toISOString();
    (db as Db)
      .update(outboxEvents)
      .set({
        status: 'pending',
        attempts: sql`${outboxEvents.attempts} + 1`,
        availableAt: nextAvailable,
        claimedAt: null,
        claimedBy: null,
        lastError: lastError ?? null,
      })
      .where(eq(outboxEvents.id, id))
      .run();
    return Promise.resolve();
  }

  markFailed(db: unknown, id: string, reason: string): Promise<void> {
    (db as Db)
      .update(outboxEvents)
      .set({
        status: 'failed',
        attempts: sql`${outboxEvents.attempts} + 1`,
        lastError: reason,
        processedAt: new Date().toISOString(),
      })
      .where(eq(outboxEvents.id, id))
      .run();
    return Promise.resolve();
  }
}
