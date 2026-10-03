import { randomUUID } from 'node:crypto';
import { and, eq, inArray, lte, or, sql } from 'drizzle-orm';
import type { BetterSQLite3Database } from 'drizzle-orm/better-sqlite3';
import type {
  EnqueueInput,
  OutboxClaim,
  OutboxEventRow,
  OutboxStore,
  ResolvedClaimerConfig,
} from '../../interfaces';
import { outboxEvents } from './schema';

type Db = BetterSQLite3Database<Record<string, never>>;

/**
 * Matches the row only while `claim` still holds it: same row, still
 * `processing`, under exactly the stamp `claimBatch` wrote. A stale worker's
 * transition then matches nothing — whether another worker reclaimed the row or
 * a later claim reused the same `workerInstanceId`.
 */
const heldBy = (claim: OutboxClaim) =>
  and(
    eq(outboxEvents.id, claim.id),
    eq(outboxEvents.status, 'processing'),
    eq(outboxEvents.claimedBy, claim.claimedBy),
    eq(outboxEvents.claimedAt, claim.claimedAt),
  );

/**
 * SQLite (better-sqlite3) outbox store. Every method runs **synchronously** —
 * `enqueue` returns the row directly so it composes inside a synchronous
 * `@Transactional` body, and the rest wrap their synchronous result in a
 * resolved Promise for the engine to await from outside the transaction.
 */
export class SqliteOutboxStore implements OutboxStore {
  enqueue(db: unknown, input: EnqueueInput<object>): OutboxEventRow {
    const now = new Date().toISOString();
    return (db as Db)
      .insert(outboxEvents)
      .values({
        id: randomUUID(),
        topic: input.topic,
        // The one place the structural input payload widens to the stored shape.
        payload: input.payload as Record<string, unknown>,
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
      // Stryker disable next-line ConditionalExpression: query-saving early return — skipping it is behaviourally identical (inArray([]) matches nothing)
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

  markCompleted(db: unknown, claim: OutboxClaim): Promise<boolean> {
    const { changes } = (db as Db)
      .update(outboxEvents)
      .set({ status: 'completed', processedAt: new Date().toISOString(), lastError: null })
      .where(heldBy(claim))
      .run();
    return Promise.resolve(changes > 0);
  }

  retry(db: unknown, claim: OutboxClaim, delayMs: number, lastError?: string): Promise<boolean> {
    const nextAvailable = new Date(Date.now() + delayMs).toISOString();
    const { changes } = (db as Db)
      .update(outboxEvents)
      .set({
        status: 'pending',
        attempts: sql`${outboxEvents.attempts} + 1`,
        availableAt: nextAvailable,
        claimedAt: null,
        claimedBy: null,
        lastError: lastError ?? null,
      })
      .where(heldBy(claim))
      .run();
    return Promise.resolve(changes > 0);
  }

  markFailed(db: unknown, claim: OutboxClaim, reason: string): Promise<boolean> {
    const { changes } = (db as Db)
      .update(outboxEvents)
      .set({
        status: 'failed',
        attempts: sql`${outboxEvents.attempts} + 1`,
        lastError: reason,
        processedAt: new Date().toISOString(),
      })
      .where(heldBy(claim))
      .run();
    return Promise.resolve(changes > 0);
  }
}
