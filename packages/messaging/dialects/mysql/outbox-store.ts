import { randomUUID } from 'node:crypto';
import { and, eq, inArray, lte, or, sql } from 'drizzle-orm';
import type { MySql2Database } from 'drizzle-orm/mysql2';
import type {
  EnqueueInput,
  OutboxClaim,
  OutboxEventRow,
  OutboxStore,
  ResolvedClaimerConfig,
} from '../../interfaces';
import { outboxEvents } from './schema';

type Db = MySql2Database<Record<string, never>>;

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
 * MySQL (mysql2) outbox store. Every method is **asynchronous** — `enqueue`
 * awaits the insert (call it with `await` inside an async `@Transactional` body),
 * and the claimer's batch claim runs in an async transaction.
 *
 * Unlike Postgres, MySQL's `INSERT` has no `RETURNING`, so `enqueue` inserts the
 * row (client-generated UUID id) and reads it back by id within the same
 * transaction to return the canonical {@link OutboxEventRow}.
 */
export class MysqlOutboxStore implements OutboxStore {
  async enqueue(db: unknown, input: EnqueueInput<object>): Promise<OutboxEventRow> {
    const id = randomUUID();
    const now = new Date().toISOString();
    await (db as Db).insert(outboxEvents).values({
      id,
      topic: input.topic,
      // The one place the structural input payload widens to the stored shape.
      payload: input.payload as Record<string, unknown>,
      status: 'pending',
      maxAttempts: input.maxAttempts ?? 10,
      idempotencyKey: input.idempotencyKey ?? null,
      availableAt: (input.availableAt ?? new Date()).toISOString(),
      createdAt: now,
    });
    const [row] = await (db as Db)
      .select()
      .from(outboxEvents)
      .where(eq(outboxEvents.id, id));
    return row;
  }

  async claimBatch(
    db: unknown,
    cfg: ResolvedClaimerConfig,
  ): Promise<OutboxEventRow[]> {
    return (db as Db).transaction(async (tx) => {
      // Stamped once the connection is ours: a checkout that waited on a busy
      // pool must not leave this claim looking older than it is, or another
      // worker would treat its rows as stuck that much sooner.
      const now = new Date();
      const nowIso = now.toISOString();
      const stuckCutoff = new Date(now.getTime() - cfg.stuckTimeoutMs).toISOString();
      const candidates = await tx
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
        .for('update', { skipLocked: true });
      if (candidates.length === 0) return [];
      const ids = candidates.map((c) => c.id);
      await tx
        .update(outboxEvents)
        .set({ status: 'processing', claimedAt: nowIso, claimedBy: cfg.workerInstanceId })
        .where(inArray(outboxEvents.id, ids));
      return tx.select().from(outboxEvents).where(inArray(outboxEvents.id, ids));
      // READ COMMITTED, not InnoDB's default REPEATABLE READ: there the locking
      // read also locks the gaps it scans, so every concurrent enqueue INSERT
      // would wait for the claim to commit.
    }, { isolationLevel: 'read committed' });
  }

  async markCompleted(db: unknown, claim: OutboxClaim): Promise<boolean> {
    const [result] = await (db as Db)
      .update(outboxEvents)
      .set({ status: 'completed', processedAt: new Date().toISOString(), lastError: null })
      .where(heldBy(claim));
    return result.affectedRows > 0;
  }

  async retry(
    db: unknown,
    claim: OutboxClaim,
    delayMs: number,
    lastError?: string,
  ): Promise<boolean> {
    const nextAvailable = new Date(Date.now() + delayMs).toISOString();
    const [result] = await (db as Db)
      .update(outboxEvents)
      .set({
        status: 'pending',
        attempts: sql`${outboxEvents.attempts} + 1`,
        availableAt: nextAvailable,
        claimedAt: null,
        claimedBy: null,
        lastError: lastError ?? null,
      })
      .where(heldBy(claim));
    return result.affectedRows > 0;
  }

  async release(db: unknown, claim: OutboxClaim): Promise<boolean> {
    const [result] = await (db as Db)
      .update(outboxEvents)
      .set({ status: 'pending', claimedAt: null, claimedBy: null })
      .where(heldBy(claim));
    return result.affectedRows > 0;
  }

  async markFailed(db: unknown, claim: OutboxClaim, reason: string): Promise<boolean> {
    const [result] = await (db as Db)
      .update(outboxEvents)
      .set({
        status: 'failed',
        attempts: sql`${outboxEvents.attempts} + 1`,
        lastError: reason,
        processedAt: new Date().toISOString(),
      })
      .where(heldBy(claim));
    return result.affectedRows > 0;
  }
}
