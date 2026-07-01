import { randomUUID } from 'node:crypto';
import { and, eq, inArray, lte, or, sql } from 'drizzle-orm';
import type { MySql2Database } from 'drizzle-orm/mysql2';
import type {
  EnqueueInput,
  OutboxEventRow,
  OutboxStore,
  ResolvedClaimerConfig,
} from '../../interfaces';
import { outboxEvents } from './schema';

type Db = MySql2Database<Record<string, never>>;

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
  async enqueue(db: unknown, input: EnqueueInput): Promise<OutboxEventRow> {
    const id = randomUUID();
    const now = new Date().toISOString();
    await (db as Db).insert(outboxEvents).values({
      id,
      topic: input.topic,
      payload: input.payload,
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
    const now = new Date();
    const nowIso = now.toISOString();
    const stuckCutoff = new Date(now.getTime() - cfg.stuckTimeoutMs).toISOString();
    return (db as Db).transaction(async (tx) => {
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
        .limit(cfg.batchSize);
      if (candidates.length === 0) return [];
      const ids = candidates.map((c) => c.id);
      await tx
        .update(outboxEvents)
        .set({ status: 'processing', claimedAt: nowIso, claimedBy: cfg.workerInstanceId })
        .where(inArray(outboxEvents.id, ids));
      return tx.select().from(outboxEvents).where(inArray(outboxEvents.id, ids));
    });
  }

  async markCompleted(db: unknown, id: string): Promise<void> {
    await (db as Db)
      .update(outboxEvents)
      .set({ status: 'completed', processedAt: new Date().toISOString(), lastError: null })
      .where(eq(outboxEvents.id, id));
  }

  async retry(
    db: unknown,
    id: string,
    delayMs: number,
    lastError?: string,
  ): Promise<void> {
    const nextAvailable = new Date(Date.now() + delayMs).toISOString();
    await (db as Db)
      .update(outboxEvents)
      .set({
        status: 'pending',
        attempts: sql`${outboxEvents.attempts} + 1`,
        availableAt: nextAvailable,
        claimedAt: null,
        claimedBy: null,
        lastError: lastError ?? null,
      })
      .where(eq(outboxEvents.id, id));
  }

  async markFailed(db: unknown, id: string, reason: string): Promise<void> {
    await (db as Db)
      .update(outboxEvents)
      .set({
        status: 'failed',
        attempts: sql`${outboxEvents.attempts} + 1`,
        lastError: reason,
        processedAt: new Date().toISOString(),
      })
      .where(eq(outboxEvents.id, id));
  }
}
