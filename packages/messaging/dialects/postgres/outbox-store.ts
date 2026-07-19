import { randomUUID } from 'node:crypto';
import { and, eq, inArray, lte, or, sql } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import type {
  EnqueueInput,
  OutboxEventRow,
  OutboxStore,
  ResolvedClaimerConfig,
} from '../../interfaces';
import { outboxEvents } from './schema';
import { assertValidWakeChannel } from './wake';

type Db = NodePgDatabase<Record<string, never>>;

export interface PostgresOutboxStoreOptions {
  /**
   * When set, `enqueue` also runs `pg_notify(wakeChannel, '')` on the same
   * handle — inside the caller's transaction, so Postgres delivers the wake
   * **on commit** and drops it on rollback: the signal is atomic with the event
   * becoming visible. Pair it with a `PostgresWakeListener` on the workers.
   */
  wakeChannel?: string;
}

/**
 * Postgres (node-postgres) outbox store. Every method is **asynchronous** —
 * `enqueue` awaits the insert (call it with `await` inside an async
 * `@Transactional` body), and the claimer's batch claim runs in an async
 * transaction.
 */
export class PostgresOutboxStore implements OutboxStore {
  constructor(private readonly options: PostgresOutboxStoreOptions = {}) {
    if (this.options.wakeChannel !== undefined) {
      assertValidWakeChannel(this.options.wakeChannel);
    }
  }

  async enqueue(db: unknown, input: EnqueueInput<object>): Promise<OutboxEventRow> {
    const now = new Date().toISOString();
    const [row] = await (db as Db)
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
      .returning();
    if (this.options.wakeChannel !== undefined) {
      // pg_notify takes the channel as a plain string parameter (safely
      // parameterized, unlike LISTEN's identifier) — delivered on commit.
      await (db as Db).execute(
        sql`select pg_notify(${this.options.wakeChannel}, '')`,
      );
    }
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
      // Stryker disable next-line ConditionalExpression: query-saving early return — skipping it is behaviourally identical (inArray([]) matches nothing)
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
