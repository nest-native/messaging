import { randomUUID } from 'node:crypto';
import { and, eq, inArray, lte, or, sql } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import type {
  EnqueueInput,
  OutboxClaim,
  OutboxEventRow,
  OutboxStore,
  ResolvedClaimerConfig,
} from '../../interfaces';
import { outboxEvents } from './schema';
import { assertValidWakeChannel } from './wake';

type Db = NodePgDatabase<Record<string, never>>;

/** A node-postgres `Pool`, recognized by shape so that loading this module never loads `pg`. */
interface PgPool {
  connect(): Promise<PgPoolClient>;
  totalCount: number;
}
interface PgPoolClient {
  query(text: string): Promise<unknown>;
  on(event: 'error', listener: (error: Error) => void): unknown;
  removeListener(event: 'error', listener: (error: Error) => void): unknown;
  release(error?: Error): void;
}

const isPgPool = (client: unknown): client is PgPool =>
  typeof client === 'object' &&
  client !== null &&
  typeof (client as Partial<PgPool>).connect === 'function' &&
  typeof (client as Partial<PgPool>).totalCount === 'number';

/** Where a drizzle database keeps its query logger and cache (drizzle internals, read defensively). */
interface DrizzleSessionOwner {
  session?: { options?: { logger?: unknown; cache?: unknown } };
}

/**
 * Runs `work` in its own READ COMMITTED transaction, whatever the server
 * default.
 *
 * On a node-postgres `Pool` it checks the client out itself instead of going
 * through drizzle's `transaction()`, which leaves the checked-out client without
 * an `error` listener and sends BEGIN outside its cleanup. A connection the
 * server dropped mid-transaction (a failover, `pg_terminate_backend`) then
 * crashed the process, and one dropped at BEGIN was never returned to the pool.
 * Here the client listens for errors while it is out, a failed ROLLBACK never
 * hides the original error, and a broken connection is released with its error
 * so the pool discards it. Anything else (PGlite, a single `Client`) goes through
 * drizzle's `transaction()`.
 */
async function readCommitted<T>(db: unknown, work: (tx: Db) => Promise<T>): Promise<T> {
  const pool = (db as { $client?: unknown }).$client;
  if (!isPgPool(pool)) {
    return (db as Db).transaction((tx) => work(tx as unknown as Db), {
      isolationLevel: 'read committed',
    });
  }
  // The pool is shaped like node-postgres's, so its drizzle driver (and `pg`)
  // loads here, before a client is checked out. The caller's query logger and
  // cache carry over, so the store's statements show up where the
  // application's do.
  const { drizzle } = await import('drizzle-orm/node-postgres');
  const { logger, cache } = (db as DrizzleSessionOwner).session?.options ?? {};
  const client = await pool.connect();
  let broken: Error | undefined;
  const onError = (error: Error): void => {
    broken = error;
  };
  client.on('error', onError);
  let result: T;
  try {
    await client.query('begin isolation level read committed');
    result = await work(drizzle(client as never, { logger, cache } as never) as unknown as Db);
    await client.query('commit');
  } catch (error) {
    await client.query('rollback').catch((rollbackError: Error) => {
      broken ??= rollbackError;
    });
    throw error;
  } finally {
    client.removeListener('error', onError);
    client.release(broken);
  }
  return result;
}

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
 * Runs one fenced transition and reports whether it wrote the row. Each runs in
 * its own READ COMMITTED transaction, like the claim: under a SERIALIZABLE
 * server default the fenced UPDATE's scan (on the status index, in steady state)
 * makes two workers' transitions abort each other (40001) while both claims
 * still hold their rows. Under READ COMMITTED an UPDATE that waited on a
 * reclaim re-checks the fence against the committed row instead, so `false`
 * means the claim really was taken over, and any error is a real one.
 */
const fenced = (
  db: unknown,
  update: (tx: Db) => PromiseLike<{ id: string }[]>,
): Promise<boolean> => readCommitted(db, async (tx) => (await update(tx)).length > 0);

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
    // READ COMMITTED whatever the database default: under REPEATABLE READ or
    // SERIALIZABLE, a row another worker claims while this scan is running
    // fails the whole claim (40001) instead of being skipped.
    return readCommitted(db, async (tx) => {
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

  async markCompleted(db: unknown, claim: OutboxClaim): Promise<boolean> {
    return fenced(db, (tx) =>
      tx
        .update(outboxEvents)
        .set({ status: 'completed', processedAt: new Date().toISOString(), lastError: null })
        .where(heldBy(claim))
        .returning({ id: outboxEvents.id }),
    );
  }

  async retry(
    db: unknown,
    claim: OutboxClaim,
    delayMs: number,
    lastError?: string,
  ): Promise<boolean> {
    const nextAvailable = new Date(Date.now() + delayMs).toISOString();
    return fenced(db, (tx) =>
      tx
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
        .returning({ id: outboxEvents.id }),
    );
  }

  async markFailed(db: unknown, claim: OutboxClaim, reason: string): Promise<boolean> {
    return fenced(db, (tx) =>
      tx
        .update(outboxEvents)
        .set({
          status: 'failed',
          attempts: sql`${outboxEvents.attempts} + 1`,
          lastError: reason,
          processedAt: new Date().toISOString(),
        })
        .where(heldBy(claim))
        .returning({ id: outboxEvents.id }),
    );
  }
}
