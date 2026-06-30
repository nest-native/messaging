import type { ModuleMetadata } from '@nestjs/common';
import type { OutboxTransport } from './transport';

export const OUTBOX_STATUSES = [
  'pending',
  'processing',
  'completed',
  'failed',
] as const;
export type OutboxStatus = (typeof OUTBOX_STATUSES)[number];

export const INBOX_STATUSES = ['processed', 'dead_lettered'] as const;
export type InboxStatus = (typeof INBOX_STATUSES)[number];

/**
 * The dialect-agnostic shape of an outbox row as the engine reasons about it.
 * Both the SQLite and Postgres stores map their Drizzle rows to this shape, so
 * the claimer never sees a dialect-specific type.
 */
export interface OutboxEventRow {
  id: string;
  topic: string;
  payload: Record<string, unknown>;
  status: OutboxStatus;
  attempts: number;
  maxAttempts: number;
  idempotencyKey: string | null;
  availableAt: string;
  claimedAt: string | null;
  claimedBy: string | null;
  processedAt: string | null;
  lastError: string | null;
  createdAt: string;
}

/** What a caller supplies to enqueue an event into the outbox. */
export interface EnqueueInput {
  topic: string;
  payload: Record<string, unknown>;
  idempotencyKey?: string;
  availableAt?: Date;
  maxAttempts?: number;
}

/** Fully-resolved claimer configuration (defaults applied). */
export interface ResolvedClaimerConfig {
  workerInstanceId: string;
  stuckTimeoutMs: number;
  batchSize: number;
  baseBackoffMs: number;
  maxBackoffMs: number;
}
export type ClaimerConfig = Partial<ResolvedClaimerConfig>;

/**
 * The DB side effect `runOnce` applies exactly once, inside the dedup
 * transaction. On the **sqlite** store it MUST be synchronous + DB-only
 * (better-sqlite3 cannot suspend a synchronous transaction); on the **postgres**
 * store an async DB-only handler is fine.
 */
export type InboxSideEffect = () => void | Promise<void>;
export type RunOnceOutcome = 'processed' | 'duplicate';

/**
 * The transactional persistence seam for the outbox. Each implementation is
 * dialect-specific and owns its Drizzle tables + the sync/async query execution;
 * `db` is passed per call (the tx-scoped instance for {@link enqueue}, the base
 * instance for the rest) and is intentionally opaque (`unknown`) to the engine.
 *
 * `enqueue` returns the store's native shape — the **sqlite** store returns a
 * synchronous `OutboxEventRow` (so it can be called inside a synchronous
 * `@Transactional` body); the **postgres** store returns a `Promise`.
 */
export interface OutboxStore {
  enqueue(db: unknown, input: EnqueueInput): OutboxEventRow | Promise<OutboxEventRow>;
  claimBatch(db: unknown, cfg: ResolvedClaimerConfig): Promise<OutboxEventRow[]>;
  markCompleted(db: unknown, id: string): Promise<void>;
  retry(db: unknown, id: string, delayMs: number, lastError?: string): Promise<void>;
  markFailed(db: unknown, id: string, reason: string): Promise<void>;
}

/**
 * The transactional dedup seam for the inbox. `runOnce` inserts the dedup row
 * and runs the side effect in one transaction; a unique-constraint violation on
 * `(source, messageKey)` means "already processed" → `'duplicate'`. Returns the
 * store's native shape (sync on sqlite, `Promise` on postgres).
 */
export interface InboxStore {
  runOnce(
    db: unknown,
    messageKey: string,
    source: string,
    handler: InboxSideEffect,
  ): RunOnceOutcome | Promise<RunOnceOutcome>;
}

/** Options for {@link MessagingModule.forRoot}. */
export interface MessagingModuleOptions {
  /**
   * Token of the base (non-transactional) Drizzle instance — the same instance
   * the `@nestjs-cls/transactional` Drizzle adapter is configured with. The
   * claimer uses it to open its own claim transaction.
   */
  drizzleInstanceToken: symbol | string;
  /**
   * Modules that provide (and export) the `drizzleInstanceToken`. Required when
   * that token is not registered by a global module — `MessagingModule` imports
   * these so it can resolve the Drizzle instance.
   */
  imports?: ModuleMetadata['imports'];
  /** The dialect-specific outbox store. */
  outboxStore: OutboxStore;
  /** The dialect-specific inbox store (omit if you only use the outbox half). */
  inboxStore?: InboxStore;
  /**
   * The publish transport the claimer relays through — `KafkaOutboxTransport`
   * (`@nest-native/messaging/kafka`) in production, or the in-memory transport
   * (`@nest-native/messaging/testing`) in tests.
   */
  transport: OutboxTransport;
  /** Register the module globally (default: true). */
  isGlobal?: boolean;
}
