import { hostname } from 'node:os';
import { Inject, Injectable, Logger } from '@nestjs/common';
import type {
  ClaimerConfig,
  OutboxClaim,
  OutboxEventRow,
  OutboxStore,
  ResolvedClaimerConfig,
} from './interfaces';
import { MESSAGING_DRIZZLE, OUTBOX_STORE } from './tokens';
import {
  OUTBOX_TRANSPORT,
  type OutboxTransport,
  PermanentError,
  RetryableError,
} from './transport';

export const DEFAULT_CLAIMER_CONFIG: ResolvedClaimerConfig = {
  workerInstanceId: `${hostname()}-${process.pid}`,
  stuckTimeoutMs: 60_000,
  batchSize: 32,
  baseBackoffMs: 1_000,
  maxBackoffMs: 60_000,
};

// The furthest a Date can sit from the epoch: a stuck cutoff beyond it is an
// invalid date, and every claim would throw.
const MAX_DATE_OFFSET_MS = 8.64e15;

// A stamp value for a log line: a string quoted, anything else by its type, so
// a Date does not pass for a valid stamp and a BigInt cannot throw.
const stampShown = (value: unknown): string =>
  typeof value === 'string' ? JSON.stringify(value) : Object.prototype.toString.call(value).slice(8, -1);

// A rejected value as the caller wrote it: a string from an env-backed config
// keeps its quotes, so `"60000" (string)` does not pass for the number 60000.
const shown = (value: unknown): string =>
  typeof value === 'number' ? String(value) : `${JSON.stringify(value)} (${typeof value})`;

/**
 * Applies `overrides` over {@link DEFAULT_CLAIMER_CONFIG}, as `tick()` does. A
 * key set to `undefined` keeps its default: `{ workerInstanceId:
 * process.env.WORKER_ID }` with the variable unset must not claim rows under no
 * owner, which no transition could then match. A value that would break
 * claiming throws, so a worker can call this at startup to check its config.
 */
export function resolveClaimerConfig(
  overrides: ClaimerConfig | null = {},
): ResolvedClaimerConfig {
  const defined = Object.fromEntries(
    Object.entries(overrides ?? {}).filter(([, value]) => value !== undefined),
  ) as Partial<ResolvedClaimerConfig>;
  const cfg: ResolvedClaimerConfig = { ...DEFAULT_CLAIMER_CONFIG, ...defined };
  if (typeof cfg.workerInstanceId !== 'string' || cfg.workerInstanceId.trim() === '') {
    throw new TypeError(
      `claimer workerInstanceId must be a non-empty string, got ${shown(cfg.workerInstanceId)}`,
    );
  }
  if (!Number.isInteger(cfg.batchSize) || cfg.batchSize < 1) {
    throw new RangeError(`claimer batchSize must be a positive integer, got ${shown(cfg.batchSize)}`);
  }
  // At zero every processing row is stuck the moment it is claimed, so two
  // workers would publish the same event side by side.
  const stuck = cfg.stuckTimeoutMs;
  if (!(typeof stuck === 'number' && stuck > 0 && stuck <= MAX_DATE_OFFSET_MS)) {
    throw new RangeError(
      `claimer stuckTimeoutMs must be a positive number of milliseconds within Date's range, got ${shown(stuck)}`,
    );
  }
  for (const key of ['baseBackoffMs', 'maxBackoffMs'] as const) {
    if (!(Number.isFinite(cfg[key]) && cfg[key] >= 0)) {
      throw new RangeError(`claimer ${key} must be a non-negative number, got ${shown(cfg[key])}`);
    }
  }
  return cfg;
}

export interface TickReport {
  claimed: number;
  completed: number;
  retried: number;
  failed: number;
  /**
   * Claimed events this worker let go without recording an outcome. Either the
   * batch had been held longer than `stuckTimeoutMs` by the time the event's
   * turn came, or another claim took the row over before the outcome could be
   * recorded; whoever claims such an event next delivers it, and a steady
   * non-zero count means batches take longer than `stuckTimeoutMs`. A row the
   * store returned without its claim's stamp counts here too, logged as an
   * error.
   */
  lost: number;
}

type ProcessOutcome = 'completed' | 'retried' | 'failed' | 'lost';

/**
 * Drains committed outbox rows to the transport. `tick()` claims a batch (the
 * store opens its own transaction), publishes each through the {@link
 * OutboxTransport}, and records the result. Runs in a background worker — never
 * inside a business transaction — so it freely awaits the store and transport.
 */
@Injectable()
export class OutboxClaimer {
  private readonly logger = new Logger(OutboxClaimer.name);

  constructor(
    @Inject(MESSAGING_DRIZZLE) private readonly db: unknown,
    @Inject(OUTBOX_STORE) private readonly store: OutboxStore,
    @Inject(OUTBOX_TRANSPORT) private readonly transport: OutboxTransport,
  ) {}

  async tick(overrides: ClaimerConfig = {}): Promise<TickReport> {
    const cfg = resolveClaimerConfig(overrides);
    const claimed = await this.store.claimBatch(this.db, cfg);
    // Measured from the batch's arrival, so its first event is always published
    // and a claim that itself took longer than stuckTimeoutMs still makes
    // progress.
    const heldSince = Date.now();
    const report: TickReport = {
      claimed: claimed.length,
      completed: 0,
      retried: 0,
      failed: 0,
      lost: 0,
    };
    // Once the batch has been held past stuckTimeoutMs, another worker may
    // already have reclaimed its remaining rows; publishing them here as well
    // would only deliver them twice.
    const expired: string[] = [];
    for (const event of claimed) {
      if (Date.now() - heldSince >= cfg.stuckTimeoutMs) {
        expired.push(event.id);
        continue;
      }
      report[await this.processOne(event, cfg)] += 1;
    }
    if (expired.length > 0) {
      report.lost += expired.length;
      this.logger.warn(
        `skipped ${expired.length} claimed outbox event(s) [${expired.join(', ')}]: the batch was held longer than stuckTimeoutMs (${cfg.stuckTimeoutMs} ms), so another worker may own them now; raise stuckTimeoutMs or lower batchSize`,
      );
    }
    return report;
  }

  private async processOne(
    event: OutboxEventRow,
    cfg: ResolvedClaimerConfig,
  ): Promise<ProcessOutcome> {
    const claim = this.claimOf(event);
    if (claim === undefined) return 'lost';
    try {
      await this.transport.publish({
        id: event.id,
        topic: event.topic,
        payload: event.payload,
        idempotencyKey: event.idempotencyKey ?? undefined,
      });
    } catch (error) {
      return this.onPublishError(event, claim, cfg, error);
    }
    // Outside the try: failing to record a delivery is not a failed publish.
    // Treating it as one burned an attempt and could mark a delivered event
    // failed. The tick throws instead, and the event, still claimed, is
    // published again once its claim goes stale.
    return this.settle(event, await this.store.markCompleted(this.db, claim), 'completed');
  }

  /**
   * The claim to publish under, or `undefined` when `claimedBy` or `claimedAt`
   * is not a string: published without a usable stamp, the event would be
   * delivered with no transition able to record it, and again after every
   * stuck timeout. Whether a string stamp is the one the claim wrote cannot be
   * told apart here; that is the store's contract.
   */
  private claimOf(event: OutboxEventRow): OutboxClaim | undefined {
    const { id, claimedBy, claimedAt } = event;
    if (typeof claimedBy === 'string' && typeof claimedAt === 'string') {
      return { id, claimedBy, claimedAt };
    }
    this.logger.error(
      `outbox event ${id} came back from claimBatch without a string claim stamp (claimedBy ${stampShown(claimedBy)}, claimedAt ${stampShown(claimedAt)}); a store must return every row it claims as its claiming UPDATE left it, with claimedBy and claimedAt as strings`,
    );
    return undefined;
  }

  /**
   * `outcome` when the transition applied; otherwise another claim took the
   * row over first. `reason` is the publish error, for the outcomes it caused.
   */
  private settle(
    event: OutboxEventRow,
    applied: boolean,
    outcome: Exclude<ProcessOutcome, 'lost'>,
    reason?: string,
  ): ProcessOutcome {
    if (applied) return outcome;
    this.logger.warn(
      reason === undefined
        ? `outbox event ${event.id} was published, but another claim took it over before it was marked completed; the new owner records its outcome and may publish it again`
        : `outbox event ${event.id} failed to publish (${reason}), but another claim took it over before it was marked ${outcome}; leaving it to the new owner`,
    );
    return 'lost';
  }

  private async onPublishError(
    event: OutboxEventRow,
    claim: OutboxClaim,
    cfg: ResolvedClaimerConfig,
    error: unknown,
  ): Promise<ProcessOutcome> {
    const message = error instanceof Error ? error.message : String(error);
    // Permanent: retrying can never succeed — fail now instead of burning attempts.
    if (error instanceof PermanentError) {
      return this.fail(event, claim, message);
    }
    // Retryable: schedule another attempt, honouring a transport-supplied delay.
    if (error instanceof RetryableError) {
      const delay = error.delayMs ?? this.backoff(event.attempts, cfg);
      return this.settle(event, await this.store.retry(this.db, claim, delay, message), 'retried', message);
    }
    // Anything else: retry with backoff until maxAttempts, then fail.
    if (event.attempts + 1 >= event.maxAttempts) {
      return this.fail(event, claim, message);
    }
    const delay = this.backoff(event.attempts, cfg);
    return this.settle(event, await this.store.retry(this.db, claim, delay, message), 'retried', message);
  }

  private async fail(
    event: OutboxEventRow,
    claim: OutboxClaim,
    reason: string,
  ): Promise<ProcessOutcome> {
    const applied = await this.store.markFailed(this.db, claim, reason);
    if (applied) this.logger.warn(`outbox event ${event.id} failed: ${reason}`);
    return this.settle(event, applied, 'failed', reason);
  }

  private backoff(attempts: number, cfg: ResolvedClaimerConfig): number {
    const base = cfg.baseBackoffMs * 2 ** attempts;
    const capped = Math.min(base, cfg.maxBackoffMs);
    return capped + Math.floor(Math.random() * cfg.baseBackoffMs);
  }
}
