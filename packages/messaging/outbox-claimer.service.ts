import { hostname } from 'node:os';
import { Inject, Injectable, Logger } from '@nestjs/common';
import type {
  ClaimerConfig,
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

export interface TickReport {
  claimed: number;
  completed: number;
  retried: number;
  failed: number;
}

type ProcessOutcome = 'completed' | 'retried' | 'failed';

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
    const cfg = { ...DEFAULT_CLAIMER_CONFIG, ...overrides };
    const claimed = await this.store.claimBatch(this.db, cfg);
    const report: TickReport = {
      claimed: claimed.length,
      completed: 0,
      retried: 0,
      failed: 0,
    };
    for (const event of claimed) {
      const outcome = await this.processOne(event, cfg);
      report[outcome] += 1;
    }
    return report;
  }

  private async processOne(
    event: OutboxEventRow,
    cfg: ResolvedClaimerConfig,
  ): Promise<ProcessOutcome> {
    try {
      await this.transport.publish({
        id: event.id,
        topic: event.topic,
        payload: event.payload,
        idempotencyKey: event.idempotencyKey ?? undefined,
      });
      await this.store.markCompleted(this.db, event.id);
      return 'completed';
    } catch (error) {
      return this.onPublishError(event, cfg, error);
    }
  }

  private async onPublishError(
    event: OutboxEventRow,
    cfg: ResolvedClaimerConfig,
    error: unknown,
  ): Promise<ProcessOutcome> {
    const message = error instanceof Error ? error.message : String(error);
    // Permanent: retrying can never succeed — fail now instead of burning attempts.
    if (error instanceof PermanentError) {
      return this.fail(event, message);
    }
    // Retryable: schedule another attempt, honouring a transport-supplied delay.
    if (error instanceof RetryableError) {
      const delay = error.delayMs ?? this.backoff(event.attempts, cfg);
      await this.store.retry(this.db, event.id, delay, message);
      return 'retried';
    }
    // Anything else: retry with backoff until maxAttempts, then fail.
    if (event.attempts + 1 >= event.maxAttempts) {
      return this.fail(event, message);
    }
    await this.store.retry(
      this.db,
      event.id,
      this.backoff(event.attempts, cfg),
      message,
    );
    return 'retried';
  }

  private async fail(event: OutboxEventRow, reason: string): Promise<'failed'> {
    this.logger.warn(`outbox event ${event.id} failed: ${reason}`);
    await this.store.markFailed(this.db, event.id, reason);
    return 'failed';
  }

  private backoff(attempts: number, cfg: ResolvedClaimerConfig): number {
    const base = cfg.baseBackoffMs * 2 ** attempts;
    const capped = Math.min(base, cfg.maxBackoffMs);
    return capped + Math.floor(Math.random() * cfg.baseBackoffMs);
  }
}
