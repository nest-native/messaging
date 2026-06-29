import { Inject, Injectable } from '@nestjs/common';
import { InjectTransaction, Transactional } from '@nestjs-cls/transactional';
import type { InboxSideEffect, InboxStore, RunOnceOutcome } from './interfaces';
import { INBOX_STORE } from './tokens';

/**
 * The idempotent inbox: process each message exactly once. Invoke `runOnce` from
 * a consumer wrapper (e.g. the Kafka consumer base) for every delivered message;
 * all async broker work (parse, ack, DLQ) happens OUTSIDE it.
 *
 * `runOnce` opens a transaction, inserts the `(source, messageKey)` dedup row,
 * and runs `handler` in the SAME transaction. A duplicate delivery violates the
 * unique index → `'duplicate'` (handler skipped); a handler throw rolls back the
 * dedup row too, so the redelivery reprocesses cleanly — effective exactly-once.
 *
 * The method delegates to the store with NO `await`, so it is correct on both a
 * synchronous (better-sqlite3) and an asynchronous (postgres) transaction: the
 * store owns the dialect's execution and the `@Transactional` adapter wraps the
 * result. On the sqlite store the `handler` must be synchronous + DB-only.
 */
@Injectable()
export class InboxService {
  constructor(
    @InjectTransaction() private readonly db: unknown,
    @Inject(INBOX_STORE) private readonly store: InboxStore,
  ) {}

  @Transactional()
  runOnce(
    messageKey: string,
    source: string,
    handler: InboxSideEffect,
  ): Promise<RunOnceOutcome> {
    return this.store.runOnce(
      this.db,
      messageKey,
      source,
      handler,
    ) as Promise<RunOnceOutcome>;
  }
}
