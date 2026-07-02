import { Inject, Injectable, type OnModuleInit } from '@nestjs/common';
import {
  InboxService,
  type OutboxMessage,
  PermanentError,
} from '@nest-native/messaging';
import {
  type OutboxHandlerResult,
  OutboxRegistry,
} from '@nest-native/messaging/in-process';
import { type AppDatabase, DRIZZLE } from './database';
import { orderAudit } from './schema';
import type { OrderPlacedPayload } from './order.service';

const isOrderPlaced = (p: unknown): p is OrderPlacedPayload =>
  typeof p === 'object' &&
  p !== null &&
  typeof (p as OrderPlacedPayload).id === 'string' &&
  typeof (p as OrderPlacedPayload).item === 'string';

/**
 * The consumer half, on the in-process transport: the claimer delivers each
 * committed `order.placed` event to this handler (at-least-once), and the
 * handler pairs with the inbox — keyed on `idempotencyKey ?? id`, the same
 * dedup key the Kafka consumer derives — so the audit row is written exactly
 * once no matter how many times the event arrives.
 */
@Injectable()
export class OrderPlacedHandler implements OnModuleInit {
  constructor(
    @Inject(DRIZZLE) private readonly db: AppDatabase,
    @Inject(OutboxRegistry) private readonly registry: OutboxRegistry,
    @Inject(InboxService) private readonly inbox: InboxService,
  ) {}

  onModuleInit(): void {
    this.registry.register('order.placed', (payload, message) =>
      this.handle(payload, message),
    );
  }

  private async handle(
    payload: Record<string, unknown>,
    message: OutboxMessage,
  ): Promise<OutboxHandlerResult> {
    // A malformed payload can never succeed — throw PermanentError so the
    // claimer fails the row now instead of burning retry attempts.
    if (!isOrderPlaced(payload)) {
      throw new PermanentError('order.placed: malformed payload');
    }
    const key = message.idempotencyKey ?? message.id;
    // Exactly-once side effect: runOnce dedups on (source, key) in the same
    // transaction as the write. Synchronous + DB-only, as sqlite requires.
    await this.inbox.runOnce(key, 'order.placed:showcase', () => {
      this.db.insert(orderAudit).values({ key, item: payload.item }).run();
    });
    return 'completed';
  }
}
