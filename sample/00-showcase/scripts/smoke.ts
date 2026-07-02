import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { NestFactory } from '@nestjs/core';
import {
  OUTBOX_TRANSPORT,
  OutboxClaimer,
  type OutboxTransport,
} from '@nest-native/messaging';
import { SqliteOutboxStore } from '@nest-native/messaging/sqlite';
import { type AppDatabase, createDatabase, DRIZZLE } from '../src/database';
import { AppModule } from '../src/app.module';
import { OrderService } from '../src/order.service';
import { orders } from '../src/schema';

async function main(): Promise<void> {
  const { db, sqlite } = createDatabase();
  const app = await NestFactory.createApplicationContext(AppModule.register(db), {
    logger: false,
  });
  const inspect = app.get<AppDatabase>(DRIZZLE);
  const count = (t: string) =>
    (sqlite.prepare(`SELECT count(*) c FROM ${t}`).get() as { c: number }).c;

  // 1. Place an order — the row and the outbox event commit atomically.
  await app.get(OrderService).placeOrder('o-1', 'widget');
  assert.equal(inspect.select().from(orders).all().length, 1, 'order persisted');
  assert.equal(count('outbox_events'), 1, 'outbox row enqueued in the same tx');

  // 2. The claimer relays the committed event through the in-process transport:
  //    it dispatches to the handler registered for `order.placed`, which pairs
  //    with the inbox — the audit row is the consumer's exactly-once side effect.
  const report = await app.get(OutboxClaimer).tick();
  assert.equal(report.completed, 1, 'one event dispatched + completed');
  assert.equal(count('order_audit'), 1, 'handler consumed the event once');

  // 3. Delivery is at-least-once: replay the same logical event through the
  //    transport (same idempotency key) — the inbox dedups it, no second row.
  await app.get<OutboxTransport>(OUTBOX_TRANSPORT).publish({
    id: 'redelivery-of-o-1',
    topic: 'order.placed',
    payload: { id: 'o-1', item: 'widget' },
    idempotencyKey: 'order:o-1',
  });
  assert.equal(count('order_audit'), 1, 'redelivery deduplicated by the inbox');

  // 4. An event on a topic nobody handles is unroutable — PermanentError, so
  //    the claimer fails the row immediately instead of retrying forever.
  new SqliteOutboxStore().enqueue(db, { topic: 'nobody.listens', payload: {} });
  const failedReport = await app.get(OutboxClaimer).tick();
  assert.equal(failedReport.failed, 1, 'unroutable event failed immediately');

  await app.close();
  sqlite.close();
  console.log(
    'Showcase smoke passed: atomic outbox → in-process dispatch → exactly-once inbox.',
  );
}

main().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
