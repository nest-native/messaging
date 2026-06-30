import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { NestFactory } from '@nestjs/core';
import { eq } from 'drizzle-orm';
import { InboxService, OutboxClaimer } from '@nest-native/messaging';
import { InMemoryOutboxTransport } from '@nest-native/messaging/testing';
import { type AppDatabase, createDatabase, DRIZZLE } from '../src/database';
import { AppModule } from '../src/app.module';
import { OrderService } from '../src/order.service';
import { orderAudit, orders } from '../src/schema';

async function main(): Promise<void> {
  const { db, sqlite } = createDatabase();
  const transport = new InMemoryOutboxTransport();
  const app = await NestFactory.createApplicationContext(
    AppModule.register(db, transport),
    { logger: false },
  );
  const inspect = app.get<AppDatabase>(DRIZZLE);

  // 1. Place an order — the row and the outbox event commit atomically.
  await app.get(OrderService).placeOrder('o-1', 'widget');
  assert.equal(inspect.select().from(orders).all().length, 1, 'order persisted');
  const count = (t: string) =>
    (sqlite.prepare(`SELECT count(*) c FROM ${t}`).get() as { c: number }).c;
  assert.equal(count('outbox_events'), 1, 'outbox row enqueued in the same tx');

  // 2. The claimer relays the committed event to the transport (here in-memory;
  //    in production the Kafka transport publishes to a broker).
  const report = await app.get(OutboxClaimer).tick();
  assert.equal(report.completed, 1, 'one event published + completed');
  assert.equal(transport.list().length, 1, 'transport received the event');

  // 3. The consumer side: dedup the delivery and apply the side effect once.
  const message = transport.list()[0]!;
  const dedupKey = message.idempotencyKey ?? message.id;
  const source = 'order.placed:showcase';
  const inbox = app.get(InboxService);
  const apply = (key: string, item: string) => () => {
    inspect.insert(orderAudit).values({ key, item }).run();
  };

  const first = await inbox.runOnce(dedupKey, source, apply(dedupKey, 'widget'));
  assert.equal(first, 'processed', 'first delivery processed');

  // 4. A redelivery (Kafka is at-least-once) is deduplicated — no second audit row.
  const second = await inbox.runOnce(dedupKey, source, apply(dedupKey, 'widget'));
  assert.equal(second, 'duplicate', 'redelivery deduplicated');
  assert.equal(count('order_audit'), 1, 'side effect applied exactly once');

  const audited = inspect.select().from(orderAudit).where(eq(orderAudit.key, dedupKey)).all();
  assert.equal(audited.length, 1);

  await app.close();
  sqlite.close();
  console.log('Showcase smoke passed: atomic outbox → claim → exactly-once inbox.');
}

main().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
