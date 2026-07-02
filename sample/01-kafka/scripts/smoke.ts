import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { NestFactory } from '@nestjs/core';
import { OutboxClaimer } from '@nest-native/messaging';
import {
  type InMemoryKafkaBroker,
  KAFKA_TEST_BROKER,
} from '@nest-native/kafka/testing';
import { type AppDatabase, createDatabase, DRIZZLE } from '../src/database';
import { AppModule } from '../src/app.module';
import { OrderService } from '../src/order.service';
import { ORDER_TOPIC } from '../src/order.consumer';

async function main(): Promise<void> {
  const { db, sqlite } = createDatabase();
  const app = await NestFactory.createApplicationContext(AppModule.register(db), {
    logger: false,
  });
  await app.init(); // fires onApplicationBootstrap → the @KafkaConsumer subscribes
  const inspect = app.get<AppDatabase>(DRIZZLE);
  const broker = app.get<InMemoryKafkaBroker>(KAFKA_TEST_BROKER);
  const count = (table: 'outbox_events' | 'order_audit') =>
    (sqlite.prepare(`SELECT count(*) c FROM ${table}`).get() as { c: number }).c;

  // 1. Place an order — the row and the outbox event commit atomically.
  await app.get(OrderService).placeOrder('o-1', 'widget');
  assert.equal(count('outbox_events'), 1, 'outbox row enqueued in the same tx');

  // 2. The claimer relays the committed event through the REAL Kafka transport
  //    (KafkaOutboxTransport) to the in-memory broker; the @KafkaConsumer then
  //    consumes it and the inbox applies the side effect exactly once.
  const report = await app.get(OutboxClaimer).tick();
  assert.equal(report.completed, 1, 'event published to Kafka + row completed');
  await broker.idle(); // settle point: every in-flight handler pipeline has finished
  assert.equal(count('order_audit'), 1, 'consumer wrote one delivery audit row');

  // 3. Redelivery (Kafka is at-least-once): re-emit the exact published message.
  //    The inbox dedups it — no second audit row.
  const published = broker.getSentTo(ORDER_TOPIC)[0];
  assert.ok(published, 'the transport published to the order topic');
  await broker.emit(ORDER_TOPIC, published);
  await broker.idle();
  assert.equal(count('order_audit'), 1, 'redelivery deduplicated — side effect ran once');

  await app.close();
  sqlite.close();
  console.log(
    'Sample 01 (Kafka) smoke passed: outbox → KafkaOutboxTransport → @KafkaConsumer → exactly-once inbox.',
  );
}

main().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
