import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { randomUUID } from 'node:crypto';
import { after, before, describe, test } from 'node:test';
import { Logger } from '@nestjs/common';
import { connect, type Channel, type ChannelModel, type ConsumeMessage } from 'amqplib';
import Database from 'better-sqlite3';
import { drizzle, type BetterSQLite3Database } from 'drizzle-orm/better-sqlite3';
import { sqliteTable, text } from 'drizzle-orm/sqlite-core';
import {
  RabbitInboxConsumer,
  RabbitOutboxTransport,
  type RabbitConfirmChannelSource,
} from '../../adapters/rabbitmq';
import { SqliteInboxStore } from '../../dialects/sqlite';
import type { InboxService } from '../../inbox.service';
import { X_ERROR } from '../../wire-contract';

// Gated end-to-end tests against a REAL RabbitMQ broker. They skip unless
// MESSAGING_RABBITMQ_URL is set, so `npm test` / `test:cov` stay hermetic. Run
// them locally via `npm run infra:up && npm run test:full` (the "Local full-mode
// verification" section in GUIDELINES_NEST_MESSAGING.md); CI does not run them.
// The connection-kill case additionally needs MESSAGING_RABBITMQ_MANAGEMENT_URL
// (the management API, with credentials), which `test:full` also sets.
//
// What the unit tests' fake channel cannot prove, and this file does: that a
// real broker returns an unroutable `mandatory` message BEFORE acking it, that a
// channel the broker kills (unknown exchange) fails the publish fast instead of
// hanging, that the transport recovers on a fresh channel — and on a recovered
// connection — and that dedup, requeue, and both dead-letter paths behave with
// real deliveries and a real SQLite inbox.

const RABBITMQ_URL = process.env.MESSAGING_RABBITMQ_URL;
const MANAGEMENT_URL = process.env.MESSAGING_RABBITMQ_MANAGEMENT_URL;

const auditLog = sqliteTable('audit_log', {
  eventKey: text('event_key').primaryKey(),
  orderId: text('order_id').notNull(),
});
const DDL = `
CREATE TABLE inbox_events (
  id TEXT PRIMARY KEY, message_key TEXT NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL,
  processed_at TEXT NOT NULL, last_error TEXT, created_at TEXT NOT NULL);
CREATE UNIQUE INDEX inbox_src_key ON inbox_events (source, message_key);
CREATE TABLE audit_log (event_key TEXT PRIMARY KEY, order_id TEXT NOT NULL);
`;

type Order = { orderId: string };
const isOrder = (p: unknown): p is Order =>
  typeof p === 'object' && p !== null && typeof (p as Order).orderId === 'string';

describe('RabbitMQ adapter against a real broker', { skip: !RABBITMQ_URL }, () => {
  const run = randomUUID().slice(0, 8);
  const exchange = `it.events.${run}`;
  const dlx = `it.dlx.${run}`;
  const ordersQueue = `it.orders.${run}`;
  const nativeQueue = `it.native.${run}`;
  const dlq = `it.dlq.${run}`;

  let admin: ChannelModel;
  let channel: Channel;
  let db: BetterSQLite3Database<Record<string, never>>;
  let consumer: RabbitInboxConsumer;

  before(async () => {
    Logger.overrideLogger(false);
    admin = await connect(RABBITMQ_URL!);
    // Surface a connection-level close (e.g. a topology the broker refuses)
    // as a failure instead of an unhandled 'error' event.
    admin.on('error', (error) => assert.fail(`admin connection failed: ${error.message}`));
    channel = await admin.createChannel();
    await channel.assertExchange(exchange, 'topic', { durable: false, autoDelete: false });
    await channel.assertExchange(dlx, 'fanout', { durable: false, autoDelete: false });
    // Durable queues, deleted in `after`: RabbitMQ 4 refuses transient shared
    // queues and closes the whole connection over one.
    await channel.assertQueue(dlq, { durable: true });
    await channel.bindQueue(dlq, dlx, '');
    await channel.assertQueue(ordersQueue, { durable: true });
    await channel.bindQueue(ordersQueue, exchange, 'order.*');
    await channel.assertQueue(nativeQueue, {
      durable: true,
      arguments: { 'x-dead-letter-exchange': dlx },
    });
    await channel.bindQueue(nativeQueue, exchange, 'native.*');

    const sqlite = new Database(':memory:');
    sqlite.exec(DDL);
    db = drizzle(sqlite);
    const store = new SqliteInboxStore();
    // What InboxService does through @Transactional: the store inserts the dedup
    // row and runs the side effect inside the CALLER's transaction, so a side
    // effect that throws rolls the dedup row back with it.
    const inbox = {
      runOnce: (key: string, source: string, sideEffect: () => void) =>
        Promise.resolve(db.transaction((tx) => store.runOnce(tx, key, source, sideEffect))),
    } as unknown as InboxService;
    consumer = new RabbitInboxConsumer(inbox);
  });

  after(async () => {
    for (const queue of [ordersQueue, nativeQueue, dlq]) await channel.deleteQueue(queue);
    for (const name of [exchange, dlx]) await channel.deleteExchange(name);
    await admin.close();
  });

  function transport(onExchange = exchange, connection: RabbitConfirmChannelSource = admin) {
    return new RabbitOutboxTransport({ connection, exchange: onExchange, confirmTimeoutMs: 5_000 });
  }

  const recordOrder = (payload: Order, key: string) => {
    db.insert(auditLog).values({ eventKey: key, orderId: payload.orderId }).run();
  };

  async function next(queue: string): Promise<ConsumeMessage> {
    for (let attempt = 0; attempt < 50; attempt += 1) {
      const message = await channel.get(queue, { noAck: false });
      // channel.get hands back a GetMessage; the consumer only reads the fields
      // both shapes share (content, properties, redelivered).
      if (message) return message as unknown as ConsumeMessage;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    assert.fail(`no message arrived on ${queue}`);
  }

  async function depth(queue: string): Promise<number> {
    return (await channel.checkQueue(queue)).messageCount;
  }

  test('publishes, delivers, and deduplicates an at-least-once redelivery', async () => {
    const out = transport();
    const order = { id: `evt-${run}-1`, topic: 'order.placed', payload: { orderId: 'o-1' } };
    await out.publish(order);
    await out.publish(order); // the outbox retried after a lost ack
    await out.close();

    const outcomes: string[] = [];
    for (let i = 0; i < 2; i += 1) {
      const message = await next(ordersQueue);
      assert.equal(message.properties.contentType, 'application/json');
      assert.equal(message.properties.deliveryMode, 2, 'persistent');
      const result = await consumer.consume({
        source: ordersQueue,
        channel,
        message,
        validate: isOrder,
        sideEffect: recordOrder,
      });
      outcomes.push(result.outcome);
    }
    assert.deepEqual(outcomes, ['processed', 'duplicate']);
    assert.deepEqual(db.select().from(auditLog).all(), [
      { eventKey: order.id, orderId: 'o-1' },
    ]);
    assert.equal(await depth(ordersQueue), 0, 'both deliveries were acked');
  });

  test('a real broker returns an unroutable mandatory message before acking it', async () => {
    const out = transport();
    await assert.rejects(
      out.publish({ id: `evt-${run}-2`, topic: 'nobody.listens', payload: {} }),
      /is unroutable: no queue is bound to exchange .* for routing key "nobody.listens"/,
    );
    await out.close();
  });

  test('an exchange that does not exist yet fails fast, and a retry succeeds once it does', async () => {
    const late = `it.late.${run}`;
    const out = transport(late);
    const started = Date.now();
    await assert.rejects(out.publish({ id: `evt-${run}-3`, topic: 'order.late', payload: {} }));
    assert.ok(Date.now() - started < 4_000, 'the broker closed the channel; no confirm timeout');

    // The deploy-order case: the consumer side declares its topology after the
    // producer's first attempt. The same transport, on a fresh channel, succeeds.
    await channel.assertExchange(late, 'topic', { durable: false, autoDelete: false });
    await channel.bindQueue(ordersQueue, late, 'order.*');
    await out.publish({ id: `evt-${run}-3`, topic: 'order.late', payload: { orderId: 'o-3' } });
    await out.close();
    const message = await next(ordersQueue);
    channel.ack(message);
    await channel.deleteExchange(late);
  });

  test('poison goes to the dead-letter target with its reason; the original is acked', async () => {
    const out = transport();
    await out.publish({ id: `evt-${run}-4`, topic: 'order.bad', payload: { wrong: true } });
    await out.close();
    const dlqPublisher = await admin.createConfirmChannel();

    const result = await consumer.consume({
      source: ordersQueue,
      channel,
      message: await next(ordersQueue),
      validate: isOrder,
      sideEffect: recordOrder,
      deadLetter: { channel: dlqPublisher, exchange: dlx, routingKey: '' },
    });
    assert.equal(result.outcome, 'dead-lettered');
    const dead = await next(dlq);
    channel.ack(dead);
    assert.equal(dead.properties.headers?.[X_ERROR], 'payload failed validation');
    assert.equal(dead.properties.messageId, `evt-${run}-4`);
    assert.equal(await depth(ordersQueue), 0);
    await dlqPublisher.close();
  });

  test('without a target, poison is rejected into the queue’s own dead-letter exchange', async () => {
    const out = transport();
    await out.publish({ id: `evt-${run}-5`, topic: 'native.bad', payload: { wrong: true } });
    await out.close();

    const result = await consumer.consume({
      source: nativeQueue,
      channel,
      message: await next(nativeQueue),
      validate: isOrder,
      sideEffect: recordOrder,
    });
    assert.equal(result.outcome, 'dead-lettered');
    const dead = await next(dlq);
    channel.ack(dead);
    const death = (dead.properties.headers?.['x-death'] as { reason: string }[] | undefined)?.[0];
    assert.equal(death?.reason, 'rejected');
    assert.equal(await depth(nativeQueue), 0);
  });

  test('a transient failure is requeued, redelivered, and then processed once', async () => {
    const out = transport();
    await out.publish({ id: `evt-${run}-6`, topic: 'order.flaky', payload: { orderId: 'o-6' } });
    await out.close();

    let failNext = true;
    const flaky = (payload: Order, key: string) => {
      if (failNext) {
        failNext = false;
        throw new Error('database is locked');
      }
      recordOrder(payload, key);
    };
    const first = await next(ordersQueue);
    const requeued = await consumer.consume({
      source: ordersQueue, channel, message: first, validate: isOrder, sideEffect: flaky,
    });
    assert.equal(requeued.outcome, 'requeued');
    const redelivered = await next(ordersQueue);
    assert.equal(redelivered.fields.redelivered, true);
    const processed = await consumer.consume({
      source: ordersQueue, channel, message: redelivered, validate: isOrder, sideEffect: flaky,
    });
    assert.equal(processed.outcome, 'processed');
    assert.equal(
      db.select().from(auditLog).all().filter((row) => row.orderId === 'o-6').length,
      1,
    );
  });

  test(
    'survives the broker killing its connection (amqplib recovery + a fresh channel)',
    { skip: !MANAGEMENT_URL },
    async () => {
      const name = `messaging-it-${run}`;
      const recovering = await connect(RABBITMQ_URL!, {
        recovery: { initialDelay: 50, maxDelay: 500 },
        clientProperties: { connection_name: name },
      });
      const out = transport(exchange, recovering);
      await out.publish({ id: `evt-${run}-7`, topic: 'order.one', payload: { orderId: 'o-7' } });

      await killConnection(name);

      await out.publish({ id: `evt-${run}-8`, topic: 'order.two', payload: { orderId: 'o-8' } });
      await out.close();
      await recovering.close();
      for (const expected of [`evt-${run}-7`, `evt-${run}-8`]) {
        const message = await next(ordersQueue);
        assert.equal(message.properties.messageId, expected);
        channel.ack(message);
      }
    },
  );
});

/** Force-closes a client connection from the broker side, via the management API. */
async function killConnection(connectionName: string): Promise<void> {
  const base = new URL(MANAGEMENT_URL!);
  const auth = `Basic ${Buffer.from(`${base.username}:${base.password}`).toString('base64')}`;
  const api = (path: string) => new URL(path, `${base.protocol}//${base.host}`);
  let connection: { name: string } | undefined;
  for (let attempt = 0; attempt < 50 && !connection; attempt += 1) {
    const response = await fetch(api('/api/connections'), { headers: { authorization: auth } });
    const all = (await response.json()) as { name: string; client_properties?: { connection_name?: string } }[];
    connection = all.find((c) => c.client_properties?.connection_name === connectionName);
    if (!connection) await new Promise((resolve) => setTimeout(resolve, 100));
  }
  assert.ok(connection, `connection ${connectionName} never showed up in the management API`);
  const response = await fetch(api(`/api/connections/${encodeURIComponent(connection.name)}`), {
    method: 'DELETE',
    headers: { authorization: auth, 'x-reason': 'integration test: simulated broker-side close' },
  });
  assert.ok(response.ok, `DELETE connection returned ${response.status}`);
}
