import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { NestFactory } from '@nestjs/core';
import {
  OutboxClaimer,
  X_ERROR,
  X_EVENT_ID,
  X_IDEMPOTENCY_KEY,
} from '@nest-native/messaging';
import type { Channel, GetMessage, RecoveringChannelModel } from 'amqplib';
import { AppModule } from '../src/app.module';
import { createDatabase } from '../src/database';
import { DeliveryLog } from '../src/delivery-log';
import { OrderService } from '../src/order.service';
import { CONNECTION_NAME, RABBITMQ } from '../src/rabbitmq';
import { TOPOLOGY } from '../src/topology';

// This sample runs against a real broker: `npm run infra:up` starts one on
// amqp://messaging:messaging@127.0.0.1:56720, and CI's sample job runs one as
// a service container. Step 6 drops the connection through the broker's
// management API, so it also needs RABBITMQ_MANAGEMENT_URL
// (http://messaging:messaging@127.0.0.1:15670 from `infra:up`).
const brokerUrl = process.env.RABBITMQ_URL;
const managementUrl = process.env.RABBITMQ_MANAGEMENT_URL;

async function main(url: string): Promise<void> {
  const { db, sqlite } = createDatabase();
  const app = await NestFactory.createApplicationContext(
    AppModule.register(db, url),
    { logger: false },
  );
  await app.init(); // declares the topology, then the billing consumer subscribes
  const rabbit = app.get<RecoveringChannelModel>(RABBITMQ);
  const deliveries = app.get(DeliveryLog);
  const probe = await rabbit.createChannel();
  // A rerun against the same broker starts with an empty dead-letter queue.
  await probe.purgeQueue(TOPOLOGY.billingDeadLetterQueue);

  const invoicesFor = (orderId: string): number =>
    (
      sqlite
        .prepare('SELECT count(*) AS c FROM invoices WHERE order_id = ?')
        .get(orderId) as { c: number }
    ).c;
  const outboxRow = (topic: string) =>
    sqlite
      .prepare(
        'SELECT id, idempotency_key, status, attempts, last_error FROM outbox_events WHERE topic = ?',
      )
      .get(topic) as {
      id: string;
      idempotency_key: string;
      status: string;
      attempts: number;
      last_error: string | null;
    };

  // 1. Place an order: the order row and the outbox event commit together.
  const orderId = `o-${randomUUID().slice(0, 8)}`;
  await app.get(OrderService).placeOrder(orderId, 'widget', 1999);
  const event = outboxRow('order.placed');
  assert.equal(event.status, 'pending', 'the event waits in the outbox');

  // 2. The claimer publishes it on a confirm channel. The row completes only
  //    once the broker acked the message and did not return it; billing then
  //    consumes it and issues exactly one invoice.
  const published = await app.get(OutboxClaimer).tick();
  assert.equal(published.completed, 1, 'published, confirmed and marked completed');
  await waitFor('the invoice', () => invoicesFor(orderId) === 1);
  assert.equal(deliveries.count('processed', event.id), 1);

  // 3. Delivery is at-least-once. Publish the very same event again — a
  //    redelivery after a lost ack looks exactly like this. The inbox knows its
  //    event id, acks it, and does not bill twice.
  probe.publish(
    TOPOLOGY.events,
    TOPOLOGY.orderPlaced,
    Buffer.from(JSON.stringify({ orderId, item: 'widget', amountCents: 1999 })),
    {
      messageId: event.id,
      contentType: 'application/json',
      persistent: true,
      headers: {
        [X_EVENT_ID]: event.id,
        [X_IDEMPOTENCY_KEY]: event.idempotency_key,
      },
    },
  );
  await waitFor('the duplicate to be acked', () =>
    deliveries.count('duplicate', event.id) === 1,
  );
  assert.equal(invoicesFor(orderId), 1, 'the duplicate did not bill again');

  // 4. Poison: a message that can never be processed is dead-lettered with its
  //    reason in an x-error header, instead of being requeued forever.
  const poisonId = `poison-${randomUUID()}`;
  probe.publish(
    TOPOLOGY.events,
    TOPOLOGY.orderPlaced,
    Buffer.from(JSON.stringify({ orderId: 42 })),
    { messageId: poisonId, headers: { [X_EVENT_ID]: poisonId } },
  );
  const deadLetter = await nextMessage(probe, TOPOLOGY.billingDeadLetterQueue);
  assert.equal(deadLetter.properties.headers?.[X_EVENT_ID], poisonId);
  assert.equal(deadLetter.properties.headers?.[X_ERROR], 'payload failed validation');
  probe.ack(deadLetter);

  // 5. Unroutable: nothing is bound to `order.refunded` yet. Published
  //    `mandatory`, the message comes back instead of being acked into the
  //    void, so the attempt fails and the outbox keeps the event for a retry.
  await app.get(OrderService).refundOrder(orderId);
  const unroutable = await app.get(OutboxClaimer).tick();
  assert.equal(unroutable.retried, 1, 'the refund is retried, not marked sent');
  const refund = outboxRow('order.refunded');
  assert.equal(refund.status, 'pending', 'still in the outbox, due again later');
  assert.equal(refund.attempts, 1);
  assert.match(refund.last_error ?? '', /unroutable/);
  await probe.close();

  // 6. The broker drops the connection, the way a restart or a network blip
  //    would. amqplib's recovery reconnects, the outbox transport opens a new
  //    confirm channel for its next publish, and billing subscribes again on
  //    its own: the next order is still invoiced, exactly once.
  if (managementUrl) {
    const api = managementApi(managementUrl);
    const dropped = await connectionOf(api, CONNECTION_NAME);
    const response = await api('DELETE', `/api/connections/${encodeURIComponent(dropped)}`);
    assert.equal(response.status, 204, 'the broker closed the connection');
    const nextOrder = `o-${randomUUID().slice(0, 8)}`;
    await app.get(OrderService).placeOrder(nextOrder, 'gadget', 4999);
    // The first publish may still meet the closing channel; the claimer
    // retries it, the way the relay loop would.
    await waitFor('the invoice after the reconnect', async () => {
      await app.get(OutboxClaimer).tick();
      return invoicesFor(nextOrder) === 1;
    });
    await connectionOf(api, CONNECTION_NAME, dropped); // on a new connection
  } else if (process.env.CI) {
    throw new Error('Sample 02 needs RABBITMQ_MANAGEMENT_URL: CI must provide the management API.');
  } else {
    console.log('Sample 02: step 6 (dropped connection) skipped: set RABBITMQ_MANAGEMENT_URL to run it.');
  }

  await app.close();
  sqlite.close();
  console.log(
    'Sample 02 (RabbitMQ) smoke passed: confirmed publish → exactly-once inbox, ' +
      'duplicate acked, poison dead-lettered with its reason, unroutable event kept in the outbox' +
      (managementUrl ? ', billing back on its own after the broker dropped the connection.' : '.'),
  );
}

type ManagementApi = (method: string, path: string) => Promise<Response>;

/** Calls the management API with the credentials from its URL (fetch refuses a URL that carries them). */
function managementApi(url: string): ManagementApi {
  const base = new URL(url);
  const credentials = `${decodeURIComponent(base.username)}:${decodeURIComponent(base.password)}`;
  const authorization = `Basic ${Buffer.from(credentials).toString('base64')}`;
  base.username = '';
  base.password = '';
  return (method, path) =>
    fetch(new URL(path, base), {
      method,
      headers: { authorization, 'x-reason': 'sample 02 smoke: a simulated broker restart' },
    });
}

/**
 * The broker's name for the connection — found by the name the application gave
 * it — once the management API lists it (its statistics lag a little). With
 * `except`, waits for a connection other than that one: a reconnect.
 */
async function connectionOf(api: ManagementApi, name: string, except?: string): Promise<string> {
  let found: string | undefined;
  await waitFor(`the ${name} connection in the management API`, async () => {
    const response = await api('GET', '/api/connections');
    const all = (await response.json()) as { name: string; client_properties?: { connection_name?: string } }[];
    found = all.find(c => c.client_properties?.connection_name === name && c.name !== except)?.name;
    return found !== undefined;
  });
  return found!;
}

async function waitFor(
  what: string,
  done: () => boolean | Promise<boolean>,
  timeoutMs = 15_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await done())) {
    if (Date.now() > deadline) {
      throw new Error(`Timed out waiting for ${what}`);
    }
    await delay(100);
  }
}

async function nextMessage(
  channel: Channel,
  queue: string,
  timeoutMs = 15_000,
): Promise<GetMessage> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const message = await channel.get(queue, { noAck: false });
    if (message) {
      return message;
    }
    if (Date.now() > deadline) {
      throw new Error(`Timed out waiting for a message on ${queue}`);
    }
    await delay(50);
  }
}

if (brokerUrl) {
  main(brokerUrl).catch((error: unknown) => {
    console.error(error);
    process.exit(1);
  });
} else if (process.env.CI) {
  console.error('Sample 02 needs RABBITMQ_URL: CI must provide a broker.');
  process.exit(1);
} else {
  console.log(
    'Sample 02 (RabbitMQ) skipped: set RABBITMQ_URL to run it ' +
      '(`npm run infra:up` starts a broker on amqp://messaging:messaging@127.0.0.1:56720).',
  );
}
