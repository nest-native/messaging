import { strict as assert } from 'node:assert';
import { type ChildProcess, fork } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { X_EVENT_ID, X_IDEMPOTENCY_KEY } from '@nest-native/messaging';
import type { RabbitConsumeResult } from '@nest-native/messaging/rabbitmq';
import { connect } from 'amqplib';
import Database from 'better-sqlite3';
import { EVENTS } from '../src/shared/contracts';
import { EXCHANGE, declareSubscription } from '../src/shared/topology';
import { ORDERS_SUBSCRIPTION } from '../src/orders/shipment-scheduled.consumer';
import { SHIPPING_SUBSCRIPTION } from '../src/shipping/order-placed.consumer';

// This sample runs against a real broker: `npm run infra:up` starts one on
// amqp://messaging:messaging@127.0.0.1:56720, and CI's sample job runs one as
// a service container. Step 4 drops connections through the broker's
// management API, so it also needs RABBITMQ_MANAGEMENT_URL
// (http://messaging:messaging@127.0.0.1:15670 from `infra:up`).
const brokerUrl = process.env.RABBITMQ_URL;
const managementUrl = process.env.RABBITMQ_MANAGEMENT_URL;

/** One running service: its process, and every delivery its inbox reported. */
interface Service {
  process: ChildProcess;
  settled: RabbitConsumeResult[];
  send(command: string, payload: Record<string, string>): Promise<void>;
  stop(): Promise<void>;
}

async function main(url: string): Promise<void> {
  // Each service gets its own process and its own database file. They share
  // nothing but the broker.
  const dir = mkdtempSync(join(tmpdir(), 'messaging-sample-03-'));
  const env = {
    RABBITMQ_URL: url,
    ORDERS_DB: join(dir, 'orders.db'),
    SHIPPING_DB: join(dir, 'shipping.db'),
  };
  const admin = await connect(url);
  const probe = await admin.createChannel();
  // A rerun against the same broker starts from empty queues: declare them
  // (idempotently, exactly as the services will) and drop anything a crashed
  // earlier run left behind.
  for (const subscription of [ORDERS_SUBSCRIPTION, SHIPPING_SUBSCRIPTION]) {
    await declareSubscription(probe, subscription);
    await probe.purgeQueue(subscription.queue);
  }

  const orders = await startService('orders', env);
  let shipping = await startService('shipping', env);
  const ordersDb = new Database(env.ORDERS_DB, { readonly: true });
  const shippingDb = new Database(env.SHIPPING_DB, { readonly: true });

  const run = randomUUID().slice(0, 6);
  const orderIds = (...names: string[]) => names.map(name => `o-${run}-${name}`);
  const place = (orderId: string) =>
    orders.send('place', { orderId, address: `${orderId}, 1 Main St` });
  const status = (orderId: string) =>
    (ordersDb.prepare('SELECT status FROM orders WHERE id = ?').get(orderId) as
      | { status: string }
      | undefined)?.status;
  const shipmentsFor = (orderId: string) =>
    count(shippingDb, 'SELECT count(*) AS c FROM shipments WHERE order_id = ?', orderId);
  const unpublished = (db: Database.Database) =>
    count(db, "SELECT count(*) AS c FROM outbox_events WHERE status != 'completed'");

  try {
    // 1. Three orders cross both services: orders publishes order.placed,
    //    shipping books a shipment and publishes shipment.scheduled in the same
    //    transaction, and orders marks each order scheduled.
    const first = orderIds('a', 'b', 'c');
    for (const orderId of first) {
      await place(orderId);
    }
    await waitFor('the first orders to be scheduled', () =>
      first.every(orderId => status(orderId) === 'scheduled'),
    );
    for (const orderId of first) {
      assert.equal(shipmentsFor(orderId), 1, `one shipment for ${orderId}`);
    }

    // 2. Shipping goes down. Orders keeps taking orders: its outbox publishes
    //    them, and RabbitMQ keeps them in shipping's durable queue.
    await shipping.stop();
    const duringOutage = orderIds('d', 'e');
    for (const orderId of duringOutage) {
      await place(orderId);
    }
    await waitFor('orders to publish during the outage', () => unpublished(ordersDb) === 0);
    for (const orderId of duringOutage) {
      assert.equal(status(orderId), 'placed', 'nobody has scheduled it yet');
    }

    // 3. Shipping comes back — a new process on the same database file — and
    //    works through the backlog the broker kept for it.
    shipping = await startService('shipping', env);
    await waitFor('the backlog to be scheduled', () =>
      duringOutage.every(orderId => status(orderId) === 'scheduled'),
    );

    // 4. The broker drops both services' connections, the way a broker restart
    //    or a network blip would. Neither service restarts: amqplib's recovery
    //    reconnects each one, its outbox transport opens a new channel, its
    //    consumer subscribes again, and the next orders flow through.
    const afterReconnect = managementUrl ? orderIds('f', 'g') : [];
    if (managementUrl) {
      const api = managementApi(managementUrl);
      const dropped = new Map<string, string>();
      for (const service of ['orders', 'shipping']) {
        const connection = await connectionOf(api, service);
        const response = await api('DELETE', `/api/connections/${encodeURIComponent(connection)}`);
        assert.equal(response.status, 204, `the broker closed ${service}'s connection`);
        dropped.set(service, connection);
      }
      for (const orderId of afterReconnect) {
        await place(orderId);
      }
      await waitFor('the orders placed after the reconnect to be scheduled', () =>
        afterReconnect.every(orderId => status(orderId) === 'scheduled'),
      );
      for (const [service, connection] of dropped) {
        await connectionOf(api, service, connection); // a new connection…
      }
      assert.equal(orders.process.exitCode, null, '…in the same orders process');
      assert.equal(shipping.process.exitCode, null, '…in the same shipping process');
    } else if (process.env.CI) {
      throw new Error('Sample 03 needs RABBITMQ_MANAGEMENT_URL: CI must provide the management API.');
    } else {
      console.log('Sample 03: step 4 (dropped connections) skipped: set RABBITMQ_MANAGEMENT_URL to run it.');
    }

    // 5. Exactly once: an event arrives again, the way it would after a lost
    //    ack. Shipping's inbox knows its event id — no second shipment, and no
    //    second shipment.scheduled.
    const replayed = ordersDb
      .prepare('SELECT id, idempotency_key, payload FROM outbox_events WHERE idempotency_key = ?')
      .get(`order-placed:${first[0]}`) as { id: string; idempotency_key: string; payload: string };
    probe.publish(EXCHANGE, EVENTS.orderPlaced, Buffer.from(replayed.payload), {
      messageId: replayed.id,
      contentType: 'application/json',
      persistent: true,
      headers: {
        [X_EVENT_ID]: replayed.id,
        [X_IDEMPOTENCY_KEY]: replayed.idempotency_key,
      },
    });
    const restarted = shipping;
    await waitFor('shipping to recognize the duplicate', () =>
      restarted.settled.some(
        result => result.outcome === 'duplicate' && result.dedupKey === replayed.id,
      ),
    );
    assert.equal(shipmentsFor(first[0]), 1, 'the duplicate booked nothing');

    // 6. Every event on both sides was published and confirmed, once per order.
    const all = [...first, ...duringOutage, ...afterReconnect];
    await waitFor('both outboxes to drain', () =>
      unpublished(ordersDb) === 0 && unpublished(shippingDb) === 0,
    );
    assert.equal(
      count(
        shippingDb,
        'SELECT count(*) AS c FROM outbox_events WHERE topic = ? AND idempotency_key LIKE ?',
        EVENTS.shipmentScheduled,
        `shipment-scheduled:o-${run}-%`,
      ),
      all.length,
      'one shipment.scheduled per order',
    );
    for (const orderId of all) {
      assert.equal(shipmentsFor(orderId), 1, `exactly one shipment for ${orderId}`);
    }
  } finally {
    await shipping.stop();
    await orders.stop();
    ordersDb.close();
    shippingDb.close();
    await probe.close();
    await admin.close();
    rmSync(dir, { recursive: true, force: true });
  }
  console.log(
    'Sample 03 (RabbitMQ services) smoke passed: orders ↔ shipping choreography across two processes, ' +
      'backlog kept through an outage, both services back on their own after the broker dropped them, ' +
      'redelivery booked nothing twice.',
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
      headers: { authorization, 'x-reason': 'sample 03 smoke: a simulated broker restart' },
    });
}

/**
 * The broker's name for a service's connection — found by the name the service
 * gave it — once the management API lists it (its statistics lag a little).
 * With `except`, waits for a connection other than that one: a reconnect.
 */
async function connectionOf(api: ManagementApi, service: string, except?: string): Promise<string> {
  let found: string | undefined;
  await waitFor(`${service}'s connection in the management API`, async () => {
    const response = await api('GET', '/api/connections');
    const all = (await response.json()) as { name: string; client_properties?: { connection_name?: string } }[];
    found = all.find(c => c.client_properties?.connection_name === service && c.name !== except)?.name;
    return found !== undefined;
  });
  return found!;
}

/** Forks a service's `main.ts` and waits until it has subscribed and started its relay. */
async function startService(name: 'orders' | 'shipping', env: Record<string, string>): Promise<Service> {
  const child = fork(join(__dirname, '..', 'src', name, 'main.ts'), [], {
    env: { ...process.env, ...env },
    execArgv: ['--require', 'ts-node/register/transpile-only'],
    stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
  });
  const settled: RabbitConsumeResult[] = [];
  const replies = new Map<string, (error?: string) => void>();
  child.on('message', (message: { type: string; id?: string; error?: string } & RabbitConsumeResult) => {
    if (message.type === 'settled') {
      settled.push({ outcome: message.outcome, dedupKey: message.dedupKey });
    } else if (message.type === 'reply' && message.id) {
      replies.get(message.id)?.(message.error);
      replies.delete(message.id);
    }
  });
  await new Promise<void>((resolve, reject) => {
    child.once('exit', code => reject(new Error(`${name} exited with ${code} before it was ready`)));
    child.on('message', (message: { type: string }) => {
      if (message.type === 'ready') {
        resolve();
      }
    });
  });
  let exited: Promise<void> | undefined;
  return {
    process: child,
    settled,
    send: (command, payload) =>
      new Promise((resolve, reject) => {
        const id = randomUUID();
        replies.set(id, error => (error ? reject(new Error(error)) : resolve()));
        child.send({ type: command, id, payload });
      }),
    stop: () => {
      exited ??= new Promise<void>(resolve => {
        if (child.exitCode !== null) {
          resolve();
          return;
        }
        child.once('exit', () => resolve());
        child.kill('SIGTERM');
      });
      return exited;
    },
  };
}

function count(db: Database.Database, sql: string, ...params: unknown[]): number {
  return (db.prepare(sql).get(...params) as { c: number }).c;
}

async function waitFor(
  what: string,
  done: () => boolean | Promise<boolean>,
  timeoutMs = 30_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await done())) {
    if (Date.now() > deadline) {
      throw new Error(`Timed out waiting for ${what}`);
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
  console.error('Sample 03 needs RABBITMQ_URL: CI must provide a broker.');
  process.exit(1);
} else {
  console.log(
    'Sample 03 (RabbitMQ services) skipped: set RABBITMQ_URL to run it ' +
      '(`npm run infra:up` starts a broker on amqp://messaging:messaging@127.0.0.1:56720).',
  );
}
