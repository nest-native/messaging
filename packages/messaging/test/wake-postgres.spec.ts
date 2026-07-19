import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import { OutboxWaker } from '../outbox-waker';
import {
  assertValidWakeChannel,
  PostgresWakeListener,
  type WakeListenConnection,
} from '../dialects/postgres';

type Listener = (...args: unknown[]) => void;

/** A scriptable stand-in for the LISTEN slice of `pg.Client`. */
class FakeConnection implements WakeListenConnection {
  queries: string[] = [];
  connectCalls = 0;
  endCalls = 0;
  failConnect = false;
  hangConnect = false;
  failQuery = false;
  failEnd = false;
  #listeners = new Map<string, Listener[]>();

  async connect(): Promise<void> {
    this.connectCalls += 1;
    if (this.failConnect) throw new Error('connect refused');
    if (this.hangConnect) {
      // Models pg's end()-during-connect behavior: the connect promise never
      // settles once the client is ending (only 'end' fires).
      return new Promise<void>(() => {});
    }
  }

  async query(text: string): Promise<unknown> {
    this.queries.push(text);
    if (this.failQuery) throw new Error('query failed');
    return undefined;
  }

  async end(): Promise<void> {
    this.endCalls += 1;
    if (this.failEnd && this.endCalls === 1) throw new Error('end failed');
    this.emit('end'); // pg.Client emits 'end' once the connection closes
  }

  on(event: string, listener: Listener): unknown {
    const list = this.#listeners.get(event) ?? [];
    list.push(listener);
    this.#listeners.set(event, list);
    return this;
  }

  emit(event: string, ...args: unknown[]): void {
    for (const listener of this.#listeners.get(event) ?? []) {
      listener(...args);
    }
  }
}

/** Factory that hands out pre-built fakes in order and records demand. */
function connectionQueue(...connections: FakeConnection[]): {
  factory: () => WakeListenConnection;
  handedOut: () => number;
} {
  let index = 0;
  return {
    factory: () => {
      const connection = connections[index];
      assert.ok(connection, `factory exhausted after ${index} connections`);
      index += 1;
      return connection;
    },
    handedOut: () => index,
  };
}

async function until(
  predicate: () => boolean,
  label: string,
  timeoutMs = 5_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    assert.ok(Date.now() < deadline, `timed out waiting for ${label}`);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

describe('assertValidWakeChannel', () => {
  test('accepts identifier-safe channels and rejects everything else', () => {
    assertValidWakeChannel('outbox_wake');
    assertValidWakeChannel('Wake_1');
    assert.throws(() => assertValidWakeChannel('bad-channel'), /invalid wake channel/);
    assert.throws(() => assertValidWakeChannel('x"; DROP TABLE y'), /invalid wake channel/);
    assert.throws(() => assertValidWakeChannel(''), /invalid wake channel/);
  });

  test('enforces the 63-byte Postgres identifier limit', () => {
    // Beyond 63, LISTEN silently truncates while pg_notify RAISES — aborting
    // the caller's business transaction on every enqueue.
    assertValidWakeChannel('a'.repeat(63));
    assert.throws(() => assertValidWakeChannel('a'.repeat(64)), /invalid wake channel/);
  });
});

describe('PostgresWakeListener', () => {
  test('connects, LISTENs on the quoted default channel, and forwards notifications', async () => {
    const connection = new FakeConnection();
    const waker = new OutboxWaker();
    const listener = new PostgresWakeListener({
      connect: connectionQueue(connection).factory,
      waker,
    });
    listener.start();
    await until(() => connection.queries.length === 1, 'LISTEN to be issued');
    assert.deepEqual(connection.queries, ['LISTEN "outbox_wake"']);

    const start = Date.now();
    const parked = waker.wait(8_000);
    connection.emit('notification');
    await parked;
    assert.ok(Date.now() - start < 1_500, 'notification must cut the wait short');
    await listener.stop();
  });

  test('quotes a custom mixed-case channel (LISTEN must stay case-sensitive)', async () => {
    const connection = new FakeConnection();
    const listener = new PostgresWakeListener({
      connect: connectionQueue(connection).factory,
      waker: new OutboxWaker(),
      channel: 'Wake_1',
    });
    listener.start();
    await until(() => connection.queries.length === 1, 'LISTEN to be issued');
    assert.deepEqual(connection.queries, ['LISTEN "Wake_1"']);
    await listener.stop();
  });

  test('a connection error is reported and a fresh connection re-LISTENs', async () => {
    const first = new FakeConnection();
    const second = new FakeConnection();
    const errors: unknown[] = [];
    const waker = new OutboxWaker();
    const listener = new PostgresWakeListener({
      connect: connectionQueue(first, second).factory,
      waker,
      reconnectDelayMs: 5,
      onError: (error) => errors.push(error),
    });
    listener.start();
    await until(() => first.queries.length === 1, 'first LISTEN');
    first.emit('error', new Error('connection reset'));
    await until(() => second.queries.length === 1, 'second LISTEN after reconnect');
    assert.match((errors[0] as Error).message, /connection reset/);

    // The NEW connection must still deliver wakes.
    const parked = waker.wait(8_000);
    const start = Date.now();
    second.emit('notification');
    await parked;
    assert.ok(Date.now() - start < 1_500);
    await listener.stop();
  });

  test('a refused connect is reported and retried', async () => {
    const refusing = new FakeConnection();
    refusing.failConnect = true;
    const healthy = new FakeConnection();
    const errors: unknown[] = [];
    const listener = new PostgresWakeListener({
      connect: connectionQueue(refusing, healthy).factory,
      waker: new OutboxWaker(),
      reconnectDelayMs: 5,
      onError: (error) => errors.push(error),
    });
    listener.start();
    await until(() => healthy.queries.length === 1, 'retry after refused connect');
    assert.match((errors[0] as Error).message, /connect refused/);
    await listener.stop();
  });

  test('a failed LISTEN query is reported and retried', async () => {
    const broken = new FakeConnection();
    broken.failQuery = true;
    const healthy = new FakeConnection();
    const errors: unknown[] = [];
    const listener = new PostgresWakeListener({
      connect: connectionQueue(broken, healthy).factory,
      waker: new OutboxWaker(),
      reconnectDelayMs: 5,
      onError: (error) => errors.push(error),
    });
    listener.start();
    await until(() => healthy.queries.length === 1, 'retry after failed LISTEN');
    assert.match((errors[0] as Error).message, /query failed/);
    await listener.stop();
  });

  test('a server-side clean end (no error) also reconnects', async () => {
    const first = new FakeConnection();
    const second = new FakeConnection();
    const listener = new PostgresWakeListener({
      connect: connectionQueue(first, second).factory,
      waker: new OutboxWaker(),
      reconnectDelayMs: 5,
    });
    listener.start();
    await until(() => first.queries.length === 1, 'first LISTEN');
    first.emit('end'); // server hung up without an error event
    await until(() => second.queries.length === 1, 'reconnect after clean end');
    await listener.stop();
  });

  test('stop() ends the live connection, halts the loop, and is idempotent', async () => {
    const connection = new FakeConnection();
    const queue = connectionQueue(connection);
    const listener = new PostgresWakeListener({
      connect: queue.factory,
      waker: new OutboxWaker(),
      reconnectDelayMs: 5,
    });
    listener.start();
    await until(() => connection.queries.length === 1, 'LISTEN');
    await listener.stop();
    assert.ok(connection.endCalls >= 1, 'stop must end the connection');
    assert.equal(queue.handedOut(), 1, 'no reconnect after stop');
    await listener.stop(); // second stop is a no-op
  });

  test('stop() during the reconnect backoff exits without a new connection', async () => {
    const failing = new FakeConnection();
    failing.failConnect = true;
    const spare = new FakeConnection();
    const queue = connectionQueue(failing, spare);
    const errors: unknown[] = [];
    const listener = new PostgresWakeListener({
      connect: queue.factory,
      waker: new OutboxWaker(),
      reconnectDelayMs: 60_000, // long backoff: only an aborted sleep exits fast
      onError: (error) => errors.push(error),
    });
    listener.start();
    await until(() => errors.length === 1, 'the failed connect to be reported');
    const start = Date.now();
    await listener.stop();
    assert.ok(Date.now() - start < 4_000, 'stop must cut the 60s backoff short');
    assert.equal(queue.handedOut(), 1, 'the spare connection must never be used');
  });

  test('stop() before start() resolves immediately', async () => {
    const listener = new PostgresWakeListener({
      connect: connectionQueue().factory,
      waker: new OutboxWaker(),
    });
    await listener.stop();
  });

  test('start() twice runs a single supervision loop', async () => {
    const connection = new FakeConnection();
    const queue = connectionQueue(connection);
    const listener = new PostgresWakeListener({
      connect: queue.factory,
      waker: new OutboxWaker(),
    });
    listener.start();
    listener.start();
    await until(() => connection.queries.length === 1, 'LISTEN');
    assert.equal(queue.handedOut(), 1);
    await listener.stop();
  });

  test('an error while closing the old connection is reported, and the loop survives', async () => {
    const first = new FakeConnection();
    first.failEnd = true; // cleanup after the drop will itself fail once
    const second = new FakeConnection();
    const errors: unknown[] = [];
    const listener = new PostgresWakeListener({
      connect: connectionQueue(first, second).factory,
      waker: new OutboxWaker(),
      reconnectDelayMs: 5,
      onError: (error) => errors.push(error),
    });
    listener.start();
    await until(() => first.queries.length === 1, 'first LISTEN');
    first.emit('error', new Error('connection reset'));
    await until(() => second.queries.length === 1, 'reconnect despite failed cleanup');
    const messages = errors.map((e) => (e as Error).message);
    assert.ok(messages.includes('connection reset'));
    assert.ok(messages.includes('end failed'));
    await listener.stop();
  });

  test('failures without an onError handler are still swallowed', async () => {
    const failing = new FakeConnection();
    failing.failConnect = true;
    const healthy = new FakeConnection();
    const listener = new PostgresWakeListener({
      connect: connectionQueue(failing, healthy).factory,
      waker: new OutboxWaker(),
      reconnectDelayMs: 5,
    });
    listener.start(); // must not crash the process
    await until(() => healthy.queries.length === 1, 'silent retry');
    await listener.stop();
  });

  test('rejects an unsafe channel at construction', () => {
    assert.throws(
      () =>
        new PostgresWakeListener({
          connect: connectionQueue().factory,
          waker: new OutboxWaker(),
          channel: 'wake; DROP TABLE outbox_events',
        }),
      /invalid wake channel/,
    );
  });

  test('rejects a reconnectDelayMs that would hot-loop', () => {
    for (const bad of [0, -5, Number.NaN]) {
      assert.throws(
        () =>
          new PostgresWakeListener({
            connect: connectionQueue().factory,
            waker: new OutboxWaker(),
            reconnectDelayMs: bad,
          }),
        /invalid reconnectDelayMs/,
        `reconnectDelayMs=${bad} must be rejected`,
      );
    }
  });

  test('stop() during a hung connect() resolves promptly (pg end-during-connect deadlock)', async () => {
    // pg never settles connect() once end() was called mid-connect; only 'end'
    // fires. An un-raced `await connect()` would park stop() forever.
    const connection = new FakeConnection();
    connection.hangConnect = true;
    const listener = new PostgresWakeListener({
      connect: connectionQueue(connection).factory,
      waker: new OutboxWaker(),
    });
    listener.start();
    await until(() => connection.connectCalls === 1, 'connect to be in flight');
    const start = Date.now();
    await listener.stop(); // end() → 'end' → the raced session returns
    assert.ok(Date.now() - start < 4_000, 'stop must not await the dead connect');
    assert.equal(connection.queries.length, 0, 'LISTEN must never be issued');
  });

  test('a throwing custom WakeSignal is routed to onError, never thrown into pg', async () => {
    const connection = new FakeConnection();
    const errors: unknown[] = [];
    const listener = new PostgresWakeListener({
      connect: connectionQueue(connection).factory,
      waker: {
        notify: () => {
          throw new Error('user waker exploded');
        },
      },
      onError: (error) => errors.push(error),
    });
    listener.start();
    await until(() => connection.queries.length === 1, 'LISTEN');
    connection.emit('notification'); // must not become an uncaught exception
    assert.equal(errors.length, 1);
    assert.match((errors[0] as Error).message, /user waker exploded/);
    await listener.stop();
  });

  test('a throwing custom WakeSignal without onError is still swallowed', async () => {
    const connection = new FakeConnection();
    const listener = new PostgresWakeListener({
      connect: connectionQueue(connection).factory,
      waker: {
        notify: () => {
          throw new Error('boom');
        },
      },
    });
    listener.start();
    await until(() => connection.queries.length === 1, 'LISTEN');
    connection.emit('notification'); // no onError, still no crash
    await listener.stop();
  });
});
