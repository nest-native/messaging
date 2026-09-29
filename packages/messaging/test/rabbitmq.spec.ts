import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { EventEmitter } from 'node:events';
import { after, before, beforeEach, describe, test } from 'node:test';
import { Logger } from '@nestjs/common';
import type { Channel, ConfirmChannel, ConsumeMessage } from 'amqplib';
import {
  RabbitInboxConsumer,
  RabbitOutboxTransport,
  X_DEAD_LETTER_ID,
  type RabbitConfirmChannelSource,
  type RabbitConsumeOptions,
  type RabbitConsumeResult,
} from '../adapters/rabbitmq';
import type { InboxService } from '../inbox.service';
import { PermanentError, RetryableError } from '../transport';
import { decodeWireValue, X_ERROR, X_EVENT_ID, X_IDEMPOTENCY_KEY } from '../wire-contract';

type Behaviour = 'ack' | 'slow-ack' | 'nack' | 'nack-non-error' | 'return' | 'return-anonymous' | 'hang';

interface Published {
  exchange: string;
  routingKey: string;
  content: Buffer;
  options: Record<string, unknown>;
}

/**
 * An amqplib 2 ConfirmChannel double that behaves like the real one where the
 * adapter depends on it: `publish` throws once the channel has closed; a
 * server-side close emits 'error' with the broker's reason, fails every
 * unconfirmed publish with a bare "channel closed", then emits 'close'; and
 * `waitForConfirms` waits for the outstanding ones.
 */
class FakeConfirmChannel extends EventEmitter {
  readonly published: Published[] = [];
  readonly calls: string[] = [];
  closed = false;
  closeFails = false;
  private readonly pending = new Set<(error: unknown) => void>();
  private readonly drained: (() => void)[] = [];

  constructor(public behaviour: Behaviour = 'ack') {
    super();
  }

  publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options: Record<string, unknown>,
    callback: (error: unknown) => void,
  ): boolean {
    if (this.closed) {
      throw new Error('Channel closed');
    }
    this.published.push({ exchange, routingKey, content, options });
    const returned = { properties: { messageId: options.messageId, headers: options.headers } };
    const settle = (error: unknown): void => {
      this.pending.delete(settle);
      callback(error);
      if (this.pending.size === 0) this.drained.splice(0).forEach((resolve) => resolve());
    };
    this.pending.add(settle);
    const answer = (): void => {
      switch (this.behaviour) {
        case 'ack':
        case 'slow-ack':
          return settle(null);
        case 'nack':
          return settle(new Error('nacked by broker'));
        case 'nack-non-error':
          return settle('channel closed');
        case 'return':
          this.emit('return', returned);
          return settle(null);
        case 'return-anonymous':
          this.emit('return', { properties: {} });
          return settle(null);
        case 'hang':
          return undefined;
      }
    };
    if (this.behaviour === 'slow-ack') setTimeout(answer, 20);
    else setImmediate(answer);
    return true;
  }

  waitForConfirms(): Promise<void> {
    this.calls.push('waitForConfirms');
    if (this.pending.size === 0) return Promise.resolve();
    return new Promise((resolve) => this.drained.push(resolve));
  }

  /** What the broker does when it closes the channel (a missing exchange, an access error, …). */
  serverClose(reason: string): void {
    this.emit('error', new Error(reason));
    this.failPending();
    this.closed = true;
    this.emit('close');
  }

  close(): Promise<void> {
    this.calls.push('close');
    this.failPending();
    this.closed = true;
    this.emit('close');
    return this.closeFails ? Promise.reject(new Error('already closed')) : Promise.resolve();
  }

  private failPending(): void {
    for (const settle of [...this.pending]) settle(new Error('channel closed'));
  }
}

/** A connection double that hands out fake channels and counts the requests. */
function connection(...channels: (FakeConfirmChannel | Error | 'never')[]) {
  let opened = 0;
  const source: RabbitConfirmChannelSource = {
    createConfirmChannel: () => {
      const next = channels[Math.min(opened, channels.length - 1)];
      opened += 1;
      if (next === 'never') return new Promise<never>(() => undefined);
      if (next instanceof Error) return Promise.reject(next);
      return Promise.resolve(next as unknown as ConfirmChannel);
    },
  };
  return { source, opened: () => opened };
}

/** A connection whose channel opens only when the test says so. */
function deferredConnection() {
  const opens: { resolve: (channel: FakeConfirmChannel) => void; reject: (error: Error) => void }[] = [];
  const source: RabbitConfirmChannelSource = {
    createConfirmChannel: () =>
      new Promise<ConfirmChannel>((resolve, reject) => {
        opens.push({ resolve: (channel) => resolve(channel as unknown as ConfirmChannel), reject });
      }),
  };
  return { source, opens };
}

const event = { id: 'evt-1', topic: 'order.placed', payload: { orderId: 7 } };
const tick = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));

async function rejection(work: Promise<unknown>): Promise<Error> {
  try {
    await work;
  } catch (error) {
    return error as Error;
  }
  assert.fail('expected the publish to reject');
}

/** The claimer retries a plain Error until maxAttempts; these must be exactly that. */
function assertPlainFailure(error: Error, pattern: RegExp): void {
  assert.match(error.message, pattern);
  assert.ok(!(error instanceof RetryableError), 'must not retry forever');
  assert.ok(!(error instanceof PermanentError), 'must not fail on the first attempt');
}

describe('RabbitOutboxTransport', () => {
  test('publishes JSON, persistent and mandatory, with the wire-contract headers', async () => {
    const channel = new FakeConfirmChannel();
    const transport = new RabbitOutboxTransport({
      connection: connection(channel).source,
      exchange: 'events',
      routingKeyPrefix: 'prod.',
    });
    await transport.publish({ ...event, idempotencyKey: 'order:7' });

    assert.equal(channel.published.length, 1);
    const [published] = channel.published;
    assert.equal(published?.exchange, 'events');
    assert.equal(published?.routingKey, 'prod.order.placed');
    assert.deepEqual(decodeWireValue(published!.content), { orderId: 7 });
    assert.equal(published?.options.persistent, true);
    assert.equal(published?.options.mandatory, true);
    assert.equal(published?.options.contentType, 'application/json');
    assert.equal(published?.options.messageId, 'evt-1');
    assert.deepEqual(published?.options.headers, {
      [X_EVENT_ID]: 'evt-1',
      [X_IDEMPOTENCY_KEY]: 'order:7',
    });
  });

  test('falls back to the event id as the idempotency key and to no prefix', async () => {
    const channel = new FakeConfirmChannel();
    await new RabbitOutboxTransport({ connection: connection(channel).source, exchange: 'events' })
      .publish(event);
    const [published] = channel.published;
    assert.equal(published?.routingKey, 'order.placed');
    assert.equal((published?.options.headers as Record<string, string>)[X_IDEMPOTENCY_KEY], 'evt-1');
  });

  test('opens one channel and reuses it', async () => {
    const channel = new FakeConfirmChannel();
    const conn = connection(channel);
    const transport = new RabbitOutboxTransport({ connection: conn.source, exchange: 'events' });
    await transport.publish(event);
    await transport.publish({ ...event, id: 'evt-2' });
    assert.equal(conn.opened(), 1);
    assert.equal(channel.published.length, 2);
  });

  test('a nack fails the attempt with a plain Error', async () => {
    const transport = new RabbitOutboxTransport({
      connection: connection(new FakeConfirmChannel('nack')).source,
      exchange: 'events',
    });
    assertPlainFailure(
      await rejection(transport.publish(event)),
      /broker did not confirm event evt-1: nacked by broker$/,
    );
  });

  test('a callback error that is not an Error is still described', async () => {
    const transport = new RabbitOutboxTransport({
      connection: connection(new FakeConfirmChannel('nack-non-error')).source,
      exchange: 'events',
    });
    assertPlainFailure(await rejection(transport.publish(event)), /: channel closed$/);
  });

  test("a channel the broker closes fails the publish with the broker's reason", async () => {
    const channel = new FakeConfirmChannel('hang');
    const transport = new RabbitOutboxTransport({
      connection: connection(channel).source,
      exchange: 'events',
    });
    const publish = rejection(transport.publish(event));
    await tick();
    channel.serverClose("NOT_FOUND - no exchange 'events' in vhost '/'");
    assertPlainFailure(
      await publish,
      /broker did not confirm event evt-1: channel closed \(NOT_FOUND - no exchange 'events' in vhost '\/'\)$/,
    );
  });

  test('a publish on a channel that has just closed fails the attempt', async () => {
    const channel = new FakeConfirmChannel();
    const transport = new RabbitOutboxTransport({ connection: connection(channel).source, exchange: 'events' });
    await transport.publish(event);
    channel.closed = true; // closed, but its 'close' has not been processed yet
    assert.match((await rejection(transport.publish({ ...event, id: 'evt-2' }))).message, /Channel closed/);
  });

  test('an unroutable event fails the attempt even though the broker acked it', async () => {
    const transport = new RabbitOutboxTransport({
      connection: connection(new FakeConfirmChannel('return')).source,
      exchange: 'events',
      routingKeyPrefix: 'prod.',
    });
    assertPlainFailure(
      await rejection(transport.publish(event)),
      /event evt-1 is unroutable: no queue is bound to exchange "events" for routing key "prod.order.placed"/,
    );
  });

  test('a return is only charged to the message it names', async () => {
    const transport = new RabbitOutboxTransport({
      connection: connection(new FakeConfirmChannel('return-anonymous')).source,
      exchange: 'events',
    });
    await transport.publish(event);
  });

  test('a confirm that never arrives times out', async () => {
    const transport = new RabbitOutboxTransport({
      connection: connection(new FakeConfirmChannel('hang')).source,
      exchange: 'events',
      confirmTimeoutMs: 20,
    });
    assertPlainFailure(
      await rejection(transport.publish(event)),
      /timed out after 20 ms waiting for the broker to confirm event evt-1/,
    );
  });

  test('a channel that never opens times out', async () => {
    const transport = new RabbitOutboxTransport({
      connection: connection('never').source,
      exchange: 'events',
      confirmTimeoutMs: 20,
    });
    assertPlainFailure(
      await rejection(transport.publish(event)),
      /timed out after 20 ms waiting for a confirm channel/,
    );
  });

  test('a channel that fails to open is retried on the next publish', async () => {
    const channel = new FakeConfirmChannel();
    const conn = connection(new Error('connection refused'), channel);
    const transport = new RabbitOutboxTransport({ connection: conn.source, exchange: 'events' });
    assertPlainFailure(
      await rejection(transport.publish(event)),
      /could not open a RabbitMQ confirm channel: connection refused/,
    );
    await transport.publish(event);
    assert.equal(conn.opened(), 2);
    assert.equal(channel.published.length, 1);
  });

  test('a channel open failure that is not an Error is still described', async () => {
    const source: RabbitConfirmChannelSource = {
      createConfirmChannel: () => Promise.reject('socket hang up'),
    };
    const transport = new RabbitOutboxTransport({ connection: source, exchange: 'events' });
    assertPlainFailure(await rejection(transport.publish(event)), /: socket hang up$/);
  });

  for (const signal of ['close', 'error'] as const) {
    test(`a channel ${signal} makes the next publish open a fresh channel`, async () => {
      const first = new FakeConfirmChannel();
      const second = new FakeConfirmChannel();
      const conn = connection(first, second);
      const transport = new RabbitOutboxTransport({ connection: conn.source, exchange: 'events' });
      await transport.publish(event);
      first.emit(signal, signal === 'error' ? new Error('PRECONDITION_FAILED') : undefined);
      await transport.publish({ ...event, id: 'evt-2' });
      assert.equal(conn.opened(), 2);
      assert.equal(second.published.length, 1);
    });
  }

  test('a late close from a replaced channel does not drop the current one', async () => {
    const first = new FakeConfirmChannel();
    const second = new FakeConfirmChannel();
    const conn = connection(first, second);
    const transport = new RabbitOutboxTransport({ connection: conn.source, exchange: 'events' });
    await transport.publish(event);
    first.emit('close');
    await transport.publish({ ...event, id: 'evt-2' });
    first.emit('close');
    await transport.publish({ ...event, id: 'evt-3' });
    assert.equal(conn.opened(), 2);
    assert.equal(second.published.length, 2);
  });

  test('close() closes its channel, and the next publish opens a new one', async () => {
    const first = new FakeConfirmChannel();
    const second = new FakeConfirmChannel();
    const conn = connection(first, second);
    const transport = new RabbitOutboxTransport({ connection: conn.source, exchange: 'events' });
    await transport.close();
    assert.equal(conn.opened(), 0, 'closing before any publish opens nothing');
    await transport.publish(event);
    first.closeFails = true;
    await transport.close();
    assert.ok(first.closed);
    await transport.publish({ ...event, id: 'evt-2' });
    assert.equal(conn.opened(), 2);
  });

  test('close() lets outstanding confirms arrive before it closes the channel', async () => {
    const channel = new FakeConfirmChannel('slow-ack');
    const transport = new RabbitOutboxTransport({ connection: connection(channel).source, exchange: 'events' });
    await transport.publish(event); // open the channel
    const inFlight = transport.publish({ ...event, id: 'evt-2' });
    await tick();
    await transport.close();
    // Closing first would have failed a publish the broker already took.
    await inFlight;
    assert.deepEqual(channel.calls, ['waitForConfirms', 'close']);
  });

  test('close() lets a publish that has not reached the channel yet finish first', async () => {
    const channel = new FakeConfirmChannel('slow-ack');
    const transport = new RabbitOutboxTransport({ connection: connection(channel).source, exchange: 'events' });
    await transport.publish(event); // open the channel
    // Called in the same job: the publish is still awaiting its channel, so the
    // channel has no unconfirmed publish for close() to wait for.
    const inFlight = transport.publish({ ...event, id: 'evt-2' });
    const closing = transport.close();
    await inFlight;
    await closing;
    assert.equal(channel.published.length, 2);
    assert.deepEqual(channel.calls, ['waitForConfirms', 'close']);
  });

  test('close() stops waiting for confirms after confirmTimeoutMs', async () => {
    const channel = new FakeConfirmChannel();
    const transport = new RabbitOutboxTransport({
      connection: connection(channel).source,
      exchange: 'events',
      confirmTimeoutMs: 20,
    });
    await transport.publish(event);
    channel.behaviour = 'hang';
    const inFlight = rejection(transport.publish({ ...event, id: 'evt-2' }));
    await tick();
    await transport.close();
    assert.ok(channel.closed, 'closed after the drain timed out');
    assert.match((await inFlight).message, /evt-2/);
  });

  test('close() does not wait for a channel that is still opening, and closes it once it opens', async () => {
    const conn = deferredConnection();
    const transport = new RabbitOutboxTransport({
      connection: conn.source,
      exchange: 'events',
      confirmTimeoutMs: 1_000,
    });
    const publish = rejection(transport.publish(event));
    await tick();
    await transport.close(); // returns although the channel has not opened
    const late = new FakeConfirmChannel();
    conn.opens[0]?.resolve(late);
    await tick();
    await tick();
    assert.ok(late.closed, 'the channel that opened after close() is closed');
    await publish;
  });

  test('a channel that opens after close() never becomes the current one', async () => {
    const conn = deferredConnection();
    const transport = new RabbitOutboxTransport({ connection: conn.source, exchange: 'events' });
    const orphaned = transport.publish(event).catch(() => undefined);
    await tick();
    await transport.close();
    const current = new FakeConfirmChannel();
    const next = transport.publish({ ...event, id: 'evt-2' });
    await tick();
    conn.opens[1]?.resolve(current);
    await next;
    conn.opens[0]?.resolve(new FakeConfirmChannel());
    await orphaned;
    await transport.publish({ ...event, id: 'evt-3' });
    assert.equal(conn.opens.length, 2, 'the late channel did not drop the current one');
    assert.equal(current.published.length, 2);
  });

  test('a channel that fails to open after close() does not clear a newer one', async () => {
    const conn = deferredConnection();
    const transport = new RabbitOutboxTransport({ connection: conn.source, exchange: 'events' });
    const failing = transport.publish(event).catch(() => undefined);
    await tick();
    await transport.close();
    const current = new FakeConfirmChannel();
    const next = transport.publish({ ...event, id: 'evt-2' });
    await tick();
    conn.opens[1]?.resolve(current);
    await next;
    conn.opens[0]?.reject(new Error('connection refused'));
    await failing;
    await transport.publish({ ...event, id: 'evt-3' });
    assert.equal(conn.opens.length, 2);
    assert.equal(current.published.length, 2);
  });
});

describe('RabbitInboxConsumer', () => {
  const logs: { level: string; message: unknown }[] = [];
  before(() => {
    Logger.overrideLogger({
      log: (message: unknown) => logs.push({ level: 'log', message }),
      error: (message: unknown) => logs.push({ level: 'error', message }),
      warn: (message: unknown) => logs.push({ level: 'warn', message }),
      debug: (message: unknown) => logs.push({ level: 'debug', message }),
      verbose: (message: unknown) => logs.push({ level: 'verbose', message }),
    });
  });
  after(() => {
    Logger.overrideLogger(false);
  });
  beforeEach(() => {
    logs.length = 0;
  });
  const logged = (text: string): boolean => logs.some((l) => String(l.message).includes(text));

  type Payload = { orderId: number };
  const validate = (p: unknown): p is Payload =>
    typeof p === 'object' && p !== null && typeof (p as Payload).orderId === 'number';
  /** Retries in the tests wait 1 ms, doubling. */
  const fast = { delayMs: 1 };

  /**
   * Records the ack/nack calls the consumer makes on the delivery channel. A
   * closed channel throws from both, as amqplib 2 does.
   */
  function deliveryChannel(closed = false) {
    const settled: { kind: 'ack' | 'nack'; requeue?: boolean }[] = [];
    const record = (entry: { kind: 'ack' | 'nack'; requeue?: boolean }) => {
      if (closed) throw new Error('Channel closed');
      settled.push(entry);
    };
    const channel = {
      ack: () => record({ kind: 'ack' }),
      nack: (_m: unknown, _allUpTo: boolean, requeue: boolean) => record({ kind: 'nack', requeue }),
    } as unknown as Channel;
    return { channel, settled };
  }

  function delivery(
    content: string,
    properties: Partial<ConsumeMessage['properties']> = {},
  ): ConsumeMessage {
    return {
      content: Buffer.from(content),
      fields: {} as ConsumeMessage['fields'],
      properties: { headers: {}, ...properties } as ConsumeMessage['properties'],
    };
  }

  function inbox(impl: InboxService['runOnce']): InboxService {
    return { runOnce: impl } as unknown as InboxService;
  }

  const runs = (outcome: 'processed' | 'duplicate') =>
    inbox(async (_key, _source, sideEffect) => {
      await sideEffect();
      return outcome;
    });

  const failing = (message = 'database is locked') =>
    inbox(async () => {
      throw new Error(message);
    });

  function options(
    channel: Channel,
    message: ConsumeMessage,
    extra: Partial<RabbitConsumeOptions<Payload>> = {},
  ): RabbitConsumeOptions<Payload> {
    return { source: 'q', channel, message, validate, sideEffect: () => undefined, ...extra };
  }

  test('processes a new message exactly once and acks it', async () => {
    const seen: { payload: Payload; key: string }[] = [];
    const calls: { key: string; source: string }[] = [];
    const consumer = new RabbitInboxConsumer(
      inbox(async (key, source, sideEffect) => {
        calls.push({ key, source });
        await sideEffect();
        return 'processed';
      }),
    );
    const { channel, settled } = deliveryChannel();
    const result = await consumer.consume({
      source: 'orders.audit',
      channel,
      message: delivery('{"orderId":7}', {
        headers: { [X_EVENT_ID]: Buffer.from('evt-1') },
        messageId: 'ignored-when-an-event-id-exists',
      }),
      validate,
      sideEffect: (payload, key) => {
        seen.push({ payload, key });
      },
    });
    assert.deepEqual(result, { outcome: 'processed', dedupKey: 'evt-1' });
    assert.deepEqual(calls, [{ key: 'evt-1', source: 'orders.audit' }]);
    assert.deepEqual(seen, [{ payload: { orderId: 7 }, key: 'evt-1' }]);
    assert.deepEqual(settled, [{ kind: 'ack' }]);
  });

  test('acks a duplicate without running the side effect again', async () => {
    const consumer = new RabbitInboxConsumer(inbox(async () => 'duplicate'));
    const { channel, settled } = deliveryChannel();
    const result = await consumer.consume(
      options(channel, delivery('{"orderId":7}', { messageId: 'evt-9' }), {
        sideEffect: () => assert.fail('must not run'),
      }),
    );
    assert.deepEqual(result, { outcome: 'duplicate', dedupKey: 'evt-9' });
    assert.deepEqual(settled, [{ kind: 'ack' }]);
    assert.ok(logs.some((l) => l.level === 'debug' && String(l.message).includes('evt-9')));
  });

  test('falls back to the AMQP messageId when no dedup header is present', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel } = deliveryChannel();
    const result = await consumer.consume(
      options(channel, delivery('{"orderId":1}', { headers: undefined, messageId: 'amqp-id' })),
    );
    assert.equal(result.dedupKey, 'amqp-id');
  });

  test('reads numeric AMQP header values as their string form, and skips values that cannot be keys', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel } = deliveryChannel();
    // amqplib decodes typed field-table values: JVM and Python producers send numbers.
    const numeric = await consumer.consume(
      options(channel, delivery('{"orderId":1}', { headers: { [X_EVENT_ID]: 12345 } })),
    );
    assert.equal(numeric.dedupKey, '12345');
    const bigint = await consumer.consume(
      options(channel, delivery('{"orderId":1}', {
        headers: { [X_EVENT_ID]: true, [X_IDEMPOTENCY_KEY]: 42n, 'x-table': { nested: 1 } },
      })),
    );
    assert.equal(bigint.dedupKey, '42', 'a boolean is not a key; the next header is');
    const messageId = await consumer.consume(
      options(channel, delivery('{"orderId":1}', { messageId: 7 as unknown as string })),
    );
    assert.equal(messageId.dedupKey, '7');
  });

  test('never takes an id amqplib rounded as a key: it falls back to the next one, or dead-letters with the reason', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel, settled } = deliveryChannel();
    // amqplib reads a 64-bit integer header through a double: 1541815603606036481
    // and 1541815603606036482 both arrive as 1541815603606036500.
    const rounded = Number(1541815603606036481n);
    const fallback = await consumer.consume(
      options(channel, delivery('{"orderId":1}', { messageId: 'evt-9', headers: { [X_EVENT_ID]: rounded } })),
    );
    assert.equal(fallback.dedupKey, 'evt-9', 'the next id the wire contract names');
    const lone = await consumer.consume(
      options(channel, delivery('{"orderId":1}', { headers: { [X_IDEMPOTENCY_KEY]: rounded } })),
    );
    assert.deepEqual(lone, { outcome: 'dead-lettered' });
    assert.ok(logged(`${X_IDEMPOTENCY_KEY} 1541815603606036500 is an integer beyond 2^53`));
    const fraction = await consumer.consume(
      options(channel, delivery('{"orderId":1}', { headers: { [X_EVENT_ID]: 1.5 } })),
    );
    assert.deepEqual(fraction, { outcome: 'dead-lettered' });
    assert.ok(logged('cannot deduplicate'), 'a fraction is simply not a key');
    assert.deepEqual(settled, [
      { kind: 'ack' },
      { kind: 'nack', requeue: false },
      { kind: 'nack', requeue: false },
    ]);
  });

  const poison: [string, ConsumeMessage, RegExp, string | undefined][] = [
    ['no dedup key at all', delivery('{"orderId":1}'), /cannot deduplicate/, undefined],
    ['a body that is not JSON', delivery('not json', { messageId: 'm' }), /not valid JSON/, 'm'],
    ['a payload validate rejects', delivery('{"nope":1}', { messageId: 'm' }), /failed validation/, 'm'],
  ];
  for (const [label, message, reason, dedupKey] of poison) {
    test(`rejects ${label} without requeue when no dead-letter target is given`, async () => {
      const consumer = new RabbitInboxConsumer(runs('processed'));
      const { channel, settled } = deliveryChannel();
      const result = await consumer.consume(
        options(channel, message, { sideEffect: () => assert.fail('must not run') }),
      );
      // The key travels with the result whenever one could be derived, so a
      // dead letter can be traced to its event.
      assert.deepEqual(
        result,
        dedupKey === undefined ? { outcome: 'dead-lettered' } : { outcome: 'dead-lettered', dedupKey },
      );
      assert.deepEqual(settled, [{ kind: 'nack', requeue: false }]);
      assert.ok(logs.some((l) => l.level === 'warn' && reason.test(String(l.message))));
    });
  }

  test('dead-letters a payload whose validate throws, instead of retrying it forever', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel, settled } = deliveryChannel();
    const result = await consumer.consume({
      source: 'q',
      channel,
      message: delivery('{"orderId":1}', { messageId: 'm' }),
      validate: (p: unknown): p is Payload => {
        return (p as { nested: { id: number } }).nested.id > 0; // throws on this payload
      },
      sideEffect: () => assert.fail('must not run'),
    });
    assert.deepEqual(result, { outcome: 'dead-lettered', dedupKey: 'm' });
    assert.deepEqual(settled, [{ kind: 'nack', requeue: false }]);
    assert.ok(logged('validate threw:'));
  });

  test('republishes poison to the dead-letter target with its reason, then acks', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel, settled } = deliveryChannel();
    const dlq = new FakeConfirmChannel();
    const message = delivery('{"nope":1}', {
      messageId: 'evt-5',
      headers: { [X_EVENT_ID]: 'evt-5', 'x-custom': 'kept' },
      contentType: 'application/json',
      expiration: '60000',
      userId: 'someone-else',
    });
    const result = await consumer.consume(
      options(channel, message, {
        sideEffect: () => assert.fail('must not run'),
        deadLetter: {
          channel: dlq as unknown as ConfirmChannel,
          exchange: 'dlx',
          routingKey: 'orders.dead',
        },
      }),
    );
    assert.deepEqual(result, { outcome: 'dead-lettered', dedupKey: 'evt-5' });
    assert.deepEqual(settled, [{ kind: 'ack' }]);
    const [republished] = dlq.published;
    assert.equal(republished?.exchange, 'dlx');
    assert.equal(republished?.routingKey, 'orders.dead');
    assert.equal(republished?.content.toString(), '{"nope":1}');
    assert.equal(republished?.options.messageId, 'evt-5');
    assert.equal(republished?.options.persistent, true);
    assert.equal(republished?.options.mandatory, true, 'an unroutable copy must come back, not vanish');
    assert.equal(republished?.options.expiration, undefined, 'the copy must not expire');
    assert.equal(republished?.options.userId, undefined, 'the broker rejects a foreign userId');
    const headers = republished?.options.headers as Record<string, unknown>;
    assert.equal(typeof headers[X_DEAD_LETTER_ID], 'string');
    const { [X_DEAD_LETTER_ID]: _id, ...rest } = headers;
    assert.deepEqual(rest, {
      [X_EVENT_ID]: 'evt-5',
      'x-custom': 'kept',
      [X_ERROR]: 'payload failed validation',
    });
  });

  test('an unroutable dead-letter copy requeues the original instead of losing it', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel, settled } = deliveryChannel();
    const dlq = new FakeConfirmChannel('return');
    const result = await consumer.consume(
      options(channel, delivery('{"nope":1}', { messageId: 'm' }), {
        retry: fast,
        deadLetter: { channel: dlq as unknown as ConfirmChannel, exchange: 'dlx', routingKey: 'dead' },
      }),
    );
    assert.deepEqual(result, { outcome: 'requeued', dedupKey: 'm' });
    assert.deepEqual(settled, [{ kind: 'nack', requeue: true }]);
    assert.ok(logged('could not dead-letter (no queue is bound to exchange "dlx" for routing key "dead")'));
  });

  test('ignores a return from the dead-letter channel that names no copy', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel, settled } = deliveryChannel();
    const dlq = new FakeConfirmChannel('return-anonymous');
    const result = await consumer.consume(
      options(channel, delivery('{"nope":1}', { messageId: 'm' }), {
        deadLetter: { channel: dlq as unknown as ConfirmChannel, exchange: 'dlx', routingKey: 'dead' },
      }),
    );
    assert.deepEqual(result, { outcome: 'dead-lettered', dedupKey: 'm' });
    assert.deepEqual(settled, [{ kind: 'ack' }]);
  });

  test('requeues the poison message when the dead-letter publish is nacked', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel, settled } = deliveryChannel();
    const result = await consumer.consume(
      options(channel, delivery('{"nope":1}', { messageId: 'm' }), {
        retry: fast,
        deadLetter: {
          channel: new FakeConfirmChannel('nack') as unknown as ConfirmChannel,
          exchange: 'dlx',
          routingKey: 'dead',
        },
      }),
    );
    assert.deepEqual(result, { outcome: 'requeued', dedupKey: 'm' });
    assert.deepEqual(settled, [{ kind: 'nack', requeue: true }]);
    assert.ok(logged('could not dead-letter (nacked by broker); requeued in 1 ms'));
  });

  test('requeues the poison message when the dead-letter channel has closed', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel, settled } = deliveryChannel();
    const dlq = new FakeConfirmChannel();
    dlq.closed = true; // publish throws synchronously, as amqplib does
    const result = await consumer.consume(
      options(channel, delivery('{"nope":1}'), {
        retry: fast,
        deadLetter: { channel: dlq as unknown as ConfirmChannel, exchange: 'dlx', routingKey: 'dead' },
      }),
    );
    assert.deepEqual(result, { outcome: 'requeued' });
    assert.deepEqual(settled, [{ kind: 'nack', requeue: true }]);
    assert.ok(logged('could not dead-letter (Channel closed)'));
  });

  test('a dead-letter callback error that is not an Error is still described', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel } = deliveryChannel();
    await consumer.consume(
      options(channel, delivery('{"nope":1}', { messageId: 'm' }), {
        retry: fast,
        deadLetter: {
          channel: new FakeConfirmChannel('nack-non-error') as unknown as ConfirmChannel,
          exchange: 'dlx',
          routingKey: 'dead',
        },
      }),
    );
    assert.ok(logged('could not dead-letter (channel closed)'));
  });

  test('listens for returns once per dead-letter channel', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel } = deliveryChannel();
    const dlq = new FakeConfirmChannel();
    const deadLetter = { channel: dlq as unknown as ConfirmChannel, exchange: 'dlx', routingKey: 'dead' };
    await consumer.consume(options(channel, delivery('{"nope":1}', { messageId: 'a' }), { deadLetter }));
    await consumer.consume(options(channel, delivery('{"nope":2}', { messageId: 'b' }), { deadLetter }));
    assert.equal(dlq.listenerCount('return'), 1);
    assert.equal(dlq.published.length, 2);
  });

  test('a dead-letter channel the broker closes is logged, not thrown out of amqplib', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel } = deliveryChannel();
    const dlq = new FakeConfirmChannel();
    const deadLetter = { channel: dlq as unknown as ConfirmChannel, exchange: 'dlx', routingKey: 'dead' };
    await consumer.consume(options(channel, delivery('{"nope":1}', { messageId: 'a' }), { deadLetter }));
    await consumer.consume(options(channel, delivery('{"nope":2}', { messageId: 'b' }), { deadLetter }));
    assert.equal(dlq.listenerCount('error'), 1);
    // amqplib emits the broker's reason as 'error'; with no listener, the
    // emitter would throw it into amqplib, which closes the whole connection.
    assert.doesNotThrow(() => dlq.serverClose("NOT_FOUND - no exchange 'dlx'"));
    assert.ok(logged("the dead-letter channel was closed by the broker: NOT_FOUND - no exchange 'dlx'"));
  });

  test('cuts a long reason in the x-error header, saying how much was left out', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel } = deliveryChannel();
    const dlq = new FakeConfirmChannel();
    const result = await consumer.consume({
      source: 'q',
      channel,
      message: delivery('{"orderId":1}', { messageId: 'm' }),
      // A schema validator's message for a large payload can run to 100 KB;
      // amqplib cannot encode a header table past 64 KiB.
      validate: (_p: unknown): _p is Payload => {
        throw new Error('x'.repeat(120_000));
      },
      sideEffect: () => assert.fail('must not run'),
      deadLetter: { channel: dlq as unknown as ConfirmChannel, exchange: 'dlx', routingKey: 'dead' },
    });
    assert.deepEqual(result, { outcome: 'dead-lettered', dedupKey: 'm' });
    const reason = (dlq.published[0]?.options.headers as Record<string, unknown>)[X_ERROR] as string;
    assert.match(reason, /^validate threw: x+… \(119016 more characters\)$/);
    assert.equal(reason.indexOf('…'), 1_000);
  });

  test('a dead-letter target that keeps failing backs off like a transient failure', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel } = deliveryChannel();
    const dlq = new FakeConfirmChannel('nack');
    const poisonAgain = () =>
      consumer.consume(
        options(channel, delivery('{"nope":1}', { messageId: 'm' }), {
          retry: { delayMs: 1, maxDelayMs: 4 },
          deadLetter: { channel: dlq as unknown as ConfirmChannel, exchange: 'dlx', routingKey: 'dead' },
        }),
      );
    for (let attempt = 1; attempt <= 4; attempt += 1) {
      await poisonAgain();
    }
    // Once the copy is stored, the count starts over.
    dlq.behaviour = 'ack';
    assert.equal((await poisonAgain()).outcome, 'dead-lettered');
    dlq.behaviour = 'nack';
    await poisonAgain();
    const waits = logs
      .map((l) => /requeued in (\d+) ms/.exec(String(l.message)))
      .filter(Boolean)
      .map((m) => m![1]);
    assert.deepEqual(waits, ['1', '2', '4', '4', '1']);
  });

  test('consumers that share a dead-letter channel share its return listener', async () => {
    const first = new RabbitInboxConsumer(runs('processed'));
    const second = new RabbitInboxConsumer(runs('processed'));
    const { channel } = deliveryChannel();
    const dlq = new FakeConfirmChannel('return');
    const deadLetter = { channel: dlq as unknown as ConfirmChannel, exchange: 'dlx', routingKey: 'dead' };
    const poisonTo = (consumer: RabbitInboxConsumer, id: string) =>
      consumer.consume(options(channel, delivery('{"nope":1}', { messageId: id }), { retry: fast, deadLetter }));
    // Each consumer sees the return of its own copy, and only one listener
    // records returns, so no consumer keeps the ids of another's copies.
    assert.equal((await poisonTo(first, 'a')).outcome, 'requeued');
    assert.equal((await poisonTo(second, 'b')).outcome, 'requeued');
    assert.equal(dlq.listenerCount('return'), 1);
    dlq.behaviour = 'ack';
    assert.equal((await poisonTo(first, 'c')).outcome, 'dead-lettered');
  });

  test('the wait before a requeue does not keep the process alive', async (t) => {
    const timers = t.mock.method(globalThis, 'setTimeout');
    const consumer = new RabbitInboxConsumer(failing());
    const { channel } = deliveryChannel();
    await consumer.consume(options(channel, delivery('{"orderId":1}', { messageId: 'm' }), { retry: { delayMs: 3 } }));
    const waits = timers.mock.calls.filter((call) => call.arguments[1] === 3);
    assert.equal(waits.length, 1);
    // After shutdown the broker already has the delivery back; the wait must not hold the process.
    assert.equal((waits[0]!.result as NodeJS.Timeout).hasRef(), false);
  });

  test('requeues a delivery whose side effect failed transiently, after a wait', async () => {
    const consumer = new RabbitInboxConsumer(failing());
    const { channel, settled } = deliveryChannel();
    const started = Date.now();
    const result = await consumer.consume(
      options(channel, delivery('{"orderId":1}', { messageId: 'm' }), { retry: { delayMs: 40 } }),
    );
    assert.ok(Date.now() - started >= 35, 'the requeue waits instead of spinning');
    assert.deepEqual(result, { outcome: 'requeued', dedupKey: 'm' });
    assert.deepEqual(settled, [{ kind: 'nack', requeue: true }]);
    assert.ok(logged('requeued for redelivery in 40 ms (attempt 1): database is locked'));
  });

  test('backs off per message: the wait doubles with every attempt, up to maxDelayMs', async () => {
    const consumer = new RabbitInboxConsumer(failing());
    const { channel } = deliveryChannel();
    for (let attempt = 1; attempt <= 4; attempt += 1) {
      await consumer.consume(
        options(channel, delivery('{"orderId":1}', { messageId: 'm' }), {
          retry: { delayMs: 1, maxDelayMs: 4 },
        }),
      );
    }
    const waits = logs
      .map((l) => /in (\d+) ms \(attempt (\d+)\)/.exec(String(l.message)))
      .filter(Boolean)
      .map((m) => `${m![2]}:${m![1]}`);
    assert.deepEqual(waits, ['1:1', '2:2', '3:4', '4:4']);
  });

  test('uses a 1 s initial wait by default', async () => {
    const consumer = new RabbitInboxConsumer(failing());
    const { channel } = deliveryChannel();
    const pending = consumer.consume(options(channel, delivery('{"orderId":1}', { messageId: 'm' })));
    await tick();
    await tick();
    assert.ok(logged('requeued for redelivery in 1000 ms (attempt 1)'));
    await pending;
  });

  test('a success resets the attempt count for that message', async () => {
    let fail = true;
    const consumer = new RabbitInboxConsumer(
      inbox(async () => {
        if (fail) throw new Error('database is locked');
        return 'processed';
      }),
    );
    const { channel } = deliveryChannel();
    const consume = () =>
      consumer.consume(options(channel, delivery('{"orderId":1}', { messageId: 'm' }), { retry: fast }));
    await consume();
    await consume();
    fail = false;
    await consume();
    fail = true;
    logs.length = 0;
    await consume();
    assert.ok(logged('(attempt 1)'), 'counting starts over after a success');
  });

  test('gives up after maxAttempts and dead-letters the message with the last error', async () => {
    const consumer = new RabbitInboxConsumer(failing('store unavailable'));
    const { channel, settled } = deliveryChannel();
    const results: RabbitConsumeResult[] = [];
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      results.push(
        await consumer.consume(
          options(channel, delivery('{"orderId":1}', { messageId: 'm' }), {
            retry: { delayMs: 1, maxAttempts: 3 },
          }),
        ),
      );
    }
    assert.deepEqual(results.map((r) => r.outcome), ['requeued', 'requeued', 'dead-lettered']);
    assert.deepEqual(settled[settled.length - 1], { kind: 'nack', requeue: false });
    assert.ok(logged('gave up after 3 attempts: store unavailable'));
    // Given up means forgotten: the next failure of that message starts over.
    logs.length = 0;
    await consumer.consume(
      options(channel, delivery('{"orderId":1}', { messageId: 'm' }), { retry: { delayMs: 1, maxAttempts: 3 } }),
    );
    assert.ok(logged('(attempt 1)'));
  });

  test('keeps attempt counts for at most 10 000 messages, forgetting the oldest', async () => {
    const consumer = new RabbitInboxConsumer(failing());
    const { channel } = deliveryChannel();
    const failOnce = (id: string) =>
      consumer.consume(options(channel, delivery('{"orderId":1}', { messageId: id }), { retry: { delayMs: 0 } }));
    await Promise.all(Array.from({ length: 10_001 }, (_, i) => failOnce(`m-${i}`)));
    logs.length = 0;
    await failOnce('m-0'); // evicted: counted from the start again
    await failOnce('m-10000'); // kept: its second attempt
    assert.ok(logged('(attempt 1)'));
    assert.ok(logged('(attempt 2)'));
  });

  test('never rejects when the delivery channel closed before the ack', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel } = deliveryChannel(true);
    const result = await consumer.consume(options(channel, delivery('{"orderId":1}', { messageId: 'm' })));
    assert.deepEqual(result, { outcome: 'processed', dedupKey: 'm' });
    assert.ok(logged('could not ack the delivery (Channel closed); the broker will redeliver it'));
  });

  test('never rejects when the delivery channel closed before a requeue or a reject', async () => {
    const consumer = new RabbitInboxConsumer(failing());
    const { channel } = deliveryChannel(true);
    const requeued = await consumer.consume(
      options(channel, delivery('{"orderId":1}', { messageId: 'm' }), { retry: fast }),
    );
    assert.deepEqual(requeued, { outcome: 'requeued', dedupKey: 'm' });
    assert.ok(logged('could not requeue the delivery (Channel closed)'));
    const rejected = await consumer.consume(options(channel, delivery('not json', { messageId: 'p' })));
    assert.deepEqual(rejected, { outcome: 'dead-lettered', dedupKey: 'p' });
    assert.ok(logged('could not reject the delivery (Channel closed)'));
  });

  test('requeues a malformed delivery that carries no properties', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel, settled } = deliveryChannel();
    const message = { content: Buffer.from('{}'), fields: {} } as unknown as ConsumeMessage;
    const result = await consumer.consume(options(channel, message, { retry: fast }));
    assert.deepEqual(result, { outcome: 'requeued' });
    assert.deepEqual(settled, [{ kind: 'nack', requeue: true }]);
  });

  test('describes a thrown value that is not an Error', async () => {
    const consumer = new RabbitInboxConsumer(
      inbox(async () => {
        throw 'store unavailable';
      }),
    );
    const { channel } = deliveryChannel();
    await consumer.consume(options(channel, delivery('{"orderId":1}', { messageId: 'm' }), { retry: fast }));
    assert.ok(logs.some((l) => String(l.message).endsWith('store unavailable')));
  });
});
