import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { EventEmitter } from 'node:events';
import { after, before, beforeEach, describe, test } from 'node:test';
import { Logger } from '@nestjs/common';
import type { Channel, ConfirmChannel, ConsumeMessage } from 'amqplib';
import {
  RabbitInboxConsumer,
  RabbitOutboxTransport,
  type RabbitConfirmChannelSource,
} from '../adapters/rabbitmq';
import type { InboxService } from '../inbox.service';
import { PermanentError, RetryableError } from '../transport';
import { decodeWireValue, X_ERROR, X_EVENT_ID, X_IDEMPOTENCY_KEY } from '../wire-contract';

type Behaviour = 'ack' | 'nack' | 'nack-non-error' | 'return' | 'return-anonymous' | 'hang';

interface Published {
  exchange: string;
  routingKey: string;
  content: Buffer;
  options: Record<string, unknown>;
}

/** An amqplib ConfirmChannel double: records publishes and answers as told. */
class FakeConfirmChannel extends EventEmitter {
  readonly published: Published[] = [];
  closed = false;
  closeFails = false;

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
    this.published.push({ exchange, routingKey, content, options });
    const messageId = options.messageId;
    setImmediate(() => {
      switch (this.behaviour) {
        case 'ack':
          return callback(null);
        case 'nack':
          return callback(new Error('nacked by broker'));
        case 'nack-non-error':
          return callback('channel closed');
        case 'return':
          this.emit('return', { properties: { messageId } });
          return callback(null);
        case 'return-anonymous':
          this.emit('return', { properties: {} });
          return callback(null);
        case 'hang':
          return undefined;
      }
    });
    return true;
  }

  close(): Promise<void> {
    this.closed = true;
    this.emit('close');
    return this.closeFails ? Promise.reject(new Error('already closed')) : Promise.resolve();
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

const event = { id: 'evt-1', topic: 'order.placed', payload: { orderId: 7 } };

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
      /broker did not confirm event evt-1: nacked by broker/,
    );
  });

  test('a callback error that is not an Error is still described', async () => {
    const transport = new RabbitOutboxTransport({
      connection: connection(new FakeConfirmChannel('nack-non-error')).source,
      exchange: 'events',
    });
    assertPlainFailure(await rejection(transport.publish(event)), /: channel closed$/);
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

  test('close() tolerates a channel that never opened', async () => {
    const transport = new RabbitOutboxTransport({
      connection: connection(new Error('connection refused')).source,
      exchange: 'events',
    });
    await rejection(transport.publish(event));
    await transport.close();
    const failing = new RabbitOutboxTransport({
      connection: connection(new Error('connection refused')).source,
      exchange: 'events',
    });
    const pending = failing.publish(event).catch(() => undefined);
    await failing.close();
    await pending;
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

  type Payload = { orderId: number };
  const validate = (p: unknown): p is Payload =>
    typeof p === 'object' && p !== null && typeof (p as Payload).orderId === 'number';

  /** Records the ack/nack calls the consumer makes on the delivery channel. */
  function deliveryChannel() {
    const settled: { kind: 'ack' | 'nack'; requeue?: boolean }[] = [];
    const channel = {
      ack: () => settled.push({ kind: 'ack' }),
      nack: (_m: unknown, _allUpTo: boolean, requeue: boolean) =>
        settled.push({ kind: 'nack', requeue }),
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
    const result = await consumer.consume({
      source: 'q',
      channel,
      message: delivery('{"orderId":7}', { messageId: 'evt-9' }),
      validate,
      sideEffect: () => assert.fail('must not run'),
    });
    assert.deepEqual(result, { outcome: 'duplicate', dedupKey: 'evt-9' });
    assert.deepEqual(settled, [{ kind: 'ack' }]);
    assert.ok(logs.some((l) => l.level === 'debug' && String(l.message).includes('evt-9')));
  });

  test('falls back to the AMQP messageId when no dedup header is present', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel } = deliveryChannel();
    const result = await consumer.consume({
      source: 'q',
      channel,
      message: delivery('{"orderId":1}', { headers: undefined, messageId: 'amqp-id' }),
      validate,
      sideEffect: () => undefined,
    });
    assert.equal(result.dedupKey, 'amqp-id');
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
      const result = await consumer.consume({
        source: 'q',
        channel,
        message,
        validate,
        sideEffect: () => assert.fail('must not run'),
      });
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
    const result = await consumer.consume({
      source: 'q',
      channel,
      message,
      validate,
      sideEffect: () => assert.fail('must not run'),
      deadLetter: {
        channel: dlq as unknown as ConfirmChannel,
        exchange: 'dlx',
        routingKey: 'orders.dead',
      },
    });
    assert.deepEqual(result, { outcome: 'dead-lettered', dedupKey: 'evt-5' });
    assert.deepEqual(settled, [{ kind: 'ack' }]);
    const [republished] = dlq.published;
    assert.equal(republished?.exchange, 'dlx');
    assert.equal(republished?.routingKey, 'orders.dead');
    assert.equal(republished?.content.toString(), '{"nope":1}');
    assert.equal(republished?.options.messageId, 'evt-5');
    assert.equal(republished?.options.persistent, true);
    assert.equal(republished?.options.expiration, undefined, 'the copy must not expire');
    assert.equal(republished?.options.userId, undefined, 'the broker rejects a foreign userId');
    assert.deepEqual(republished?.options.headers, {
      [X_EVENT_ID]: 'evt-5',
      'x-custom': 'kept',
      [X_ERROR]: 'payload failed validation',
    });
  });

  test('requeues the poison message when the dead-letter publish fails', async () => {
    const consumer = new RabbitInboxConsumer(runs('processed'));
    const { channel, settled } = deliveryChannel();
    const result = await consumer.consume({
      source: 'q',
      channel,
      message: delivery('{"nope":1}', { messageId: 'm' }),
      validate,
      sideEffect: () => undefined,
      deadLetter: {
        channel: new FakeConfirmChannel('nack') as unknown as ConfirmChannel,
        exchange: 'dlx',
        routingKey: 'dead',
      },
    });
    assert.deepEqual(result, { outcome: 'requeued', dedupKey: 'm' });
    assert.deepEqual(settled, [{ kind: 'nack', requeue: true }]);
    assert.ok(logs.some((l) => String(l.message).includes('could not dead-letter (nacked by broker)')));
  });

  test('requeues a delivery whose side effect failed transiently', async () => {
    const consumer = new RabbitInboxConsumer(
      inbox(async () => {
        throw new Error('database is locked');
      }),
    );
    const { channel, settled } = deliveryChannel();
    const result = await consumer.consume({
      source: 'q',
      channel,
      message: delivery('{"orderId":1}', { messageId: 'm' }),
      validate,
      sideEffect: () => undefined,
    });
    assert.deepEqual(result, { outcome: 'requeued', dedupKey: 'm' });
    assert.deepEqual(settled, [{ kind: 'nack', requeue: true }]);
    assert.ok(logs.some((l) => String(l.message).includes('database is locked')));
  });

  test('describes a thrown value that is not an Error', async () => {
    const consumer = new RabbitInboxConsumer(
      inbox(async () => {
        throw 'store unavailable';
      }),
    );
    const { channel } = deliveryChannel();
    await consumer.consume({
      source: 'q',
      channel,
      message: delivery('{"orderId":1}', { messageId: 'm' }),
      validate,
      sideEffect: () => undefined,
    });
    assert.ok(logs.some((l) => String(l.message).endsWith('store unavailable')));
  });
});
