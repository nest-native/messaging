import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import { Inject, Injectable, Module, type OnModuleInit } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import {
  InProcessOutboxTransport,
  type OutboxHandlerResult,
  OutboxRegistry,
} from '../in-process';
import {
  type OutboxMessage,
  PermanentError,
  RetryableError,
} from '../transport';

const msg = (
  topic: string,
  payload: Record<string, unknown> = {},
): OutboxMessage => ({ id: 'evt-1', topic, payload, idempotencyKey: 'idem-1' });

describe('OutboxRegistry', () => {
  test('register + get round-trip; an unregistered topic is undefined', () => {
    const registry = new OutboxRegistry();
    const handler = (): OutboxHandlerResult => 'completed';
    registry.register('user.invited', handler);
    assert.equal(registry.get('user.invited'), handler);
    assert.equal(registry.get('other.topic'), undefined);
  });

  test('a second register for the same topic throws (wiring bug, not silent replace)', () => {
    const registry = new OutboxRegistry();
    registry.register('t', () => 'completed');
    assert.throws(
      () => registry.register('t', () => 'completed'),
      /already registered for topic "t"/,
    );
  });
});

describe('InProcessOutboxTransport', () => {
  test('publish dispatches the payload AND the full message to the topic handler', async () => {
    const registry = new OutboxRegistry();
    const seen: unknown[] = [];
    registry.register('order.placed', (payload, message) => {
      seen.push(payload, message);
      return 'completed';
    });
    const transport = new InProcessOutboxTransport(registry);
    const message = msg('order.placed', { n: 1 });

    await transport.publish(message);
    assert.deepEqual(seen, [{ n: 1 }, message]);
  });

  test('no registered handler rejects with PermanentError (row fails immediately)', async () => {
    const transport = new InProcessOutboxTransport(new OutboxRegistry());
    await assert.rejects(
      () => transport.publish(msg('nobody.listens')),
      (error: unknown) => {
        assert.ok(error instanceof PermanentError);
        assert.match(error.message, /no handler registered for topic "nobody\.listens"/);
        return true;
      },
    );
  });

  test('a { retryAfterMs } result maps to RetryableError carrying that delay', async () => {
    const registry = new OutboxRegistry();
    registry.register('later', () => ({ retryAfterMs: 5_000 }));
    const transport = new InProcessOutboxTransport(registry);

    await assert.rejects(
      () => transport.publish(msg('later')),
      (error: unknown) => {
        assert.ok(error instanceof RetryableError);
        assert.equal(error.delayMs, 5_000);
        assert.match(error.message, /handler for "later" requested retry/);
        return true;
      },
    );
  });

  test('a handler throw propagates untouched (claimer generic retry/backoff)', async () => {
    const registry = new OutboxRegistry();
    const boom = new Error('handler boom');
    registry.register('explosive', () => {
      throw boom;
    });
    const transport = new InProcessOutboxTransport(registry);

    await assert.rejects(
      () => transport.publish(msg('explosive')),
      (error: unknown) => error === boom,
    );
  });

  test('async handlers are awaited before the outcome is mapped', async () => {
    const registry = new OutboxRegistry();
    registry.register('async.ok', async () => 'completed' as const);
    registry.register('async.retry', async () => ({ retryAfterMs: 100 }));
    const transport = new InProcessOutboxTransport(registry);

    await transport.publish(msg('async.ok'));
    await assert.rejects(() => transport.publish(msg('async.retry')), RetryableError);
  });
});

describe('in-process wiring through Nest DI', () => {
  test('the registry injects into the transport and handlers register on module init', async () => {
    const delivered: string[] = [];

    // The documented consumer pattern: an @Injectable that registers itself.
    @Injectable()
    class GreetingHandler implements OnModuleInit {
      constructor(
        @Inject(OutboxRegistry) private readonly registry: OutboxRegistry,
      ) {}

      onModuleInit(): void {
        this.registry.register('greeting.sent', (payload) => {
          delivered.push(String(payload.name));
          return 'completed';
        });
      }
    }

    @Module({
      providers: [OutboxRegistry, InProcessOutboxTransport, GreetingHandler],
    })
    class FixtureModule {}

    const app = await NestFactory.createApplicationContext(FixtureModule, {
      logger: false,
    });
    const transport = app.get(InProcessOutboxTransport);

    await transport.publish({
      id: 'e1',
      topic: 'greeting.sent',
      payload: { name: 'ada' },
    });
    assert.deepEqual(delivered, ['ada']);

    await app.close();
  });
});
