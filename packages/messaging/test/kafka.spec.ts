import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { after, before, beforeEach, describe, test } from 'node:test';
import { Logger } from '@nestjs/common';
import type { KafkaProducerService } from '@nest-native/kafka';
import {
  KafkaInboxConsumer,
  KafkaOutboxTransport,
} from '../adapters/kafka';
import type { InboxService } from '../inbox.service';
import { PermanentError, RetryableError } from '../transport';
import { decodeWireValue, X_ERROR, X_EVENT_ID, X_IDEMPOTENCY_KEY } from '../wire-contract';

interface SentRecord {
  topic: string;
  messages: { key?: unknown; value?: unknown; headers?: Record<string, unknown> }[];
}

/** A mock KafkaProducerService that records every send. */
function mockProducer() {
  const sent: SentRecord[] = [];
  const producer = {
    send: (record: SentRecord) => {
      sent.push(record);
      return Promise.resolve([]);
    },
  } as unknown as KafkaProducerService;
  return { producer, sent };
}

describe('KafkaOutboxTransport', () => {
  test('publishes JSON value, prefixed topic, key and dedup headers', async () => {
    const { producer, sent } = mockProducer();
    const transport = new KafkaOutboxTransport(producer, 'prod.');
    await transport.publish({
      id: 'evt-1',
      topic: 'user.invited',
      payload: { a: 1 },
      idempotencyKey: 'idem-1',
    });
    assert.equal(sent.length, 1);
    assert.equal(sent[0]?.topic, 'prod.user.invited');
    const m = sent[0]!.messages[0]!;
    assert.equal(m.key, 'idem-1');
    assert.deepEqual(decodeWireValue(m.value as string), { a: 1 });
    assert.equal(m.headers?.[X_EVENT_ID], 'evt-1');
    assert.equal(m.headers?.[X_IDEMPOTENCY_KEY], 'idem-1');
  });

  test('falls back to id as the key and defaults the prefix to empty', async () => {
    const { producer, sent } = mockProducer();
    await new KafkaOutboxTransport(producer).publish({
      id: 'evt-2',
      topic: 't',
      payload: {},
    });
    assert.equal(sent[0]?.topic, 't');
    assert.equal(sent[0]?.messages[0]?.key, 'evt-2');
    assert.equal(sent[0]?.messages[0]?.headers?.[X_IDEMPOTENCY_KEY], 'evt-2');
  });
});

describe('KafkaInboxConsumer', () => {
  // Capture the consumer's Nest Logger output (debug for duplicates, warn for
  // dead-letters) so the log *content* is asserted, not just the outcome.
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

  // Build a fake KafkaContext exposing the raw message (key/value/headers).
  function context(key: string | null, value: string | null) {
    return {
      getMessage: () => ({ key, value }),
    } as never;
  }

  // A fake InboxService.runOnce that returns a fixed outcome (and runs the
  // side effect so its branch is covered) or throws.
  function inbox(impl: InboxService['runOnce']): InboxService {
    return { runOnce: impl } as unknown as InboxService;
  }

  type Payload = { ok: true };
  const validate = (p: unknown): p is Payload =>
    typeof p === 'object' && p !== null && (p as { ok?: unknown }).ok === true;

  let sent: SentRecord[];
  let producer: KafkaProducerService;
  beforeEach(() => {
    ({ producer, sent } = mockProducer());
    logs.length = 0;
  });

  const duplicateDebugLogs = () =>
    logs.filter(
      (l) => l.level === 'debug' && /duplicate skipped/.test(String(l.message)),
    );

  test('acks a freshly processed message and runs the side effect', async () => {
    let ran = 0;
    const consumer = new KafkaInboxConsumer(
      inbox(async (_k, _s, h) => {
        await h();
        return 'processed';
      }),
      producer,
    );
    const result = await consumer.consume<Payload>({
      source: 'topic:group',
      context: context('k', '{}'),
      headers: { [X_EVENT_ID]: 'evt-1' },
      payload: { ok: true },
      validate,
      sideEffect: () => {
        ran += 1;
      },
      dlqTopic: 't.DLQ',
    });
    assert.deepEqual(result, { outcome: 'processed', dedupKey: 'evt-1' });
    assert.equal(ran, 1);
    assert.equal(sent.length, 0);
    // The duplicate-skipped debug line is duplicate-only — never on a fresh message.
    assert.deepEqual(duplicateDebugLogs(), []);
  });

  test('acks (logs) a duplicate without re-running the side effect', async () => {
    const consumer = new KafkaInboxConsumer(
      inbox(async () => 'duplicate'),
      producer,
    );
    const result = await consumer.consume<Payload>({
      source: 's',
      context: context('k', '{}'),
      headers: { [X_EVENT_ID]: 'evt-1' },
      payload: { ok: true },
      validate,
      sideEffect: () => {},
      dlqTopic: 't.DLQ',
    });
    assert.equal(result.outcome, 'duplicate');
    // The skip is logged at debug level with the dedup key for traceability.
    const skips = duplicateDebugLogs();
    assert.equal(skips.length, 1);
    assert.match(String(skips[0]?.message), /duplicate skipped: evt-1/);
  });

  test('dead-letters an invalid payload (with x-error) and acks', async () => {
    const consumer = new KafkaInboxConsumer(
      inbox(async () => 'processed'),
      producer,
    );
    const result = await consumer.consume<Payload>({
      source: 's',
      context: context('k', 'raw-bytes'),
      headers: { [X_EVENT_ID]: 'evt-1' },
      payload: { ok: false },
      validate,
      sideEffect: () => {},
      dlqTopic: 't.DLQ',
    });
    assert.equal(result.outcome, 'dead-lettered');
    assert.equal(sent[0]?.topic, 't.DLQ');
    assert.equal(sent[0]?.messages[0]?.key, 'k');
    assert.equal(sent[0]?.messages[0]?.value, 'raw-bytes');
    assert.match(String(sent[0]?.messages[0]?.headers?.[X_ERROR]), /validation/);
    // The dead-letter warn names the DLQ topic and the reason.
    const warns = logs.filter((l) => l.level === 'warn');
    assert.equal(warns.length, 1);
    assert.match(String(warns[0]?.message), /dead-lettered to t\.DLQ: .*validation/);
  });

  test('dead-letters a keyless message (null key → DLQ with null key)', async () => {
    const consumer = new KafkaInboxConsumer(
      inbox(async () => 'processed'),
      producer,
    );
    const result = await consumer.consume<Payload>({
      source: 's',
      context: context(null, null),
      headers: {},
      payload: { ok: true },
      validate,
      sideEffect: () => {},
      dlqTopic: 't.DLQ',
    });
    assert.equal(result.outcome, 'dead-lettered');
    assert.equal(sent[0]?.messages[0]?.key, null);
  });

  test('rethrows a transient failure so the broker redelivers (no DLQ)', async () => {
    const consumer = new KafkaInboxConsumer(
      inbox(async () => {
        throw new RetryableError('audit db down');
      }),
      producer,
    );
    await assert.rejects(
      () =>
        consumer.consume<Payload>({
          source: 's',
          context: context('k', '{}'),
          headers: { [X_EVENT_ID]: 'evt-1' },
          payload: { ok: true },
          validate,
          sideEffect: () => {},
          dlqTopic: 't.DLQ',
        }),
      RetryableError,
    );
    assert.equal(sent.length, 0);
  });

  test('reads a string message key (dedup falls back to it without headers)', async () => {
    let seenKey: string | undefined;
    const consumer = new KafkaInboxConsumer(
      inbox(async (k) => {
        seenKey = k;
        return 'processed';
      }),
      producer,
    );
    const result = await consumer.consume<Payload>({
      source: 's',
      context: context('strkey', '{}'),
      headers: {},
      payload: { ok: true },
      validate,
      sideEffect: () => {},
      dlqTopic: 't.DLQ',
    });
    assert.deepEqual(result, { outcome: 'processed', dedupKey: 'strkey' });
    assert.equal(seenKey, 'strkey');
  });

  test('reads a Buffer message key', async () => {
    let seenKey: string | undefined;
    const consumer = new KafkaInboxConsumer(
      inbox(async (k) => {
        seenKey = k;
        return 'processed';
      }),
      producer,
    );
    await consumer.consume<Payload>({
      source: 's',
      context: { getMessage: () => ({ key: Buffer.from('bufkey'), value: '{}' }) } as never,
      headers: {},
      payload: { ok: true },
      validate,
      sideEffect: () => {},
      dlqTopic: 't.DLQ',
    });
    assert.equal(seenKey, 'bufkey');
  });

  test('surfaces a PermanentError from the side effect as a dead-letter', async () => {
    const consumer = new KafkaInboxConsumer(
      inbox(async (_k, _s, h) => {
        await h();
        return 'processed';
      }),
      producer,
    );
    const result = await consumer.consume<Payload>({
      source: 's',
      context: context('k', '{}'),
      headers: { [X_EVENT_ID]: 'evt-1' },
      payload: { ok: true },
      validate,
      sideEffect: () => {
        throw new PermanentError('unprocessable');
      },
      dlqTopic: 't.DLQ',
    });
    assert.equal(result.outcome, 'dead-lettered');
    assert.match(String(sent[0]?.messages[0]?.headers?.[X_ERROR]), /unprocessable/);
  });
});
