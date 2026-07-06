import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import {
  actionForError,
  actionForOutcome,
  deriveDedupKey as deriveDedupKeyStrict,
} from '../adapters/kafka/idempotent-consumer';
import { OUTBOX_TRANSPORT, PermanentError, RetryableError } from '../transport';
import {
  INBOX_STORE,
  MESSAGING_DRIZZLE,
  MESSAGING_OPTIONS,
  OUTBOX_STORE,
} from '../tokens';
import { InMemoryOutboxTransport } from '../testing';
import {
  decodeWireValue,
  deriveDedupKey,
  encodeWireValue,
  headerToString,
  X_EVENT_ID,
  X_IDEMPOTENCY_KEY,
} from '../wire-contract';

describe('transport errors', () => {
  test('RetryableError carries name and optional delay', () => {
    const e = new RetryableError('soon', 250);
    assert.equal(e.name, 'RetryableError');
    assert.equal(e.message, 'soon');
    assert.equal(e.delayMs, 250);
    assert.equal(new RetryableError('no delay').delayMs, undefined);
    assert.ok(e instanceof Error);
  });

  test('PermanentError carries name', () => {
    const e = new PermanentError('nope');
    assert.equal(e.name, 'PermanentError');
    assert.equal(e.message, 'nope');
    assert.ok(e instanceof Error);
  });
});

describe('wire contract', () => {
  test('headerToString handles string, buffer, array, undefined', () => {
    assert.equal(headerToString('s'), 's');
    assert.equal(headerToString(Buffer.from('b', 'utf8')), 'b');
    assert.equal(headerToString(['first', 'second']), 'first');
    assert.equal(headerToString([Buffer.from('bf')]), 'bf');
    assert.equal(headerToString(undefined), undefined);
  });

  test('deriveDedupKey follows event-id → idempotency-key → message-key order', () => {
    assert.equal(
      deriveDedupKey({ [X_EVENT_ID]: 'evt', [X_IDEMPOTENCY_KEY]: 'idem' }, 'k'),
      'evt',
    );
    assert.equal(deriveDedupKey({ [X_IDEMPOTENCY_KEY]: 'idem' }, 'k'), 'idem');
    assert.equal(deriveDedupKey({}, 'k'), 'k');
    assert.equal(deriveDedupKey(undefined, 'k'), 'k');
    assert.equal(deriveDedupKey({}, ''), undefined);
    assert.equal(deriveDedupKey(undefined, undefined), undefined);
  });

  test('encode/decode round-trips and decode handles buffer + null', () => {
    const v = encodeWireValue({ a: 1 });
    assert.equal(v, '{"a":1}');
    assert.deepEqual(decodeWireValue(v), { a: 1 });
    assert.deepEqual(decodeWireValue(Buffer.from(v, 'utf8')), { a: 1 });
    assert.equal(decodeWireValue(null), null);
  });
});

describe('DI tokens', () => {
  test('tokens are global Symbol.for registrations under the package namespace', () => {
    // Symbol.for keys are the cross-module-instance identity of each token; a
    // changed key silently splits providers between duplicated package copies.
    assert.equal(Symbol.keyFor(OUTBOX_STORE), '@nest-native/messaging:outbox-store');
    assert.equal(Symbol.keyFor(INBOX_STORE), '@nest-native/messaging:inbox-store');
    assert.equal(Symbol.keyFor(MESSAGING_DRIZZLE), '@nest-native/messaging:drizzle');
    assert.equal(Symbol.keyFor(MESSAGING_OPTIONS), '@nest-native/messaging:options');
    assert.equal(
      Symbol.keyFor(OUTBOX_TRANSPORT),
      '@nest-native/messaging:outbox-transport',
    );
  });
});

describe('kafka pure helpers', () => {
  test('deriveDedupKey (strict) returns the key or throws PermanentError', () => {
    assert.equal(deriveDedupKeyStrict({ [X_EVENT_ID]: 'evt' }, undefined), 'evt');
    // The message names the missing inputs — it is the operator's diagnostic.
    assert.throws(
      () => deriveDedupKeyStrict({}, undefined),
      (e: unknown) =>
        e instanceof PermanentError && /cannot deduplicate/.test(e.message),
    );
  });

  test('actionForOutcome always acks; actionForError maps permanent vs transient', () => {
    assert.equal(actionForOutcome('processed'), 'ack');
    assert.equal(actionForOutcome('duplicate'), 'ack');
    assert.equal(actionForError(new PermanentError('x')), 'dead-letter');
    assert.equal(actionForError(new RetryableError('x')), 'redeliver');
    assert.equal(actionForError(new Error('x')), 'redeliver');
  });
});

describe('InMemoryOutboxTransport', () => {
  const msg = (id: string, topic = 't') => ({ id, topic, payload: { id } });

  test('records published messages and filters by topic', async () => {
    const t = new InMemoryOutboxTransport();
    await t.publish(msg('1', 'a'));
    await t.publish(msg('2', 'b'));
    assert.equal(t.list().length, 2);
    assert.deepEqual(
      t.listTopic('a').map((m) => m.id),
      ['1'],
    );
  });

  test('failWith makes publish reject until cleared; reset clears all', async () => {
    const t = new InMemoryOutboxTransport();
    t.failWith(new RetryableError('down'));
    await assert.rejects(() => t.publish(msg('1')), RetryableError);
    t.clearFailure();
    await t.publish(msg('2'));
    assert.equal(t.list().length, 1);
    t.reset();
    assert.equal(t.list().length, 0);
  });
});
