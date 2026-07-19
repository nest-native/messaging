import { strict as assert } from 'node:assert';
import { getEventListeners } from 'node:events';
import { describe, test } from 'node:test';
import { OutboxWaker } from '../outbox-waker';

describe('OutboxWaker', () => {
  test('wait() resolves on the backstop timer when nothing wakes it', async () => {
    const waker = new OutboxWaker();
    const start = Date.now();
    await waker.wait(40);
    const gap = Date.now() - start;
    assert.ok(gap >= 25, `expected the timer to elapse, got ${gap}ms`);
    assert.ok(gap < 1_000, `timer overshot: ${gap}ms`);
  });

  test('notify() wakes a parked wait immediately', async () => {
    const waker = new OutboxWaker();
    const start = Date.now();
    const parked = waker.wait(8_000); // long backstop we should never reach
    waker.notify();
    await parked;
    assert.ok(Date.now() - start < 1_000, 'notify should cut the long wait short');
  });

  test('a notify() with no waiter is latched and consumed by the next wait', async () => {
    const waker = new OutboxWaker();
    waker.notify(); // nobody parked yet — must latch
    waker.notify(); // a second one collapses into the same single latch
    const start = Date.now();
    await waker.wait(8_000); // returns at once from the latch
    assert.ok(Date.now() - start < 1_000, 'latched wake should resolve the next wait');

    // The latch is one-shot: the following wait falls through to the backstop.
    const secondStart = Date.now();
    await waker.wait(40);
    assert.ok(
      Date.now() - secondStart >= 25,
      'the latch must be consumed, not sticky',
    );
  });

  test('wait() returns immediately when the signal is already aborted', async () => {
    const waker = new OutboxWaker();
    const controller = new AbortController();
    controller.abort();
    const start = Date.now();
    await waker.wait(8_000, controller.signal);
    assert.ok(Date.now() - start < 1_000);
  });

  test('an abort during the wait wakes it and removes the listener', async () => {
    const waker = new OutboxWaker();
    const controller = new AbortController();
    const start = Date.now();
    const parked = waker.wait(8_000, controller.signal);
    setTimeout(() => controller.abort(), 20);
    await parked;
    assert.ok(Date.now() - start < 1_000);
    // settle must drop the abort listener — no leak across waits.
    assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
  });

  test('notify() with a signal present still cleans up the abort listener', async () => {
    const waker = new OutboxWaker();
    const controller = new AbortController();
    const parked = waker.wait(8_000, controller.signal);
    waker.notify();
    await parked;
    assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
  });

  test('works without a signal: both the backstop and notify paths', async () => {
    const waker = new OutboxWaker();
    // notify path with no signal (removeEventListener/addEventListener optional
    // chains take their absent branch).
    const start = Date.now();
    const parked = waker.wait(8_000);
    waker.notify();
    await parked;
    assert.ok(Date.now() - start < 1_000);
  });
});
