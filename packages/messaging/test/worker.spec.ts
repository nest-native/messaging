import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { getEventListeners } from 'node:events';
import { describe, test } from 'node:test';
import type { OutboxClaimer, TickReport } from '../outbox-claimer.service';
import { OutboxWaker } from '../outbox-waker';
import { runWorkerLoop } from '../outbox-worker';

const report = (claimed: number): TickReport => ({
  claimed,
  completed: claimed,
  retried: 0,
  failed: 0,
});

/** Build a fake claimer from a tick implementation (the loop only calls tick). */
function fakeClaimer(tick: () => Promise<TickReport>): OutboxClaimer {
  return { tick } as unknown as OutboxClaimer;
}

describe('runWorkerLoop', () => {
  test('drains a backlog, then idles, then stops on abort', async () => {
    const controller = new AbortController();
    const reports: TickReport[] = [];
    let call = 0;
    const claimer = fakeClaimer(async () => {
      call += 1;
      return report(call === 1 ? 2 : 0);
    });
    await runWorkerLoop(claimer, {
      pollIntervalMs: 5,
      signal: controller.signal,
      onTick: (r) => {
        reports.push(r);
        if (reports.length === 3) controller.abort();
      },
    });
    assert.equal(reports.length, 3);
    assert.equal(reports[0]?.claimed, 2);
    assert.equal(reports[1]?.claimed, 0);
  });

  test('reports a throwing tick via onError and continues', async () => {
    const controller = new AbortController();
    const errors: unknown[] = [];
    let call = 0;
    const claimer = fakeClaimer(async () => {
      call += 1;
      if (call === 1) throw new Error('boom');
      controller.abort();
      return report(0);
    });
    await runWorkerLoop(claimer, {
      pollIntervalMs: 5,
      signal: controller.signal,
      onError: (e) => errors.push(e),
    });
    assert.equal(errors.length, 1);
    assert.match((errors[0] as Error).message, /boom/);
  });

  test('returns immediately when the signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    let called = false;
    await runWorkerLoop(
      fakeClaimer(async () => {
        called = true;
        return report(0);
      }),
      { signal: controller.signal },
    );
    assert.equal(called, false);
  });

  test('aborting during a tick short-circuits the idle sleep', async () => {
    // tick aborts synchronously and returns an empty batch, so sleep is entered
    // with an already-aborted signal (its early-return path), then the loop exits.
    const controller = new AbortController();
    let calls = 0;
    await runWorkerLoop(
      fakeClaimer(async () => {
        calls += 1;
        controller.abort();
        return report(0);
      }),
      { pollIntervalMs: 5, signal: controller.signal },
    );
    assert.equal(calls, 1);
  });

  test('uses the default poll interval and is abortable mid-sleep', async () => {
    const controller = new AbortController();
    const start = Date.now();
    await runWorkerLoop(
      fakeClaimer(async () => {
        // Abort after this tick so the default 2s sleep is cut short by the
        // abort listener rather than the timer.
        queueMicrotask(() => controller.abort());
        return report(0);
      }),
      { signal: controller.signal },
    );
    assert.ok(controller.signal.aborted);
    // The abort must short-circuit the default 2s sleep — if the aborted-signal
    // fast path were gone the loop would wait the full interval.
    assert.ok(Date.now() - start < 1_500);
  });

  test('loops immediately (no idle sleep) while ticks are claiming', async () => {
    const controller = new AbortController();
    let calls = 0;
    const start = Date.now();
    await runWorkerLoop(
      fakeClaimer(async () => {
        calls += 1;
        if (calls === 3) controller.abort();
        return report(1);
      }),
      // A poll interval far above the runtime bound: any sleep after a claiming
      // tick (inverted/hardcoded `claimed === 0` guard) blows the assertion.
      { pollIntervalMs: 8_000, signal: controller.signal },
    );
    assert.equal(calls, 3);
    assert.ok(Date.now() - start < 4_000);
  });

  test('waits pollIntervalMs (and not the 2s default) between idle ticks', async () => {
    const controller = new AbortController();
    const timestamps: number[] = [];
    await runWorkerLoop(
      fakeClaimer(async () => {
        timestamps.push(Date.now());
        if (timestamps.length === 2) controller.abort();
        return report(0);
      }),
      { pollIntervalMs: 100, signal: controller.signal },
    );
    const gap = timestamps[1]! - timestamps[0]!;
    // Lower bound: the idle path really sleeps (a skipped sleep gives ~0ms).
    assert.ok(gap >= 80, `expected an idle wait, got ${gap}ms`);
    // Upper bound: the override is honoured (falling back to the 2s default —
    // e.g. a mutated `??` — would gap well beyond this).
    assert.ok(gap < 1_500, `idle wait too long: ${gap}ms`);
  });

  test('an abort during the idle sleep wakes it and removes the listener', async () => {
    const controller = new AbortController();
    let calls = 0;
    const start = Date.now();
    await runWorkerLoop(
      fakeClaimer(async () => {
        calls += 1;
        // Fire mid-sleep so the abort listener (not the timer) ends the wait.
        setTimeout(() => controller.abort(), 25);
        return report(0);
      }),
      { pollIntervalMs: 8_000, signal: controller.signal },
    );
    assert.equal(calls, 1);
    assert.ok(Date.now() - start < 4_000);
    // { once: true } — the fired listener must not linger on the signal.
    assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
  });

  test('a throwing tick without onError still continues (onError is optional)', async () => {
    const controller = new AbortController();
    let calls = 0;
    await runWorkerLoop(
      fakeClaimer(async () => {
        calls += 1;
        if (calls === 1) throw new Error('boom');
        controller.abort();
        return report(0);
      }),
      { pollIntervalMs: 5, signal: controller.signal },
    );
    assert.equal(calls, 2);
  });

  test('a claiming tick without onTick reports no error (onTick is optional)', async () => {
    const controller = new AbortController();
    const errors: unknown[] = [];
    let calls = 0;
    await runWorkerLoop(
      fakeClaimer(async () => {
        calls += 1;
        if (calls === 2) controller.abort();
        return report(calls === 1 ? 1 : 0);
      }),
      { pollIntervalMs: 5, signal: controller.signal, onError: (e) => errors.push(e) },
    );
    assert.deepEqual(errors, []);
    assert.equal(calls, 2);
  });

  test('a waker cuts the idle wait short instead of sleeping the full interval', async () => {
    // The idle wait is set to 8s — far above the runtime bound — so the test can
    // only pass if the waker's notify() (not the timer) ends the wait.
    const controller = new AbortController();
    const waker = new OutboxWaker();
    const timestamps: number[] = [];
    let calls = 0;
    await runWorkerLoop(
      fakeClaimer(async () => {
        calls += 1;
        timestamps.push(Date.now());
        if (calls === 1) setTimeout(() => waker.notify(), 20);
        if (calls === 2) controller.abort();
        return report(0); // always idle, so every gap is a waker-woken wait
      }),
      { pollIntervalMs: 8_000, signal: controller.signal, waker },
    );
    assert.equal(calls, 2);
    const gap = timestamps[1]! - timestamps[0]!;
    assert.ok(gap < 1_500, `waker should cut the 8s idle wait, got ${gap}ms`);
  });

  test('a waker-driven loop still stops promptly on abort (waker path honours the signal)', async () => {
    const controller = new AbortController();
    const waker = new OutboxWaker();
    let calls = 0;
    const start = Date.now();
    await runWorkerLoop(
      fakeClaimer(async () => {
        calls += 1;
        setTimeout(() => controller.abort(), 25); // abort mid idle-wait
        return report(0);
      }),
      { pollIntervalMs: 8_000, signal: controller.signal, waker },
    );
    assert.equal(calls, 1);
    assert.ok(Date.now() - start < 4_000, 'abort must short-circuit the waker wait');
  });

  test('runs without a signal: the loop keeps going (and never settles)', async () => {
    let calls = 0;
    let settled = false;
    // No signal: the loop must treat the missing signal as "never aborted".
    // The second tick parks forever so the test can observe a still-running
    // loop without leaving live timers behind.
    const loop = runWorkerLoop(
      fakeClaimer(() => {
        calls += 1;
        if (calls >= 2) return new Promise<TickReport>(() => {});
        return Promise.resolve(report(0));
      }),
      { pollIntervalMs: 1 },
    );
    loop.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    // Wait (bounded) for the second tick — the loop must get there on its own.
    const deadline = Date.now() + 5_000;
    while (calls < 2 && !settled && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    // tick → idle sleep (signal-less) → tick again: both the loop guard and the
    // sleep must tolerate `signal === undefined` (a non-optional access throws
    // and settles the loop before the second tick).
    assert.equal(calls, 2);
    assert.equal(settled, false);
  });
});
