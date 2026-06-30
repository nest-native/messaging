import 'reflect-metadata';
import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import type { OutboxClaimer, TickReport } from '../outbox-claimer.service';
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
  });
});
