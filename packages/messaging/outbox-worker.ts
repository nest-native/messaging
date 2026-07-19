import type { ClaimerConfig } from './interfaces';
import type { OutboxClaimer, TickReport } from './outbox-claimer.service';
import type { OutboxWaker } from './outbox-waker';

export interface WorkerLoopOptions {
  /** Delay between ticks when the last tick claimed nothing (default 2000ms). */
  pollIntervalMs?: number;
  /** Claimer overrides applied to every tick. */
  claimer?: ClaimerConfig;
  /** Abort to stop the loop. */
  signal?: AbortSignal;
  /** Called after each successful tick. */
  onTick?: (report: TickReport) => void;
  /** Called when a tick throws — the loop reports and continues. */
  onError?: (error: unknown) => void;
  /**
   * Optional in-process wake. When provided, a producer that calls
   * {@link OutboxWaker.notify} after committing an event cuts the idle wait short
   * so the next tick runs immediately; `pollIntervalMs` becomes a backstop rather
   * than a per-event latency tax. Omit it to keep pure polling.
   */
  waker?: OutboxWaker;
}

/**
 * Runs `claimer.tick()` in a loop until `signal` aborts. When a tick claims a
 * batch it loops immediately to drain the backlog; when it claims nothing it
 * waits `pollIntervalMs`. A throwing tick is reported via `onError` and the loop
 * continues after the same wait.
 *
 * Pass a {@link OutboxWaker} to have that idle wait woken early by an in-process
 * `notify()`; without one, the loop is a pure poller.
 */
export async function runWorkerLoop(
  claimer: OutboxClaimer,
  options: WorkerLoopOptions = {},
): Promise<void> {
  const pollIntervalMs = options.pollIntervalMs ?? 2_000;
  const { signal, waker } = options;
  // The idle/error wait is either woken early by the waker or a plain sleep.
  const wait = waker
    ? (ms: number): Promise<void> => waker.wait(ms, signal)
    : (ms: number): Promise<void> => sleep(ms, signal);
  while (!signal?.aborted) {
    try {
      const report = await claimer.tick(options.claimer);
      options.onTick?.(report);
      if (report.claimed === 0) {
        await wait(pollIntervalMs);
      }
    } catch (error) {
      options.onError?.(error);
      await wait(pollIntervalMs);
    }
  }
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener(
      'abort',
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}
