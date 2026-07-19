/**
 * Anything a producer can nudge after committing an event: the in-process
 * {@link OutboxWaker}, or a {@link WakeSocketClient} when the worker runs in a
 * separate process. Producers depend on this shape so swapping deployment
 * topology (single process ↔ app + worker processes) doesn't touch domain code.
 */
export interface WakeSignal {
  notify(): void;
}

/**
 * An in-process wake signal for {@link runWorkerLoop}.
 *
 * The worker only pays the idle `pollIntervalMs` latency when a tick claims
 * nothing. A producer that has just committed a new event can call
 * {@link OutboxWaker.notify} to cut that idle wait short so the next tick runs
 * immediately — turning the poll interval from a per-event latency tax into a
 * mere backstop. A missed or absent `notify()` never stalls delivery; it only
 * widens latency back to one poll interval, so correctness never depends on it.
 *
 * **Same-process only.** `notify()` cannot cross process boundaries — a worker
 * running in a separate process from the producer still relies on polling. A
 * cross-process wake (e.g. Postgres `LISTEN`/`NOTIFY`) is a future, dialect-
 * specific addition; this primitive is the dialect-agnostic half that works today
 * and gives that future bridge something to drive.
 *
 * **Wakes are latched.** A `notify()` that arrives while the worker is mid-tick
 * (after an idle tick decided to wait, but before — or between — waits) is
 * remembered, so the very next idle wait returns at once. This closes the classic
 * lost-wakeup race where an event commits in the sliver between a tick and its
 * sleep.
 */
export class OutboxWaker implements WakeSignal {
  /** Resolver of the wait currently parked, or `null` when none is waiting. */
  #wake: (() => void) | null = null;
  /** A `notify()` that arrived with no waiter parked; consumed by the next wait. */
  #pending = false;

  /**
   * Wake the idle worker so its next tick runs now instead of after the poll
   * interval. Safe to call at any time and from anywhere in the same process —
   * ideally right after the transaction that enqueued the event commits (before
   * commit, the row is not yet visible to the claimer's own transaction).
   */
  notify(): void {
    const wake = this.#wake;
    if (wake) {
      // A waiter is parked: resolve it now. Clear the slot first so a re-entrant
      // notify (or the settle path) latches rather than double-firing.
      this.#wake = null;
      wake();
    } else {
      // Nobody waiting yet — latch it for the next wait so the wake isn't lost.
      this.#pending = true;
    }
  }

  /**
   * Internal — used by {@link runWorkerLoop} in place of a plain sleep. Resolves
   * on the first of: a {@link notify}, the `ms` timer (the poll backstop), or
   * `signal` aborting. A latched pending wake (or an already-aborted signal)
   * resolves synchronously.
   */
  wait(ms: number, signal?: AbortSignal): Promise<void> {
    if (this.#pending || signal?.aborted) {
      this.#pending = false;
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      // One idempotent settle for all three trigger sources: whichever fires
      // first disarms the others (clears the timer, drops the abort listener,
      // frees the wake slot), so settle runs at most once per wait.
      const settle = (): void => {
        clearTimeout(timer);
        signal?.removeEventListener('abort', settle);
        this.#wake = null;
        resolve();
      };
      const timer = setTimeout(settle, ms);
      this.#wake = settle;
      signal?.addEventListener('abort', settle, { once: true });
    });
  }
}
