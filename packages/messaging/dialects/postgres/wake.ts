import type { WakeSignal } from '../../outbox-waker';

/**
 * The cross-MACHINE wake, for the Postgres dialect: workers on other hosts
 * can't be reached by an in-memory `OutboxWaker` or a same-machine
 * `WakeSocket…` pair, but they all share the database — so the database carries
 * the signal. Producers `NOTIFY` (see `PostgresOutboxStore`'s `wakeChannel`
 * option, which piggybacks `pg_notify` on the enqueue transaction — Postgres
 * delivers it **on commit** and drops it on rollback, so the wake is atomic
 * with the event becoming visible); each worker holds a `LISTEN` connection
 * that feeds its `OutboxWaker`.
 *
 * Best-effort by the same contract as every other wake tier: the listener
 * reconnects with a fixed delay when its connection drops, and any
 * notification missed during the gap is NOT recovered — polling remains the
 * delivery backstop, so a lost wake only costs one poll interval, never an
 * event. Correctness never depends on the wake arriving.
 */

/**
 * The slice of `pg.Client` the listener uses — structural, so tests fake it
 * hermetically and any LISTEN-capable client satisfies it. Must be a
 * DEDICATED, non-pooled connection: notifications arrive only on the exact
 * connection that issued `LISTEN`, and a pool may recycle it.
 */
export interface WakeListenConnection {
  // `Promise<unknown>`, not `Promise<void>`: pg.Client's promise overloads
  // resolve with values (`connect()` → the client), and TS's void-return
  // leniency does not pierce the Promise generic.
  connect(): Promise<unknown>;
  query(text: string): Promise<unknown>;
  end(): Promise<unknown>;
  on(event: 'notification', listener: () => void): unknown;
  on(event: 'error', listener: (error: unknown) => void): unknown;
  on(event: 'end', listener: () => void): unknown;
}

export interface PostgresWakeListenerOptions {
  /**
   * Factory for a fresh dedicated connection per attempt — e.g.
   * `() => new pg.Client({ connectionString, keepAlive: true })`. A factory
   * (not an instance) because every reconnect needs a new client. Set
   * `keepAlive: true` (pg leaves it off by default): a LISTEN connection is
   * pure-receive, so without TCP keepalives a half-open death (NAT/firewall
   * idle eviction, a host vanishing without FIN/RST) is never detected — the
   * listener would sit parked forever believing it is healthy while every
   * wake silently falls back to the polling interval.
   */
  connect: () => WakeListenConnection;
  /** The wake target — the same `OutboxWaker` passed to `runWorkerLoop`. */
  waker: WakeSignal;
  /** NOTIFY channel to listen on; must match the producer side. Default `outbox_wake`. */
  channel?: string;
  /** Delay before re-connecting after a drop or failed attempt. Default 5000ms. */
  reconnectDelayMs?: number;
  /** Connection/listen failures are reported here instead of thrown. */
  onError?: (error: unknown) => void;
}

// LISTEN takes an identifier (it cannot be parameterized like pg_notify's
// string argument), so the channel is allow-listed to a safe charset and then
// double-quoted — quoting also keeps LISTEN case-sensitive, matching the exact
// string the producer passes to pg_notify.
const CHANNEL_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

export function assertValidWakeChannel(channel: string): void {
  // 63 = NAMEDATALEN - 1, the Postgres identifier limit. Beyond it, LISTEN
  // silently truncates while pg_notify RAISES — which would abort the caller's
  // business transaction on every enqueue. The charset is ASCII-only, so
  // characters equal bytes.
  if (!CHANNEL_PATTERN.test(channel) || channel.length > 63) {
    throw new Error(
      `invalid wake channel ${JSON.stringify(channel)}: must match ${String(CHANNEL_PATTERN)} and be at most 63 characters`,
    );
  }
}

/**
 * Holds a dedicated `LISTEN` connection and forwards every notification on the
 * channel to `waker.notify()`. `start()` launches a supervision loop that
 * reconnects (with `reconnectDelayMs` between attempts) until `stop()`;
 * failures go to `onError`, never to the caller — the worker must boot and keep
 * polling even when Postgres is briefly unreachable.
 */
export class PostgresWakeListener {
  readonly #channel: string;
  readonly #reconnectDelayMs: number;
  #loop: Promise<void> | null = null;
  #current: WakeListenConnection | null = null;
  #stopped = false;
  #abortSleep: (() => void) | null = null;

  constructor(private readonly options: PostgresWakeListenerOptions) {
    this.#channel = options.channel ?? 'outbox_wake';
    assertValidWakeChannel(this.#channel);
    this.#reconnectDelayMs = options.reconnectDelayMs ?? 5_000;
    // Fail fast on a config that would turn a Postgres outage into a hot
    // reconnect loop (0/negative/NaN all collapse to ~immediate retries).
    if (!Number.isFinite(this.#reconnectDelayMs) || this.#reconnectDelayMs < 1) {
      throw new Error(
        `invalid reconnectDelayMs ${this.#reconnectDelayMs}: must be a finite number >= 1`,
      );
    }
  }

  /** Launch the supervision loop (idempotent — a second call is a no-op). */
  start(): void {
    this.#loop ??= this.#supervise();
  }

  /** End the current connection and stop reconnecting. Awaitable; idempotent. */
  async stop(): Promise<void> {
    this.#stopped = true;
    this.#abortSleep?.();
    const current = this.#current;
    if (current) {
      await this.#endQuietly(current); // emits 'end' → the parked session resolves
    }
    await this.#loop;
  }

  async #supervise(): Promise<void> {
    while (!this.#stopped) {
      try {
        await this.#session();
      } catch (error) {
        this.options.onError?.(error);
      }
      if (!this.#stopped) {
        await this.#sleep(this.#reconnectDelayMs);
      }
    }
  }

  /** One connection lifetime: connect, LISTEN, park until drop/stop. */
  async #session(): Promise<void> {
    const client = this.options.connect();
    this.#current = client;
    try {
      // Park BEFORE connect(), and race connect against it: a client that
      // stop() ends mid-connect emits 'end' but its connect() promise NEVER
      // settles (pg skips the connect callback once `_ending` is set) — an
      // un-raced `await client.connect()` would deadlock stop() forever. The
      // early 'error' listener also keeps an async connection error from
      // becoming an uncaught event that crashes the worker.
      let dead = false;
      const parked = new Promise<void>((resolve, reject) => {
        client.on('error', (error) => {
          dead = true;
          reject(error);
        });
        client.on('end', () => {
          dead = true;
          resolve();
        });
      });
      parked.catch(() => undefined); // consumed via the awaits below; never unhandled
      client.on('notification', () => this.#forwardWake());
      await Promise.race([client.connect(), parked]);
      if (dead) {
        return; // ended (stop, or a server drop) before LISTEN could be issued
      }
      await client.query(`LISTEN "${this.#channel}"`);
      await parked;
    } finally {
      this.#current = null;
      await this.#endQuietly(client);
    }
  }

  // pg emits 'notification' synchronously from its socket-data handler; a
  // throwing custom WakeSignal must be routed to onError, not thrown into that
  // path (an uncaught exception there kills the worker process). The stock
  // OutboxWaker cannot throw, but WakeSignal is a public structural seam.
  #forwardWake(): void {
    try {
      this.options.waker.notify();
    } catch (error) {
      this.options.onError?.(error);
    }
  }

  async #endQuietly(client: WakeListenConnection): Promise<void> {
    try {
      await client.end();
    } catch (error) {
      this.options.onError?.(error);
    }
  }

  /** Reconnect backoff that `stop()` can cut short. */
  #sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.#abortSleep = null;
        resolve();
      }, ms);
      this.#abortSleep = () => {
        clearTimeout(timer);
        this.#abortSleep = null;
        resolve();
      };
    });
  }
}
