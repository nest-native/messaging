import { unlinkSync } from 'node:fs';
import { connect, createServer, type Server, type Socket } from 'node:net';
import type { WakeSignal } from './outbox-waker';

/**
 * The cross-process half of the wake story, for processes on the SAME machine —
 * the classic deployment where the HTTP app and the outbox worker
 * (`runWorkerLoop`) are separate processes sharing one database. An in-memory
 * `OutboxWaker.notify()` cannot cross that boundary; this bridge carries it over
 * a unix domain socket (a `\\.\pipe\…` name on Windows), using only `node:net` —
 * no new dependencies, no dialect coupling.
 *
 * Topology: the worker process runs a {@link WakeSocketServer} pointed at its
 * `OutboxWaker`; each producer process holds a {@link WakeSocketClient} on the
 * same path and calls `notify()` after its enqueueing transaction commits. The
 * connection itself is the entire signal — there is no payload protocol to
 * version — and polling remains the backstop, so a failed or missed wake only
 * widens latency back to one poll interval, never drops an event.
 *
 * Cross-MACHINE wake (e.g. Postgres `LISTEN`/`NOTIFY`) is a separate, dialect-
 * specific concern this bridge deliberately does not attempt.
 */
export interface WakeSocketOptions {
  /**
   * Unix-domain-socket path (Windows: a `\\.\pipe\` name). Both processes must
   * use the same value; one server owns a path at a time.
   */
  path: string;
  /**
   * Socket-level errors are reported here instead of thrown — a wake is an
   * optimization, so its failures must never reach the request path.
   */
  onError?: (error: unknown) => void;
}

export interface WakeSocketServerOptions extends WakeSocketOptions {
  /** The wake target — the same `OutboxWaker` passed to `runWorkerLoop`. */
  waker: WakeSignal;
}

/**
 * Worker-process side: listens on the socket path and forwards every incoming
 * connection to `waker.notify()`. Handles the crashed-predecessor case: a stale
 * socket file (bind refused, probe connect refused) is unlinked and rebound; a
 * LIVE server on the path (probe connect succeeds) is a real conflict and throws.
 */
export class WakeSocketServer {
  #server: Server | null = null;

  constructor(private readonly options: WakeSocketServerOptions) {}

  async listen(): Promise<void> {
    try {
      this.#server = await this.bind();
    } catch (error) {
      if (!isAddrInUse(error)) {
        throw error;
      }
      // EADDRINUSE: a live worker owns the path, or a crashed one left a stale
      // socket file behind. Probe to tell the two apart.
      if (await isAlive(this.options.path)) {
        throw new Error(
          `another wake-socket server is already listening on ${this.options.path}`,
        );
      }
      tryUnlink(this.options.path);
      this.#server = await this.bind();
    }
  }

  /** Stop accepting wakes and remove the socket file. Idempotent. */
  async close(): Promise<void> {
    const server = this.#server;
    if (!server) {
      return;
    }
    this.#server = null;
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
    tryUnlink(this.options.path);
  }

  private bind(): Promise<Server> {
    return new Promise((resolve, reject) => {
      const server = createServer((socket) => this.onConnection(socket));
      server.once('error', reject);
      server.listen(this.options.path, () => {
        server.removeListener('error', reject);
        resolve(server);
      });
    });
  }

  private onConnection(socket: Socket): void {
    // The connection IS the signal: guard, hang up, wake.
    socket.once('error', this.onSocketError);
    socket.destroy();
    this.options.waker.notify();
  }

  // A property (not a method) so the exact listener identity is unit-testable;
  // a teardown-race error on an accepted socket must be swallowed, not crash the
  // worker — the wake is best-effort by contract.
  private readonly onSocketError = (error: unknown): void => {
    this.options.onError?.(error);
  };
}

/**
 * Producer-process side: `notify()` fires one short-lived connection at the
 * server — fire-and-forget, never throws, never blocks the request path. A
 * failure (worker down, path missing) goes to `onError` and costs nothing but
 * latency: the worker's poll interval remains the delivery backstop.
 */
export class WakeSocketClient implements WakeSignal {
  constructor(private readonly options: WakeSocketOptions) {}

  notify(): void {
    const socket = connect(this.options.path);
    socket.once('connect', () => socket.destroy());
    socket.once('error', (error) => {
      socket.destroy();
      this.options.onError?.(error);
    });
  }
}

function isAddrInUse(error: unknown): boolean {
  return (error as NodeJS.ErrnoException).code === 'EADDRINUSE';
}

/** Probe whether something is actually accepting on the path. */
function isAlive(path: string): Promise<boolean> {
  return new Promise((resolve) => {
    const probe = connect(path);
    probe.once('connect', () => {
      probe.destroy();
      resolve(true);
    });
    probe.once('error', () => {
      probe.destroy();
      resolve(false);
    });
  });
}

function tryUnlink(path: string): void {
  try {
    unlinkSync(path);
  } catch {
    // Already gone, or a Windows pipe name with no filesystem entry.
  }
}
