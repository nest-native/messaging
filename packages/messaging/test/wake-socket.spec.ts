import { strict as assert } from 'node:assert';
import { unlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, describe, test } from 'node:test';
import { OutboxWaker } from '../outbox-waker';
import { WakeSocketClient, WakeSocketServer } from '../wake-socket';

// Unix-socket paths have a low length cap (~104 bytes on macOS) — keep names short.
let seq = 0;
function socketPath(): string {
  seq += 1;
  return join(tmpdir(), `wk-${process.pid}-${seq}.sock`);
}

const openServers: WakeSocketServer[] = [];
async function startServer(
  options: ConstructorParameters<typeof WakeSocketServer>[0],
): Promise<WakeSocketServer> {
  const server = new WakeSocketServer(options);
  await server.listen();
  openServers.push(server);
  return server;
}

after(async () => {
  for (const server of openServers) {
    await server.close();
  }
});

describe('WakeSocketServer + WakeSocketClient', () => {
  test('a client notify() crosses the socket and wakes a parked waker', async () => {
    const path = socketPath();
    const waker = new OutboxWaker();
    await startServer({ path, waker });

    const start = Date.now();
    const parked = waker.wait(8_000); // backstop far above the runtime bound
    new WakeSocketClient({ path }).notify();
    await parked;
    assert.ok(
      Date.now() - start < 1_500,
      'the socket wake should cut the 8s wait short',
    );
  });

  test('a notify() BEFORE the worker parks is latched by the waker (no lost wakeup)', async () => {
    const path = socketPath();
    const waker = new OutboxWaker();
    await startServer({ path, waker });

    new WakeSocketClient({ path }).notify();
    // Give the connection time to land while nobody is parked…
    await new Promise((resolve) => setTimeout(resolve, 50));
    // …then the next wait must resolve from the latch, not the 8s backstop.
    const start = Date.now();
    await waker.wait(8_000);
    assert.ok(Date.now() - start < 1_500, 'latched socket wake was lost');
  });

  test('recovers a stale socket path left by a crashed predecessor', async () => {
    const path = socketPath();
    writeFileSync(path, ''); // a dead filesystem entry occupying the path
    const waker = new OutboxWaker();
    await startServer({ path, waker }); // must unlink the stale entry and bind

    const start = Date.now();
    const parked = waker.wait(8_000);
    new WakeSocketClient({ path }).notify();
    await parked;
    assert.ok(Date.now() - start < 1_500, 'server on a recovered path must work');
  });

  test('refuses the path when a LIVE server already owns it', async () => {
    const path = socketPath();
    await startServer({ path, waker: new OutboxWaker() });

    const second = new WakeSocketServer({ path, waker: new OutboxWaker() });
    await assert.rejects(
      () => second.listen(),
      /already listening/,
      'a live server is a conflict, not something to unlink',
    );
  });

  test('a non-EADDRINUSE bind error is rethrown as-is', async () => {
    const server = new WakeSocketServer({
      path: join(tmpdir(), 'no-such-dir-wake', 'w.sock'), // parent dir missing
      waker: new OutboxWaker(),
    });
    await assert.rejects(
      () => server.listen(),
      (error: NodeJS.ErrnoException) => error.code !== 'EADDRINUSE',
    );
  });

  test('close() is idempotent and safe before listen and after manual unlink', async () => {
    const unlistened = new WakeSocketServer({
      path: socketPath(),
      waker: new OutboxWaker(),
    });
    await unlistened.close(); // never listened — early return

    const path = socketPath();
    const server = new WakeSocketServer({ path, waker: new OutboxWaker() });
    await server.listen();
    unlinkSync(path); // yank the file out from under close()
    await server.close(); // tryUnlink's catch path — must not throw
    await server.close(); // double close — early return again
  });

  test('an accepted-socket error is swallowed into onError, not thrown', () => {
    // The listener guards a teardown race that is not deterministically
    // triggerable over a real socket, so it is unit-tested directly: it must
    // route to onError (and tolerate onError being absent) without throwing.
    const errors: unknown[] = [];
    const withHandler = new WakeSocketServer({
      path: socketPath(),
      waker: new OutboxWaker(),
      onError: (error) => errors.push(error),
    });
    type HasListener = { onSocketError: (error: unknown) => void };
    const boom = new Error('teardown race');
    (withHandler as unknown as HasListener).onSocketError(boom);
    assert.deepEqual(errors, [boom]);

    const withoutHandler = new WakeSocketServer({
      path: socketPath(),
      waker: new OutboxWaker(),
    });
    (withoutHandler as unknown as HasListener).onSocketError(boom); // no throw
  });

  test('client notify() against a missing path reports to onError and never throws', async () => {
    const errors: unknown[] = [];
    const client = new WakeSocketClient({
      path: socketPath(), // nothing listening here
      onError: (error) => errors.push(error),
    });
    client.notify();
    const deadline = Date.now() + 5_000;
    while (errors.length === 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(errors.length, 1);
    assert.equal((errors[0] as NodeJS.ErrnoException).code, 'ENOENT');
  });

  test('client notify() without onError still swallows the failure', async () => {
    const client = new WakeSocketClient({ path: socketPath() });
    client.notify(); // must not throw now…
    await new Promise((resolve) => setTimeout(resolve, 100)); // …or async-crash later
  });
});
