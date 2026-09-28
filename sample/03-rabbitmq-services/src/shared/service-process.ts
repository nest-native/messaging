import type { DynamicModule, INestApplicationContext } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { DeliveryLog } from './delivery-log';

/** A command the process accepts from whoever forked it, and its reply. */
export type CommandHandler = (
  app: INestApplicationContext,
  payload: Record<string, string>,
) => Promise<void>;

export interface ServiceProcessOptions {
  module: DynamicModule;
  commands?: Record<string, CommandHandler>;
  /** Releases what the application does not own (the database file). */
  close: () => void;
}

/**
 * Runs one service as its own process, the way it would be deployed.
 *
 * It has to be its own process: `@nestjs-cls/transactional` keeps its
 * transaction host in process-global state, keyed by connection name, and the
 * outbox and inbox use the default connection. Two applications started in one
 * process would share one of the two databases' transactions.
 *
 * The smoke test drives the process over Node's IPC channel, which exists only
 * when a parent forked it: it sends commands, and the process reports every
 * delivery its inbox settled. A real service would take commands over HTTP and
 * report deliveries as logs and metrics. SIGTERM shuts it down gracefully —
 * the outbox relay stops first, then the broker connection closes, and any
 * unacked delivery goes back to its queue.
 */
export async function runService(options: ServiceProcessOptions): Promise<void> {
  const app = await NestFactory.createApplicationContext(options.module, {
    logger: ['error', 'warn'],
  });
  await app.init(); // declares the queue, subscribes, starts the outbox relay

  app.get(DeliveryLog).listen(result => process.send?.({ type: 'settled', ...result }));

  process.on('message', (message: { type: string; id: string; payload: Record<string, string> }) => {
    const handler = options.commands?.[message.type];
    if (!handler) {
      process.send?.({ type: 'reply', id: message.id, error: `unknown command ${message.type}` });
      return;
    }
    handler(app, message.payload).then(
      () => process.send?.({ type: 'reply', id: message.id }),
      (error: unknown) => process.send?.({ type: 'reply', id: message.id, error: String(error) }),
    );
  });

  process.once('SIGTERM', () => {
    app
      .close()
      .then(() => {
        options.close();
        process.exit(0);
      })
      .catch((error: unknown) => {
        console.error(error);
        process.exit(1);
      });
  });

  process.send?.({ type: 'ready' });
}

export function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is not set`);
  }
  return value;
}
