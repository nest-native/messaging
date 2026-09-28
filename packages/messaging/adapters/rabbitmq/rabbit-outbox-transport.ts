import type { ConfirmChannel, Message } from 'amqplib';
import type { OutboxMessage, OutboxTransport } from '../../transport';
import { encodeWireValue, X_EVENT_ID, X_IDEMPOTENCY_KEY } from '../../wire-contract';

/**
 * Anything that opens confirm channels: amqplib's `ChannelModel` from
 * `connect(url)`, or the `RecoveringChannelModel` from
 * `connect(url, { recovery: true })`. The application owns the connection;
 * the transport only opens (and re-opens) its own confirm channel on it.
 */
export interface RabbitConfirmChannelSource {
  createConfirmChannel(): Promise<ConfirmChannel>;
}

/** Options for {@link RabbitOutboxTransport}. */
export interface RabbitOutboxTransportOptions {
  /** The connection the transport opens its confirm channel on. */
  connection: RabbitConfirmChannelSource;
  /**
   * The exchange every outbox event is published to. The event's topic becomes
   * the routing key, so a `topic` exchange lets consumers bind by pattern.
   */
  exchange: string;
  /** Prepended to every routing key (e.g. `prod.`). Default: none. */
  routingKeyPrefix?: string;
  /**
   * How long a publish may wait — for a channel, then for the broker's
   * confirm — before it is abandoned as a failed attempt. Default: 10 000 ms.
   */
  confirmTimeoutMs?: number;
}

const DEFAULT_CONFIRM_TIMEOUT_MS = 10_000;

/**
 * The RabbitMQ {@link OutboxTransport}: publishes a claimed outbox event to an
 * exchange on a confirm channel and resolves only when the broker has taken
 * responsibility for it. This is the RabbitMQ counterpart of the Kafka
 * transport; the transactional guarantee is upstream (the row committed with
 * the business write), and this class decides when a publish counts as done.
 *
 * A publish succeeds only when the broker ACKS it and did not RETURN it. Every
 * message is published `mandatory`, so an event no queue is bound for comes
 * back as a return before its ack — without that, RabbitMQ acks unroutable
 * messages and drops them, and the outbox row would be marked processed for an
 * event nobody will ever receive.
 *
 * Every failure is thrown as a plain `Error`, never a `PermanentError` or
 * `RetryableError`, so the claimer retries it with backoff until the row's
 * `maxAttempts` and then marks it failed — the same budget the Kafka transport
 * gets. That includes an unroutable event: the usual cause is a consumer that
 * has not declared its queue yet, which a retry fixes, while a routing mistake
 * that never gets fixed still ends in a failed row instead of retrying forever.
 *
 * The value is JSON (`application/json`), the message is persistent, and the
 * wire contract matches the Kafka transport: `messageId` and `x-event-id` carry
 * the outbox row id, `x-idempotency-key` the business key (or the id).
 */
export class RabbitOutboxTransport implements OutboxTransport {
  private channel: Promise<ConfirmChannel> | undefined;
  private readonly returned = new Set<string>();
  private readonly confirmTimeoutMs: number;

  constructor(private readonly options: RabbitOutboxTransportOptions) {
    this.confirmTimeoutMs = options.confirmTimeoutMs ?? DEFAULT_CONFIRM_TIMEOUT_MS;
  }

  async publish(message: OutboxMessage): Promise<void> {
    const channel = await this.withTimeout(this.openChannel(), 'waiting for a confirm channel');
    const routingKey = `${this.options.routingKeyPrefix ?? ''}${message.topic}`;
    const confirmed = new Promise<void>((resolve, reject) => {
      channel.publish(
        this.options.exchange,
        routingKey,
        Buffer.from(encodeWireValue(message.payload)),
        {
          persistent: true,
          mandatory: true,
          contentType: 'application/json',
          messageId: message.id,
          headers: {
            [X_EVENT_ID]: message.id,
            [X_IDEMPOTENCY_KEY]: message.idempotencyKey ?? message.id,
          },
        },
        (error: unknown) => {
          // The broker sends a return BEFORE the ack of the same publish, so by
          // the time this callback runs the return handler has already fired.
          const wasReturned = this.returned.delete(message.id);
          if (error) {
            reject(new Error(`broker did not confirm event ${message.id}: ${describe(error)}`));
          } else if (wasReturned) {
            reject(
              new Error(
                `event ${message.id} is unroutable: no queue is bound to exchange ` +
                  `"${this.options.exchange}" for routing key "${routingKey}"`,
              ),
            );
          } else {
            resolve();
          }
        },
      );
    });
    try {
      await this.withTimeout(confirmed, `waiting for the broker to confirm event ${message.id}`);
    } finally {
      this.returned.delete(message.id);
    }
  }

  /** Closes the transport's channel. The connection stays open; it is not ours. */
  async close(): Promise<void> {
    const pending = this.channel;
    this.channel = undefined;
    if (!pending) return;
    const channel = await pending.catch(() => undefined);
    await channel?.close().catch(() => undefined);
  }

  // One channel per transport, opened on first use and dropped when it closes
  // or errors, so the next publish opens a fresh one. With amqplib's recovering
  // connection that is what survives a broker restart: the connection comes
  // back on its own, channels do not.
  private openChannel(): Promise<ConfirmChannel> {
    this.channel ??= this.options.connection.createConfirmChannel().then(
      (channel) => {
        const opened = this.channel;
        const forget = () => {
          if (this.channel === opened) this.channel = undefined;
        };
        channel.on('return', (returned: Message) => {
          const id = returned.properties.messageId as string | undefined;
          if (id) this.returned.add(id);
        });
        channel.on('close', forget);
        // A channel error is always followed by 'close'; the listener exists so
        // an error never reaches EventEmitter's unhandled-'error' throw.
        channel.on('error', forget);
        return channel;
      },
      (error: unknown) => {
        this.channel = undefined;
        throw new Error(`could not open a RabbitMQ confirm channel: ${describe(error)}`);
      },
    );
    return this.channel;
  }

  private async withTimeout<T>(work: Promise<T>, what: string): Promise<T> {
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(
        () => reject(new Error(`timed out after ${this.confirmTimeoutMs} ms ${what}`)),
        this.confirmTimeoutMs,
      );
    });
    try {
      return await Promise.race([work, timeout]);
    } finally {
      clearTimeout(timer);
    }
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
