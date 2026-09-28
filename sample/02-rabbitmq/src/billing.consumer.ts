import {
  type BeforeApplicationShutdown,
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
} from '@nestjs/common';
import { RabbitInboxConsumer } from '@nest-native/messaging/rabbitmq';
import type { Channel, RecoveringChannelModel } from 'amqplib';
import { DeliveryLog } from './delivery-log';
import { InvoiceService } from './invoice.service';
import type { OrderPlaced } from './order.service';
import { RABBITMQ } from './rabbitmq';
import { TOPOLOGY } from './topology';

/** The pause before subscribing again, so a queue that is gone is not a hot loop. */
const RESUBSCRIBE_DELAY_MS = 1_000;

function isOrderPlaced(value: unknown): value is OrderPlaced {
  const order = value as OrderPlaced;
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof order.orderId === 'string' &&
    typeof order.item === 'string' &&
    Number.isInteger(order.amountCents)
  );
}

/**
 * Billing's consumer: a thin shell over the library's {@link RabbitInboxConsumer}.
 * It owns the queue, the prefetch and the dead-letter target, and supplies the
 * payload validator and the exactly-once side effect (the invoice). The engine
 * dedups by the event id the outbox stamped on the message, runs the side effect
 * inside the dedup transaction, and settles the delivery itself.
 *
 * It also keeps itself subscribed. Its channels can go away under it: the
 * connection drops (amqplib's recovering connection comes back, its channels
 * do not), the broker closes a channel (an ack timeout, an access error), or it
 * cancels the consumer (the queue was deleted). Every one of those ends with a
 * channel closing, and billing then subscribes again on new channels —
 * `createChannel()` waits while the connection is recovering.
 */
@Injectable()
export class BillingConsumer implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly logger = new Logger(BillingConsumer.name);
  private stopped = false;
  private retry: NodeJS.Timeout | undefined;

  constructor(
    @Inject(RABBITMQ) private readonly rabbit: RecoveringChannelModel,
    @Inject(RabbitInboxConsumer) private readonly inbox: RabbitInboxConsumer,
    @Inject(InvoiceService) private readonly invoices: InvoiceService,
    @Inject(DeliveryLog) private readonly deliveries: DeliveryLog,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    try {
      await this.subscribe();
    } catch (error) {
      this.beforeApplicationShutdown(); // a queue billing cannot consume fails the boot instead
      throw error;
    }
  }

  /** Runs before the connection closes, so its channels closing are not mistaken for a failure. */
  beforeApplicationShutdown(): void {
    this.stopped = true;
    clearTimeout(this.retry);
  }

  private async subscribe(): Promise<void> {
    const channels: Channel[] = [];
    const open = async <C extends Channel>(opening: Promise<C>): Promise<C> => {
      const channel = await opening;
      channels.push(channel);
      // The broker's reason for closing a channel arrives as 'error', and an
      // EventEmitter with no 'error' listener throws it, crashing the process.
      channel.on('error', (error: Error) =>
        this.logger.warn(`channel closed by the broker: ${error.message}`),
      );
      // Either channel closing ends this subscription: close the other one and
      // start over.
      channel.once('close', () => {
        for (const each of channels) {
          each.close().catch(() => undefined);
        }
        this.resubscribe();
      });
      return channel;
    };

    const channel = await open(this.rabbit.createChannel());
    // Dead letters go out on a confirm channel, so a poison message is stored
    // before the original delivery is acked.
    const deadLetterChannel = await open(this.rabbit.createConfirmChannel());
    await channel.prefetch(10);
    await channel.consume(TOPOLOGY.billingQueue, message => {
      if (!message) {
        // The broker cancelled the consumer; closing the channel starts over.
        channel.close().catch(() => undefined);
        return;
      }
      // `consume` never rejects — it settles the delivery itself — but the
      // record of it can fail.
      this.inbox
        .consume<OrderPlaced>({
          source: TOPOLOGY.billingQueue,
          channel,
          message,
          validate: isOrderPlaced,
          sideEffect: (order, eventId) => this.invoices.issue(order, eventId),
          deadLetter: {
            channel: deadLetterChannel,
            exchange: TOPOLOGY.deadLetters,
            routingKey: TOPOLOGY.billingQueue,
          },
          // No `retry.maxAttempts`: a database outage should delay invoices,
          // not dead-letter them. Each failed attempt waits longer before its
          // requeue (1 s, doubling to 30 s), so a failure that persists never
          // spins.
        })
        .then(result => this.deliveries.record(result))
        .catch((error: unknown) =>
          this.logger.error(`could not record a delivery: ${String(error)}`),
        );
    });
  }

  private resubscribe(): void {
    if (this.stopped || this.retry) {
      return;
    }
    this.retry = setTimeout(() => {
      this.retry = undefined;
      if (this.stopped) {
        return;
      }
      this.subscribe().catch((error: unknown) => {
        this.logger.error(`could not subscribe: ${String(error)}`);
        this.resubscribe();
      });
    }, RESUBSCRIBE_DELAY_MS);
  }
}
