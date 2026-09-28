import {
  type BeforeApplicationShutdown,
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
} from '@nestjs/common';
import { InjectTransaction } from '@nestjs-cls/transactional';
import { RabbitInboxConsumer } from '@nest-native/messaging/rabbitmq';
import type { RecoveringChannelModel } from 'amqplib';
import { eq } from 'drizzle-orm';
import { EVENTS, isShipmentScheduled } from '../shared/contracts';
import { DeliveryLog } from '../shared/delivery-log';
import { consumeWithInbox, type InboxSubscription } from '../shared/inbox-subscription';
import { RABBITMQ } from '../shared/infrastructure';
import type { Subscription } from '../shared/topology';
import { orders, type OrdersDatabase } from './schema';

export const ORDERS_SUBSCRIPTION: Subscription = {
  queue: 'orders.shipment-scheduled',
  event: EVENTS.shipmentScheduled,
};

/**
 * Orders learns that its shipment was booked. The update runs inside the
 * inbox's dedup transaction, so however often the event arrives, the order is
 * marked scheduled exactly once.
 */
@Injectable()
export class ShipmentScheduledConsumer implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly logger = new Logger(ShipmentScheduledConsumer.name);
  private subscription: InboxSubscription | undefined;

  constructor(
    @Inject(RABBITMQ) private readonly rabbit: RecoveringChannelModel,
    @Inject(RabbitInboxConsumer) private readonly inbox: RabbitInboxConsumer,
    @Inject(DeliveryLog) private readonly deliveries: DeliveryLog,
    @InjectTransaction() private readonly db: OrdersDatabase,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    this.subscription = await consumeWithInbox(this.rabbit, this.inbox, {
      queue: ORDERS_SUBSCRIPTION.queue,
      validate: isShipmentScheduled,
      sideEffect: shipment => {
        this.db
          .update(orders)
          .set({
            status: 'scheduled',
            shipmentId: shipment.shipmentId,
            carrier: shipment.carrier,
          })
          .where(eq(orders.id, shipment.orderId))
          .run();
      },
      onSettled: result => this.deliveries.record(result),
      logger: this.logger,
    });
  }

  /** Runs before the connection closes, so its channel closing is not mistaken for a failure. */
  beforeApplicationShutdown(): void {
    this.subscription?.stop();
  }
}
