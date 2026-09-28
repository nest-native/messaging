import {
  type BeforeApplicationShutdown,
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
} from '@nestjs/common';
import { RabbitInboxConsumer } from '@nest-native/messaging/rabbitmq';
import type { RecoveringChannelModel } from 'amqplib';
import { EVENTS, isOrderPlaced } from '../shared/contracts';
import { DeliveryLog } from '../shared/delivery-log';
import { consumeWithInbox, type InboxSubscription } from '../shared/inbox-subscription';
import { RABBITMQ } from '../shared/infrastructure';
import type { Subscription } from '../shared/topology';
import { ShipmentService } from './shipment.service';

export const SHIPPING_SUBSCRIPTION: Subscription = {
  queue: 'shipping.order-placed',
  event: EVENTS.orderPlaced,
};

/** Shipping books a shipment for every order placed — once per order, however often it arrives. */
@Injectable()
export class OrderPlacedConsumer implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly logger = new Logger(OrderPlacedConsumer.name);
  private subscription: InboxSubscription | undefined;

  constructor(
    @Inject(RABBITMQ) private readonly rabbit: RecoveringChannelModel,
    @Inject(RabbitInboxConsumer) private readonly inbox: RabbitInboxConsumer,
    @Inject(DeliveryLog) private readonly deliveries: DeliveryLog,
    @Inject(ShipmentService) private readonly shipments: ShipmentService,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    this.subscription = await consumeWithInbox(this.rabbit, this.inbox, {
      queue: SHIPPING_SUBSCRIPTION.queue,
      validate: isOrderPlaced,
      sideEffect: order => this.shipments.schedule(order),
      onSettled: result => this.deliveries.record(result),
      logger: this.logger,
    });
  }

  /** Runs before the connection closes, so its channel closing is not mistaken for a failure. */
  beforeApplicationShutdown(): void {
    this.subscription?.stop();
  }
}
