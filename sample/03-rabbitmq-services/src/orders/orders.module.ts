import { type DynamicModule, Module } from '@nestjs/common';
import { RabbitInboxConsumer } from '@nest-native/messaging/rabbitmq';
import { DeliveryLog } from '../shared/delivery-log';
import { OutboxRelay, serviceInfrastructure } from '../shared/infrastructure';
import { OrderService } from './order.service';
import type { OrdersDatabase } from './schema';
import {
  ORDERS_SUBSCRIPTION,
  ShipmentScheduledConsumer,
} from './shipment-scheduled.consumer';

/** The orders service: takes orders, and learns when they are scheduled. */
@Module({})
export class OrdersModule {
  static register(db: OrdersDatabase, rabbitmqUrl: string): DynamicModule {
    return {
      module: OrdersModule,
      imports: serviceInfrastructure({
        name: 'orders',
        db,
        rabbitmqUrl,
        subscription: ORDERS_SUBSCRIPTION,
      }),
      providers: [
        OrderService,
        RabbitInboxConsumer,
        DeliveryLog,
        ShipmentScheduledConsumer,
        OutboxRelay,
      ],
    };
  }
}
