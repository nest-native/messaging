import { type DynamicModule, Module } from '@nestjs/common';
import { RabbitInboxConsumer } from '@nest-native/messaging/rabbitmq';
import { DeliveryLog } from '../shared/delivery-log';
import { OutboxRelay, serviceInfrastructure } from '../shared/infrastructure';
import {
  OrderPlacedConsumer,
  SHIPPING_SUBSCRIPTION,
} from './order-placed.consumer';
import type { ShippingDatabase } from './schema';
import { ShipmentService } from './shipment.service';

/** The shipping service: books a shipment for every order, and says so. */
@Module({})
export class ShippingModule {
  static register(db: ShippingDatabase, rabbitmqUrl: string): DynamicModule {
    return {
      module: ShippingModule,
      imports: serviceInfrastructure({
        name: 'shipping',
        db,
        rabbitmqUrl,
        subscription: SHIPPING_SUBSCRIPTION,
      }),
      providers: [
        ShipmentService,
        RabbitInboxConsumer,
        DeliveryLog,
        OrderPlacedConsumer,
        OutboxRelay,
      ],
    };
  }
}
