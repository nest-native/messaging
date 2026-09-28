import { type DynamicModule, Module } from '@nestjs/common';
import { ClsPluginTransactional } from '@nestjs-cls/transactional';
import { TransactionalAdapterDrizzleOrm } from '@nestjs-cls/transactional-adapter-drizzle-orm';
import { ClsModule } from 'nestjs-cls';
import { MessagingModule } from '@nest-native/messaging';
import {
  RabbitInboxConsumer,
  RabbitOutboxTransport,
} from '@nest-native/messaging/rabbitmq';
import {
  SqliteInboxStore,
  SqliteOutboxStore,
} from '@nest-native/messaging/sqlite';
import type { RecoveringChannelModel } from 'amqplib';
import { BillingConsumer } from './billing.consumer';
import { type AppDatabase, DRIZZLE } from './database';
import { DeliveryLog } from './delivery-log';
import { InvoiceService } from './invoice.service';
import { OrderService } from './order.service';
import { RABBITMQ, RabbitmqModule } from './rabbitmq';
import { TOPOLOGY } from './topology';

// A global module exporting the Drizzle instance (mirrors how @nest-native/drizzle
// registers), so the CLS adapter, MessagingModule and the services resolve it.
@Module({})
class DbModule {}

@Module({})
export class AppModule {
  static register(db: AppDatabase, rabbitmqUrl: string): DynamicModule {
    const dbModule: DynamicModule = {
      module: DbModule,
      global: true,
      providers: [{ provide: DRIZZLE, useValue: db }],
      exports: [DRIZZLE],
    };
    return {
      module: AppModule,
      imports: [
        dbModule,
        ClsModule.forRoot({
          global: true,
          plugins: [
            new ClsPluginTransactional({
              adapter: new TransactionalAdapterDrizzleOrm({
                drizzleInstanceToken: DRIZZLE,
              }),
              enableTransactionProxy: true,
            }),
          ],
        }),
        RabbitmqModule.forRoot(rabbitmqUrl),
        // The outbox relays over RabbitMQ: each event is published to the
        // `shop.events` exchange with its topic as the routing key, on a confirm
        // channel, and counts as sent only once the broker acked it and did not
        // return it.
        MessagingModule.forRootAsync({
          drizzleInstanceToken: DRIZZLE,
          outboxStore: new SqliteOutboxStore(),
          inboxStore: new SqliteInboxStore(),
          inject: [RABBITMQ],
          useTransport: (connection: RecoveringChannelModel) =>
            new RabbitOutboxTransport({ connection, exchange: TOPOLOGY.events }),
        }),
      ],
      providers: [
        OrderService,
        InvoiceService,
        DeliveryLog,
        RabbitInboxConsumer,
        BillingConsumer,
      ],
    };
  }
}
