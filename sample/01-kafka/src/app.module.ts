import { type DynamicModule, Module } from '@nestjs/common';
import { ClsPluginTransactional } from '@nestjs-cls/transactional';
import { TransactionalAdapterDrizzleOrm } from '@nestjs-cls/transactional-adapter-drizzle-orm';
import { ClsModule } from 'nestjs-cls';
import { KafkaProducerService } from '@nest-native/kafka';
import { KafkaTestModule } from '@nest-native/kafka/testing';
import { MessagingModule } from '@nest-native/messaging';
import {
  SqliteInboxStore,
  SqliteOutboxStore,
} from '@nest-native/messaging/sqlite';
import { KafkaInboxConsumer, KafkaOutboxTransport } from '@nest-native/messaging/kafka';
import { type AppDatabase, DRIZZLE } from './database';
import { OrderAuditService } from './order-audit.service';
import { OrderConsumer } from './order.consumer';
import { OrderService } from './order.service';

// A global module exporting the Drizzle instance (mirrors how @nest-native/drizzle
// registers), so the CLS adapter, MessagingModule, and OrderAuditService resolve it.
@Module({})
class DbModule {}

@Module({})
export class AppModule {
  static register(db: AppDatabase): DynamicModule {
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
        // The in-memory Kafka broker (no real cluster). Provides KafkaProducerService
        // + discovers the @KafkaConsumer classes registered as providers below.
        KafkaTestModule.forRoot(),
        // The outbox relays through the REAL Kafka transport (KafkaOutboxTransport),
        // built from the KafkaProducerService the test module provides.
        MessagingModule.forRootAsync({
          drizzleInstanceToken: DRIZZLE,
          outboxStore: new SqliteOutboxStore(),
          inboxStore: new SqliteInboxStore(),
          inject: [KafkaProducerService],
          useTransport: (producer: KafkaProducerService) =>
            new KafkaOutboxTransport(producer),
        }),
      ],
      providers: [OrderService, OrderAuditService, KafkaInboxConsumer, OrderConsumer],
      exports: [OrderService],
    };
  }
}
