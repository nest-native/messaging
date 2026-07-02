import { type DynamicModule, Module } from '@nestjs/common';
import { ClsPluginTransactional } from '@nestjs-cls/transactional';
import { TransactionalAdapterDrizzleOrm } from '@nestjs-cls/transactional-adapter-drizzle-orm';
import { ClsModule } from 'nestjs-cls';
import { MessagingModule } from '@nest-native/messaging';
import {
  InProcessOutboxTransport,
  OutboxRegistry,
} from '@nest-native/messaging/in-process';
import {
  SqliteInboxStore,
  SqliteOutboxStore,
} from '@nest-native/messaging/sqlite';
import { type AppDatabase, DRIZZLE } from './database';
import { OrderPlacedHandler } from './order-placed.handler';
import { OrderService } from './order.service';

// A global module exporting the Drizzle instance, so both the CLS adapter and
// MessagingModule resolve it (mirrors how @nest-native/drizzle registers).
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
    // The in-process (no-broker) default profile: the claimer "publishes" by
    // dispatching to the handler registered for the topic. The same instance
    // backs the transport AND the OutboxRegistry provider, so handlers register
    // into the registry the transport reads from.
    const registry = new OutboxRegistry();
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
        MessagingModule.forRoot({
          drizzleInstanceToken: DRIZZLE,
          outboxStore: new SqliteOutboxStore(),
          inboxStore: new SqliteInboxStore(),
          transport: new InProcessOutboxTransport(registry),
        }),
      ],
      providers: [
        { provide: OutboxRegistry, useValue: registry },
        OrderPlacedHandler,
        OrderService,
      ],
      exports: [OrderService],
    };
  }
}
