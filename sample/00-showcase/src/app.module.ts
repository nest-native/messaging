import { type DynamicModule, Module } from '@nestjs/common';
import { ClsPluginTransactional } from '@nestjs-cls/transactional';
import { TransactionalAdapterDrizzleOrm } from '@nestjs-cls/transactional-adapter-drizzle-orm';
import { ClsModule } from 'nestjs-cls';
import { MessagingModule } from '@nest-native/messaging';
import {
  SqliteInboxStore,
  SqliteOutboxStore,
} from '@nest-native/messaging/sqlite';
import { InMemoryOutboxTransport } from '@nest-native/messaging/testing';
import { type AppDatabase, DRIZZLE } from './database';
import { OrderService } from './order.service';

// A global module exporting the Drizzle instance, so both the CLS adapter and
// MessagingModule resolve it (mirrors how @nest-native/drizzle registers).
@Module({})
class DbModule {}

@Module({})
export class AppModule {
  static register(db: AppDatabase, transport: InMemoryOutboxTransport): DynamicModule {
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
        MessagingModule.forRoot({
          drizzleInstanceToken: DRIZZLE,
          outboxStore: new SqliteOutboxStore(),
          inboxStore: new SqliteInboxStore(),
          transport,
        }),
      ],
      providers: [OrderService],
      exports: [OrderService],
    };
  }
}
