---
sidebar_position: 2
title: Quick Start
---

# Quick Start

This walkthrough wires the outbox and inbox end to end on **SQLite** with
better-sqlite3 — the same path the [`00-showcase` sample](./samples.md) proves.
The Postgres dialect is identical except for the import path and an async side
effect; see the [API Reference](./api-reference.md).

## 1. Install

```bash
npm install @nest-native/messaging
# plus your driver + transaction library (peer dependencies):
npm install drizzle-orm @nestjs-cls/transactional @nestjs-cls/transactional-adapter-drizzle-orm nestjs-cls better-sqlite3
# for the Kafka transport + consumer:
npm install @nest-native/kafka
```

The published package declares **zero runtime dependencies** — Nest, Drizzle,
your driver, and the optional Kafka client are peer dependencies you already
control.

## 2. Add the table factories to your schema

Import the dialect's `outbox_events` / `inbox_events` table factories and add
them to your Drizzle schema alongside your business tables.

```ts title="schema.ts"
import { sqliteTable, text } from 'drizzle-orm/sqlite-core';
import { inboxEvents, outboxEvents } from '@nest-native/messaging/sqlite';

export const orders = sqliteTable('orders', {
  id: text('id').primaryKey(),
  item: text('item').notNull(),
});

// The consumer's exactly-once side effect writes here — a real row, so dedup is
// observable.
export const orderAudit = sqliteTable('order_audit', {
  key: text('key').primaryKey(),
  item: text('item').notNull(),
});

export const schema = { outboxEvents, inboxEvents, orders, orderAudit };
```

## 3. Generate the migration

Point drizzle-kit at your schema and generate the SQL for the new tables (the
outbox/inbox tables ship their indexes through the factories):

```bash
npx drizzle-kit generate
npx drizzle-kit migrate
```

## 4. Configure transactions and register the module

`OutboxProducer` writes inside the caller's transaction, so the host app must
configure `@nestjs-cls/transactional` with the Drizzle adapter
(`enableTransactionProxy: true`). Then register `MessagingModule.forRoot(...)`
with the dialect stores and a transport. Use the same DI token for the Drizzle
instance everywhere.

```ts title="app.module.ts"
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

@Module({})
export class AppModule {
  static register(db: AppDatabase, transport: InMemoryOutboxTransport): DynamicModule {
    // A global module that provides + exports the Drizzle instance, so both the
    // CLS adapter and MessagingModule resolve it by the same token.
    const dbModule: DynamicModule = {
      module: class DbModule {},
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
```

The `transport` is the only piece that differs between tests and production. The
showcase uses `InMemoryOutboxTransport`; in production you bind the Kafka
transport — see [step 7](#7-relay-to-kafka-in-production).

## 5. Enqueue inside your transaction

Inject `OutboxProducer` and call `enqueue()` next to your business writes inside
a `@Transactional()` method. On the **sqlite** store `enqueue` returns the row
synchronously (no `await`); type the producer as
`OutboxProducer<SqliteOutboxStore>` to get the exact shape. If the method throws,
both the order row and the outbox event roll back together.

```ts title="order.service.ts"
import { Injectable } from '@nestjs/common';
import { InjectTransaction, Transactional } from '@nestjs-cls/transactional';
import { OutboxProducer } from '@nest-native/messaging';
import type { SqliteOutboxStore } from '@nest-native/messaging/sqlite';
import type { AppDatabase } from './database';
import { orders } from './schema';

@Injectable()
export class OrderService {
  constructor(
    @InjectTransaction() private readonly db: AppDatabase,
    private readonly producer: OutboxProducer<SqliteOutboxStore>,
  ) {}

  @Transactional()
  placeOrder(id: string, item: string): void {
    this.db.insert(orders).values({ id, item }).run();
    this.producer.enqueue({
      topic: 'order.placed',
      payload: { id, item },
      idempotencyKey: `order:${id}`,
    });
  }
}
```

## 6. Drain the outbox with a worker

`OutboxClaimer` claims committed rows in batches and publishes each through the
transport. Run it in a background loop with `runWorkerLoop`; it drains the
backlog, then polls when idle, and keeps going if a tick throws. Stop it by
aborting the signal on shutdown.

```ts title="worker.ts"
import { OutboxClaimer, runWorkerLoop } from '@nest-native/messaging';

const claimer = app.get(OutboxClaimer);
const controller = new AbortController();

void runWorkerLoop(claimer, {
  pollIntervalMs: 1_000,
  signal: controller.signal,
  onTick: (report) => {
    if (report.claimed > 0) {
      console.log(`relayed ${report.completed}/${report.claimed} events`);
    }
  },
  onError: (error) => console.error('claimer tick failed', error),
});

// on graceful shutdown:
// controller.abort();
```

## 7. Relay to Kafka in production

Bind the Kafka transport instead of the in-memory one. Because the transport
needs the runtime `KafkaProducerService`, build it with `forRootAsync`:

```ts title="messaging (production)"
import { MessagingModule } from '@nest-native/messaging';
import { KafkaOutboxTransport } from '@nest-native/messaging/kafka';
import {
  SqliteInboxStore,
  SqliteOutboxStore,
} from '@nest-native/messaging/sqlite';
import { KafkaProducerService } from '@nest-native/kafka';

MessagingModule.forRootAsync({
  drizzleInstanceToken: DRIZZLE,
  outboxStore: new SqliteOutboxStore(),
  inboxStore: new SqliteInboxStore(),
  inject: [KafkaProducerService],
  useTransport: (producer: KafkaProducerService) =>
    new KafkaOutboxTransport(producer),
});
```

## 8. Consume exactly-once

Write a thin `@KafkaConsumer` and delegate to `KafkaInboxConsumer.consume`. It
runs all async broker work (parse, validate, ack, dead-letter) **outside** the
dedup transaction and only the side effect inside it. The `sideEffect` writes
through `@InjectTransaction()` so it shares the dedup transaction; on the sqlite
store it must be synchronous and DB-only.

```ts title="order-placed.consumer.ts"
import { Injectable } from '@nestjs/common';
import { InjectTransaction } from '@nestjs-cls/transactional';
import {
  KafkaConsumer,
  KafkaContext,
  KafkaCtx,
  KafkaHandler,
  KafkaHeaders,
  KafkaMessage,
} from '@nest-native/kafka';
import { KafkaInboxConsumer } from '@nest-native/messaging/kafka';
import type { AppDatabase } from './database';
import { orderAudit } from './schema';

interface OrderPlaced {
  id: string;
  item: string;
}

const isOrderPlaced = (p: unknown): p is OrderPlaced =>
  typeof p === 'object' &&
  p !== null &&
  typeof (p as OrderPlaced).id === 'string' &&
  typeof (p as OrderPlaced).item === 'string';

@Injectable()
@KafkaConsumer('order.placed', { groupId: 'orders-service' })
export class OrderPlacedConsumer {
  constructor(
    @InjectTransaction() private readonly db: AppDatabase,
    private readonly inbox: KafkaInboxConsumer,
  ) {}

  @KafkaHandler()
  async handle(
    @KafkaMessage() payload: unknown,
    @KafkaHeaders() headers: Record<string, string | Buffer | undefined>,
    @KafkaCtx() context: KafkaContext,
  ): Promise<void> {
    await this.inbox.consume({
      source: 'order.placed:orders-service',
      context,
      headers,
      payload,
      validate: isOrderPlaced,
      // Runs once, inside the dedup transaction. DB-only + synchronous on sqlite.
      sideEffect: (order, dedupKey) => {
        this.db
          .insert(orderAudit)
          .values({ key: dedupKey, item: order.item })
          .run();
      },
      dlqTopic: 'order.placed.dlq',
    });
  }
}
```

A first delivery processes the side effect and acks; a redelivery is deduplicated
(no second audit row); a payload that fails `validate` is dead-lettered to
`dlqTopic` and acked so it stops redelivering. See [Testing](./testing.md) to
exercise all of this without a broker.
