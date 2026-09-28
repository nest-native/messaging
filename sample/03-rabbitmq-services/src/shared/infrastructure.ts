import {
  type BeforeApplicationShutdown,
  type DynamicModule,
  Inject,
  Injectable,
  Logger,
  Module,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
  type OnModuleInit,
} from '@nestjs/common';
import { ClsPluginTransactional } from '@nestjs-cls/transactional';
import { TransactionalAdapterDrizzleOrm } from '@nestjs-cls/transactional-adapter-drizzle-orm';
import { ClsModule } from 'nestjs-cls';
import { MessagingModule, OutboxClaimer, runWorkerLoop } from '@nest-native/messaging';
import { RabbitOutboxTransport } from '@nest-native/messaging/rabbitmq';
import {
  SqliteInboxStore,
  SqliteOutboxStore,
} from '@nest-native/messaging/sqlite';
import { connect, type RecoveringChannelModel } from 'amqplib';
import { DRIZZLE } from './database';
import { EXCHANGE, declareSubscription, type Subscription } from './topology';

/** DI token for the service's RabbitMQ connection. */
export const RABBITMQ = Symbol('RABBITMQ');
const SUBSCRIPTION = Symbol('SUBSCRIPTION');
const connectionLogger = new Logger('RabbitMQ');

/** Declares the queue this service consumes before anything subscribes or publishes. */
@Injectable()
class RabbitTopology implements OnModuleInit {
  constructor(
    @Inject(RABBITMQ) private readonly connection: RecoveringChannelModel,
    @Inject(SUBSCRIPTION) private readonly subscription: Subscription,
  ) {}

  async onModuleInit(): Promise<void> {
    const channel = await this.connection.createChannel();
    try {
      await declareSubscription(channel, this.subscription);
    } finally {
      await channel.close();
    }
  }
}

/** Closes the connection last; every channel on it closes with it. */
@Injectable()
class RabbitConnectionCloser implements OnApplicationShutdown {
  constructor(@Inject(RABBITMQ) private readonly connection: RecoveringChannelModel) {}

  async onApplicationShutdown(): Promise<void> {
    await this.connection.close();
  }
}

/**
 * The service's outbox relay: `runWorkerLoop` drains committed events to
 * RabbitMQ for as long as the service runs. It stops in
 * `beforeApplicationShutdown`, a phase that completes before any
 * `onApplicationShutdown`, so it never publishes on a connection that is
 * closing. The short poll interval keeps the sample quick; a real service keeps
 * the default, or wakes the loop with an OutboxWaker after each commit.
 */
@Injectable()
export class OutboxRelay implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly logger = new Logger(OutboxRelay.name);
  private readonly abort = new AbortController();
  private loop: Promise<void> | undefined;

  constructor(@Inject(OutboxClaimer) private readonly claimer: OutboxClaimer) {}

  onApplicationBootstrap(): void {
    this.loop = runWorkerLoop(this.claimer, {
      pollIntervalMs: 100,
      signal: this.abort.signal,
      onError: error => this.logger.error(`relay tick failed: ${String(error)}`),
    });
  }

  async beforeApplicationShutdown(): Promise<void> {
    this.abort.abort();
    await this.loop;
  }
}

@Module({})
class DbModule {}

@Module({})
class RabbitmqModule {}

export interface ServiceOptions {
  /** The service's name, which its broker connection carries (see the management UI). */
  name: string;
  /** The service's own Drizzle instance. */
  db: unknown;
  rabbitmqUrl: string;
  /** The queue this service consumes, declared at startup. */
  subscription: Subscription;
}

/**
 * The infrastructure every service here is built on: its own database, the
 * `@nestjs-cls/transactional` Drizzle adapter, its own RabbitMQ connection, and
 * `MessagingModule` publishing through `RabbitOutboxTransport`. A service adds
 * its own providers and a consumer on top.
 */
export function serviceInfrastructure(options: ServiceOptions): DynamicModule[] {
  return [
    {
      module: DbModule,
      global: true,
      providers: [{ provide: DRIZZLE, useValue: options.db }],
      exports: [DRIZZLE],
    },
    {
      module: RabbitmqModule,
      global: true,
      providers: [
        {
          provide: RABBITMQ,
          useFactory: async () => {
            const connection = await connect(options.rabbitmqUrl, {
              recovery: true,
              clientProperties: { connection_name: options.name },
            });
            // amqplib re-emits connection errors, and an EventEmitter with no
            // 'error' listener throws them: a lost socket would crash the
            // service before recovery could reconnect it.
            connection.on('error', (error: Error) =>
              connectionLogger.warn(`${options.name}: ${error.message}`),
            );
            return connection;
          },
        },
        { provide: SUBSCRIPTION, useValue: options.subscription },
        RabbitTopology,
        RabbitConnectionCloser,
      ],
      exports: [RABBITMQ],
    },
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
    MessagingModule.forRootAsync({
      drizzleInstanceToken: DRIZZLE,
      outboxStore: new SqliteOutboxStore(),
      inboxStore: new SqliteInboxStore(),
      inject: [RABBITMQ],
      useTransport: (connection: RecoveringChannelModel) =>
        new RabbitOutboxTransport({ connection, exchange: EXCHANGE }),
    }),
  ];
}
