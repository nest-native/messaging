import {
  type DynamicModule,
  Inject,
  Injectable,
  Logger,
  Module,
  type OnApplicationShutdown,
  type OnModuleInit,
} from '@nestjs/common';
import { connect, type RecoveringChannelModel } from 'amqplib';
import { declareTopology } from './topology';

/** DI token for the application's RabbitMQ connection. */
export const RABBITMQ = Symbol('RABBITMQ');

/** The name the connection carries on the broker (see the management UI). */
export const CONNECTION_NAME = 'billing';

/**
 * Declares the topology before anything uses it. `onModuleInit` completes before
 * any `onApplicationBootstrap`, so the queues exist by the time a consumer
 * subscribes, and before the outbox worker (started after bootstrap) publishes.
 */
@Injectable()
class RabbitTopology implements OnModuleInit {
  constructor(@Inject(RABBITMQ) private readonly connection: RecoveringChannelModel) {}

  async onModuleInit(): Promise<void> {
    const channel = await this.connection.createChannel();
    try {
      await declareTopology(channel);
    } finally {
      await channel.close();
    }
  }
}

/** Closes the connection on shutdown; every channel on it closes with it. */
@Injectable()
class RabbitConnectionCloser implements OnApplicationShutdown {
  constructor(@Inject(RABBITMQ) private readonly connection: RecoveringChannelModel) {}

  async onApplicationShutdown(): Promise<void> {
    await this.connection.close();
  }
}

/**
 * The application owns its RabbitMQ connection — the outbox transport and the
 * inbox consumer only open channels on it. `recovery: true` is amqplib 2's
 * built-in reconnection: the connection comes back after a broker restart, but
 * its channels do not, which is why the transport reopens its own channel lazily
 * and the consumer subscribes again whenever its channel closes.
 */
@Module({})
export class RabbitmqModule {
  static forRoot(url: string): DynamicModule {
    return {
      module: RabbitmqModule,
      global: true,
      providers: [
        {
          provide: RABBITMQ,
          useFactory: async () => {
            const connection = await connect(url, {
              recovery: true,
              clientProperties: { connection_name: CONNECTION_NAME },
            });
            // amqplib re-emits connection errors, and an EventEmitter with no
            // 'error' listener throws them: a lost socket would crash the
            // process before recovery could reconnect it.
            const logger = new Logger('RabbitMQ');
            connection.on('error', (error: Error) => logger.warn(error.message));
            return connection;
          },
        },
        RabbitTopology,
        RabbitConnectionCloser,
      ],
      exports: [RABBITMQ],
    };
  }
}
