import {
  type DynamicModule,
  type InjectionToken,
  Module,
  type ModuleMetadata,
  type OptionalFactoryDependency,
  type Provider,
} from '@nestjs/common';
import { InboxService } from './inbox.service';
import type {
  InboxStore,
  MessagingModuleOptions,
  OutboxStore,
} from './interfaces';
import { OutboxClaimer } from './outbox-claimer.service';
import { OutboxProducer } from './outbox-producer.service';
import {
  INBOX_STORE,
  MESSAGING_DRIZZLE,
  MESSAGING_OPTIONS,
  OUTBOX_STORE,
} from './tokens';
import { OUTBOX_TRANSPORT, type OutboxTransport } from './transport';

/**
 * Async configuration. Everything except the transport is static (a DI token and
 * the dialect stores are known at module-definition time); the transport is
 * built by a factory so it can inject runtime providers (e.g. a Kafka producer).
 */
export interface MessagingModuleAsyncOptions {
  isGlobal?: boolean;
  /** Token of the base (non-transactional) Drizzle instance. */
  drizzleInstanceToken: symbol | string;
  outboxStore: OutboxStore;
  inboxStore?: InboxStore;
  imports?: ModuleMetadata['imports'];
  inject?: (InjectionToken | OptionalFactoryDependency)[];
  // `any[]` (not `unknown[]`) mirrors Nest's own `FactoryProvider.useFactory`, so
  // an idiomatic factory whose params match `inject` (e.g. `(p: KafkaProducer) => …`)
  // is assignable under `strictFunctionTypes` without forcing the caller to cast.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  useTransport: (...args: any[]) => OutboxTransport | Promise<OutboxTransport>;
}

@Module({})
export class MessagingModule {
  static forRoot(options: MessagingModuleOptions): DynamicModule {
    return assemble(options.isGlobal ?? true, options.imports ?? [], [
      { provide: MESSAGING_OPTIONS, useValue: options },
      { provide: OUTBOX_STORE, useValue: options.outboxStore },
      { provide: OUTBOX_TRANSPORT, useValue: options.transport },
      { provide: MESSAGING_DRIZZLE, useExisting: options.drizzleInstanceToken },
    ], options.inboxStore);
  }

  static forRootAsync(options: MessagingModuleAsyncOptions): DynamicModule {
    return assemble(options.isGlobal ?? true, options.imports ?? [], [
      { provide: OUTBOX_STORE, useValue: options.outboxStore },
      {
        provide: OUTBOX_TRANSPORT,
        useFactory: options.useTransport,
        inject: options.inject ?? [],
      },
      { provide: MESSAGING_DRIZZLE, useExisting: options.drizzleInstanceToken },
    ], options.inboxStore);
  }
}

function assemble(
  global: boolean,
  imports: NonNullable<ModuleMetadata['imports']>,
  base: Provider[],
  inboxStore: InboxStore | undefined,
): DynamicModule {
  const providers: Provider[] = [...base, OutboxProducer, OutboxClaimer];
  const moduleExports: NonNullable<DynamicModule['exports']> = [
    OutboxProducer,
    OutboxClaimer,
    OUTBOX_TRANSPORT,
  ];
  if (inboxStore) {
    providers.push(
      { provide: INBOX_STORE, useValue: inboxStore },
      InboxService,
    );
    moduleExports.push(InboxService);
  }
  return { module: MessagingModule, global, imports, providers, exports: moduleExports };
}
