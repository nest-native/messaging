// Public entrypoint for @nest-native/messaging (core engine).
// Dialect-specific stores + schema factories and the broker adapters ship from
// their own modules; this barrel is the dialect-agnostic engine.
export * from './transport';
export * from './wire-contract';
export * from './interfaces';
export * from './tokens';
export * from './outbox-producer.service';
export * from './outbox-claimer.service';
export * from './inbox.service';
export * from './outbox-worker';
export * from './messaging.module';
