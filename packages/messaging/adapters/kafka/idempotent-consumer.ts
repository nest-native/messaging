// The consumer-side helpers moved to the package root when the RabbitMQ adapter
// started sharing them; this re-export keeps `@nest-native/messaging/kafka`'s
// public surface unchanged.
export * from '../../idempotent-consumer';
