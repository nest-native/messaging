// @nest-native/messaging/rabbitmq — the opt-in RabbitMQ transport + idempotent
// consumer engine, over amqplib. The application owns the amqplib connection and
// passes it in; this entry point only imports amqplib's types, so it adds no
// runtime dependency of its own.
export * from './rabbit-outbox-transport';
export * from './rabbit-inbox-consumer';
// The broker-neutral consumer helpers, exported here as they are from /kafka.
export * from '../../idempotent-consumer';
