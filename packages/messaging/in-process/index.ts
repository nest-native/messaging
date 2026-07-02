// @nest-native/messaging/in-process — the no-broker default transport: the
// claimer "publishes" a claimed event by dispatching it to the handler
// registered for its topic, in the same process. Depends only on
// @nestjs/common (already a required peer) — no broker client, no network.
export * from './outbox-registry';
export * from './in-process-outbox-transport';
