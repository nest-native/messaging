// Public entrypoint for @nest-native/messaging (core).
// Transport seam + wire contract are stable; the engine (producer, claimer,
// inbox, module) and the Drizzle stores are added on top of these.
export * from './transport';
export * from './wire-contract';
