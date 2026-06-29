// DI tokens for the messaging engine. The active transport token lives in
// `transport.ts` (OUTBOX_TRANSPORT) since it is part of the dependency-free seam.

/** The dialect-specific {@link OutboxStore} provided to `MessagingModule`. */
export const OUTBOX_STORE = Symbol.for('@nest-native/messaging:outbox-store');

/** The dialect-specific {@link InboxStore} provided to `MessagingModule`. */
export const INBOX_STORE = Symbol.for('@nest-native/messaging:inbox-store');

/**
 * The base (non-transactional) Drizzle instance the claimer opens its own
 * transaction on. `MessagingModule` aliases this to the user-supplied
 * `drizzleInstanceToken`.
 */
export const MESSAGING_DRIZZLE = Symbol.for('@nest-native/messaging:drizzle');

/** The resolved {@link MessagingModuleOptions}. */
export const MESSAGING_OPTIONS = Symbol.for('@nest-native/messaging:options');
