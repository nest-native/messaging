# Changelog

All notable user-facing changes to `@nest-native/messaging` are tracked here.

This project follows semantic versioning for the published package. Sample,
documentation, and CI-only changes may remain in `Unreleased` until the next
package release is useful for users.

## Unreleased

## 0.2.0 - 2026-07-01

### Added

- **MySQL store** — `@nest-native/messaging/mysql` (mysql2, async): the
  `outbox_events`/`inbox_events` table factories + the MySQL Outbox/Inbox stores,
  with `isMysqlUniqueViolation` (errno `1062` / `ER_DUP_ENTRY`, unwrapping
  `DrizzleQueryError.cause`). `mysql2` is an optional peer.
- A **gated real-service integration test** (round-trip produce → claim → consume
  → dedup) that runs against a real database when its connection env is set and
  skips otherwise, keeping the default suite hermetic.

## 0.1.0 - 2026-06-30

The first release — the reliable-messaging pair extracted from
`nest-native/reference-app` into a standalone library.

### Added

- **Core engine** (`@nest-native/messaging`): the dialect-agnostic
  `OutboxProducer`, `OutboxClaimer` + `runWorkerLoop`, `InboxService`, the
  `OutboxTransport`/`OutboxStore`/`InboxStore` seams, `RetryableError`/
  `PermanentError`, the wire contract, and `MessagingModule.forRoot`/`forRootAsync`.
- **Drizzle stores + schema factories** for two dialects:
  `@nest-native/messaging/sqlite` (better-sqlite3, synchronous) and
  `@nest-native/messaging/postgres` (node-postgres, async).
- **Kafka adapter** (`@nest-native/messaging/kafka`): `KafkaOutboxTransport` and
  the idempotent `KafkaInboxConsumer` engine.
- **Testing harness** (`@nest-native/messaging/testing`): `InMemoryOutboxTransport`
  for broker-free tests.

### Notes

These API choices were shaped by dogfooding the reference-app onto the library
before release:

- `MessagingModule.forRootAsync`'s `useTransport` factory is typed
  `(...args: any[])` (matching Nest's own `FactoryProvider.useFactory`) so an
  idiomatic factory whose parameters match `inject` is assignable under `strict`
  without casting.
- `KafkaInboxConsumer`'s `sideEffect` receives the derived dedup key as its
  second argument (`(payload, dedupKey) => …`), so consumers can stamp it into
  their own records.
