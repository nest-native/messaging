# Changelog

All notable user-facing changes to `@nest-native/messaging` are tracked here.

This project follows semantic versioning for the published package. Sample,
documentation, and CI-only changes may remain in `Unreleased` until the next
package release is useful for users.

## Unreleased

### Changed

- `MessagingModule.forRootAsync`'s `useTransport` factory is typed `(...args: any[])`
  (matching Nest's own `FactoryProvider.useFactory`) so an idiomatic factory whose
  parameters match `inject` is assignable under `strict` without casting.
- `KafkaInboxConsumer`'s `sideEffect` now receives the derived dedup key as its
  second argument (`(payload, dedupKey) => …`), so consumers can stamp it into
  their own records.
  (Both surfaced by dogfooding the reference-app onto the library before release.)

### Added

- Initial extraction of the reliable-messaging pair (transactional outbox →
  Kafka + idempotent inbox) from the `nest-native/reference-app` into a
  standalone library: the dialect-agnostic core engine (`OutboxProducer`,
  `OutboxClaimer`, `InboxService`, the `OutboxTransport`/`OutboxStore`/
  `InboxStore` seams, the wire contract, `MessagingModule`), per-dialect Drizzle
  stores (better-sqlite3 + Postgres), the `@nest-native/messaging/kafka` adapter,
  and the `@nest-native/messaging/testing` in-memory harness.
