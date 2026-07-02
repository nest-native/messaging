# Changelog

All notable user-facing changes to `@nest-native/messaging` are tracked here.

This project follows semantic versioning for the published package. Sample,
documentation, and CI-only changes may remain in `Unreleased` until the next
package release is useful for users.

## Unreleased

## 0.3.0 - 2026-07-01

Both changes come from dogfooding the reference-app onto 0.2.0.

### Added

- **In-process transport** — `@nest-native/messaging/in-process`: `OutboxRegistry`
  (topic → handler) + `InProcessOutboxTransport`, the no-broker default profile
  the README always promised (previously every app had to hand-roll it). The
  transport maps handler outcomes for the claimer: no handler registered →
  `PermanentError` (the row fails immediately), `{ retryAfterMs }` →
  `RetryableError` with that delay, a handler throw → propagates untouched into
  the claimer's generic retry/backoff. Handlers receive `(payload, message)` so
  they can derive the dedup key (`idempotencyKey ?? id`) and pair with
  `InboxService.runOnce`; delivery is at-least-once via the claimer, so handlers
  must be idempotent or use the inbox. Depends only on `@nestjs/common`.
- The `00-showcase` sample now runs the in-process profile end to end (registry
  handler + inbox pairing) instead of the `/testing` in-memory transport.

### Changed

- **`enqueue` accepts structurally-typed payloads** — `EnqueueInput` is now
  generic (`EnqueueInput<TPayload extends object = Record<string, unknown>>`)
  and `OutboxProducer.enqueue<TPayload extends object>` threads it through, so a
  payload typed as a plain interface (no index signature) compiles without
  `as unknown as Record<string, unknown>` casts. Non-breaking: the default type
  argument preserves the old shape, `OutboxStore.enqueue` now takes
  `EnqueueInput<object>` (parameter bivariance keeps existing custom stores
  assignable), and the stored row payload stays `Record<string, unknown>` — the
  dialect stores widen internally, exactly once.

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
