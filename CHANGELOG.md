# Changelog

All notable user-facing changes to `@nest-native/messaging` are tracked here.

This project follows semantic versioning for the published package. Sample,
documentation, and CI-only changes may remain in `Unreleased` until the next
package release is useful for users.

## 0.4.0 - 2026-07-19

- **Added `OutboxWaker` — an in-process wake for the worker loop.** The worker
  only waits `pollIntervalMs` when a tick claims nothing, so that interval is the
  worst-case latency for a lone event landing in an idle outbox. Pass an
  `OutboxWaker` to `runWorkerLoop({ waker })` and call `waker.notify()` after the
  enqueueing transaction commits to cut that idle wait short — the worker relays
  immediately instead of on the next poll. Polling stays the backstop, so a missed
  or absent `notify()` never stalls delivery (it only widens latency back to one
  interval), and wakes are latched so an event committed in the sliver between a
  tick and its sleep is never lost. Same-process only; a cross-process wake
  (Postgres `LISTEN`/`NOTIFY`) is a planned follow-up. Opt-in and fully backward
  compatible — omit the `waker` and the loop is an unchanged pure poller.
- **Added `WakeSocketServer` / `WakeSocketClient` — the cross-process wake for
  processes on the same machine** (the classic app + `start:worker` split sharing
  one database, where an in-memory `notify()` can't cross the boundary). The
  worker listens on a unix domain socket (Windows: a `\\.\pipe\…` name) and feeds
  incoming connections into its `OutboxWaker`; producers hold a
  `WakeSocketClient` on the same path and `notify()` after the enqueueing
  transaction commits — fire-and-forget, never throwing into the request path.
  Built on `node:net` alone (zero new dependencies, dialect-agnostic; for the
  SQLite store this covers every supported deployment, since processes sharing a
  SQLite file are on one machine by definition). The server recovers a stale
  socket path left by a crashed predecessor and refuses a path a live server
  owns; polling remains the backstop, so a failed wake only costs one poll
  interval. The shared `WakeSignal` interface lets producers swap
  `OutboxWaker` ↔ `WakeSocketClient` without touching domain code. A
  cross-MACHINE wake (Postgres `LISTEN`/`NOTIFY`) remains the planned follow-up.
- Internal simplifications surfaced by the full-package mutation pass (no
  behavior change): `headerToString` drops a redundant `undefined` guard
  (the fall-through already returns `undefined`), and the Kafka inbox
  consumer's `readKey` collapses its `null`/`undefined` checks into a single
  `typeof`/`Buffer.isBuffer` dispatch.
- Local full-mode verification and mutation testing (repo tooling; nothing
  ships in the package): `compose.yaml` + `npm run infra:up`/`infra:down`
  start disposable MySQL/PostgreSQL containers, the new `test:integration`
  script wires up the previously unreachable gated round-trip specs,
  `npm run test:full` runs them against those containers, and Stryker
  mutation testing is available via `npm run test:mutation` (incremental) /
  `test:mutation:full` with `STRYKER_MUTATE` scoping and
  `STRYKER_WITH_INFRA=1` for I/O-inclusive runs. All of it is opt-in and
  local-only — CI is unchanged and Docker-free. See the new "Local Full-Mode
  Verification" section in GUIDELINES_NEST_MESSAGING.md.

## 0.3.1 - 2026-07-01

### Fixed

- **The `@nest-native/kafka` optional-peer range excluded kafka 0.3.x** — the
  peer was declared `^0.2.0`, which on a 0.x line means `>=0.2.0 <0.3.0`, so
  installing the messaging + kafka pair with `@nest-native/kafka@^0.3.0` failed
  with `ERESOLVE` (or, in workspace layouts, silently split kafka into two
  copies, breaking `KafkaInboxConsumer`'s injection of `KafkaProducerService`).
  Widened to `^0.2.0 || ^0.3.0`; kafka 0.3.0 is additive on the surface the
  `/kafka` entrypoint uses.

### Samples & tooling

- `sample/01-kafka` now runs on `@nest-native/kafka@^0.3.0` and settles with
  `await broker.idle()` (the 0.3.0 testing API) instead of fixed `sleep(50)`
  waits after produce/emit.
- `scripts/check-published-release.mjs`'s embedded consumer smoke now validates
  this package's entry points (core, `/in-process`, `/sqlite`, `/postgres`,
  `/mysql`, `/testing`) against the registry install — it was an unadapted copy
  from the drizzle repo and failed for every published version.

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
