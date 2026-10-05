# Changelog

All notable user-facing changes to `@nest-native/messaging` are tracked here.

This project follows semantic versioning for the published package. Sample,
documentation, and CI-only changes may remain in `Unreleased` until the next
package release is useful for users.

## Unreleased

## 0.8.3 - 2026-10-04

- **The SQLite claim no longer fails with "database is locked" when another
  process writes to the same file.** It ran in a deferred transaction: it read
  first and asked for the write lock at its UPDATE, and when another process
  held that lock SQLite failed the upgrade at once, without consulting the
  busy timeout, because waiting could deadlock. The claim now opens with
  `BEGIN IMMEDIATE`, which takes the write lock up front and waits out
  better-sqlite3's `timeout` like any writer, and it stamps `claimedAt` once
  the lock is held. A spec holds the lock from a second process; on the
  previous claim it fails with "database is locked".
- **The READ COMMITTED pin is now proven against the server, not just passed
  as config.** Real-Postgres specs ask the server, on the store's own
  connection and inside its transaction, which isolation level the claim and
  its transitions run under, on a pool and on a single `Client`, with the
  server default at SERIALIZABLE. A real-MySQL spec pauses the claim between
  its locking read and its UPDATE and enqueues from another connection, which
  REPEATABLE READ's gap locks would block. Removing each pin fails its spec.

- **Tooling: the cognitive complexity gate moved from ESLint to Biome**, as
  `@nest-native/jobs` did. No change to the published package: this repo only
  used ESLint for `sonarjs/cognitive-complexity`, with `@typescript-eslint/parser`
  there to parse TypeScript, and that parser refuses TypeScript 7 outright, so a
  lint dependency was gating the compiler. Biome enforces the same ceiling of
  15 with `complexity/noExcessiveCognitiveComplexity` (config in `biome.json`),
  has no TypeScript dependency, and drops the ESLint toolchain from the dev
  tree. Its metric is its own implementation of the SonarSource definition and
  scores slightly higher at identical code, and it cannot report below 2, so
  `complexity:report` lists the non-trivial functions.

## 0.8.2 - 2026-10-04

- **A failed transition no longer strands the rest of the batch.** When
  recording an outcome fails (the database went away mid-batch), the tick
  throws, and the batch's unpublished rows used to wait out `stuckTimeoutMs`
  before any worker could take them. The claimer now hands them back first,
  best effort, through the new optional `OutboxStore.release`: `pending` and
  unclaimed, attempts and due time unchanged, fenced on the claim like every
  transition. After a transient error the next claim takes them at once; if the
  database is still down the hand-back fails too and they wait as before. The
  row whose outcome failed to record stays claimed, since it was published. The
  shipped stores implement `release`; a custom store without it behaves as
  before.
- **A claim is stamped once its connection is checked out.** The Postgres and
  MySQL claims took `claimedAt` before waiting for a pooled connection, so a
  slow checkout made the claim look older than it was, and other workers
  treated its rows as stuck that much sooner. The stamp is now taken inside the
  claim's transaction.
- **A dropped MySQL connection is now pinned by real-database specs.** Killing
  the connection mid-claim or mid-transition rejects the call without crashing
  the process, and the pool replaces the connection: mysql2's pooled
  connections listen for their own errors, unlike node-postgres clients.

## 0.8.1 - 2026-10-04

- **`@nestjs-cls/transactional` 4 is supported.** The peer range is now
  `^3.0.0 || ^4.0.0`; transactional 4 needs `nestjs-cls` 7 and, for Drizzle,
  `@nestjs-cls/transactional-adapter-drizzle-orm` 2. Their only breaking change
  is an `exports` map that exposes just each package root, and this package
  imports nothing deeper. A CI leg runs the suite on that set; the
  devDependencies stay on transactional 3.

## 0.8.0 - 2026-10-04

- **Concurrent workers no longer publish the same outbox event twice** (#62).
  On Postgres and MySQL, two workers claiming at the same moment could both
  select the same pending rows and both publish them. A worker whose stalled
  claim had been taken over could also still mark the row completed, retried
  or failed.
  - The claim now locks its rows with `FOR UPDATE SKIP LOCKED`, so concurrent
    claims split the backlog. Found and fixed by @donfreddy in #72.
  - The claim runs at READ COMMITTED whatever the server default. Under
    InnoDB's default REPEATABLE READ, that locking read would hold gap locks
    and make every concurrent `enqueue` wait. On a Postgres server defaulting
    to REPEATABLE READ or SERIALIZABLE, a row claimed by another worker during
    the scan would fail the claim instead of being skipped.
  - Every transition applies only while the row is still `processing` under the
    exact claim that took it: its `claimedBy` and `claimedAt`. This holds even
    when two loops share a `workerInstanceId`. A transition that loses this race
    writes nothing.
  - On Postgres each transition also runs in its own READ COMMITTED
    transaction. Under a SERIALIZABLE server default, two workers' transitions
    would otherwise abort each other while both claims still held their rows,
    and the events would be published again.
  - Once a batch has been held longer than `stuckTimeoutMs`, the worker skips
    its remaining events instead of publishing them again. The first event of a
    batch is always published, so a slow claim still makes progress.
  - A transition that lost its claim and an event skipped from an expired batch
    both count in the new `TickReport.lost`, and each logs a warning, with the
    publish error when there was one.
  - Give every worker on one outbox table the same `stuckTimeoutMs`, longer
    than the slowest single publish, and keep their clocks in sync. Each worker
    reclaims rows by its own clock and its own value, so the smallest value in
    the fleet is the one in force.
- **Upgrade every worker on a table together.** These guarantees hold once
  every worker draining a table runs this version. A 0.7.x worker claims
  without `SKIP LOCKED`, so it can take over a row a newer worker has just
  claimed, and both publish the event; and its transitions match on the row's
  id alone, so it can complete, retry or fail a row another worker holds. Stop
  the 0.7.x workers before starting the new ones, or expect duplicates and
  `lost` warnings while both run. The schema is unchanged.
- **The outbox claim no longer crashes the process when the database drops
  its connection.** The claim ran through drizzle's `transaction()`, which
  leaves the checked-out client without an `error` listener and sends BEGIN
  outside its cleanup: a failover, a restart or `pg_terminate_backend` during a
  claim killed the process, and a connection lost at BEGIN was never returned
  to the pool. On a node-postgres `Pool` the Postgres store now checks the
  client out itself, for the claim and for each transition. It listens for
  errors while the client is out, rolls back without hiding the original
  error, and returns a broken client with its error so the pool discards it.
  The call rejects instead, and `runWorkerLoop` reports it through `onError`.
  As node-postgres requires of every pool, give the pool an `error` listener:
  without one, a connection an idle client loses still crashes the process.
  The store logs a warning once when the pool has none.
  The inbox and your own `@Transactional` bodies still go through drizzle's
  `transaction()`; a per-client listener
  (`pool.on('connect', (client) => client.on('error', handle))`) keeps a
  dropped connection there from crashing the process too.
- **Failing to record a delivery no longer retries a published event.** A
  database error from `markCompleted` was handled like a failed publish: it
  spent an attempt, and on the last one marked the delivered event failed.
  `tick()` now throws, and the event is published again once its claim goes
  stale.
- **A `ClaimerConfig` field set to `undefined` keeps its default.**
  `{ workerInstanceId: process.env.WORKER_ID }` with the variable unset used to
  claim rows under no owner.
  - **Breaking:** invalid values now throw, numeric strings included. 0.7.x
    accepted them: the timeouts and backoffs worked through arithmetic, but a
    string `batchSize` dropped the claim's LIMIT, so each claim took every due
    row. Pass numbers, and leave a field `undefined` to keep its default:
    `batchSize: env.BATCH_SIZE ? Number(env.BATCH_SIZE) : undefined`. The error
    names a string as a string.
  - **Breaking:** `runWorkerLoop` rejects at once instead of failing every
    tick. Keep its promise and handle that rejection: discarded with `void`,
    it ends the process as an unhandled rejection, without reaching `onError`.
  - `resolveClaimerConfig()` is exported, so a worker can check its config at
    startup.
- **Breaking for custom `OutboxStore` implementations.**
  - `markCompleted`, `retry` and `markFailed` take the row's claim (`OutboxClaim`:
    `{ id, claimedBy, claimedAt }`) instead of its id.
  - They resolve `true` when they wrote the row and `false` when the claim no
    longer held it.
  - `claimBatch` must never return one row to two concurrent callers, and must
    return every row as its claiming UPDATE left it, with the `claimedBy` and
    `claimedAt` it wrote. The claimer refuses a row without a string stamp,
    counting it as lost with an error, but it cannot tell a stale stamp from a
    fresh one: a store that returns rows as read before its UPDATE gets each
    reclaimed event published again after every stuck timeout.
  - The shipped stores are updated.
- **Breaking: MySQL 8.0.1 or later is now required**, for `SKIP LOCKED`. With
  binary logging on, `binlog_format` must also be ROW (the MySQL 8 default) or
  MIXED: InnoDB refuses writes from the claim's READ COMMITTED transaction
  under `binlog_format=STATEMENT`.
- **CI no longer fails a fork's pull request** on the report steps that post PR
  comments.

## 0.7.0 - 2026-09-29

- **RabbitMQ transport: `@nest-native/messaging/rabbitmq`.** A
  `RabbitOutboxTransport` that relays the outbox over RabbitMQ and a
  `RabbitInboxConsumer` that runs the idempotent inbox on RabbitMQ deliveries,
  over the application's own `amqplib` 2 connection (`amqplib` is a new
  optional peer; the entry point imports only its types). A publish counts as
  done only when the broker acked it on a confirm channel and did not return
  it — every message is published `mandatory`, because RabbitMQ otherwise acks
  and drops a message no queue is bound for. Broker failures are plain errors,
  retried by the claimer until `maxAttempts`, the same budget as Kafka. The
  consumer acks processed and duplicate deliveries, requeues transient
  failures, and dead-letters poison either by republishing it with an
  `x-error` header or by rejecting it into the queue's own dead-letter
  exchange. The consumer never rejects (a channel that closed before the ack
  leaves the delivery with the broker, which redelivers it), requeues a
  transient failure only after a backoff (1 s doubling to 30 s per message,
  with an optional `maxAttempts` that dead-letters it — RabbitMQ 4 does not
  count an explicit requeue toward a quorum queue's delivery limit, and an
  immediate requeue spun at about 1 400 redeliveries a second), publishes its
  dead-letter copies `mandatory` so an unbound dead-letter exchange cannot
  swallow one, backs off on a dead-letter target that keeps failing, bounds
  the `x-error` reason to 1 000 characters, listens for `error` on the
  dead-letter channel so a broker-closed one cannot close the connection,
  treats a `validate` that throws as poison, reads exact integer header values
  as keys — never a 64-bit id amqplib rounded past 2^53, which could make two
  events one — and reports the dedup key on every outcome. The transport
  carries the broker's reason when it closes the channel, lets publishes
  already under way and outstanding confirms settle before `close()` closes
  it, and never waits in `close()` for a channel that is still opening.
  Verified against a real RabbitMQ 4 broker by a new gated spec, which CI
  runs on every PR (below). The broker-neutral
  consumer helpers (`deriveDedupKey`, `actionForError`, …) are now shared by
  both adapters and exported from `/rabbitmq` as well as `/kafka`.
  See the new RabbitMQ docs page.

- **Two RabbitMQ samples.** `sample/02-rabbitmq` runs the outbox and the
  inbox against a real broker and asserts every path the adapter promises: a
  confirmed publish, a redelivered duplicate acked without a second side
  effect, poison dead-lettered with its reason, and an unroutable event kept in
  the outbox instead of acked into the void. `sample/03-rabbitmq-services` is
  two services — orders and shipping, each its own process and database —
  choreographed over one broker: shipping consumes `order.placed` and publishes
  `shipment.scheduled` in one transaction, keeps working through an outage from
  the backlog RabbitMQ held for it, and books nothing twice when an event
  arrives again. In both, the broker then drops the connections, and the
  services reconnect and subscribe again on their own: each consumer subscribes
  again whenever its channel closes, which also covers a channel the broker
  closes on its own and a cancelled consumer — subscribing again only on the
  connection's `connect` event misses both. The RabbitMQ docs page now shows
  that loop. Both samples run in CI against a RabbitMQ service container, in the
  sample job and in both NestJS compatibility legs, and fail rather than skip
  there when no broker or management API is provided. Sample 03 also records why each service
  needs its own process: `@nestjs-cls/transactional` keeps its transaction host
  in process-global state, so two applications in one process share one
  database's transactions.

- **CI runs every gated real-backend spec, and a skip fails the build.** A new
  `integration` job runs the MySQL and PostgreSQL round-trips and the RabbitMQ
  transport and inbox specs against service containers built from the same
  images `compose.yaml` uses locally. Until now those specs only ran when
  someone ran `test:full` by hand, so the claims they back — the RabbitMQ
  return-before-ack ordering among them — were never checked on a PR. The job
  runs them through the new `test:integration:strict`, which fails unless the
  run is non-empty and has no `# SKIP` or `# TODO` marker: the specs skip
  themselves when their URL is unset, and Node's summary prints `skipped 0`
  even when a whole suite was skipped, so neither a green exit nor the summary
  proves anything ran. It reads a TAP copy of the run, because the spec
  reporter prints a skip's reason in place of the word SKIP. `test:mutant:full` now also passes the RabbitMQ URLs,
  so `STRYKER_WITH_INFRA=1` runs the RabbitMQ specs too.

- **`@nest-native/kafka` 0.6.x is now an allowed peer**
  (`^0.2.0 || ^0.3.0 || ^0.4.0 || ^0.5.0 || ^0.6.0`). Under 0.x caret rules
  the range excluded 0.6.0, so an application installing this package next to
  Kafka 0.6.0 failed with `ERESOLVE` — the same blocker the 0.5.2 widening
  removed for 0.4 and 0.5. Kafka 0.6.0's changes do not touch the surfaces
  this package uses (`KafkaProducerService.send`, the consumer decorators,
  `KafkaContext`): a retried message now backs off (the partition pauses, 1 s
  doubling to 30 s) instead of redelivering at once, subscriptions may name
  topics by `RegExp`, and an error mapper may be async. Verified rather than
  assumed: the suite, the 100% coverage gate, the complexity gate and every
  sample run green with the devDependency and `sample/01-kafka` on 0.6.0.

- **Both ends of the NestJS peer range are now CI legs.** The single
  `nestjs-latest-major` job that resolved the tree against `^12` is replaced
  by a `nestjs-compat` matrix: an `11 floor` leg pinned exactly to `11.0.0`
  (the oldest graph the published range can produce, with the reason next to
  the pin) and a `12` leg on `^12.0.0`, both fresh-resolved. Each leg runs
  the new `scripts/check-nestjs-resolution.mjs`, which proves the exact
  version from inside every workspace and checks every peer range in the
  NestJS ecosystem against the final tree (npm overrides a peer conflict it
  can override with a warning and exit 0). The same script runs against the
  lockfile in `release:check`, and found the lockfile hoisting
  `@nestjs/microservices` 11.1.27 at the root while sample 01 carried a nested
  11.2.1 — repaired (the root now hoists 11.2.3). No published range changed.

## 0.6.0

- **NestJS 12 is supported.** The `@nestjs/common` and `@nestjs/core` peer
  ranges widen from `^11.0.0` to `^11.0.0 || ^12.0.0`. Nothing in the package
  had to change: NestJS 12 is ESM-only with an exports map under which a deep
  import of a `@nestjs/*` *directory* (`@nestjs/common/interfaces`) no longer
  resolves — the one thing that broke `@nest-native/kafka` and
  `@nest-native/trpc` — and this package makes no deep import into `@nestjs/*`
  at all; 12 also reorders lifecycle hooks across providers by
  module-hierarchy level, and nothing here depends on a cross-provider hook
  order. Verified rather than assumed: the full suite (135 tests), the package
  build, and both samples were run against 12.0.1 before widening, and a new
  CI leg keeps running them there — the tree is resolved against 12 in every
  workspace and each sample proves it resolves 12 before anything runs. The
  `@nestjs/*` devDependencies stay on 11.x. The 12 end of the range needs
  Node.js `>=22.12`, where `require(esm)` is no longer behind a flag;
  `engines` stays `>=22` because the 11 end does not need more. On NestJS 12
  you also need the first releases of the neighbours whose own peer ranges
  admit it: `nestjs-cls` 6.3, `@nestjs-cls/transactional` 3.3 and, for the
  Kafka transport, `@nest-native/kafka` 0.5.1. Dependabot's peer group now
  includes majors, so the next NestJS major arrives as one installable PR
  instead of one ERESOLVE per package.

## 0.5.2 - 2026-08-31

- **`@nest-native/kafka` 0.4.x and 0.5.x are now allowed peers**
  (`^0.2.0 || ^0.3.0 || ^0.4.0 || ^0.5.0`). The range had been capped at `^0.3.0`
  since it was written, so once Kafka reached 0.4.0 any application depending on
  both packages could not upgrade: `npm ci` failed outright with `ERESOLVE`,
  naming this package's `peerOptional` as the blocker. Found by dogfooding — the
  reference app hit it the moment it adopted Kafka 0.5.0 for request-reply.

  Isolated review of the widened range: the Kafka releases in it are additive
  (0.4.0 proved broker-restart recovery and routed raw `librdkafka` properties,
  0.4.1 extended that routing to the undotted ones, 0.5.0 added opt-in
  request-reply). The surfaces this package uses — `KafkaProducerService.send`,
  the consumer decorators, and `KafkaContext` — are unchanged across all three.
  Verified rather than assumed: the suite, the 100% coverage gate, the
  complexity gate and both samples run green against Kafka 0.5.0, and the Kafka
  sample typechecks against it.

  No code changed; this is the peer range and the sample pins only.

## 0.5.1 - 2026-07-30

- **`better-sqlite3` 13 is now an allowed peer** (`^11 || ^12 || ^13`). Isolated
  major-version review: v13 is an N-API rewrite whose JavaScript surface is
  purely additive (`db.explain()`, `statement.toString()`) with no removals, and
  `SqliteError.code` — which the inbox's exactly-once dedup keys on
  (`SQLITE_CONSTRAINT_UNIQUE`) — is unchanged. The full suite (135 tests), 100%
  coverage, and both sample smokes were run against 13.0.2 before widening, and
  a new CI leg keeps running them there.
  better-sqlite3 13 requires **Node >=22** while this package still supports
  Node >=20, so the range is widened, not moved: the devDependency stays on
  12.x. Nothing changed behaviorally; consumers on Node 22+ can now upgrade
  their own `better-sqlite3` without an npm `ERESOLVE`.

## 0.5.0 - 2026-07-19

- **Added the cross-machine wake for the Postgres dialect — `LISTEN`/`NOTIFY`.**
  Completes the wake tiers from 0.4.0: `new PostgresOutboxStore({ wakeChannel })`
  piggybacks `pg_notify` on the enqueue transaction (Postgres delivers it **on
  commit** and drops it on rollback, so the wake is atomic with the event
  becoming visible — no post-commit discipline needed), and the new
  `PostgresWakeListener` (at `@nest-native/messaging/postgres`) holds a
  dedicated `LISTEN` connection that feeds the worker's `OutboxWaker`,
  reconnecting with a fixed delay when the connection drops. Best-effort by the
  same contract as the other tiers: notifications missed during a reconnect gap
  are not recovered — polling remains the delivery backstop, so a lost wake only
  costs one poll interval, never an event. Channels are allow-listed to an
  identifier-safe charset (`LISTEN` cannot be parameterized), capped at
  Postgres's 63-byte identifier limit (beyond it `LISTEN` silently truncates
  while `pg_notify` RAISES — which would abort the caller's business
  transaction), and double-quoted, keeping `LISTEN` case-sensitivity aligned
  with `pg_notify`'s exact-string channel. The listener survives the pg
  end-during-connect edge (a client ended mid-connect never settles its
  `connect()` promise — the session races it against the connection's own
  `end`/`error` events so `stop()` cannot hang), guards a throwing custom
  `WakeSignal` from crashing the worker, validates `reconnectDelayMs`, and the
  documented factory sets `keepAlive: true` so a half-open LISTEN socket is
  detected by the OS. Postgres-only by nature; SQLite/MySQL use the
  `WakeSocket` tier. Verified against real Postgres: delivered on commit,
  dropped on rollback.

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
