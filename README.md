# @nest-native/messaging

<p align="center">Transactional outbox + idempotent inbox for NestJS — persisted with Drizzle ORM (SQLite, Postgres &amp; MySQL), delivered in-process or over Kafka.</p>

<p align="center">
  <a href="https://www.npmjs.com/package/@nest-native/messaging"><img src="https://img.shields.io/npm/v/@nest-native/messaging.svg" alt="NPM Version" /></a>
  <a href="https://www.npmjs.com/package/@nest-native/messaging"><img src="https://img.shields.io/npm/dm/@nest-native/messaging.svg" alt="NPM Downloads" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/license-MIT-green.svg" alt="Package License" /></a>
  <img src="https://img.shields.io/badge/coverage-100%25-brightgreen.svg" alt="Test Coverage" />
  <a href="https://nest-native.dev/messaging/"><img src="https://img.shields.io/badge/docs-%40nest--native%2Fmessaging-0f766e.svg" alt="Documentation" /></a>
</p>

> [!NOTE]
> **v0.x — early but stable.** The public API (the producer, claimer, inbox, transport seam, and the Drizzle stores) is implemented and tested at 100% coverage. SQLite, Postgres, and MySQL are supported, with in-process (no broker) and Kafka transports.

## The problem it solves

"Write rows **and** publish an event" is a **dual write** — two systems that can't be updated atomically. If the process crashes between the DB commit and the broker publish, the event is lost; if it publishes then fails to commit, you emit a phantom event.

`@nest-native/messaging` closes that gap with the two halves of the reliable-messaging pattern:

- **Transactional outbox (producer):** `enqueue()` writes the event into an `outbox_events` row **inside your business transaction** (via [`@nestjs-cls/transactional`](https://www.npmjs.com/package/@nestjs-cls/transactional)). A background **claimer** then relays committed rows to the broker — at-least-once, with retry/backoff.
- **Idempotent inbox (consumer):** `runOnce()` dedups redeliveries via a unique `(source, message_key)` row written **in the same transaction as the side effect**, yielding **effective exactly-once** processing.

It is **not** a generic multi-broker abstraction — it is the outbox/inbox pattern, done natively for the Drizzle + Kafka + NestJS stack.

## Install

```bash
npm install @nest-native/messaging
# plus your driver + transport (peers):
npm install drizzle-orm @nestjs-cls/transactional better-sqlite3   # or pg / mysql2
npm install @nest-native/kafka                                     # only for the Kafka transport
```

## Entry points

| Import | Contents |
| --- | --- |
| `@nest-native/messaging` | core engine — `OutboxProducer`, `OutboxClaimer` + worker loop, `InboxService`, the `OutboxTransport`/`OutboxStore`/`InboxStore` seams, the wire contract, `MessagingModule` |
| `@nest-native/messaging/in-process` | the no-broker default transport — `OutboxRegistry` (topic → handler) + `InProcessOutboxTransport` |
| `@nest-native/messaging/sqlite` | better-sqlite3 (synchronous) stores + `outbox_events`/`inbox_events` table factories |
| `@nest-native/messaging/postgres` | node-postgres (async) stores + table factories |
| `@nest-native/messaging/mysql` | mysql2 (async) stores + table factories |
| `@nest-native/messaging/kafka` | `KafkaOutboxTransport` + the idempotent `@KafkaConsumer` base, over `@nest-native/kafka` |
| `@nest-native/messaging/testing` | in-memory transport + harness for broker-free tests |

## Status & scope

- **Drivers:** SQLite (better-sqlite3, sync), Postgres (`pg`, async), and MySQL (`mysql2`, async) via per-dialect stores.
- **Transports:** in-process (default, `@nest-native/messaging/in-process` — no broker, at-least-once via the claimer) and Kafka (`@nest-native/kafka`).
- **Roadmap:** additional transports. CDC (Debezium) is an intentional non-goal — this is the app-level outbox.

## Quality Gates

Every PR runs the full gate — build, typecheck, coverage with `c8` enforced at
100% for statements, branches, functions, and lines, cognitive complexity
enforcement (SonarJS threshold `15`), tarball validation, sample version sync,
and a supply-chain audit:

```bash
npm run ci
```

Two **optional, local-only** layers sit on top (neither runs in CI, and forks
work without them):

- **Full mode** — `npm run infra:up && npm run test:full` runs the gated
  MySQL/PostgreSQL round-trip specs against disposable Docker containers
  (`compose.yaml`); `npm run infra:down` cleans up.
- **Mutation testing** — `npm run test:mutation` (incremental Stryker run;
  `test:mutation:full` re-tests everything). Scope with `STRYKER_MUTATE`,
  include the gated I/O specs with `STRYKER_WITH_INFRA=1`.

Details — including the pre-PR ritual and agent instructions — in
[GUIDELINES_NEST_MESSAGING.md](GUIDELINES_NEST_MESSAGING.md#local-full-mode-verification-optional-infra--mutation-testing).

See the [documentation](https://nest-native.dev/messaging/) for the full guide. Part of the [nest-native](https://github.com/nest-native) family. Not affiliated with the NestJS core team.
