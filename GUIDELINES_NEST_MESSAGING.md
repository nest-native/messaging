# GUIDELINES_NEST_MESSAGING.md
## Core Philosophy — this library MUST feel native in NestJS + Drizzle projects

`@nest-native/messaging` implements the **transactional-outbox** and
**idempotent-inbox** patterns, nothing more. It is decorator-first, DI-first, and
integrates with `@nestjs-cls/transactional` so the outbox write shares the user's
business transaction. It is **not** a generic multi-broker messaging abstraction.

### 1. Architecture assumptions (never break these)
- **Dialect-agnostic core, dialect-specific stores.** The engine (producer,
  claimer + worker loop, inbox, transport seam, wire contract, `MessagingModule`)
  knows nothing about the SQL dialect. All transactional persistence lives behind
  the `OutboxStore`/`InboxStore` interfaces. Ship a **better-sqlite3** store (sync)
  and a **Postgres** store (async); users may provide their own.
- **The Store owns the transactional methods** (`enqueue`, `runOnce`,
  `claimBatch`, `mark*`). The engine only *calls* them and awaits results from
  outside their transactions — safe on sync and async drivers alike. This is the
  generalization of the reference-app's sqlite-only synchronous casts.
- **Transport seam.** The claimer publishes through `OutboxTransport`; the
  in-process default and the `@nest-native/messaging/kafka` adapter implement it.
  The core never imports a broker client.
- Support line: Node `>=20`, NestJS `11.x`, Drizzle `0.44`/`0.45`,
  `@nestjs-cls/transactional` `3.x`.

### 2. Public API
- `MessagingModule.forRoot({ store, transport })` / `forRootAsync(...)`.
- `OutboxProducer.enqueue(...)` — called inside the user's `@Transactional`.
  Returns the store's native shape (sync `OutboxEvent` on sqlite, `Promise` on pg).
- `OutboxClaimer.tick()` + a worker-loop helper.
- `InboxService.runOnce(messageKey, source, handler)` → `'processed' | 'duplicate'`.
- Exported per-dialect schema factories for `outbox_events` / `inbox_events`;
  consumers add them to their schema and generate migrations with drizzle-kit.
- Subpaths: `.` (core), `./kafka`, `./testing`.

### 3. Implementation rules
- The published `packages/messaging/package.json` keeps an explicit empty
  `"dependencies": {}` block; runtime integrations are `peerDependencies`
  (`better-sqlite3`, `pg`, `@nest-native/kafka` optional).
- **Side-effect rule:** `runOnce`'s handler runs inside the dedup transaction — on
  the sqlite store it must be **synchronous + DB-only**; on Postgres an async
  DB-only handler is fine. Document this on every public surface.
- Keep the wire contract a single in-package source of truth shared by the Kafka
  transport and the inbox consumer.

### 4. Non-negotiable style
- NestJS naming + DI conventions; full enhancer-pipeline compatibility for the
  Kafka consumer base.
- 100% test coverage (branches/functions/lines/statements) on the core package;
  SonarJS cognitive complexity ≤ 15 per function.
- Tests cover both dialects (sqlite + pg) and the Kafka path via the in-memory
  broker; a gated real-broker e2e proves exactly-once under redelivery.

### 5. Security Review Requirements (MANDATORY)
- Every PR includes an explicit supply-chain + application-security pass.
- **Audit scope.** The `security:audit` release gate audits the *published*
  surface — `audit-production-surface.mjs` packs the tarball and audits its
  production closure. Since the package publishes `"dependencies": {}`, this is
  exactly what consumers install. Advisories confined to dev/peer/build tooling or
  the docs `website/` are tracked by Dependabot but do not block releases.
- **Strictness scope.** The non-negotiables (100% coverage, complexity ≤ 15, zero
  published runtime deps, isolated major-version review) govern the *core* package
  (`packages/messaging`). Non-core code — `sample/*`, the `website/`, dev tooling —
  uses lighter rules: dependency updates there (including majors) may merge on
  green CI without the core's major-isolation ceremony.
- No secret leakage in code, tests, samples, logs, or docs.

### 6. Release version synchronization (MANDATORY)
- When bumping `packages/messaging/package.json` version, update every
  `sample/*/package.json` `@nest-native/messaging` pin to the exact version, run
  `npm install`, and `npm run release:check`. Publish via a `vX.Y.Z` tag →
  `release.yml` (provenance + the `NPM_TOKEN` secret).
