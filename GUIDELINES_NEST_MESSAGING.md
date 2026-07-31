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
  `@nestjs-cls/transactional` `3.x`, `better-sqlite3` `11.x`/`12.x`/`13.x`.
  **Peer majors are widened, never swapped**: the devDependency stays on the
  newest major that still installs on the OLDEST supported Node (today 12.x,
  because `better-sqlite3` 13 requires Node `>=22`), and a dedicated CI leg
  exercises the newest supported major so both ends of the range are tested
  rather than assumed. A dependabot PR that bumps such a devDependency past
  that line is declined — merging it would silently drop a supported Node.

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

## Local Full-Mode Verification (optional infra + mutation testing)

Everything in this section is **opt-in and local-only**. Plain `npm test` and
CI run without Docker and skip the gated specs; forks work out of the box.
**CI never runs mutation testing** — it is an on-demand, local-only gate.

### Gated I/O specs (real MySQL / PostgreSQL)

- `npm run infra:up` — disposable containers from `compose.yaml`
  (MySQL on `127.0.0.1:33062`, PostgreSQL on `127.0.0.1:54322`). Needs Docker.
- `npm run test:full` — the hermetic suite plus the gated round-trip specs
  against those containers (`MESSAGING_MYSQL_URL` /
  `MESSAGING_POSTGRES_URL` are set inline to the compose URLs). Each
  dialect's block skips independently when its URL is missing.
- `npm run infra:down` — removes containers and volumes.
- Using your own databases instead: export those two env vars (either or
  both) and run `npm run test:integration` — the specs gate purely on the
  env vars.

**AI agents working on this repo**: when Docker is available, run
`npm run infra:up && npm run test:full` before opening a PR that touches
package source, and report the result (including the gated specs) in the PR
body. When Docker is not available, run `npm test` and state that the gated
specs were skipped. Never wire any of this into CI.

### Mutation testing (Stryker — occasional targeted audit, local only, never in CI)

- `npm run test:mutation` — **incremental** run (cache:
  `reports/stryker-incremental.json`; only re-tests what changed). This is the
  pre-PR ritual for changes to package source.
- `npm run test:mutation:full` — every mutant from scratch (`--force`).
- `STRYKER_MUTATE='packages/messaging/dialects/**,packages/messaging/tokens.ts'` —
  comma-separated globs to scope a run to the files a change touched.
- `STRYKER_WITH_INFRA=1` — each mutant also runs the gated I/O specs
  (`npm run test:mutant:full` per mutant, concurrency forced to 1 because the
  specs share one database per dialect; run `npm run infra:up` first). Slow by
  design; use it when a change touches store-adjacent code.
- Report: `reports/mutation/mutation.html`. Thresholds are advisory
  (`break: null`) — the signal is *which mutants survive*, not the score.

**Occasional targeted audit, not a per-PR gate.** Run mutation testing
deliberately when you've reworked a file's logic — not on every PR. Scope
`STRYKER_MUTATE` to that one file, keep `--concurrency 2`, and verify a kill the
fast way: hand-apply the surviving mutation, run the plain suite, confirm your
new test fails, then `git checkout --` to revert. Full/unscoped runs re-test
every mutant against the whole suite and are slow to impractical — lean on
scoped runs plus hand-verification, and `kill -9` any leftover `stryker`
processes after a timeout. Treat survivors by the doctrine (add a test /
simplify redundant code / `// Stryker disable` a true equivalent / assert bounds
for timing). Keep CI fast and Docker-free — that is a deliberate contract.
