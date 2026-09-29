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
  in-process default and the `@nest-native/messaging/kafka` and
  `@nest-native/messaging/rabbitmq` adapters implement it. The core never imports
  a broker client; the RabbitMQ adapter imports only amqplib's *types* and runs on
  the application's own connection.
- Support line: Node `>=22` (`>=22.12` on the NestJS 12 end — see section 3),
  NestJS `^11.0.0 || ^12.0.0`, Drizzle `0.44`/`0.45`,
  `@nestjs-cls/transactional` `3.x`, `better-sqlite3` `11.x`/`12.x`/`13.x`.
  **Peer majors are widened, never swapped**: the devDependency stays on the
  newest major that still installs on the OLDEST supported Node (today 12.x,
  because `better-sqlite3` 13 requires Node `>=22`), and a dedicated CI leg
  exercises the newest supported major so both ends of the range are tested
  rather than assumed. A dependabot PR that bumps such a devDependency past
  that line is declined — merging it would silently drop a supported Node.
- **The same recipe, applied to NestJS.** NestJS 12 (2026-08) is supported as
  `^11.0.0 || ^12.0.0` on `@nestjs/common` and `@nestjs/core`; the `@nestjs/*`
  devDependencies and the lockfile stay on an 11.x in the middle of the range,
  so every default CI job tests that, and the `nestjs-compat` matrix makes the
  two *ends* tested claims: one entry per end resolves the tree against it and
  runs the suite, the package build, and the sample matrix. The `11 floor`
  entry pins `11.0.0` exactly, with the reason next to the pin (this package
  imports `@nestjs/*` roots only and uses nothing 11.x-added); the `12` entry
  floats on `^12.0.0`. A floor is an install-graph fact, not a source fact —
  sibling `@nestjs/*` packages carry their own peer lines
  (`@nestjs/platform-fastify` 11.0.0 and 11.0.1 shipped peering `^10`; every
  `@nestjs/swagger@11.x` peers `common ^11.0.1`), so a repo that declares one
  pins it separately and its framework floor is the oldest graph npm can
  actually produce. Such floors are not peer-range corrections (no consumer
  can reach the versions below them), and the published range changes only
  if the suite actually fails at a floor. Three details of the leg are
  load-bearing. It installs with `npm install --no-save --workspaces
  --include-workspace-root`, because the samples declare `@nestjs/*` as
  `^11.x` and a root-only install satisfies them with a *nested* copy while
  the root moves — a second 11 leg wearing another label. It drops the
  lockfile from its throwaway checkout first, because — unlike the
  `better-sqlite3` leg — npm cannot layer this set on top of the 11 lockfile:
  it refuses to replace the `@nestjs/core` ⇄ `@nestjs/microservices` peer pair
  in place (ERESOLVE on core@12's optional peer `microservices@^12` against
  the lockfile's `microservices@11`, whatever else is in the set; verified
  again after the lockfile drift below was repaired — `@nestjs/microservices`
  reaches this tree only through `@nest-native/kafka`'s peer and sample 01,
  never from the root), so each end is resolved fresh and nothing is written
  back: `--no-save` writes no manifest and produces no lockfile, and the
  checkout is discarded. That makes the command a fresh-checkout recipe — an
  empty `node_modules` and no lockfile — not one to run on an existing
  install: with the 11 tree already in `node_modules`, its hidden
  `node_modules/.package-lock.json` replays the same ERESOLVE after
  `rm package-lock.json`, and every workspace stays on 11. To reproduce a leg
  locally, start from a clean worktree or `rm -rf node_modules
  package-lock.json` first. And before a leg tests anything,
  `scripts/check-nestjs-resolution.mjs <spec>` proves the tree is the one it
  claims: it requires the *exact* pinned version from inside every workspace
  (a resolve that lands elsewhere must not pass as a floor run), fails on
  nested copies, and checks every peer range in the NestJS ecosystem — every
  installed package at any depth that is `@nestjs/*` or peers on one:
  `nestjs-cls`, `@nestjs-cls/transactional`, `@nest-native/kafka`, and this
  package's own published ranges — against the tree the suite will run on.
  The same script runs with no argument in `release:check`, against the
  lockfile; it is what found the lockfile hoisting `@nestjs/microservices`
  11.1.27 at the root (pulled through `@nest-native/kafka`'s peer) while
  sample 01 carried a nested 11.2.1 — drift `npm ci` accepts, repaired with
  `npm update @nestjs/microservices`. It is the gate because npm gives you
  nothing better: a peer conflict npm can override is `npm warn ERESOLVE
  overriding peer dependency` plus exit 0, which neither `npm ls` nor
  `--strict-peer-deps` reports afterwards — and grepping the install log for
  that warning is not a gate either, because npm also prints it for
  transitional states that end coherent. The neighbours whose own peer ranges
  gate 12 — `nestjs-cls` 6.3, `@nestjs-cls/transactional` 3.3 (6.2 / 3.2 say
  `< 12`) and `@nest-native/kafka` 0.6.0 (0.5.1 was the first to admit 12) —
  are the devDependency floors, so a
  fresh resolve picks them up without naming them; a peer that does not admit
  an end is a real finding, never hidden with `--legacy-peer-deps`, and the
  leg is red until the peer ships. Dependabot cannot deliver a NestJS major:
  the `@nestjs/*` packages peer on each other, so one-package-per-PR bumps fail
  `npm ci` with ERESOLVE before a single test runs (NestJS 12 opened fifteen
  such PRs across the org). The `messaging-peer` group in
  `.github/dependabot.yml` therefore groups majors too, so the next major
  arrives as one PR whose result carries information — and even that PR is
  evidence for the widening recipe above, not a replacement for it.
- **The default major flips on a trigger, not per PR.** The devDependencies
  and the lockfile move from 11 to 12 when either NestJS 12 exceeds 50% of
  `@nestjs/core`'s weekly downloads or NestJS 11 stops receiving patches,
  whichever comes first. Read the split from
  `https://api.npmjs.org/versions/@nestjs%2Fcore/last-week` (on 2026-09-12:
  11 at 71%, 10 at 19%, 12 at 5%). NestJS has no LTS; the previous major has
  received patches for roughly a year after the next one shipped. Flipping
  means the `12` matrix entry becomes the default install, the `11 floor`
  entry stays, and the standing grouped dependabot PR for the peer set is
  merged. Until then that PR stays open as the signal that the upgrade is one
  merge away — a green run is not a reason to merge it.
- **Dual CommonJS/ESM publishing is a dated non-goal; revisit in 2027.**
  Every community NestJS library that supports 12 today (nestjs-cls,
  nestjs-pino, the OpenTelemetry and throttler packages) publishes CommonJS
  and loads 12 through `require(esm)` exactly as this package does, and no
  consumer has asked for ESM output. An ESM or dual build is a breaking
  change with a real cost and no demonstrated benefit, so do not start one
  "while at it". Revisit when a consumer cannot load the package, or when
  those community libraries move.

### 2. Public API
- `MessagingModule.forRoot({ store, transport })` / `forRootAsync(...)`.
- `OutboxProducer.enqueue(...)` — called inside the user's `@Transactional`.
  Returns the store's native shape (sync `OutboxEvent` on sqlite, `Promise` on pg).
- `OutboxClaimer.tick()` + a worker-loop helper.
- `InboxService.runOnce(messageKey, source, handler)` → `'processed' | 'duplicate'`.
- Exported per-dialect schema factories for `outbox_events` / `inbox_events`;
  consumers add them to their schema and generate migrations with drizzle-kit.
- Subpaths: `.` (core), `./in-process`, `./kafka`, `./rabbitmq`, `./testing`,
  and the dialect stores `./sqlite`, `./postgres`, `./mysql`.

### 3. Implementation rules
- The published `packages/messaging/package.json` keeps an explicit empty
  `"dependencies": {}` block; runtime integrations are `peerDependencies`
  (`better-sqlite3`, `pg`, `mysql2`, `@nest-native/kafka` and `amqplib` optional).
- **Side-effect rule:** `runOnce`'s handler runs inside the dedup transaction — on
  the sqlite store it must be **synchronous + DB-only**; on Postgres an async
  DB-only handler is fine. Document this on every public surface.
- Keep the wire contract a single in-package source of truth shared by both
  broker transports (Kafka, RabbitMQ) and their inbox consumers.
- **A transport throws plain `Error`s for broker failures, never
  `RetryableError`.** The claimer retries a `RetryableError` with no attempt
  limit, and a plain error with backoff until `maxAttempts` before marking the
  row failed. A broker transport's failures — an outage, a nack, an unroutable
  message — must end in a failed row if they never clear, so they are plain
  errors (the Kafka transport lets `send()` failures through untouched; the
  RabbitMQ transport wraps each with its reason). `RetryableError` is for a
  transport that knows a *specific* retry-after, and `PermanentError` for a
  message that can never be delivered.
- **RabbitMQ: a publish is done only when acked AND not returned.** The
  RabbitMQ transport publishes `mandatory` on a confirm channel: without
  `mandatory`, RabbitMQ acks a message no queue is bound for and drops it, and
  the outbox row would be marked processed for an event nobody receives. The
  broker sends the return before the ack of the same publish — RabbitMQ
  documents that ordering for publisher confirms, and the gated spec (CI's
  `integration` job runs it on every PR) checks it against a real RabbitMQ 4
  broker, because the bookkeeping is only correct while it holds; do not
  change the return/ack bookkeeping without that spec staying green. amqplib's
  recovering connection survives a broker restart but its channels do not,
  which is why the transport reopens its channel lazily and bounds every
  publish with `confirmTimeoutMs`.
- **RabbitMQ consumer: settle safely, back off, and never lose a dead letter.**
  Three rules came out of the adversarial review of the first cut, each
  measured on RabbitMQ 4 and pinned by a gated spec. `RabbitInboxConsumer.consume`
  never rejects: amqplib throws from `ack`/`nack` once the channel has closed,
  and a rejection from a `void`-called consume would crash the process, so a
  settle that fails is logged and the broker's own redelivery (deduplicated) is
  the recovery. A transient failure is requeued only after a backoff (1 s,
  doubling to 30 s, per message; optional `maxAttempts` dead-letters it): an
  explicit requeue goes back to the head of the queue and is **not** counted
  toward a quorum queue's delivery limit, so an immediate requeue redelivered
  the same message about 1 400 times a second, forever. And a dead-letter copy
  is published `mandatory` like the outbox's own publishes — otherwise an
  unbound dead-letter exchange acks the copy and drops it, and the original,
  acked next, is lost. Do not reintroduce an immediate requeue, an unguarded
  settle, or a non-mandatory dead-letter publish.
- **A dedup key must name exactly one message.** amqplib decodes a 64-bit
  integer header through a JavaScript number, so two ids past 2^53 can arrive
  as the same value — and a shared key acks the second event as a duplicate,
  unprocessed. The RabbitMQ consumer therefore reads an integer header as a key
  only while `Number.isSafeInteger` holds, and otherwise falls through to the
  next id in the wire contract or dead-letters with that reason. Any new way
  of turning a header into a key keeps that rule: a lossy conversion is never
  a key.
- **NestJS 12 is ESM-only: never import a directory index from `@nestjs/*`.**
  `@nestjs/common` and `@nestjs/core` 12 ship an exports map of
  `{".", "./internal", "./*.js", "./*": "./*.js"}`. A deep import of a *file*
  (`@nestjs/core/injector/constants`) still resolves under it; a deep import of
  a *directory* (`@nestjs/common/interfaces`) does not, because there is no
  `interfaces.js` and ESM never completes a directory to its `index`. That one
  import was the whole NestJS 12 failure in `@nest-native/kafka` and
  `@nest-native/trpc`. This package makes **no** deep import into `@nestjs/*`
  at all — everything comes from the package roots — and that is the rule:
  keep it that way. Should a deep import ever become necessary, it names a
  file, and it lands together with the scanner test the kafka and trpc repos
  carry (`test/nestjs-deep-imports.spec.ts`: every `@nestjs/<pkg>/<subpath>`
  import must resolve to a `.js` / `.ts` / `.d.ts` file inside the installed
  package, never a directory) so the trap cannot come back unnoticed. Do not
  reach for `@nestjs/common/interfaces/controllers/controller.interface` as a
  workaround either — still an internal path, and 12 defines `Controller` as
  plain `object`. Loading NestJS 12 from this CommonJS package goes through
  Node's `require(esm)`, which is behind a flag before Node 22.12.0 (and
  20.19, below this package's floor), so the 12 end of the range needs Node
  `>=22.12`. `engines` stays `>=22`: it describes the whole peer range, and
  the 11 end runs on any Node 22. Node 22.0–22.11 satisfies `engines` and
  still cannot load NestJS 12, which is why every place that states the
  floor — the support line above, the README and docs compatibility tables,
  the changelog — carries the `>=22.12` qualifier for 12 instead of leaving
  `>=22` to imply it. Raising `engines` to `>=22.12` would be a floor change
  for NestJS 11 users and is a separate decision, not part of widening the
  peer range.
- **Lifecycle-hook order across providers is not a contract.** NestJS 12
  reordered lifecycle hooks (`onModuleInit`, `onApplicationBootstrap`,
  `onModuleDestroy`, `beforeApplicationShutdown`, `onApplicationShutdown`) by
  the component's level in the module hierarchy, so the order in which two
  providers see the *same* hook differs between 11 and 12. The phase order did
  not change. This package implements no lifecycle hook itself; the only
  sequencing it relies on is phase-level — in-process consumers register on the
  `OutboxRegistry` in their own `onModuleInit`, and the claimer that reads the
  registry runs after bootstrap, from the worker loop the application starts.
  Nothing assumes an order among providers within a phase, and nothing may
  start to: a change that needs another provider's same-phase hook to have run
  first expresses that as a dependency (inject it, or move the work to an
  earlier phase), never as an assumption about hook sequencing. No test
  asserts a within-phase hook order, and none should.

### 4. Non-negotiable style
- NestJS naming + DI conventions; full enhancer-pipeline compatibility for the
  Kafka consumer base.
- 100% test coverage (branches/functions/lines/statements) on the core package;
  SonarJS cognitive complexity ≤ 15 per function.
- Tests cover every dialect (sqlite, pg, mysql) and the Kafka path via the
  in-memory broker; gated real-service specs prove the round-trip on MySQL and
  Postgres, and on RabbitMQ: exactly-once under redelivery, unroutable and
  missing-exchange publishes, requeue, both dead-letter paths, and a
  broker-side connection close. Those specs run on every PR in CI's
  `integration` job, against the same images `compose.yaml` runs locally.
- **Every backend the docs claim runs for real in CI, and a skip is a
  failure.** The gated specs skip themselves when their URL is unset so a fork
  or a laptop without Docker stays green — and that same skip turns a broken CI
  wiring into a pass. CI therefore runs them through
  `test:integration:strict` (`scripts/run-integration-strict.mjs`), which
  requires a non-empty run with no `# SKIP` or `# TODO` marker at all. The
  marker scan is load-bearing: Node's test runner prints `ℹ skipped 0` even
  when an entire `describe(..., { skip })` block was skipped, because a
  skipped suite is not a skipped test, so the summary alone passes a run in
  which every backend was missing. A new backend or a new gated spec lands
  with its service in the `integration` job in the same PR.

### 5. Security Review Requirements (MANDATORY)
- Every PR includes an explicit supply-chain + application-security pass.
- **Audit scope.** The `security:audit` release gate audits the *published*
  surface — `audit-production-surface.mjs` packs the tarball and audits its
  production closure. Since the package publishes `"dependencies": {}`, this is
  exactly what consumers install. Advisories confined to dev/peer/build tooling or
  the docs `website/` do not block releases.
- **The docs audit reports, it does not gate.** `security:audit` hard-fails only
  on the *published* surface; `security:audit:docs` still runs and prints, but
  cannot fail the build. This makes the gate match the rule above — website
  advisories cannot reach consumers, so they must not block every PR in the
  repo. Precedent: `@nest-native/cache` and `@nest-native/trpc` were already
  package-only. Trigger: `image-size` (GHSA-w3rx-r6r6-pgpr,
  GHSA-5p2g-fcmc-qvqq) had NO patched version then — 2.0.2 was both the latest
  release and vulnerable (2.0.4 has since fixed it) — and arrives through
  `@docusaurus/mdx-loader`, so the gate was unfixable by any dependency change.
- **Fix docs advisories when a fix exists, and do not wait for Dependabot to
  report them.** The `/website` entry in `.github/dependabot.yml` brings
  version updates, yet an audit on 2026-09-29 found 29 advisories (5 high)
  that no Dependabot alert or PR had surfaced; `npm --prefix website audit` is
  the check. `audit fix` cleared 12, all five highs among them. The other 17
  were one advisory, `uuid` < 11.1.1 under `sockjs` (the dev server only),
  which `website/package.json` overrides to `^11.1.1`: `sockjs` calls only
  `require('uuid').v4()`, which 11.x keeps in CommonJS. Drop the override once
  Docusaurus's own chain moves past it.

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

Plain `npm test` runs without Docker and skips the gated specs, so forks work
out of the box. CI's `integration` job runs the gated specs against real
service containers on every PR (see section 4); the compose flow below is the
same check on your machine. **CI never runs mutation testing** — it is an
on-demand, local-only gate.

### Gated I/O specs (real MySQL / PostgreSQL / RabbitMQ)

- `npm run infra:up` — disposable containers from `compose.yaml`
  (MySQL on `127.0.0.1:33062`, PostgreSQL on `127.0.0.1:54322`, RabbitMQ 4 on
  `127.0.0.1:56720` with its management API on `127.0.0.1:15670`). Needs Docker.
- `npm run test:full` — the hermetic suite plus the gated round-trip specs
  against those containers (`MESSAGING_MYSQL_URL`, `MESSAGING_POSTGRES_URL`,
  `MESSAGING_RABBITMQ_URL` and `MESSAGING_RABBITMQ_MANAGEMENT_URL` are set
  inline to the compose URLs). Each block skips independently when its URL is
  missing. Test queues must be durable: RabbitMQ 4 refuses a transient shared
  queue by closing the whole connection.
- `npm run infra:down` — removes containers and volumes.
- Using your own services instead: export any of those env vars and run
  `npm run test:integration` — the specs gate purely on the env vars. With all
  four set, `npm run test:integration:strict` is exactly CI's check: it fails
  if anything skipped.

**AI agents working on this repo**: when Docker is available, run
`npm run infra:up && npm run test:full` before opening a PR that touches
package source; CI's `integration` job runs the same specs, so the PR body
needs only a result CI cannot show. When Docker is not available, run
`npm test` and let the `integration` job prove the gated specs. Mutation
testing stays out of CI.

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
for timing). Keep CI's unit path fast and Docker-free — that is a deliberate
contract; real services live only in the `integration` job.
