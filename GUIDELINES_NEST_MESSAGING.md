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
  devDependencies and the lockfile stay on 11.x, so every default CI job keeps
  testing the 11 end, and the `nestjs-latest-major` leg resolves the tree
  against 12 and runs the suite, the package build, and the sample matrix.
  Three details of that leg are load-bearing. It installs with
  `npm install --no-save --workspaces --include-workspace-root`, because the
  samples declare `@nestjs/*` as `^11.x` and a root-only install satisfies them
  with a *nested* 11 while the root moves to 12 — a second 11 leg wearing a 12
  label; the leg therefore asserts from inside `packages/messaging` and each
  sample that `@nestjs/core` resolves to 12 before it runs anything. It drops
  the lockfile from its throwaway checkout first, because — unlike the
  `better-sqlite3` leg — npm cannot layer this set on top of the 11 lockfile:
  it refuses to replace the `@nestjs/core` ⇄ `@nestjs/microservices` peer pair
  in place (ERESOLVE on core@12's optional peer `microservices@^12` against the
  lockfile's `microservices@11`, whatever else is in the set), so the 12 tree
  is resolved fresh and nothing is written back: `--no-save` writes no
  manifest and produces no lockfile, and the checkout is discarded. That
  makes the command a fresh-checkout recipe — an empty `node_modules` and no
  lockfile — not one to run on an existing install: with the 11 tree already
  in `node_modules`, its hidden `node_modules/.package-lock.json` replays the
  same ERESOLVE after `rm package-lock.json`, and every workspace stays on 11.
  To reproduce the leg locally, start from a clean worktree or
  `rm -rf node_modules package-lock.json` first. And it re-resolves the
  neighbours whose own peer ranges gate 12 — `nestjs-cls` 6.3,
  `@nestjs-cls/transactional` 3.3 (6.2 / 3.2 say `< 12`) and
  `@nest-native/kafka` 0.5.1 — instead of hiding the gap with
  `--legacy-peer-deps`; a peer that does not admit 12 is a real finding, and
  the leg is red until the peer ships. Dependabot cannot deliver a NestJS major:
  the `@nestjs/*` packages peer on each other, so one-package-per-PR bumps fail
  `npm ci` with ERESOLVE before a single test runs (NestJS 12 opened fifteen
  such PRs across the org). The `messaging-peer` group in
  `.github/dependabot.yml` therefore groups majors too, so the next major
  arrives as one PR whose result carries information — and even that PR is
  evidence for the widening recipe above, not a replacement for it.

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
- Tests cover both dialects (sqlite + pg) and the Kafka path via the in-memory
  broker; a gated real-broker e2e proves exactly-once under redelivery.

### 5. Security Review Requirements (MANDATORY)
- Every PR includes an explicit supply-chain + application-security pass.
- **Audit scope.** The `security:audit` release gate audits the *published*
  surface — `audit-production-surface.mjs` packs the tarball and audits its
  production closure. Since the package publishes `"dependencies": {}`, this is
  exactly what consumers install. Advisories confined to dev/peer/build tooling or
  the docs `website/` are tracked by Dependabot but do not block releases.
- **The docs audit reports, it does not gate.** `security:audit` hard-fails only
  on the *published* surface; `security:audit:docs` still runs and prints, but
  cannot fail the build. This makes the gate match the rule above — website
  advisories cannot reach consumers, so they must not block every PR in the
  repo. Precedent: `@nest-native/cache` and `@nest-native/trpc` were already
  package-only. Trigger: `image-size` (GHSA-w3rx-r6r6-pgpr,
  GHSA-5p2g-fcmc-qvqq) has NO patched version — 2.0.2 is both the latest
  release and vulnerable — and arrives through `@docusaurus/mdx-loader`, so the
  gate was unfixable by any dependency change. Dependabot still tracks the
  website tree; fix docs advisories when a fix exists.

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
