# CLAUDE.md

@GUIDELINES_NEST_MESSAGING.md

The imported guidelines are binding. Two always-on rules:
- Stryker mutation testing is local-only — never wire it into CI. The gated real-backend specs are the opposite: CI's `integration` job runs them against service containers through `test:integration:strict`, which fails if any spec or suite skips; `infra:up` + `test:full` is the same check locally.
- Mutation testing is an **occasional, targeted audit — not a per-PR gate**. Run it deliberately when you've reworked a file's logic: scope with `STRYKER_MUTATE` to that one file, `--concurrency 2`, and verify a kill by hand-applying the mutation + running the plain suite (see the guidelines' Mutation testing section).
