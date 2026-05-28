# sluice v0.86.1 — postgres-trigger CDC is actually reachable via the operator CLI (CRITICAL hotfix for v0.86.0)

**Headline:** v0.86.0 shipped the `postgres-trigger` cross-engine capability, but post-release testing found its operator-facing path was broken: `sluice sync start` / `migrate` with `--source-driver=postgres-trigger` routed CDC through the **slot-based pgoutput** reader instead of the trigger capture-log poller — so on the managed-PG tiers the engine exists to serve (Heroku Essential / Render Basic / Supabase free, where logical-replication slots are unavailable) the documented flow could not engage the trigger path at all. v0.86.1 fixes that (Bug 94, CRITICAL) plus a related cross-engine `migrate` hard-failure (Bug 93). **Anyone using or evaluating `postgres-trigger` should upgrade past v0.86.0.**

## Fixed

- **Bug 94 (CRITICAL) — `postgres-trigger` now uses the trigger poller via the Streamer, never a replication slot.** The orchestrator's engine-neutral cold-start calls `OpenSnapshotStream`, which the trigger engine had **delegated** to the composed slot-based `postgres` engine. Result: on a slot-less managed source the run would fail creating a slot; on a slot-capable server it silently created a `sluice%` replication slot and streamed pgoutput while the `sluice_change_log` capture table accumulated unconsumed — defeating the engine's whole purpose. The existing congruence tests missed it because they drove the trigger reader via a manual path that bypasses the Streamer.

  **Fix:** `pgtrigger.OpenSnapshotStream` is now **trigger-native** — a `REPEATABLE READ READ ONLY` snapshot for the bulk copy, a gapless hand-off to the trigger CDC poller, and **no replication slot / no pgoutput**. The hand-off anchor is the load-bearing correctness point: the capture log's `BIGSERIAL` id is not commit-ordered, so a naive `MAX(id)` anchor could silently skip an in-flight low id masked by a committed higher id. The anchor is instead the **contiguous committed-prefix** (`MIN(id where xmin ≥ snapshot xmin) − 1`, else `MAX(id)`), captured in the same transaction as the snapshot — everything ≤ anchor is in the bulk copy, everything > anchor is replayed by CDC; over-replay is idempotent-safe, a gap is impossible.

- **Bug 93 (HIGH) — cross-engine `migrate` no longer chokes on the engine's own capture tables.** The postgres `SchemaReader` returned `sluice_change_log` / `sluice_change_log_meta` as user tables, hard-failing a cross-engine `migrate` to MySQL at create-tables (`Error 3770`, untranslatable `statement_timestamp()` default) and dragging them onto same-engine targets. They're now excluded alongside the existing `sluice_cdc_state` / `sluice_migrate_state` bookkeeping tables — fixing both the Migrator and Streamer paths via the shared reader.

## Tests

A new integration test drives the **real `Streamer.Run`** (the actual `sync start` path) for both `postgres-trigger → postgres-trigger` and `postgres-trigger → mysql`, asserting: (a) **no `sluice%` replication slot** is created (on a `wal_level=logical` source, so a regression to the slot path would be caught), (b) the capture-log poller is consumed (INSERT/UPDATE/DELETE land), and (c) **no row loss under writes that interleave the bulk-copy window** — the empirical proof of the hand-off anchor. This closes the coverage gap (Streamer-bypassing tests) that let Bug 94 ship.

## Compatibility

- **Drop-in patch from v0.86.0.** No config / schema / IR changes.
- The only behavior change is that `postgres-trigger` sources now correctly use the slot-less trigger poller through `sync start` / `migrate`, and the engine's own capture tables are never migrated as user data.
- **Severity CRITICAL** (the v0.86.0 `postgres-trigger` CDC path did not work via the CLI). The bulk-copy, cutover/AUTO_INCREMENT priming, and all non-trigger directions were unaffected in v0.86.0 and remain so.

## Who needs this

- **Anyone on v0.86.0 using `postgres-trigger`** — required; the CDC path now actually engages the trigger poller on slot-less managed PG.
- **Operators evaluating sluice for Heroku / Render / Supabase-class managed PG → MySQL/Postgres migrations** — this is the release where the documented `trigger setup → migrate → sync start` flow works end-to-end.
