# Cutover

The operator-driven moment at the end of a sluice migration: source writes are stopped, the CDC catch-up window has drained, and application traffic is about to switch from the old source to the new target. This page covers `sluice cutover` — the subcommand introduced in [v0.83.0](https://github.com/sluicesync/sluice/releases/tag/v0.83.0) (F10, ADR-0062) — and the procedural shape of cutover as a whole.

The summary: between the snapshot and the traffic switch, source advances sequences as new rows land. CDC ships those rows to target, but target's *sequence value* still lags source by the catch-up window. Without a priming step, the operator's first post-cutover `INSERT` on target collides with an already-replicated row's PK. Pre-v0.83.0, operators ran `SELECT setval(...)` per table by hand; v0.83.0 makes it one command.

---

## When to run cutover

The full sluice-managed lifecycle:

```sh
# 1. Snapshot — bulk-copy source → target, with deferred index/constraint creation.
sluice migrate --config sluice.yaml

# 2. CDC catch-up — continuous-sync apply from source binlog/WAL to target.
sluice sync start --config sluice.yaml --resume

# 3. Drain — stop the source-side writes (application-level: deploy a read-only
#    config, drain user sessions, halt batch jobs). CDC catches up to source-current.
sluice sync stop --config sluice.yaml --wait

# 4. Cutover — prime target's sequences above source's current value with margin.
sluice cutover --config sluice.yaml

# 5. Switch — application traffic flips from old source to new target.

# 6. Verify (post-flip) — confirm row counts and schema parity.
sluice verify --config sluice.yaml
```

Run `sluice cutover` **after** the CDC drain completes and **before** the traffic flip. Running it earlier wastes the priming pass (source keeps advancing); running it later means a window of post-flip `INSERT`s race against unprimed sequences.

---

## What it does

For each table on the target:

1. **Read source state.** Postgres: `pg_sequences.last_value` for owned sequences (NULL → never called → treated as 0). MySQL: `INFORMATION_SCHEMA.TABLES.AUTO_INCREMENT` (with `information_schema_stats_expiry = 0` to bypass catalog cache).
2. **Read target's current value.** Same source-of-truth query against the target endpoint.
3. **Decide what to do** (target-side-read-guarded):
   - **target ≥ applyValue + margin** → `refused` — target is far enough ahead that the operator inserted on it post-cutover *before* running this command. Exit non-zero with "manual re-snapshot recommended" hint.
   - **target ≥ applyValue** → `noop` — target is already at or above where this run would land it. Idempotent re-run, no work.
   - **otherwise** → `primed` — Postgres: `setval('<target_seq>', source_value + margin, true)`. MySQL: `ALTER TABLE … AUTO_INCREMENT = source_value + margin`.

Tables without an owning sequence (composite PK, UUID PK, identifier-only) are **skipped** with a clear reason in the report. No false-positive refusal on identifier-only tables.

---

## The safety margin

`--cutover-sequence-margin=N` (default 1000) is the buffer added to each `setval` / `AUTO_INCREMENT` apply. Two things ride on it:

- **In-flight INSERT headroom.** Between the read and the apply (and between the apply and the operator flipping traffic), source may receive a few more INSERTs that CDC will ship. The margin keeps the post-cutover first INSERT comfortably above them.
- **Idempotency tolerance.** A re-run within `margin` rows of the first run reports `noop`, not `refused`. Operators recovering from a partial-network-failure mid-run can simply re-invoke.

The default 1000 is conservative; busy systems with sustained-INSERT workloads on the source may want 10000+. Quiet systems can run smaller. The refuse-loudly threshold is `2 × margin` — beyond that, sluice assumes the operator inserted on target post-cutover and refuses to advance further.

---

## Refuse-loudly classes

Sluice exits non-zero (with the per-table reason rendered to stdout) in these cases:

| Condition | What it means | What to do |
|---|---|---|
| target ≥ source + 2× margin | The operator INSERTed on target post-cutover before running this command. Forward-bumping risks future collisions. | **Manual re-snapshot recommended.** Sluice doesn't auto-recover from this — the operator decides whether to drop the target table and re-bulk-copy, or to accept the existing target state and manually `setval` to a hand-chosen value. |
| Source endpoint unreachable | Network / firewall / DSN error | Fix the connection. The command is idempotent — re-run when reachable. |
| Target endpoint unreachable | Same. The partial report still renders to stdout for tables that succeeded before the failure. | Same. |
| Sequence not found on target | Schema drift between source and target — the source has a sequence the target's schema doesn't match. | Investigate via `sluice schema diff`. Likely an out-of-band `DROP SEQUENCE` on target, or a sluice translation gap. |

---

## Idempotency contract

Running `sluice cutover` twice in a row does **not** regress sequence values. The decision tree above is target-side-read-guarded — the second invocation sees target already at the primed value and reports `noop`, no write.

This is load-bearing for operators who pipe sluice into automation: a flaky network during the first invocation can be retried without thinking, and a deployment script that runs cutover after every sync stop won't cause sequence churn on no-op runs.

---

## Cross-engine cutover

`sluice cutover` is engine-pair-aware. The three directions it handles directly:

- **PG → PG.** Source `pg_sequences.last_value` → target `setval(seq, N+margin, true)`. Per-source target schema (`--target-schema NAME`) is threaded into `pg_get_serial_sequence` so sequences resolve in the namespace the migration landed in (see ADR-0031).
- **MySQL → MySQL.** Source `AUTO_INCREMENT` → target `ALTER TABLE … AUTO_INCREMENT = N+margin`.
- **PG → MySQL** (cross-engine). Source `pg_sequences` → target `AUTO_INCREMENT`. The IR-level `SequenceState` interface is the cross-engine contract; reader and writer never share types directly.

MySQL → PG (the inverse cross-engine direction) is also supported when source rows use `AUTO_INCREMENT` PKs that the PG target schema models as `BIGSERIAL`.

---

## Rollback after cutover

`sluice` does not ship a one-button rollback. Operators wanting to retain a rollback path during the cutover window have two procedural options:

### Procedural option 1: Hot-standby reverse direction (recommended for high-stakes cutovers)

Set up a second sluice instance in the *opposite* direction *before* the traffic flip. After cutover, the old source becomes the *target* of a reverse-direction CDC stream from the new target:

```sh
# Before traffic flip, on a second machine / process:
sluice migrate --config reverse-direction.yaml   # cold-start old-source from new-target
sluice sync start --config reverse-direction.yaml --resume

# Both directions now run in parallel.
# - Forward (sluice-1): old-source → new-target  (still draining residual)
# - Reverse (sluice-2): new-target → old-source  (now the active path; old-source is hot-standby)
```

If something goes wrong post-flip (target hits a bug, query plans regress, unexpected behavior surfaces), flip traffic back to old source — it's been continuously synced from new target via sluice-2. Once the operator commits to the new target, stop sluice-2 and let the old source decommission.

Cross-reference: [`docs/use-cases.md`](use-cases.md#cross-engine-mysql--postgres-consolidation) covers the bidirectional-during-transition shape in more detail (including same-engine version-upgrade variants, not just cross-engine).

### Procedural option 2: Periodic snapshot of the new target

A coarser-grained rollback: take a `pg_dump` / `mysqldump` of new target periodically post-flip. If a rollback is needed, restore the dump on old source and switch traffic back. The window-of-loss is the time between the last dump and the rollback decision.

Less load on both endpoints than option 1, but recovery is bulk-replay rather than incremental.

---

## Cross-references

- [v0.83.0 release notes](https://github.com/sluicesync/sluice/releases/tag/v0.83.0) — F10 ship narrative
- [`docs/adr/adr-0062-cutover-sequence-priming.md`](adr/adr-0062-cutover-sequence-priming.md) — design rationale, idempotency contract, two-phase model
- [`docs/use-cases.md`](use-cases.md) — concrete scenarios where cutover lands in the operator workflow
- [`docs/architecture.md`](architecture.md) — the IR + engine pattern that makes cross-engine cutover work
- [`docs/postgres-source-prep.md`](postgres-source-prep.md) — PG source-side requirements (WAL, role attributes) that the cutover phase inherits
