# sluice v0.99.237

**The `sync start` live panel now shows a truthful, persisted cumulative rows-applied count and a live throughput readout — filling the v0.99.236 panel's `rows: n/a (phase 1)` gap.**

## Added

- **Truthful lifetime rows-applied counter + live throughput in the `sync start` panel (ADR-0156 phase 2).** sluice now tracks a cumulative count of row-level changes (INSERT + UPDATE + DELETE) applied to the target for a stream, persisted in the CDC control table and incremented **in the same transaction that advances the stream position** — so the counter can never lead the durable position, and a rolled-back or failed batch contributes nothing. The live panel renders the count and computes throughput itself from successive readings (guarding the first reading and any zero/negative time or non-monotonic delta, so it never shows a negative or a bogus spike). The count is also surfaced on `ir.StreamStatus.RowsApplied` via `ListStreams`, ready for a future `sync status` field.

  **Exactly-once, everywhere.** The count is exactly-once across all three CDC apply paths — serial per-change, the batched loop (a mid-transaction flush that defers its position write carries its DML forward and folds it into the boundary write, only after the commit succeeds), and the concurrent key-hash lane apply (the coordinator aggregates each lane's committed DML and realizes it only at a checkpoint boundary that is durable across every lane). The count semantics have a single owner (`ir.IsRowDMLChange`), pinned across the whole change family so "what counts as a row applied" can't drift between paths.

  **Honest about warm-resume.** The counter counts rows *applied*: after a warm-resume, CDC re-delivers a bounded already-applied tail that is re-applied idempotently and re-counted, so the lifetime total can slightly *exceed* the distinct source-change count. It is never an under-count and never counts uncommitted rows — this is documented on `StreamStatus.RowsApplied`. For a fresh stream the count is exact.

  Engines: Postgres and MySQL (the CDC-target appliers); the Postgres-trigger engine inherits it via Postgres.

## Compatibility

- **Additive control-table column, no break.** `rows_applied BIGINT NOT NULL DEFAULT 0` is added to the CDC control table on the same detect-then-`ALTER` / `ADD COLUMN IF NOT EXISTS` path as the existing `slot_name` / `source_dsn_fingerprint` / `target_schema` columns. Legacy rows and streams that pre-date the column read as `0` and begin counting from the first post-upgrade apply (pre-upgrade applies were never tracked). No manual migration; existing streams resume cleanly.

## Who needs this

Anyone watching a `sync start` at an interactive terminal — the panel now shows how many rows have been applied and how fast, instead of `n/a`. Non-TTY / `--log-format=json` output is unchanged; the new column is additive and back-compatible.

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
