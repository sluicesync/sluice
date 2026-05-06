# ADR-0027: Source-transaction-boundary aware CDC batching

## Status

Accepted. Implemented in `internal/ir/change.go` (new `TxBegin` / `TxCommit` variants), `internal/engines/{mysql,postgres}/cdc_reader.go` (boundary emission), and `internal/engines/{mysql,postgres}/change_applier_batch.go` (TxCommit-driven flush). Same-engine integration tests live in `change_applier_batch_integration_test.go` for each engine.

## Context

[ADR-0017](adr-0017-batched-cdc-apply.md) added a batched apply path that commits N source changes per target transaction, amortising the per-commit fsync round-trip on bulk traffic. v0.6.x ships with three flush triggers: row-count cap (`maxBatchSize`), idle timer (`defaultIdleFlushPeriod`), and schema-changing events (`ir.Truncate`). The deliberate gap noted in ADR-0017's "What this design does not do (yet)":

> Source-transaction boundaries don't flush early in v1. PG pgoutput emits Begin/Commit messages and MySQL binlog has XID/GTID events, but the IR currently filters those before the applier sees them.

Two consequences of that gap:

1. **Target tx boundaries don't align with source tx boundaries.** A 5000-row source transaction is split into 50 target transactions of 100 rows each (with `--apply-batch-size=100`). The receiver's commit-history loses the source's transactional grouping; downstream consumers reading the target see partially-applied source transactions during the apply window.
2. **Resume window can straddle source transactions.** A crash mid-batch replays both source transactions on resume — idempotency makes that safe, but the replay window is larger than the operator-tunable batch size suggests.

The follow-up was deferred because v0.6.0 needed a working baseline before tuning the boundary semantics. Now that ApplyBatch is shipping and stable, surfacing the source's transactional structure to the applier is straightforward.

## Decision

Surface source-transaction boundaries as new IR change variants and use them to drive the batched applier's flush logic. Three concrete pieces:

### IR plumbing

Two new sealed-interface variants on `ir.Change`:

- `TxBegin{Position}` — start of a source transaction.
- `TxCommit{Position}` — end of a source transaction.

Carrying boundary information as discrete events (rather than a single `TxBoundary` with a `Kind` field) keeps the existing type-switch dispatch idiom uniform across engines and avoids stuffing flags into struct shapes that already type-switch cleanly. Both variants return empty string from `QualifiedName()` — they're not table-scoped — and the pipeline's table-filter explicitly bypasses them so a filter never silently drops a boundary signal.

### Engine emission

- **Postgres** (`cdc_reader.go::dispatchWAL`):
  - `BeginMessage` → `ir.TxBegin{Position: <FinalLSN>}` (FinalLSN is the commit LSN of the transaction this Begin opens; pgoutput emits Begin only after the source tx has committed, so the value is known up front).
  - `CommitMessage` → `ir.TxCommit{Position: <CommitLSN>}`.
  - `StreamStartMessageV2` / `StreamStopMessageV2` (the pgoutput protocol's streaming-in-progress chunking for huge transactions) → mapped to `TxBegin` / `TxCommit` for now. **Simplification:** a single huge source transaction streamed in N chunks produces N target transactions on the receiver. That's still correct under ADR-0010 idempotency (replay reproduces the same final state) but loses the "one source tx → one target tx" alignment for transactions that overflow the source's `logical_decoding_work_mem`. A future revision could buffer chunks and emit `TxCommit` only on the protocol's actual commit; v1 of this ADR accepts the simpler shape.
- **MySQL** (`cdc_reader.go::dispatch`):
  - `QueryEvent` with `BEGIN` → `ir.TxBegin`.
  - `XIDEvent` (the InnoDB transaction commit marker) → `ir.TxCommit`. This is the canonical commit boundary for transactional storage engines.
  - `QueryEvent` with `COMMIT` (rare, non-InnoDB only) → `ir.TxCommit` defensively. Sluice doesn't formally target non-transactional storage engines, but if one shows up the boundary signal is preserved rather than silently dropped.

### Applier consumption

- **Batched path** (`change_applier_batch.go::applyOneBatch`):
  - `TxCommit` flushes the in-flight target transaction. The position written is the source's commit position — exactly the right resume point under ADR-0007 (durably-applied work) and ADR-0010 (idempotent replay).
  - `TxBegin` is a no-op (the applier's row-count cap, idle flush, and TxCommit flush already drive boundary alignment correctly).
  - Boundary events received before the first row of a fresh batch (the pre-target-tx wait state) are consumed without opening a target tx. This handles the empty-source-tx case (a `TxBegin` immediately followed by `TxCommit` with no row events between them) — the applier never opens an empty target tx, so there's nothing to commit.
- **Per-change path** (`change_applier.go::Apply`): both variants are no-ops. Each row event already commits its own target transaction, so a boundary signal carries no extra information here.

### Configuration

No new operator-facing flag. `--apply-batch-size` becomes a *cap* rather than a target — exactly as ADR-0017 envisioned. A 5000-row source transaction with `--apply-batch-size=10000` commits as one 5000-row target tx; the same source tx with `--apply-batch-size=100` still commits in chunks of 100, because the row-count cap fires first. Operators who want tx-aligned commits raise the cap; operators who want bounded replay windows lower it.

### Backwards compatibility

CDCReaders that don't emit `TxBegin` / `TxCommit` continue to work unchanged: the applier sees no boundary events, the row-count cap and idle flush drive batching as before, and there's no behavioural change for any engine that doesn't opt in. Future engines can omit the boundary events entirely if their replication protocol doesn't surface them; sluice degrades gracefully to the v0.6.x batching shape.

## What this design deliberately does not do (yet)

- **Multi-batch source transactions.** A source transaction larger than `--apply-batch-size` still gets split: the row-count cap fires first, the in-flight batch flushes mid-source-tx, and the next batch picks up from the next change. Idempotency makes the resulting partial-source-tx replay safe, but the receiver's commit history doesn't preserve the source's grouping for those transactions. Raising `--apply-batch-size` is the operator-side mitigation; a "soft cap" mode that ignores the row count when in-flight could be added if real workloads ask for it, but it would unbound memory pressure on pathological large transactions.
- **PG streaming-in-progress fidelity.** As above, `StreamStart` / `StreamStop` produce one target tx per chunk rather than buffering. The simplification is documented; a future ADR could revisit if operators report cross-stream consumer breakage on the chunked-commit shape.
- **MySQL non-transactional storage engines.** sluice targets InnoDB; the defensive `COMMIT` QueryEvent handler exists so boundaries aren't silently dropped on non-InnoDB sources, but the rest of the pipeline assumes XID-driven commits.
- **GTID-aware boundary emission.** MySQL's `GTIDEvent` precedes each transaction in GTID mode; we emit `TxBegin` from the `BEGIN` QueryEvent rather than the GTID event. The two are functionally equivalent for boundary-flush purposes; if a future feature needs the GTID itself at the boundary, the emit point can move.

## Consequences

The receiver's commit history matches the source's transactional structure for the common case (source tx ≤ apply batch size). Cross-stream consumers — debezium downstream of sluice, audit triggers on the receiver, etc. — see whole source transactions land atomically rather than partial states. The replay window on crash is bounded by the source transaction size when boundaries are emitted, smaller than the row-count-cap-only window the v0.6.x batched applier provided.

The position-and-data atomicity invariant from ADR-0007 is preserved: the position written when a TxCommit triggers the flush is the source TxCommit's own position. Idempotency from ADR-0010 is preserved: the per-flush data writes still go through the upsert / tolerant-zero-rows path, so replay from any TxCommit boundary reproduces the same final state.

Empty source transactions don't produce empty target transactions — the lazy target-tx-open in `applyOneBatch` means a `TxBegin` → `TxCommit` pair with no row events between them is a no-op. This matters because Postgres logical decoding emits `BEGIN`/`COMMIT` for transactions that touched only catalog data (e.g. unrelated DDL on tables outside the publication scope), and pre-ADR-0027 those would have been filtered before reaching the applier; post-ADR-0027 they're still filtered, just by the applier's empty-tx skip rather than by the CDC reader.

The interface remains opt-in. Engines that don't emit `TxBegin` / `TxCommit` (existing or future) keep the row-count + idle flush behaviour. Adding boundary emission to a new engine is a localised change in its CDC reader; no orchestrator or interface plumbing required.
