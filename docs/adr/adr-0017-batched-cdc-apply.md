# ADR-0017: Batched CDC apply for throughput

## Status

Accepted. Implemented in `internal/engines/{mysql,postgres}/change_applier_batch.go`; CLI surface in `cmd/sluice/cli.go` (`--apply-batch-size`); programmatic surface on `pipeline.Streamer.ApplyBatchSize`.

## Context

The v0.3.x applier commits one source change per target transaction. That choice is correct for resume safety — [ADR-0007](adr-0007-position-persistence.md) puts the position write inside the data-write transaction, and [ADR-0010](adr-0010-idempotent-applier.md) makes Insert / Update / Delete idempotent so replay after a partial-apply is benign — but it bottoms out at the per-commit fsync latency rather than row-application work. v0.3.0's robustness testing measured the applier at ~6.5 rows/sec on PG → MySQL CDC when one source transaction contained 5000 INSERTs (~150 ms per row). Production workloads where one source transaction touches thousands of rows are common; the per-row-tx default is too conservative for them.

Three options for amortising per-tx commit overhead:

1. **Group-commit on the source side**, then ship one giant change to the target. Requires reaching into engine-specific replication protocols and re-coalescing rows; brittle and engine-specific.
2. **Pipeline N transactions in flight on the target.** Concurrent appliers that all write the same target. Reorders writes — breaks source-ordering — and complicates position-persistence atomicity.
3. **Batch N source changes into a single target transaction**, with the position write at the end of the batch. Linear, single-threaded, source-ordering preserved. Resume idempotency from ADR-0010 already makes the replay-on-crash window safe.

## Decision

Option 3. Operator-tunable via `--apply-batch-size N`. Default 1 keeps current behavior bit-for-bit (one source change per target tx). Production tuning is 100–500 typically; 100x throughput improvement on bulk CDC traffic was the v0.3.0-test measurement target.

The shape:

- Add an optional `BatchedChangeApplier` interface to `internal/ir`. It extends `ChangeApplier` with `ApplyBatch(ctx, streamID, changes, maxBatchSize) error`. The Streamer probes for it via type assertion; engines that don't implement it fall back to per-change `Apply` even when `ApplyBatchSize > 1` (with a warning log).
- Both shipping engines (`mysql`, `postgres`) implement it. The implementation lives in a sibling file (`change_applier_batch.go`) so the per-change Apply path is untouched.
- A batch flushes early on:
  - **Channel close** — clean shutdown; the partial batch commits with the position of its last applied change.
  - **`ctx` cancel** — the in-flight transaction rolls back; the remaining changes replay on resume via idempotency.
  - **Target write error** — same as ctx cancel (rollback + propagate the error).
  - **Schema-change events** — `ir.Truncate` today; `AddColumn` / `DropColumn` if the IR grows them. Schema events apply alone (Postgres: in their own transaction; MySQL: TRUNCATE implicit-commits the open tx so the in-flight batch is committed *first*, then the truncate runs as a "batch of 1").
  - **Row-count cap** — `maxBatchSize` reached.
- Position written at the end of each batch is the position of the **last applied change in the batch**. Replay from that position via idempotency reproduces any unwritten work; the ADR-0007 invariant (position-and-data atomicity) is preserved per batch.

## What this design deliberately does not do (yet)

- **Source-transaction boundaries don't flush early in v1.** PG pgoutput emits Begin/Commit messages and MySQL binlog has XID/GTID events, but the IR currently filters those before the applier sees them. Surfacing them as `ir.BatchBoundary` events (or similar) is a follow-up; v1 batches purely by row count plus the schema-event flushes above. Tradeoff: a batch may span the boundary of two source transactions, which means a crash mid-batch replays both source transactions in the same target transaction on resume. Idempotency makes that safe. *(Follow-up landed in [ADR-0027](adr-0027-source-transaction-boundary-cdc-batching.md): `ir.TxBegin` / `ir.TxCommit` variants drive a flush-on-source-commit path while keeping the row-count cap as the upper bound.)*
- **Multi-batch transactions are not supported.** One commit per N changes; a single target tx never spans multiple batches.
- **No-PK tables are still best-effort.** Replay on a no-PK table produces duplicates per ADR-0010, and batching amplifies the failure mode (a 100-row batch replayed produces 100 duplicates rather than 1). The applier doc-comment surfaces this; operators should require PRIMARY KEYs on continuous-sync source tables. Same pre-existing caveat, larger blast radius.
- **No per-row position-write within a batch.** The position written is the last change's, not every change's. Batching is meaningless if every change writes its own position.

## Consequences

100x-ish throughput improvement on bulk CDC traffic at the cost of a larger replay-on-crash window (up to N changes can replay rather than 1). For workloads with frequent multi-row source transactions — i.e. nearly all production OLTP — this is the right default trade-off, but conservative operators can leave the default at 1.

The interface is opt-in via type assertion; future engines (MongoDB, ClickHouse, etc.) can ship a per-change-only applier and pick up batching support when they implement `BatchedChangeApplier` later. The Streamer's dispatch logs a warning when `--apply-batch-size > 1` lands on an applier without batched support, so operators see why their throughput tuning didn't take effect.

The flush-on-Truncate behavior is the load-bearing wart: MySQL's TRUNCATE implicit-commits, so a naïve "batch with TRUNCATE inline" would silently break ADR-0007's atomicity invariant for whatever rows preceded the truncate in the batch. The fix is the explicit pre-flush in the batched applier. Future schema events (AddColumn, DropColumn) will need the same treatment — DDL is DDL on MySQL, and the applier's column-type cache wants invalidation around schema changes anyway.
