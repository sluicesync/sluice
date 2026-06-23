# ADR-0112: Within-table chunk parallelism for restore

- Status: Accepted
- Date: 2026-06-23
- Deciders: sluice maintainers
- Supersedes / relates: ADR-0084 (cross-table restore pool), ADR-0019 (within-table PK-range parallelism on migrate), ADR-0076 (two-axis copy parallelism + connection-budget split)

## Context

`sluice restore` parallelizes the bulk-apply phase **across tables** (ADR-0084): a bounded worker pool applies up to `--table-parallelism` tables concurrently, each through its own row-writer connection. But **within** a single table, restore is single-streamed — `restore.go`'s `restoreTable` runs one producer goroutine that reads the table's chunks strictly **in order** through one channel into one `WriteRows` call.

That leaves a real gap whenever the corpus is **a few very large tables** (rather than many medium ones) or the target is **INSERT-bound** (a PlanetScale target disallows `LOAD DATA LOCAL`, so every chunk is applied via batched multi-row INSERT) and **cross-region** (every INSERT round-trips the continent). In those cases the cross-table pool saturates quickly — once its slots are chewing on the big tables, each big table is a single stream, and restore wall-time (which **is** the operator's recovery-time objective) collapses to "slowest single table, serially."

Observed live (Track C, 2026-06-23): a 43 GB MySQL backup restoring into a non-Metal PlanetScale MySQL target across regions applied each 15–35 M-row table as one stream at ~5 min/15 M-rows, with the target's apply concurrency far from saturated — the bottleneck was sluice's single intra-table writer, not the target.

The backup already stores each table's rows as **independent, fixed-size chunk files** (`--chunk-size`, default 100k rows), each with its own SHA-256. For a snapshot restore these chunks are a **disjoint partition of the table's rows** — there is no intra-snapshot ordering requirement (a backup is a point-in-time snapshot, not a CDC change stream), so the chunks can be applied in any order and **in parallel**.

`migrate` already has the equivalent second axis (ADR-0019 within-table PK-range parallelism, ADR-0076 composed with the cross-table axis under one connection-budget chokepoint). Restore lacked it.

## Decision

Add a **second concurrency axis to restore: within-table chunk parallelism**, exposed as `--bulk-parallelism` (mirroring `migrate --bulk-parallelism`; `0` = auto).

When a table has ≥2 chunks and the resolved within-table parallelism `P > 1`, `restoreTable` partitions that table's chunk list across `P` workers. Each worker:

- acquires its **own** row writer (a dedicated connection, via the existing `openTargetRowWriter` factory — the single construction path, so buffer-cap + target-schema routing can't drift);
- streams **its** assigned chunks through **one** channel into **one** `WriteRows` call (so per-worker batch continuity is preserved — batching does not reset per chunk, the property the original single-stream comment protected);
- verifies **each** chunk's SHA-256 exactly as today (the load-bearing layer-1 integrity check, unchanged and still per-chunk);
- returns its rows-applied count.

The orchestrator sums the per-worker counts for the **layer-2 row-count check** (`Σ rows == manifest RowCount`), so the manifest cross-check is exactly as strong as the serial path. The Bug-40b cancel-on-writer-error shape is replicated per worker via an `errgroup` derived context: the first worker error cancels its siblings' producers so no goroutine blocks on a channel send.

**The two axes multiply** (`table-parallelism × bulk-parallelism`) and are bounded at the **single existing connection-budget chokepoint** (`resolveTargetCopyParallelism` + `resolveCopyParallelismBudget`, the same split `migrate` uses, ADR-0076): the within-table axis is satisfied first, the table axis takes the remainder, and the product never exceeds the target's measured `CopyBudget`. Targets without a budget prober (MySQL) pass through unclamped — the same contract as migrate and as the restore cross-table pool.

**Engine-generic**, like ADR-0084: parallel **writers** need no shared snapshot (unlike the backup read side's FTWRL/exported-snapshot requirement), so this engages for every target (PG and MySQL alike).

`--bulk-parallelism=1` (or a single-chunk table) collapses to the pre-ADR single-stream path through the same code, with a loud INFO naming the reason (ADR-0079 disposition: never a silent no-op).

## Correctness (why parallel chunk apply is safe)

1. **Disjoint rows.** A full/snapshot backup partitions a table's rows into chunks; no two chunks share a primary key. Parallel INSERT of distinct chunks therefore cannot collide on a PK on a fresh (cold-start) target.
2. **DataOnly (rotation-segment) restores** use the idempotent `WriteRowsIdempotent` (ON CONFLICT / ON DUPLICATE KEY UPDATE); each worker type-asserts its own writer, and idempotent upsert is order- and concurrency-independent for disjoint rows.
3. **No intra-snapshot ordering.** A snapshot has no apply-order dependency (it is not a CDC change stream where an UPDATE must follow its INSERT). Update-collapse logic does not apply within a snapshot restore.
4. **Integrity unchanged.** Per-chunk SHA-256 stays per-chunk; layer-2 row-count is the sum across workers — both checks are byte-for-byte as strong as the serial path. A mismatch is a hard failure (no silent corruption).

## Consequences

- **Throughput.** Big-table restores into INSERT-bound / cross-region targets parallelize within the table, closing the "slowest single table serially" wall-time gap. Restore wall-time = RTO, so this is a direct DR win.
- **Connections.** The product `table × bulk` opens more concurrent target connections; bounded by the measured budget at one chokepoint, refusing loudly if the budget can't cover even one writer.
- **Small tables unaffected.** A table with one chunk (≤ `--chunk-size` rows) stays single-stream — the fan-out only engages where it helps.
- **Composes with the reparent-retry (ADR-0108).** Each per-chunk-group writer flushes through `flushWithReparentRetry`, so within-table workers independently ride a storage-grow reparent.

## Alternatives considered

- **Bigger INSERT batches only** (raise the multi-row INSERT size to amortize cross-region RTT). A smaller, complementary lever — kept as a possible follow-up knob — but it does not parallelize a single huge table, so it cannot close the wall-time gap on a few-big-tables corpus.
- **Reuse the cross-table pool at chunk granularity** (treat each chunk as a pool task). Rejected: it would break per-worker batch continuity (batching resets per chunk) and complicate the per-table row-count/SHA bookkeeping. A dedicated within-table fan-out that keeps each worker's chunk-run on one `WriteRows` is cleaner.
