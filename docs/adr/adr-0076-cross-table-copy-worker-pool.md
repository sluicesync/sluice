# ADR-0076: Cross-table copy worker pool for `sluice migrate`

## Status

Accepted (shipped). Builds on [ADR-0019](adr-0019-parallel-within-table-bulk-copy.md) (within-table PK-range chunking, the `--bulk-parallelism` axis), [ADR-0018](adr-0018-per-batch-bulk-copy-checkpointing.md) (per-batch cursor checkpointing), and the connection-resilience arc ([ADR-0043](adr-0043-native-bulk-loader-on-parallel-copy-path.md) fast-loader selection + the ADR-0042/0043 copy-pool gate and the connection-budget preflight). This is **phase (a)** of roadmap item 3; phase (b) (adaptive `--bulk-parallel-min-rows`) shipped first.

### Implementation notes (what landed)

- **The second concurrency axis.** `runBulkCopyPhases`'s serial per-table loop (`for _, table := range schema.Tables { bulkCopyOneTable(...) }`) is replaced by a bounded `errgroup` over tables (`runBulkCopyTablePool`, `internal/pipeline/migrate_table_pool.go`): `tg.SetLimit(tableParallelism)`, one `tg.Go` per table calling `bulkCopyOneTable`. The two axes compose — each concurrent table can itself run the within-table parallel-copy path. Scoped to the `migrate` path ONLY (see "Why the sync path stays serial").
- **Per-table connections.** Each concurrent table needs its own primary reader/writer pair, since the orchestrator's primaries can serve only one table at a time. `openTablePair` (a thin wrapper over the existing per-chunk `openOneChunkConn`) opens a dedicated pair per table; a 1-slot channel hands the orchestrator's already-open "free pair" to exactly one running table at a time (returned to the pool when that table finishes), mirroring the within-table chunk-0 optimisation at the table granularity. Dedicated pairs are closed deterministically on table completion.
- **Combined connection budget — the load-bearing constraint.** Max concurrent target connections = `tableParallelism × withinParallelism`, and that PRODUCT must be `<= CopyBudget`. It is enforced at the SINGLE existing chokepoint, never per-axis: `resolveTargetCopyParallelism` probes the target's `ConnectionBudget` once and resolves the within factor; the new `resolveCopyParallelismBudget` then splits the budget — satisfy within first (better locality, the well-tuned shipped axis), then `tableP = clamp(requestedTable, 1, budget / withinP)` where `budget = min(CopyBudget, --max-target-connections)`. Because each table's own `copyParallelismGate` is seeded with `<= withinP` tokens and the table pool is capped at `tableP`, the product is bounded **by construction** — no new shared runtime semaphore. The resolved INFO log prints both factors + their product. `--max-target-connections` bounds the PRODUCT (not either axis alone).
- **Resume under concurrency.** Peer tables now write `state.TableProgress[name]` concurrently (distinct keys, same Go map — not concurrent-write-safe). Every table-level state write in `bulkCopyOneTable` / `copyTableWithCursor` (previously lock-free, because the loop was serial) now takes `stateMu` and deep-clones the state UNDER the lock via `cloneStateForWrite` before the JSON-encoding `writeState` call, exactly the discipline the chunk axis already proved (`setTableProgressAndWrite` helper). `classifyTableForResume` and the in-flight entry reads are guarded the same way; failure-path writes go through `markFailedLocked`. Resume stays order-independent — each table's progress entry is self-contained.
- **Flag.** `--table-parallelism` (default `0` = auto). `0` resolves to `4` (matching pgcopydb's `--table-jobs` default) bounded by the budget split; `1` disables cross-table concurrency (the pre-ADR-0076 serial behaviour). `TableParallelism int` on `Migrator`, wired through `cmd/sluice/cli.go`.

**Measured (PG → PG, 30 tables × 50k rows each, each below the within-table-split threshold):** `--table-parallelism=1` (serial loop) = ~5.88 s, `--table-parallelism=6` = ~2.13 s — a **~2.76× speedup**, closing the ~2.6× gap the roadmap benchmark measured against pgcopydb on this shape. Every run zero-loss (per-table COUNT + SUM content checksum).

## Context

A controlled PG → PG benchmark vs pgcopydb (roadmap item 3) surfaced a throughput/UX gap on **many-table** schemas: `sluice migrate` had **no cross-table concurrency**. The per-table loop copied tables strictly serially, and `--bulk-parallelism` only split work *within* a table (ADR-0019). On 30 tables × 50k rows — each below the within-table-split threshold, so each was both single-streamed AND serially scheduled — sluice was ~16.1 s vs pgcopydb's 6.1 s (~2.6× slower); cores sat idle between tables. pgcopydb's `--table-jobs` (default 4) copies multiple tables at once.

Phase (b) (adaptive `--bulk-parallel-min-rows`) closed most of the gap by lowering the within-table-split threshold on many-table schemas (so each medium table engages within-table parallelism), with no new concurrency surface. It left the residual **serial-table scheduling overhead**, which (a) removes.

Correctness was never in question for the serial path — every benchmark run was zero-loss. This is a throughput gap, not a silent-loss class; it is demand/competitive-gated, not correctness-gating. The discipline below exists so that *adding the concurrency* doesn't introduce a silent-loss or connection-exhaustion class.

## Decision

Add a bounded cross-table worker pool over the per-table copy loop, governed by `--table-parallelism`, composed with the existing within-table `--bulk-parallelism` axis. Resolve the two factors from a single connection budget so their product can't exhaust the target's slots.

### Combined-budget policy: the conservative static split (option (i), recommended)

Two factors must satisfy `tableP × withinP <= CopyBudget`. We considered:

- **(i) Static split at the budget chokepoint (CHOSEN).** Resolve within first (it has better locality and is the well-tuned shipped axis), then give the table axis the whole multiples of `withinP` that fit. The product bound holds *by construction* — each table's chunk gate already caps its own connections at `withinP`, and the table pool is capped at `tableP = budget/withinP`. No runtime coordination, no shared semaphore, no new failure mode. The within factor is never lowered by the split (its value is `resolveTargetCopyParallelism`'s result, which itself clamps within to `<= CopyBudget`).
- **(ii) A single global shared gate counting every open copy connection across both axes (REJECTED).** This would let the two axes dynamically rebalance, but it hoists a new shared runtime semaphore into the hot path, complicates the AIMD slot-backoff (which is per-table today), and turns a construction-time invariant into a runtime one that must be re-proven under `-race`. The static split gets the same safety with strictly less machinery.

`--max-target-connections` bounds the PRODUCT (`budget = min(CopyBudget, ceiling)`), not either axis alone — so an operator ceiling caps total concurrency exactly as expected.

### Resume-under-concurrency discipline (extending the chunk axis's invariant)

The within-table chunk path already proved the discipline: mutate your slot under `stateMu`, `cloneStateForWrite` UNDER the lock, then `writeState` outside it. Cross-table concurrency means peer *tables* now also mutate the shared `TableProgress` map (distinct keys). The same three-step discipline is extended to every table-level write and read. `cloneStateForWrite` already re-allocates the map + per-entry slices correctly; distinct keys under one mutex is exactly its design. This is a concurrency chunk — the `-race` integration gate must pass before any tag (the box this was developed on is CGO=0 and can't run `-race` locally; the gate is CI-only).

### Snapshot window (documented, not changed)

`migrate` uses per-connection snapshots (ADR-0019): each reader observes its own snapshot. (a) widens the concurrent-reader window (more readers open at once) — fine for a quiesced source, which is the documented `migrate` precondition. We do NOT touch the `sync start` cold-start path, which pins one exported snapshot via `ir.SnapshotImporter` and is not parallelized here.

### Why the sync path stays serial

`runBulkCopyWithOpts` (the `sync start` cold-start path) stays serial by design. It has no `parallelBulkCopyDeps`, no connection-budget split, and no resume-state mutex, and its snapshot-pinning + idempotent-COPY interplay (the `CopyDurableProgressSink` durable-write watermark, in-flight ordering) is delicate enough that parallelising it is a separate, deliberately deferred chunk. Only `sluice migrate` (`runBulkCopyPhases`) drives cross-table concurrency.

## Consequences

- Many-medium-table cold migrates are ~2.6–2.8× faster at the default `--table-parallelism=4`, closing the pgcopydb gap on this shape. Single-large-table migrates are unaffected (the within-table auto-split win from ADR-0019 still dominates there).
- The connection budget now governs both axes at one chokepoint; a wide schema can no longer multiply two independently-resolved factors past the target's slots.
- More source connections open concurrently on `migrate` (the snapshot-window widening) — acceptable on a quiesced source.
- Resume remains correct and order-independent under concurrency; the state map is mutated under one mutex with a clone-before-write.
- The sync cold-start path is unchanged (still serial).

## Alternatives considered

- **Fold both axes into a single total-parallelism budget the orchestrator splits dynamically (option (ii) above).** Rejected — a shared runtime gate buys nothing the static split doesn't, at the cost of a new runtime invariant to prove under `-race`.
- **Parallelise the sync cold-start path too.** Deferred — the snapshot-pinning + durable-watermark interplay is its own chunk; bundling it here would conflate two correctness surfaces.
- **No new flag — auto-derive table parallelism solely from the budget.** Rejected — operators want an explicit knob to disable (`=1`) or tune cross-table concurrency, mirroring pgcopydb's `--table-jobs`; the `0 = auto` sentinel still gives a sane default.
