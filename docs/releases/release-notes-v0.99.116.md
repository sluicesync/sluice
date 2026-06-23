# sluice v0.99.116

**`sluice restore` now parallelizes *within* a table, not just across tables — a second concurrency axis (`--bulk-parallelism`) that fans a single big table's chunks across N writers, closing the "slowest single table, serially" restore wall-time gap on INSERT-bound / cross-region (PlanetScale) targets.**

## Added

Restore already applied tables concurrently (`--table-parallelism`, ADR-0084), but **within** a table it was single-streamed — one producer reading the table's chunks in order through one writer. On a corpus of a few very large tables, or into an INSERT-bound cross-region target (a PlanetScale target disallows `LOAD DATA LOCAL`, so every chunk is a batched INSERT that round-trips the network), that collapsed restore wall-time to "slowest single table, applied serially" — and restore wall-time *is* the operator's recovery-time objective.

ADR-0112 adds the within-table axis. When a table has ≥2 chunks and the resolved within-table parallelism is >1, restore partitions that table's chunk list into disjoint, contiguous groups and applies them through N dedicated writer connections concurrently. Each worker streams its own chunk run through one `WriteRows` call (so per-worker batch continuity is preserved) on its own connection (worker 0 reuses the table's primary writer; peers open dedicated ones through the same single construction path, so buffer-cap + schema routing can't drift).

`--bulk-parallelism 0` (default) = auto (`min(8, NumCPU)`); `1` = the prior single-stream-per-table behaviour. The two axes **multiply** (`table × bulk`) and are bounded at the **same** connection-budget chokepoint migrate uses (ADR-0076): the within-table axis is satisfied first, the table axis takes the remainder, and the product never exceeds the target's measured connection budget. Targets without a budget prober (MySQL) pass through unclamped, the same contract as the cross-table axis. The fan-out applies to chain restores too (each segment full's bulk-apply); incremental change replay stays strictly ordered.

## Correctness

Unchanged-strong. A snapshot's chunks are a **disjoint partition** of the table's rows, so parallel INSERT cannot collide on a primary key on a cold target; DataOnly (rotation-segment) restores use idempotent upsert, which is order- and concurrency-independent for disjoint rows. Per-chunk SHA-256 verification stays per-chunk (the load-bearing integrity check). The layer-2 row-count check is the **exact sum of actually-decoded rows across all workers** versus the manifest's count — byte-for-byte as strong as the serial path — and a mismatch is still a hard failure. The Bug-40b cancel-on-writer-error shape is replicated per worker.

Pinned by serial-vs-parallel **byte-identical** integration tests on both Postgres and MySQL targets across a varied value matrix (int / decimal / text / json(b) / bytea-varbinary / uuid / temporal / bool), plus single-chunk-stays-serial and row-count-mismatch-fails-hard pins, and two-axis connection-budget unit tests (product ≤ measured budget; MySQL passthrough).

## Compatibility

Additive and opt-in-by-default-auto. New `--bulk-parallelism` flag on `sluice restore`; the prior behaviour is exactly `--bulk-parallelism 1`. No change to backup, sync, or migrate; no value-path code touched (values flow through the unchanged decode + `WriteRows` path — only *which writer applies which chunk* changed). The per-worker writer composes automatically with the storage-grow reparent ride-through (ADR-0108).

## Who needs this

Anyone restoring a backup whose data is concentrated in a handful of large tables, or restoring into a cross-region / INSERT-bound target (notably **PlanetScale**, where `LOAD DATA LOCAL` is unavailable) — the big tables now restore in parallel within themselves instead of one stream at a time.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.116
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.116
```
