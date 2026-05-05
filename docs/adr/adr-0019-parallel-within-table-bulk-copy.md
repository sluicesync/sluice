# ADR-0019: Parallel within-table bulk copy with per-chunk checkpointing

## Status

Accepted. Implemented in `internal/pipeline/chunk.go`,
`internal/pipeline/migrate_parallel.go`,
`internal/engines/{mysql,postgres}/row_reader_range.go`,
`internal/engines/postgres/snapshot_importer.go`,
`internal/ir/interfaces.go` (TableChunkProgress, RangeBoundsQuerier,
RowCounter, SnapshotImporter), `internal/ir/migration_state.go`
(JSON marshalling for Chunks), and the throughput-metric extensions
in `internal/pipeline/progress.go`. CLI surface in `cmd/sluice/cli.go`
(`--bulk-parallelism`, `--bulk-parallel-min-rows`); programmatic
surface on `pipeline.Migrator`.

## Context

[ADR-0018](adr-0018-per-batch-bulk-copy-checkpointing.md) made
mid-table resume work; v0.4.0's measurement was ~5 seconds per 20,000
rows on PG → PG COPY. That maths fine on hundred-thousand-row tables
but bottoms out badly on multi-TB workloads where a single fact table
can be 500 GB / hundreds of millions of rows. v0.4.0 copies each table
sequentially with one reader and one writer connection: on a 16-vCPU
host with one such fact table, 15 cores sit idle the whole copy.

pgcopydb's signature performance optimisation is splitting each large
table into N PK ranges and copying them in parallel to N target
connections. 4–8× wall-clock improvement is realistic on cores that
would otherwise be idle. The same shape works for any engine pair as
long as:

- The source supports concurrent reads from disjoint PK ranges (every
  engine sluice targets does).
- The target supports concurrent writes to the same table (likewise,
  with the caveat that index maintenance during inserts can serialise
  on hot pages — but bulk copy without indexes already addresses that
  in the three-phase apply from ADR-0004).
- The orchestrator can plant per-chunk checkpoints so a crash mid-
  parallel-copy resumes without re-doing completed chunks.

Three coupled design questions had to settle before code:

1. **How to split.** Equal PK-value ranges (cheap, skewed if PKs are
   sparse), equal row counts via OFFSET (slow on huge tables), NTILE
   (requires scanning all PKs). Each is correct; the choice is a
   tradeoff between setup cost and skew risk.
2. **Where to record per-chunk progress.** The v0.4.0 `TableProgress`
   struct holds a single LastPK; per-chunk cursors need a sub-shape.
   The wire format has to stay backward-compatible with v0.4.0 rows
   and forward-compatible with future fields.
3. **How to keep the snapshot consistent across N readers.** PG's
   `EXPORT_SNAPSHOT` plus `SET TRANSACTION SNAPSHOT '<name>'` makes
   this a documented one-liner per connection, but the snapshot stays
   valid only as long as the exporting transaction is open. MySQL
   has no shareable snapshot — N MySQL readers necessarily see N
   independent REPEATABLE-READ views.

## Decision

**Splitting strategy.** v1 ships **MIN/MAX/divide on a single
integer PK column**. One small `SELECT MIN(pk), MAX(pk)` per table
produces N near-equal numeric slices. The cost is one query per table
regardless of size, and the implementation has no surprising failure
modes. Tables that don't fit the shape — composite PK, non-integer
leading column, no PK at all — fall back to the v0.4.x single-reader
path. ADR-0019 records (b) OFFSET-based and (c) NTILE-window splitting
as future enhancements; both lift the v1 limitations at the cost of
either slow setup (OFFSET) or one full scan (NTILE).

**Per-chunk progress.** `TableProgress` gains a `Chunks []TableChunkProgress`
field. Each chunk records its `(LowerPK, UpperPK]` range, its in-flight
`LastPK` cursor, its committed `RowsCopied`, and its lifecycle state.
The chunk shape mirrors the single-chunk shape so the cursor-driven
batched-INSERT loop in `copyChunk` is a near-clone of
`copyTableWithCursor` — only the bounds-clipping differs. v0.4.0
single-cursor rows decode with `Chunks == nil` and stay on the
single-reader path; v0.5.0 chunked rows decode their per-chunk cursors
and resume each chunk independently. The forward-compat caveat from
ADR-0018 still holds (older binaries reading newer rows lose the new
field) but the same JSON object form means both shapes coexist on
disk.

**Boundary stability across resume.** Chunk boundaries are recorded
once on the first attempt and never recomputed. A resume run after a
mid-copy failure reads the persisted boundaries verbatim — even if the
source PK distribution shifted (new inserts above MAX, deletes below
MIN), the chunk layout stays aligned with what was already copied.
This avoids double-copying or skipping rows when the resume's
recomputed boundaries would land in different places.

**PG snapshot infrastructure.** A new optional `SnapshotImporter`
interface returns N RowReaders, each pinned to a separate `*sql.Conn`
that has imported the named snapshot. The Postgres engine implements
it via N `db.Conn(ctx)` + `BEGIN ISOLATION LEVEL REPEATABLE READ READ
ONLY` + `SET TRANSACTION SNAPSHOT 'name'`. The snapshot stays valid as
long as the original `OpenSnapshotStream` transaction is open;
closing the importer's pinned conns rolls back their READ ONLY
transactions and returns the underlying connections to the pool.

**MySQL deliberately does not implement** `SnapshotImporter`. MySQL's
REPEATABLE-READ snapshot is per-session with no shareable name. N
parallel MySQL readers necessarily observe N independent snapshots.
For the `sluice migrate` (no-CDC) path that's an acceptable
inconsistency window when the source isn't quiesced; for the
snapshot+CDC handoff path the per-connection-snapshot inconsistency
gets caught up by the binlog catch-up. ADR-0017's batched apply
amplifies the implications less than one might expect — the per-
session snapshot inconsistency window is bounded by the bulk-copy
duration, not the entire migration window.

**Throughput metrics.** The progress ticker emits `total_rows`,
`bytes`, `rate_rows_per_sec`, `rate_mb_per_sec`, and `eta_seconds` on
every periodic line. Per-chunk progress lines carry an additional
`chunk` attribute. The row-count probe (`pg_class.reltuples` on PG,
`information_schema.tables.TABLE_ROWS` on MySQL) runs asynchronously
on a separate connection so the bulk-copy critical path is not
blocked. Bytes-per-row is plumbed via an `addBytes` hook on the
ticker; v0.5.0 ships the hook and uses zero-bytes by default —
engines can wire scan-buffer sizes in a follow-up without changing
the ticker shape.

**Eligibility threshold.** `--bulk-parallel-min-rows` (default 100k)
guards against the per-chunk overhead dominating on small tables.
Below the threshold, even an integer-PK table takes the single-reader
path. The threshold is checked via the same `RowCounter` surface the
ETA uses, on a separate connection from the bulk read so the probe
doesn't contend with the data path.

**Default parallelism.** `--bulk-parallelism=0` resolves to
`min(8, NumCPU)`. 8 is pgcopydb's documented sweet spot for typical
PG hardware; on smaller hosts NumCPU is the safer cap. Operators with
unusual workloads can override.

## What this design deliberately does not do (yet)

- **OFFSET- or NTILE-based splitting.** v1 is MIN/MAX/divide on a
  single integer column. Composite PKs, UUID PKs, and badly-skewed
  integer PKs all stay on the single-reader path. Future iterations
  can add (b) OFFSET-based for arbitrary PK types and (c) NTILE for
  even row-count splits; both involve a second source-side query and
  a more complex chunk-boundary state shape.
- **Source-side snapshot for the simple `sluice migrate` path.**
  The cold-start `sluice migrate` path doesn't currently capture an
  EXPORT_SNAPSHOT — each parallel reader opens its own connection and
  observes its own per-connection snapshot. For PG sources running
  OLTP traffic during the migration the small inconsistency window is
  the v1 trade-off. A follow-up can capture a temporary
  replication-slot-based snapshot for the bulk-copy phase, plant the
  snapshot name into the orchestrator, and use SnapshotImporter to
  pin all N readers to it. The hook is in place; the wiring is not.
- **Bytes-per-row from engine readers.** The progress-ticker `addBytes`
  hook is plumbed but engines don't yet supply the per-row byte
  count; the operator-facing rate line reports `rate_mb_per_sec=0`
  until engines wire it in. Done as a focused follow-up to keep the
  v0.5.0 surface area manageable.
- **Memory-bounded per-chunk batching.** The per-batch row-count cap
  (`--bulk-batch-size`) is the only knob; very wide rows can produce
  large batches in absolute byte terms. The roadmap's "Memory-bounded
  streaming" item addresses this orthogonally.
- **Parallel-table copy.** This chunk parallelises *within* one
  table. Running N tables concurrently is a separate optimisation
  with its own connection-pool and locking implications.
- **Streamer cold-start parallel copy.** `pipeline.Streamer.runBulkCopy`
  (the snapshot+CDC handoff cold-start branch) currently uses the
  single-reader path. Wiring SnapshotImporter into that branch is a
  small follow-up; the design doesn't change.

## Consequences

**Win.** Multi-TB tables with integer PKs gain 4–8× wall-clock
parallelism. Operators above the row-count threshold see the parallel
path picked automatically; operators below it stay on the v0.4.x
single-reader path with no change in behaviour. The state-row format
is forward-compatible: a v0.4.0 migration mid-flight resumes on
v0.5.0 without losing progress, and an in-progress chunked migration
on v0.5.0 reads cleanly on a future binary.

**Cost.** N additional source connections + N additional target
connections per parallel table. Engines connect to the source/target
through their existing `Open*` methods; the connection-pool overhead
should be invisible on PG and MySQL with default settings, but
`max_connections` on small target instances may need tuning.
Documented in the help text for `--bulk-parallelism`.

**Engine-asymmetric snapshot.** PG sources with the snapshot+CDC
handoff get gapless cross-chunk consistency via SnapshotImporter;
MySQL sources do not. The operator-facing implication is that a MySQL
source with active OLTP during a parallel cold-start may have a small
window where chunk K observes rows that chunk K+1 doesn't (and vice
versa). The binlog catch-up phase closes the window for snapshot+CDC
migrations. For the simple `sluice migrate` path the operator should
quiesce the source — the same advice that already applies to v0.4.x.
Documented in the operator-facing release notes.

**Per-chunk progress wire shape.** The new `Chunks` field on
TableProgress carries N chunk entries (typically 4–8). On an
in-flight migration the JSON column grows from ~50 bytes per
chunked-table-entry to ~300 bytes — still trivial for any realistic
schema size. Operators inspecting `psql` output see a more verbose
JSON map for chunked tables but the same compact "complete" form
once the table finishes.

**ETA accuracy.** `pg_class.reltuples` and MySQL's `TABLE_ROWS` are
both autovacuum-maintained estimates; on a freshly-loaded source
that hasn't been autovacuumed yet, both report 0 and the ETA stays
at "unknown" for the duration of the table's copy. Production
migrations of long-lived source tables don't hit this; testbench
seeds need to ANALYZE before the migration runs to exercise the
parallel-copy path. Documented in the integration-test fixture.

**Default boundary stability.** Boundaries are computed once via
MIN/MAX on the first attempt and persisted to the state row; resume
runs reuse the same boundaries even if the source MIN/MAX has
shifted. Without this, a resume run could compute boundaries that
don't align with completed chunks and silently double-copy or skip
rows. The cost is one extra field in the state row; the win is
correctness across an arbitrary number of resume attempts.
