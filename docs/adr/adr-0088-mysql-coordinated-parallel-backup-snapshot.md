# ADR-0088: MySQL coordinated parallel backup snapshot (FTWRL-aligned consistent multi-reader)

## Status

Accepted. Implementing in `internal/engines/mysql/backup_snapshot.go`
(coordinated open), `internal/ir/backup/snapshot.go`
(`SnapshotOptions.ReaderParallelism`, `Snapshot.ExtraReaders`),
`internal/pipeline/backup.go` + `internal/pipeline/backup_table_pool.go`
(resolve-then-open reorder, gate, extras-factory). Extends ADR-0084
(cross-table backup parallelism) and revisits the ADR-0019 assumption
that "N MySQL readers necessarily see N independent snapshots".

## Context

ADR-0084 added cross-table parallel reads to `backup full`, but only
Postgres engages them. The gate ([`backupParallelEligible`]) requires a
**shareable exported snapshot** (`pg_export_snapshot()` +
`SET TRANSACTION SNAPSHOT '<name>'`, lazily imported onto N readers via
`ir.SnapshotImporter`). MySQL has no exported snapshot, so its backup
sweep runs **serially on one pinned `START TRANSACTION WITH CONSISTENT
SNAPSHOT` connection** — `backup_snapshot.go` opens one conn, sweeps
every table on it, end to end.

The 2026-06-13 MySQL fair-fight measured the cost: on a ~16 GB single
mysql:8 source, `mydumper --threads 8` dumped in **18.4 s** vs sluice's
**~251 s** — a ~14× gap dominated by sluice's single-reader serial sweep
(zstd is a distant second and is a *feature* — it buys the 2× smaller
artifact and the 3.2× faster restore sluice already wins). sluice's own
log names the cause: `cross-table parallel reads not engaged; sweeping
tables serially — source snapshot is not shareable (per-session)`.

The "not shareable" framing is true about snapshot *export* but
**incomplete**: mydumper (and `pscale database`'s *import* path, which
"tries `FLUSH TABLES WITH READ LOCK`, falls back to `LOCK TABLES` on a
permission error") prove a consistent *parallel* MySQL dump is
achievable on a single (non-Vitess) server — you don't *share* one
snapshot, you make N independent snapshots **coincide** under a brief
global read lock. (Note: `pscale database dump` itself does NOT do this —
its session vars are just `set workload=olap` + a `@replica` route,
parallel-but-not-consistent, because Vitess can't FTWRL across shards.
sluice already routes PlanetScale/Vitess sources through a separate
VStream COPY path, so this ADR is scoped to **vanilla MySQL only** and
does not touch the Vitess path.)

`migrate`'s bulk-copy already opens N *independent* (inconsistent) MySQL
readers and lets the CDC catch-up heal the window. `backup` cannot — it
is a point-in-time artifact with a recorded `EndPosition`; an
inconsistent multi-reader read would be silent cross-table corruption.
So backup needs **consistent** parallel readers, which is exactly what
the FTWRL-coordination gives.

## Decision

Open N reader connections that all observe the **same** consistent point
via a brief `FLUSH TABLES WITH READ LOCK` window, then sweep tables
across them with the existing ADR-0084 pool.

### The coordination algorithm (vanilla MySQL, `OpenBackupSnapshot` when `opts.ReaderParallelism > 1`)

1. Open a dedicated **coordinator** connection `C` and the **N reader**
   connections `R[0..N-1]` from the pool (N+1 conns total).
2. On `C`: `FLUSH TABLES WITH READ LOCK` (freezes all writes globally).
   - On a **permission error** (no `RELOAD`/`FLUSH_TABLES` — common on
     managed tiers) OR any FTWRL failure: **abort the coordinated path,
     close C + R[*], and fall back to today's single-reader serial open**
     (loud INFO naming the reason). Do NOT attempt `LOCK TABLES` in v1
     (a future enhancement; serial is the safe, correct fallback).
3. On **each** `R[i]`: `SET SESSION TRANSACTION ISOLATION LEVEL
   REPEATABLE READ` then `START TRANSACTION WITH CONSISTENT SNAPSHOT`.
   Because `C` holds FTWRL, no writes occur between the first and last
   `START TRANSACTION`, so all N read views are **byte-identical**.
4. Capture the `EndPosition` (`@@global.gtid_executed` or `(file,pos)`)
   **while FTWRL is still held** — on `C` or any `R[i]`; it refers to
   the frozen instant. (Reuse `captureBackupPositionInTx`.)
5. On `C`: `UNLOCK TABLES`, then close `C`. Writes resume; each `R[i]`
   keeps its consistent view via its open REPEATABLE-READ tx.
6. Return `Snapshot{ Rows: R[0] (primary), Position, ExtraReaders:
   R[1..N-1], CloseFn: commits/closes all R[i] + closes the pool }`.

Each `R[i]` is a `&RowReader{q: conn, schema: cfg.DBName}` — identical
binding to today's primary, so the parallel sweep reads exactly what the
serial sweep would.

### The orchestrator seam (engine-neutral, IR-first)

- **`irbackup.SnapshotOptions` gains `ReaderParallelism int`** — the
  resolved cross-table parallelism the orchestrator wants readers for.
  PG ignores it (lazy import is unchanged); MySQL vanilla uses it.
- **`irbackup.Snapshot` gains `ExtraReaders []ir.RowReader`** — the
  N-1 pre-opened coordinated readers (nil for PG / serial / Vitess).
- **Reorder in `Backup.Run`:** resolve the *requested* parallelism
  (the cheap `min(TableParallelism-or-auto, taskCount)` bounded by the
  source connection budget) **before** opening the snapshot, and pass it
  as `opts.ReaderParallelism`. (Today `resolveBackupTableParallelism`
  also takes `snapshotName` for the gate; split the requested-count
  computation out so it can run pre-open.)
- **`backupParallelEligible` accepts EITHER** capability: (a) the
  existing `snapshotName != "" && source.(SnapshotImporterOpener)` (PG
  lazy import) **OR** (b) `len(snap.ExtraReaders) > 0` (MySQL eager
  coordinated). `tableParallelism > 1` still required. Presence-driven,
  never an engine-name check.
- **`openBackupReaderFactory`:** when `snap.ExtraReaders` is non-empty,
  build the factory by seeding a buffered channel with those readers and
  popping one per `factory(ctx)` call (no `OpenSnapshotImporter` call);
  otherwise the existing importer path. The pool's free-reader primary
  (`snap.Rows`) + factory-popped peers is unchanged from ADR-0084.
- **CloseFn ownership:** the coordinated `Snapshot.CloseFn` closes
  every `R[i]` (primary + extras) and the pool. The pool must NOT
  double-close a popped extra — popped readers are owned by the snapshot
  lifecycle (mirror the PG importer-reader ownership note).

### Scope / gating

- **Vanilla MySQL only.** `Flavor.usesVStream()` (PlanetScale/Vitess)
  keeps the existing VStream COPY path untouched — `ReaderParallelism`
  is ignored there.
- **Serial remains the floor.** `ReaderParallelism <= 1`,
  FTWRL-denied, or any coordinated-open failure → today's single-reader
  path, byte-identical, loud INFO.

## Consequences

- **Closes most of the fair-fight gap.** PG's parallel path got ~3.4×
  from parallelism alone; 8 coordinated MySQL readers should land
  similarly (~251 s → an estimated ~50–80 s). Still likely shy of
  mydumper's 18 s (zstd + the IR row codec), but a major improvement
  that keeps the 2× size + restore-speed wins.
- **Adds a brief global write-stall** (the FTWRL window — only as long
  as N `START TRANSACTION`s take, typically sub-second). Today's
  single-reader open takes no lock; this is the cost of consistency
  across N readers, and it is exactly mydumper's default posture.
- **Needs `RELOAD` (or `FLUSH_TABLES`).** Managed tiers that forbid it
  fall back to serial — no regression, loud INFO.
- **`-race`-before-tag.** Parallel readers + shared coordination +
  pooled sweep is the concurrency class; the integration `-race` gate
  MUST pass before the tag (push-first-tag-after).
- **Future:** the same coordinated-snapshot capability could later
  unblock MySQL **CDC cold-start** parallelism and **within-table
  chunking** (ADR-0019), both serial today for the same reason — out of
  scope here.

## Test matrix (the implementation's behavioural oracle)

- **Consistency pin (the value-fidelity oracle — REQUIRED).** Two
  tables; engage the coordinated parallel backup but pause the sweep
  after readers are opened (a test seam); while paused, `INSERT` into
  BOTH an already-listed and a not-yet-listed table on the source;
  resume — the backup artifact must contain **neither** insert (both
  readers' snapshot predates the writes), proving the N readers share
  one consistent point. A serial backup of the same scenario is the
  control. This is the test that would catch a broken FTWRL window.
- **Parallel-engaged log + dispatch observer** fires with
  `tableParallelism > 1, reason=""` on a vanilla MySQL source with
  `ReaderParallelism > 1` and FTWRL permitted.
- **FTWRL-denied → serial fallback:** a role without `RELOAD` (or a
  failpoint) makes the coordinated open fall back to serial — dispatch
  observer sees the serial reason, backup still succeeds, checksum
  matches.
- **Zero-loss parity:** a multi-table corpus backed up with
  `ReaderParallelism=4` restores byte-equal (per-table
  COUNT==DISTINCT==content-hash) to the same corpus backed up serially.
- **Crash/resume under parallel MySQL:** kill mid-parallel-sweep →
  resume → artifact restores exactly (the ADR-0084 ≤N-partials contract
  applies unchanged; coordinated readers don't change the manifest
  shape).
- **Vitess untouched:** a PlanetScale-flavor source ignores
  `ReaderParallelism` and takes the VStream path (existing tests stay
  green).
- **Connection-budget bound:** N is bounded so N readers + 1 coordinator
  never exceed the source budget (reuse the existing bound).
