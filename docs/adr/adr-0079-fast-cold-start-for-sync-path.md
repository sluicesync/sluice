# ADR-0079: Fast cold-start for the `sync` path (PG-source, capability-gated)

## Status

Accepted — design. Roadmap item 3d. v1 shipped v0.99.29; v1.1 (within-table chunking, CORRECTION addendum) shipped v0.99.30. Implementation notes below.

### Implementation notes (what landed)

PG-source-first, single-database. The fast path engages on a PG→PG (any
same-engine that surfaces a shareable snapshot) `sync start` cold-start;
MySQL/VStream stay serial. As-built:

- **Step 1 — `ir.SnapshotStream.SnapshotName`** (additive field, `internal/ir/snapshot.go`).
  Populated for PG in `openSnapshotStreamShared` (`cdc_snapshot.go`) from the
  already-captured `snapshotName`; the multi-DB opener inherits it via the
  shared body. MySQL/VStream leave it empty (their `OpenSnapshotStream` never
  sets it). No new interface.
- **Step 2 — shareable gate.** `rawCopyGate` now takes an engine-neutral
  `rawCopyConfig` (`migrate_raw_copy.go`); `rawCopyConfigForMigrator` /
  `rawCopyConfigForStreamer` project each orchestrator's transform fields onto
  it. Migrate behaviour is byte-identical (the existing negative matrix passes
  via the new projection); the Streamer parity matrix is pinned in
  `TestRawCopyGate_StreamerParity`.
- **Step 3 — `runColdStartParallel`** (`streamer_coldstart_parallel.go`)
  reuses `runBulkCopyPhases` directly, with two deltas confined to the
  `parallelBulkCopyDeps`: (a) a new `chunkReaderFactory` field that mints every
  parallel reader via the source's `ir.SnapshotImporter` pinned to the one
  exported snapshot (default nil ⇒ migrate's independent `OpenRowReader`, so
  the migrate path is unchanged); (b) a disabled `resumeContext` (the fast path
  is fresh-cold-start-only). The gate (`coldStartFastEligible`) is
  field/interface-presence-driven, never an engine-name check.
- **Step 4 — budget.** `resolveColdStartCopyBudget` mirrors migrate's
  chokepoint and RESERVES one slot for the CDC connection BEFORE the
  copy/index split, so `tableP × withinP + indexBudget + 1 ≤ CopyBudget`.
- **Step 5 — snapshot lifetime.** Unchanged: `stream.ReleaseRows()` still runs
  AFTER bulk-copy; the snapshot tx stays live for every parallel reader's
  `SET TRANSACTION SNAPSHOT`. The snapshot-importer pool is closed when
  `runColdStartParallel` returns.
- **Step 6 — multidb/multi-schema stays serial** (deferred). `coldStartCopyOneDatabase`
  is untouched; only single-database `coldStart` gained the fast gate.

CLI: `sync start` gained `--bulk-parallelism`, `--table-parallelism`,
`--bulk-parallel-min-rows`, `--bulk-batch-size`, `--raw-copy-format`
(`--max-target-connections` already existed; its help now notes the fast
path). All inert on non-qualifying sources.

Tests: unit — `coldStartFastEligible` matrix, `resolveColdStartCopyBudget`
CDC-reservation invariant, `openChunkReader` factory-selection, the
`rawCopyGate` Streamer parity matrix. Integration (PG, real container,
all green locally on Docker) — many-table zero-loss + index-overlap-proves-
fast-path + CDC-continues; raw-lane-taken; per-transform negative fallback
(redaction / type-override / expr-override / shard); the load-bearing
snapshot-consistency pin (post-snapshot writes fired from the dispatch seam
arrive exactly-once via CDC, gap-free checksum); MySQL serial-fallback
dispatch assertion. `-race` is CI-only (this is a concurrency chunk — the
new pool + snapshot-importer readers across goroutines + the CDC-slot
reservation); the `-race` Integration gate must pass before any tag.

### v1.1 addendum — within-table chunking re-enabled (roadmap 3d v1.1)

**Why.** The v1 fast cold-start got cross-table parallelism + index-overlap +
raw passthrough but NOT within-table PK-range chunking (ADR-0019), because the
deadlock fix that landed during the v0.99.29 release (`a8d065d`) made the
chunk-size probe (`CountRows`) return `0` for snapshot-pinned readers — so every
sync-fast-path table single-streams, and a huge table copies on one connection.

**Root cause of the original deadlock (the thing v1.1 must not reintroduce).**
`CountRows`'s exact-`COUNT(*)` fallback fired a SECOND `QueryContext` on the
table's single pinned `*sql.Conn` **while its first reltuples `Rows` was still
open** (the deferred `Close` hadn't run) — database/sql serialises a `*sql.Conn`
through `closemu`, so the second query blocked forever (goroutine-dump
confirmed). Migrate's `*sql.DB`-backed readers never hit it (each query draws a
fresh pooled conn). The v1 fix returned `0` for pinned readers to dodge it.

**Verified sequencing (the load-bearing finding).** The chunk DECISION phase
(`shouldParallelChunk` → `CountRows`, then `resolveChunks` → MIN/MAX) runs
STRICTLY BEFORE `runChunks` opens any copy stream, single-goroutine and
single-conn — each table's primary reader is owned exclusively by that table's
goroutine. So the decision probes never race an in-flight stream; the *only*
hazard was the intra-`CountRows` self-overlap above.

**Fix (conservative, two independent guarantees against the deadlock).**
(a) Make `CountRows` sequential — explicitly close the reltuples `Rows` before
the `exactCount` fallback, so two queries never overlap on a pinned conn
(a correctness fix valuable on its own). (b) Let pinned readers run the
decision-phase probes (reltuples + MIN/MAX, single sequential statements) but
**skip the exact-`COUNT(*)` seq scan** — return `reltuples`, or `0` when stats
are absent. A `0` estimate just means single-stream (= current behavior), so the
within-table win is realized only when `reltuples` is populated (the common
case). **Named limitation:** a freshly-loaded, never-`ANALYZE`d huge table on the
sync path stays single-stream until analyzed — acceptable, it equals today's
behavior and degrades to slower, never lossy. The only production change is the
two Postgres `RowReader` methods (`CountRows`, `RangeBounds`) + unifying the
"is-pinned" signal (`snapshotPinned`) across the stream reader and the importer
readers; the orchestrator, budget (`tableP × withinP` already provisioned), and
snapshot-consistency (chunk readers still minted via `SnapshotImporter`) are
unchanged. No-silent-loss: the change touches only count/range *estimation*; a
wrong estimate degrades to single-stream, never corrupts the copy. `-race`
concurrency chunk; gate before any tag.

### v1.1 addendum CORRECTION — the first attempt regressed; the as-built design

The v1.1 approach described above (relax `CountRows` for pinned readers, make it
sequential) **was implemented and REVERTED** (revert of `0ddd04e`) because it
regressed a broad set of cold-start integration tests. The above text is retained
as the *rejected* design; the paragraphs below are what actually landed.

**Why the first attempt regressed.** Relaxing `CountRows` so pinned readers run
the reltuples probe overlooked a SECOND caller of `CountRows` that runs DURING the
copy: the throughput/ETA probe (`kickOffRowCount`, `internal/pipeline/progress.go`)
fires `CountRows` on the SAME pinned `*sql.Conn` **concurrently with the in-flight
copy stream**. database/sql serialises a `*sql.Conn` through `closemu`, so the ETA
probe's query racing the live stream produced `driver: bad connection`, poisoned
the pinned conn, and the cold-start delivered 0 rows. The "decision phase runs
strictly pre-stream" reasoning was correct for the chunk-decision caller — but
`CountRows` has *two* callers and only one of them is pre-stream.

**As-built corrected design (the load-bearing change).** Keep `CountRows`
returning `0` for pinned readers EXACTLY as on `main` (the a8d065d fix) — that
keeps the ETA probe safe *by construction*, since it can never extract a non-zero
count from a pinned reader and so never queries it. Add a SEPARATE, optional IR
surface for the chunk DECISION only:

```
type RowCountEstimator interface {
    EstimateRowCount(ctx context.Context, table *Table) (int64, error)
}
```

- The orchestrator's `approximateRowCount` (the chunk-decision path, caller A)
  type-asserts `RowCountEstimator` FIRST and prefers it; it falls back to
  `RowCounter.CountRows` only when the reader doesn't implement the estimator.
  `kickOffRowCount` (caller B, the ETA path) is UNTOUCHED — it still uses
  `CountRows`, which still returns 0 for pinned readers. The two callers are now
  cleanly separated.
- PG's `EstimateRowCount`: on a **pinned** reader (the snapshot stream reader,
  `closer == nil`, OR a `SnapshotImporter` reader, `snapshotPinned`) it runs the
  `pg_class.reltuples` lookup on a **FRESH off-snapshot connection** opened from a
  new `estimatorDSN` field (open, query, defer-close). reltuples is
  snapshot-insensitive catalog metadata, so the off-conn read is correct, and a
  fresh conn cannot race the pinned reader's in-flight stream — closing the
  connection-conflict hole the first attempt opened. The exact-`COUNT(*)` fallback
  is DECLINED on a pinned reader (cost: that seq scan would run on the live
  snapshot conn). On a **non-pinned** migrate reader it queries `r.q` and KEEPS the
  exact-`COUNT(*)` fallback, so migrate's chunk decision is byte-identical
  (ADR-0042 N1 preserved).
- `RangeBounds` drops the `closer == nil` refusal (verified pre-stream-only:
  `resolveChunks` → `computeChunkBoundaries` precedes `runChunks`) and is a single
  fully-closed statement. `estimatorDSN` is threaded onto the stream reader
  (`cdc_snapshot.go`) and the importer readers (`snapshot_importer.go`); both
  qualify by a shared `effectiveSchema(table)` helper (a correctness fix for the
  multi-schema spanning reader, no-op for single-schema).

**Named limitation (unchanged):** a never-`ANALYZE`d huge table on the sync path
reports the reltuples sentinel and stays single-stream — equals today's behavior,
degrades to slower, never lossy. **No-silent-loss:** the change touches only the
chunk-decision *estimate*; a wrong estimate degrades to single-stream, never
corrupts the copy. **Cost note:** the pinned-reader estimate opens one short-lived
connection per table at decision time (off the snapshot, pre-stream) — acceptable
for the chunk decision; it is never on the per-row or ETA path. `-race` concurrency
chunk; the `-race` Integration gate must pass before any tag.

## Context

The three cold-start copy speedups — the cross-table worker pool (ADR-0076), index-build overlap (ADR-0077), and the PG→PG raw-copy identity passthrough (ADR-0078) — are **`migrate`-only**. `sluice sync start`'s initial cold-start calls the serial `runBulkCopyWithOpts` (`internal/pipeline/migrate.go:1116`), so the **copy-then-continuously-follow** workflow — the one-command equivalent of pgcopydb's `--follow` — does its initial copy on the slow path. A user who wants *both* a fast initial copy *and* continuous CDC currently cannot get the fast copy: `migrate` is fast but one-shot; `sync start` follows but copies serially. pgcopydb gives fast-copy + follow together; sluice should too.

The sync cold-start is serial "by design" (`migrate.go:1159-1166`) for three coupled reasons, each verified in code:

1. **Durable-write watermark** (v0.99.9, `migrate.go:1141-1158`): on the resumable VStream cold-start the reader's `CopyDurableProgressSink` is coupled to the writer's `CopyDurableProgressReporter` so the COPY checkpoint never advances past durably-committed rows. The VStream pump advances its `TablePKs` cursor as rows are *received* into the in-flight buffer, ahead of the consumer — a hard crash resuming past un-written rows is the silent-loss class this prevents.
2. **Idempotent COPY** (Bug 125, `migrate.go:1137-1140`): the VStream/PlanetScale snapshot reader re-emits COPY-phase rows out of PK order; `ir.IdempotentCopyReader.CopyNeedsIdempotentWriter()` routes to the upsert path.
3. **Single ordered snapshot stream**: sync cold-start consumes ONE `ir.RowReader` (`stream.Rows`) emitting all tables in one sequence (for VStream, literally one gRPC stream tied to the CDC start position). `migrate` instead opens INDEPENDENT per-table / per-PK-chunk readers — which is exactly why migrate parallelizes and sync doesn't.

The crucial asymmetry is the **source's snapshot model**. Postgres exports a snapshot (`OpenSnapshotStream` → `consistent_point` + `snapshotName`) that can be SHARED across many connections via `SET TRANSACTION SNAPSHOT '<name>'` — pgcopydb's exact parallel-worker mechanism. MySQL's `WITH CONSISTENT SNAPSHOT` is per-session and not shareable; the Vitess VStream snapshot is a single ordered stream with the durable-watermark + idempotent coupling above.

Two facts discovered during design framing change the calculus:

- **`ir.SnapshotImporter` / `SnapshotImporterOpener` already exist** (`internal/ir/interfaces.go`) with a working PG implementation (`internal/engines/postgres/snapshot_importer.go`) that does `BEGIN REPEATABLE READ; SET TRANSACTION SNAPSHOT` — but have **zero production callers**. Even `migrate` doesn't use them (its parallel chunks open independent `OpenRowReader`s, accepting the documented ADR-0019 v1 inconsistency window). This chunk finally wires the latent surface in.
- **`migrate` persists no CDC position and creates no replication slot** — it opens `OpenRowReader`, never `OpenSnapshotStream`. So a `migrate`→`sync` position handoff (shape B) would require net-new slot-creation + position-persistence + keeping the slot alive across the inter-command gap + a new gap-free boundary proof. That is a bigger lift than the roadmap assumed, for a worse two-command UX.

## Decision

Implement **shape (A): bring the fast machinery to the sync cold-start, PG-source first, behind a source-capability gate.** Fast where provably safe; the existing serial path stays the loud, correct fallback everywhere else.

**The capability gate.** A source qualifies for the parallel/overlap/raw cold-start iff its `ir.SnapshotStream` carries a non-empty shareable exported-snapshot name (new additive field `SnapshotName`) AND the source engine implements `ir.SnapshotImporterOpener`. Postgres qualifies; MySQL and VStream/PlanetScale do not, so they fall through to today's `runBulkCopyWithOpts` with an INFO log naming the reason. The gate is interface-/field-presence-driven, never an engine-name check (the IR-first tenet).

**The key risk reduction:** the durable-watermark + idempotent-COPY coupling (constraints 1+2) lives ONLY on the VStream path, which stays serial. The raw byte-pipe (PG-only) and the watermark (VStream-only) therefore **never coexist** — this chunk never has to make them compatible.

**Scope of the fast path (when the gate holds — PG→PG, fresh cold-start, no transform):**
- Parallel cross-table pool + within-table chunks, readers minted via `ir.SnapshotImporter` so all N are pinned to the ONE exported snapshot (gap-free, the slot's `consistent_point` is the handoff boundary).
- Index-build overlap (PG writer implements `ir.IncrementalIndexBuilder`).
- Raw-copy passthrough, engaging only on a fresh / `--restart-from-scratch` cold-start (degrading to IR on the resumable path, mirroring the migrate `useFastLoader` gate), behind the SAME value-fidelity gate as migrate.

**Shared value-fidelity gate.** `rawCopyGate` is refactored off `*Migrator` onto a small transform-config struct that both `Migrator` and `Streamer` populate, so the no-transform guarantee (no redaction / type-override / expr-override / shard-injection) is byte-identical on both paths, with the same negative-fallback pins.

**Connection budget.** The sync cold-start resolves a real copy/index budget (mirroring migrate) instead of the current `requested=1`, with ONE extra reservation migrate doesn't need: 1 slot held for the CDC connection that goes live immediately after bulk-copy, so the pool can't starve it. The product `tableP × withinP + indexBudget + 1(CDC) ≤ CopyBudget` holds at the single chokepoint (ADR-0076 discipline).

**Resume stays serial.** The resumable cold-start path (ADR-0072, the VStream-cursor + durable-watermark path) is excluded from the gate and keeps `runBulkCopyWithOpts` unchanged.

## Consequences

- A PG→PG `sluice sync start` cold-start now copies at `migrate` speed and then follows — the one-command pgcopydb-`--follow`-at-full-speed equivalent. Every other source (MySQL, VStream/PlanetScale) is byte-identical to before: serial cold-start, loudly logged.
- The latent `ir.SnapshotImporter` surface gets its first production caller; PG cold-start parallel reads are now snapshot-CONSISTENT (all readers pinned to one exported snapshot), strictly better than migrate's current per-connection-snapshot v1 window.
- One auditable value-fidelity gate now governs both migrate and sync; the raw lane can never silently skip a transform on either path.
- `-race` concurrency chunk (new pool on the critical sync path + snapshot-importer readers across goroutines + the CDC-slot reservation). The `-race` Integration gate must pass before any release tag (CLAUDE.md release discipline).

## Alternatives considered

- **Shape (B): `migrate`→`sync` position handoff.** Rejected for v1 — migrate persists no position and holds no slot today, so (B) needs slot-creation + position-persistence + a slot-kept-alive-across-the-gap + a brand-new gap-free boundary proof (the v0.99.16 silent-loss class), and lands a two-command UX. (A) reuses the already-proven `OpenSnapshotStream` boundary. (B) remains a worthwhile *separate* primitive for a "migrate once, follow much later" workflow; deferred.
- **Parallelize the VStream/MySQL cold-start too.** Rejected — MySQL per-session snapshots aren't shareable (N independent `WITH CONSISTENT SNAPSHOT` txns = N inconsistent views, unsafe on a live source); the VStream single-stream + durable-watermark + idempotent path is the delicate coupling this ADR deliberately leaves untouched. The capability gate exists precisely to leave them serial.
- **Make the raw byte-pipe checkpoint-compatible with the durable watermark.** Unnecessary — they never coexist (raw is PG-fresh-cold-start-only; the watermark is VStream-only). Avoiding this is the central risk reduction.
- **Multi-database / multi-schema PG sync cold-start parallel in v1.** Deferred to a fast-follow (v1.1) — the spanning DB-wide snapshot is also shareable so the same trick applies, but single-database PG validates first.
