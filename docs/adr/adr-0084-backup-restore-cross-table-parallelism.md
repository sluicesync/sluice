# ADR-0084: Cross-table parallelism for backup (and, forthcoming, restore)

- **Status:** Accepted (backup side implemented; task #39 PR 1 — the
  restore side is PR 2, forthcoming)
- **Date:** 2026-06-10
- **Relates to:** ADR-0076 (cross-table copy worker pool), ADR-0079
  (fast parallel cold-start for the sync path; the capability-gate +
  snapshot-importer pattern this reuses), ADR-0083 (chain provisioning;
  landed in the same backup-benchmark arc)

## Context

The 2026-06-10 backup benchmark (133 GB / 422M rows / 43 tables,
Postgres source) measured `sluice backup full` at **2367 s** against
`pg_dump -Fd -j8` at **232 s** — a 10.2× gap. pg_dump's own j1→j8
delta on the same corpus (798 s → 232 s) shows **~3.4× of that gap is
pure cross-table parallelism**: sluice's backup orchestrator streamed
tables strictly serially on the snapshot's one pinned reader, leaving
the source's cores and the store's upload bandwidth idle between
tables. The gap was named in code ("Phase 2 will add parallel reads —
same shape as the parallel bulk-copy path").

The machinery to close it already exists on the migrate/sync side:

- **ADR-0076** built the bounded cross-table `errgroup` pool with the
  free-pair 1-slot channel and the single-chokepoint connection
  budget.
- **ADR-0079** built the capability gate for "may parallel readers
  share the source's consistent view?": a SHAREABLE exported snapshot
  (Postgres `CREATE_REPLICATION_SLOT … EXPORT_SNAPSHOT` →
  `SET TRANSACTION SNAPSHOT` on N connections) surfaced as a
  `SnapshotName` field + the `ir.SnapshotImporter` surface, with
  MySQL's per-session snapshot excluded by construction.

What backup adds that migrate didn't have: the **manifest** is a
single mutable artifact that every table's worker checkpoints into
(per-chunk and per-table, Bug 34b), and the resume classifier reads it
back after a crash. Concurrency must not corrupt it, reorder it, or
change the resume contract.

## Decision

### 1. Same pool shape, gated on the shareable snapshot — never an engine name

`internal/pipeline/backup_table_pool.go` mirrors
`migrate_table_pool.go`: a bounded `errgroup` (one goroutine per
table, `SetLimit(tableParallelism)`, first error cancels peers) with a
**free-reader 1-slot channel** — the snapshot's already-pinned reader
is claimed by one in-flight table; peers mint dedicated readers via
`ir.SnapshotImporter.ImportSnapshot(snapshotName, 1)`, every reader
pinned to the SAME exported snapshot, so cross-table consistency is
byte-identical to the serial sweep.

The gate (`backupParallelEligible`) is the ADR-0079 pattern: it
requires a non-empty `ir.BackupSnapshot.SnapshotName` (new field,
mirroring `SnapshotStream.SnapshotName`; Postgres populates it from
EXPORT_SNAPSHOT) AND `ir.SnapshotImporterOpener` on the source AND
`tableParallelism > 1`. **MySQL backups stay sequential** (per-session
snapshot, no shareable name) and the **v0.17.x non-snapshot fallback
stays sequential** (no snapshot at all); each non-engaged case logs a
loud INFO naming the reason. Not-eligible collapses to
`tableParallelism = 1` through the SAME pool function — one code path,
exactly like `runBulkCopyTablePool`.

Knob: `backup full --table-parallelism` (pipeline field
`Backup.TableParallelism`), 0 = auto = 4 (pgcopydb `--table-jobs`
parity, the same default as migrate's), 1 = serial. The resolved value
is bounded at the single pre-pool chokepoint by the SOURCE's measured
connection budget (`resolveTargetCopyParallelism` against the source
DSN), **reserving one slot for the snapshot's slot-creation
replication conn** (the ADR-0079 CDC-conn reservation pattern — that
conn stays open for the whole sweep to keep the exported snapshot
valid), and clamped to the number of tables actually being swept.

### 2. Manifest determinism via pre-staging; one mutex for all manifest writes

All table entries are **pre-staged into `manifest.Tables` in schema
order before the pool starts** (prior-complete entries verbatim; fresh
`Partial=true` placeholders for the rest). Workers fill in their own
entry through a `manifestCommitter` whose single mutex serializes
**every entry mutation + the manifest marshal + the same-key
`manifest.json` Put** — the marshal reads every worker's entry, so
mutating outside the lock would race it, and same-key Puts must be
serialized on stores without atomic rename. Data-plane chunk Puts
(distinct keys) stay outside the lock.

Result: manifest table order == schema order regardless of completion
order; a serial and a parallel run over the same idle source produce
manifests equal modulo CreatedAt / BackupID / EndPosition (pinned by
integration test).

### 3. Crash/resume contract under concurrency

A crashed parallel backup leaves at most `tableParallelism` tables
with `Partial=true` and a per-chunk-accurate chunk list (the in-flight
workers, each checkpointing after every chunk), plus the pre-staged
not-yet-started entries (`Partial=true`, zero chunks). The existing
resume classifier (`tableManifestFullyComplete`) already handles all
three states — `Partial=true` routes to the per-chunk resume path, and
a zero-chunk entry simply re-streams from scratch. **The one
observable change:** the crashed manifest now lists every staged table
(previously only tables already started appeared); the resume-shape
unit test was updated to pin the new shape.

### 4. Thread-safety audit (verified, not assumed)

- `chunkWriter` is per-chunk-local inside each worker — no sharing.
- `redact.Registry` concurrent use has the migrate-pool precedent
  (shared across table goroutines since ADR-0076).
- Envelope `WrapCEK` under per-chunk-mode concurrency: all four
  implementations (Passphrase, AWS/GCP/Azure KMS) are read-only on
  envelope state (`e.kek` / SDK clients, which are goroutine-safe) and
  `crypto/rand` + `EncryptChunk` are per-call — no mutex needed.
  `rebindForChain` mutates the envelope but only pre-pool
  (`setupChainEncryption`).
- Importer-minted readers are single-schema, bound to the DSN's
  schema — the SAME binding the snapshot's primary backup reader
  carries (`&RowReader{q: conn, schema: cfg.schema}`), so the parallel
  sweep reads exactly what the serial sweep would. (PG backups are
  single-schema today on both paths; the ADR-0075 multi-schema
  spanning reader exists only on the sync cold-start path and never
  reaches the backup orchestrator.)

### Measured expectation

pg_dump's j1→j8 = 3.4× on the motivating corpus is the ceiling for
this chunk; the remaining gap (format/encoding work per row — JSONL +
gzip/zstd vs pg_dump's COPY format) is separate work. To be
re-benchmarked after PR 2 (restore side) lands.

## Alternatives rejected

- **A dedicated commit goroutine** (workers send manifest deltas over
  a channel; one goroutine owns the manifest). More moving parts for
  the same serialization guarantee; error propagation from a failed
  checkpoint back to the owning worker becomes indirect. The mutex is
  smaller and keeps the failure attribution (worker X's checkpoint
  failed → table X's error) exact.
- **Parallelising the v0.17.x non-snapshot fallback** (N independent
  `OpenRowReader` connections). Each reader would observe a different
  point in time — the fallback already carries a documented
  write-window gap, but cross-table *skew* on top of it is a new
  inconsistency class for zero benefit on the engines that matter (PG
  has the snapshot path; the fallback exists for degraded PG only).
- **MySQL multi-snapshot parallelism** (N `START TRANSACTION WITH
  CONSISTENT SNAPSHOT` sessions). N independent snapshots ≠ one
  consistent view; the binlog catch-up that bounds this window on the
  sync path has no counterpart inside a one-shot full backup. Serial
  is the correct MySQL shape until something like a FTWRL-coordinated
  multi-session snapshot is deemed worth its locking cost.
- **Restore-side parallelism in this PR.** Deliberately split (PR 2):
  restore writes through a different orchestrator with its own
  ordering constraints (schema → rows → indexes/constraints) and
  deserves its own review.
