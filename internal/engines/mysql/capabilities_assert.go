// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// Compile-time declarations of the OPTIONAL ir interfaces this
// engine's concrete types intentionally implement.
//
// Why this file exists: the orchestrator discovers optional surfaces
// by runtime type-assertion (`handle.(ir.CopyCheckpointer)` etc.), so
// a method-set break — a signature change, a renamed method, a
// receiver flipped between value and pointer — doesn't fail the build;
// it makes the assertion quietly stop matching and the pipeline
// SILENTLY downgrades to the fallback path (slower copy, lost resume
// position, skipped preflight). The blank-var assertions below turn
// that silent downgrade into a compile error in this package.
//
// When removing an interface from a type ON PURPOSE, delete its line
// here in the same commit and call out the downgrade in the commit
// message.
var (
	// Engine-level optional openers / orderers (value type — the
	// registry holds Engine values, see init()).
	_ ir.Engine                                = Engine{}
	_ ir.CollationResolverProvider             = Engine{}
	_ irbackup.SnapshotOpener                  = Engine{}
	_ ir.CDCSchemaSnapshotNormalizer           = Engine{}
	_ ir.DatabaseDSNDeriver                    = Engine{}
	_ ir.DatabaseLister                        = Engine{}
	_ ir.DefaultTableExcluder                  = Engine{}
	_ ir.MigrationStateStoreOpener             = Engine{}
	_ ir.MultiDatabaseSnapshotOpener           = Engine{}
	_ ir.NamespaceFolder                       = Engine{}
	_ ir.PositionOrderer                       = Engine{}
	_ ir.ServerCDCReaderOpener                 = Engine{}
	_ ir.ShardDiscoverer                       = Engine{}
	_ ir.SnapshotStreamResumer                 = Engine{}
	_ ir.TargetConnectionBudgetProber          = Engine{}
	_ irbackup.TableScopedBackupSnapshotOpener = Engine{}
	_ ir.TableScopedSnapshotOpener             = Engine{}
	// ADR-0174 Piece 2 — continuous filtered sync (`sync --where`) pushes
	// the predicate into the VStream COPY at OPEN time (a post-open
	// RowFilterSetter is too late for the eager COPY). A method-set drift
	// here would silently drop the filtered-open dispatch and leave the
	// cold-start COPY unfiltered — a silent leak of out-of-scope rows.
	_ ir.FilteredSnapshotOpener  = Engine{}
	_ ir.FilteredSnapshotResumer = Engine{}

	// SchemaReader optional surfaces.
	_ irbackup.PositionCapturer = (*SchemaReader)(nil)
	_ ir.DiagnoseProber         = (*SchemaReader)(nil)
	_ ir.HealthReporter         = (*SchemaReader)(nil)
	_ ir.HeartbeatWriter        = (*SchemaReader)(nil)
	_ ir.MultiDatabaseScoper    = (*SchemaReader)(nil)
	_ ir.RowFilterSetter        = (*SchemaReader)(nil)
	_ ir.SampleVerifier         = (*SchemaReader)(nil)
	_ ir.SequenceStateReader    = (*SchemaReader)(nil)
	_ ir.Verifier               = (*SchemaReader)(nil)

	// SchemaWriter optional surfaces.
	_ ir.ColumnDDLPreviewer      = (*SchemaWriter)(nil)
	_ ir.DDLPreviewer            = (*SchemaWriter)(nil)
	_ ir.IncrementalIndexBuilder = (*SchemaWriter)(nil)
	_ ir.IndexBuildBudgetSetter  = (*SchemaWriter)(nil)
	_ ir.SchemaDeltaApplier      = (*SchemaWriter)(nil)
	_ ir.SequencePrimer          = (*SchemaWriter)(nil)
	_ ir.ShapeDeltaApplier       = (*SchemaWriter)(nil)
	_ ir.TableAnalyzer           = (*SchemaWriter)(nil)
	_ ir.TableIndexedNotifier    = (*SchemaWriter)(nil)

	// RowReader optional surfaces.
	_ ir.BatchedRowReader   = (*RowReader)(nil)
	_ ir.KeysetSampler      = (*RowReader)(nil)
	_ ir.RangeBoundsQuerier = (*RowReader)(nil)
	_ ir.RowCounter         = (*RowReader)(nil)
	_ ir.RowFilterSetter    = (*RowReader)(nil)

	// RowWriter optional surfaces.
	_ ir.BulkTableDropper            = (*RowWriter)(nil)
	_ ir.CopyDurableProgressReporter = (*RowWriter)(nil)
	_ ir.FloatRepairWriter           = (*RowWriter)(nil) // cold-start FLOAT re-read repair (ADR-0153; audit ARCH-F1)
	_ ir.IdempotentCopyWriter        = (*RowWriter)(nil)
	_ ir.IdempotentRowWriter         = (*RowWriter)(nil)
	_ ir.MaxBufferBytesSetter        = (*RowWriter)(nil)
	_ ir.TableDropper                = (*RowWriter)(nil)
	_ ir.TableEmptyChecker           = (*RowWriter)(nil)
	_ ir.TableTruncator              = (*RowWriter)(nil)

	// ChangeApplier optional surfaces.
	_ ir.ApplyExecTimeoutSetter         = (*ChangeApplier)(nil)
	_ ir.BatchObserverSetter            = (*ChangeApplier)(nil)
	_ ir.BatchSizeProviderSetter        = (*ChangeApplier)(nil)
	_ ir.ApplyConcurrencySetter         = (*ChangeApplier)(nil)
	_ ir.LaneAIMDSetter                 = (*ChangeApplier)(nil)
	_ ir.BatchedChangeApplier           = (*ChangeApplier)(nil)
	_ ir.MaxBufferBytesSetter           = (*ChangeApplier)(nil)
	_ ir.MultiDatabaseRouter            = (*ChangeApplier)(nil)
	_ ir.PositionWriter                 = (*ChangeApplier)(nil)
	_ ir.RedactorSetter                 = (*ChangeApplier)(nil)
	_ ir.SchemaHistoryCompactor         = (*ChangeApplier)(nil)
	_ ir.SchemaHistoryReader            = (*ChangeApplier)(nil)
	_ ir.ShardColumnSetter              = (*ChangeApplier)(nil)
	_ ir.ShardConsolidationLeaseDeleter = (*ChangeApplier)(nil)
	_ ir.ShardConsolidationLeaseLister  = (*ChangeApplier)(nil)
	_ ir.ShardConsolidationLeaseStore   = (*ChangeApplier)(nil)
	_ ir.ShardConsolidationProber       = (*ChangeApplier)(nil)
	_ ir.SourceFingerprintRecorder      = (*ChangeApplier)(nil)
	_ ir.StreamCleaner                  = (*ChangeApplier)(nil)
	_ ir.StreamIDSetter                 = (*ChangeApplier)(nil)

	// Binlog CDC reader optional surfaces.
	_ ir.CDCDatabaseScoper = (*CDCReader)(nil)
	// FullBeforeImageSetter backs `sync --where` (ADR-0173 Phase 2): the
	// pipeline type-asserts the binlog reader onto it to request un-narrowed
	// before-images for filtered tables. A rename / re-signature would compile
	// green while flipping every filtered MySQL sync to a runtime
	// "cannot emit full row before-images" refuse (audit 2026-07-18 M-A2). The
	// VStream readers below carry it too (no-op there — VStream filters
	// server-side — but pinned so the surface stays uniform across the flavor).
	_ ir.FullBeforeImageSetter = (*CDCReader)(nil)

	// VStream (PlanetScale / Vitess flavor) types. The snapshot-rows
	// reader's resume surfaces are the ADR-0072 crash-resume path —
	// losing any of these silently turns a resumable COPY into a
	// start-over.
	_ ir.CDCReader             = (*vstreamCDCReader)(nil)
	_ ir.ReshardReopener       = (*vstreamCDCReader)(nil)
	_ ir.FullBeforeImageSetter = (*vstreamCDCReader)(nil)
	// ADR-0174 Piece 2 / audit F-P1 — the warm-resume server-side filter push-
	// down. A drift here silently reverts warm resume to the full-keyspace
	// unfiltered stream (a perf regression) while go build stays green.
	_ ir.ServerSideCDCFilterSetter = (*vstreamCDCReader)(nil)
	_ ir.CDCReader                 = (*vstreamSnapshotChanges)(nil)
	_ ir.ReshardReopener           = (*vstreamSnapshotChanges)(nil)
	_ ir.FullBeforeImageSetter     = (*vstreamSnapshotChanges)(nil)
	_ ir.CopyCheckpointer          = (*vstreamSnapshotRows)(nil)
	_ ir.CopyDurableProgressSink   = (*vstreamSnapshotRows)(nil)
	_ ir.IdempotentCopyReader      = (*vstreamSnapshotRows)(nil)
	// LossyFloatCopyReader signals the VStream COPY phase rounds FLOATs
	// (the 17-year MySQL display-rounding bug), which TRIGGERS the
	// cold-start FLOAT repair. Dispatched by runtime type-assertion at
	// backup.go / streamer_coldstart_float_repair.go; a drift here would
	// silently skip the repair, shipping rounded floats (audit ARCH-F1).
	_ ir.LossyFloatCopyReader = (*vstreamSnapshotRows)(nil)
	_ ir.MaxBufferBytesSetter = (*vstreamSnapshotRows)(nil)
	// ADR-0174 Piece 2 — the RowFilterSetter capability gate the pipeline
	// runs on the cold-start snapshot Rows (the actual push-down happens at
	// open via FilteredSnapshotOpener; this satisfies the gate).
	_ ir.RowFilterSetter = (*vstreamSnapshotRows)(nil)

	// Migration-state store.
	_ ir.MigrationStateStore = (*MigrationStateStore)(nil)

	// audit-2026-07-11 M-3: finish the ARCH-F1 sweep — runtime-dispatched
	// optional surfaces that were implemented but unpinned, so a method-set
	// drift would silently disable them. The IndexVerifier / DSNValidator pair
	// are the silent-loss-adjacent ones (a drift disables the SLUICE-E-INDEX-
	// MISSING net / the driver-host-mismatch preflight refusal); the rest are
	// perf/telemetry surfaces that would silently degrade.
	_ ir.DSNValidator              = Engine{}
	_ ir.SourceHostAdvisor         = Engine{}
	_ ir.SourceProbedAdvisor       = Engine{}
	_ ir.IndexVerifier             = (*SchemaWriter)(nil)
	_ ir.TransientClassifier       = (*SchemaWriter)(nil)
	_ ir.GrowGateSetter            = (*RowWriter)(nil)
	_ ir.ReparentObserverSetter    = (*RowWriter)(nil)
	_ ir.ParallelCopyWriter        = (*RowWriter)(nil)
	_ ir.TargetMetricsHistoryStore = (*ChangeApplier)(nil)
	_ ir.ConcurrentCopyPartitioner = (*concurrentBinlogRows)(nil)
	_ ir.WorkStealingCopyReader    = (*concurrentBinlogRows)(nil)
	_ ir.ConcurrentCopyPartitioner = (*vstreamSnapshotRows)(nil)

	// audit-2026-07-15 M3.4 / carried MED-3: the last two unpinned
	// runtime-dispatched surfaces this engine implements.
	// ControlTableDDLProvider backs `sluice control-tables ddl` — a drift
	// degrades it to the runtime "engine does not provide control-table
	// DDL" refusal, breaking the documented PlanetScale safe-migrations
	// bootstrap recipe (deploy-request path, ADR-0162).
	// ParallelIdempotentCopyWriter is the ADR-0097 cold-start COPY write
	// fan-out — a drift silently collapses the N-connection apply to the
	// serial path (a large perf downgrade, no correctness signal at all).
	_ ir.ControlTableDDLProvider      = Engine{}
	_ ir.ParallelIdempotentCopyWriter = (*RowWriter)(nil)
)
