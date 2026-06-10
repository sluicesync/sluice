// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import "sluicesync.dev/sluice/internal/ir"

// Compile-time declarations of the OPTIONAL ir interfaces this
// engine's concrete types intentionally implement.
//
// Why this file exists: the orchestrator discovers optional surfaces
// by runtime type-assertion (`handle.(ir.RawCopyExporter)` etc.), so
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
	// Engine-level optional openers / probers (value type — the
	// registry holds an Engine value, see init()).
	_ ir.Engine                         = Engine{}
	_ ir.BackupSnapshotOpener           = Engine{}
	_ ir.CDCReaderWithSlotOpener        = Engine{}
	_ ir.CDCSchemaSnapshotNormalizer    = Engine{}
	_ ir.ConnectionSlotClassifier       = Engine{}
	_ ir.CrossEngineExtensionTranslator = Engine{}
	_ ir.DatabaseDSNDeriver             = Engine{}
	_ ir.DatabaseLister                 = Engine{}
	_ ir.MigrationStateStoreOpener      = Engine{}
	_ ir.MultiDatabaseSnapshotOpener    = Engine{}
	_ ir.PositionMonotonicChecker       = Engine{}
	_ ir.PositionOrderer                = Engine{}
	_ ir.ServerCDCReaderOpener          = Engine{}
	_ ir.SlotManagerOpener              = Engine{}
	_ ir.SnapshotImporterOpener         = Engine{}
	_ ir.SnapshotStreamWithSlotOpener   = Engine{}
	_ ir.TargetConnectionBudgetProber   = Engine{}
	_ ir.TargetStaleBackendReaper       = Engine{}

	// SchemaReader optional surfaces.
	_ ir.BackupPositionCapturer        = (*SchemaReader)(nil)
	_ ir.BytesLagReporter              = (*SchemaReader)(nil)
	_ ir.DiagnoseProber                = (*SchemaReader)(nil)
	_ ir.ExtensionAware                = (*SchemaReader)(nil)
	_ ir.HealthReporter                = (*SchemaReader)(nil)
	_ ir.HeartbeatWriter               = (*SchemaReader)(nil)
	_ ir.MultiDatabaseScoper           = (*SchemaReader)(nil)
	_ ir.PositionFromManifestPreflight = (*SchemaReader)(nil)
	_ ir.SampleVerifier                = (*SchemaReader)(nil)
	_ ir.SchemaSetter                  = (*SchemaReader)(nil)
	_ ir.SequenceStateReader           = (*SchemaReader)(nil)
	_ ir.SlotHealthReporter            = (*SchemaReader)(nil)
	_ ir.SlotSpillReporter             = (*SchemaReader)(nil)
	_ ir.TableScoper                   = (*SchemaReader)(nil)
	_ ir.VerbatimExtensionAware        = (*SchemaReader)(nil)
	_ ir.Verifier                      = (*SchemaReader)(nil)

	// SchemaWriter optional surfaces.
	_ ir.ColumnDDLPreviewer      = (*SchemaWriter)(nil)
	_ ir.DDLPreviewer            = (*SchemaWriter)(nil)
	_ ir.DegradedFKAllower       = (*SchemaWriter)(nil)
	_ ir.DegradedFKReporter      = (*SchemaWriter)(nil)
	_ ir.ExtensionAware          = (*SchemaWriter)(nil)
	_ ir.IncrementalIndexBuilder = (*SchemaWriter)(nil)
	_ ir.IndexBuildBudgetSetter  = (*SchemaWriter)(nil)
	_ ir.IndexBuildTuner         = (*SchemaWriter)(nil)
	_ ir.SchemaDeltaApplier      = (*SchemaWriter)(nil)
	_ ir.SchemaSetter            = (*SchemaWriter)(nil)
	_ ir.SequencePrimer          = (*SchemaWriter)(nil)
	_ ir.ShapeDeltaApplier       = (*SchemaWriter)(nil)
	_ ir.TableIndexedNotifier    = (*SchemaWriter)(nil)

	// RowReader optional surfaces (RawCopy* is the ADR-0043 raw-COPY
	// fast path; RowCountEstimator drives the parallel-copy split).
	_ ir.BatchedRowReader     = (*RowReader)(nil)
	_ ir.RangeBoundsQuerier   = (*RowReader)(nil)
	_ ir.RawCopyExporter      = (*RowReader)(nil)
	_ ir.RawCopyVersionProber = (*RowReader)(nil)
	_ ir.RowCountEstimator    = (*RowReader)(nil)
	_ ir.RowCounter           = (*RowReader)(nil)
	_ ir.SchemaSetter         = (*RowReader)(nil)

	// RowWriter optional surfaces.
	_ ir.BulkTableDropper            = (*RowWriter)(nil)
	_ ir.CopyDurableProgressReporter = (*RowWriter)(nil)
	_ ir.IdempotentCopyWriter        = (*RowWriter)(nil)
	_ ir.IdempotentRowWriter         = (*RowWriter)(nil)
	_ ir.MaxBufferBytesSetter        = (*RowWriter)(nil)
	_ ir.RawCopyImporter             = (*RowWriter)(nil)
	_ ir.RawCopyVersionProber        = (*RowWriter)(nil)
	_ ir.SchemaSetter                = (*RowWriter)(nil)
	_ ir.SchemaTypeDropper           = (*RowWriter)(nil)
	_ ir.TableDropper                = (*RowWriter)(nil)
	_ ir.TableEmptyChecker           = (*RowWriter)(nil)
	_ ir.TableTruncator              = (*RowWriter)(nil)

	// ChangeApplier optional surfaces.
	_ ir.ApplyExecTimeoutSetter         = (*ChangeApplier)(nil)
	_ ir.BatchObserverSetter            = (*ChangeApplier)(nil)
	_ ir.BatchSizeProviderSetter        = (*ChangeApplier)(nil)
	_ ir.BatchedChangeApplier           = (*ChangeApplier)(nil)
	_ ir.MaxBufferBytesSetter           = (*ChangeApplier)(nil)
	_ ir.MultiDatabaseRouter            = (*ChangeApplier)(nil)
	_ ir.PositionWriter                 = (*ChangeApplier)(nil)
	_ ir.RedactorSetter                 = (*ChangeApplier)(nil)
	_ ir.SchemaHistoryCompactor         = (*ChangeApplier)(nil)
	_ ir.SchemaHistoryReader            = (*ChangeApplier)(nil)
	_ ir.SchemaSetter                   = (*ChangeApplier)(nil)
	_ ir.ShardColumnSetter              = (*ChangeApplier)(nil)
	_ ir.ShardConsolidationLeaseDeleter = (*ChangeApplier)(nil)
	_ ir.ShardConsolidationLeaseLister  = (*ChangeApplier)(nil)
	_ ir.ShardConsolidationLeaseStore   = (*ChangeApplier)(nil)
	_ ir.ShardConsolidationProber       = (*ChangeApplier)(nil)
	_ ir.SourceFingerprintRecorder      = (*ChangeApplier)(nil)
	_ ir.StreamCleaner                  = (*ChangeApplier)(nil)
	_ ir.StreamIDSetter                 = (*ChangeApplier)(nil)

	// Logical-replication CDC reader optional surfaces.
	_ ir.CDCDatabaseScoper = (*CDCReader)(nil)

	// Slot manager, snapshot importer, migration-state store — the
	// concrete types behind the Engine-level openers above.
	_ ir.SlotManager         = (*SlotManager)(nil)
	_ ir.SnapshotImporter    = (*SnapshotImporter)(nil)
	_ ir.MigrationStateStore = (*MigrationStateStore)(nil)
)
