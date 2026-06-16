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
	_ irbackup.TableScopedBackupSnapshotOpener = Engine{}
	_ ir.TableScopedSnapshotOpener             = Engine{}

	// SchemaReader optional surfaces.
	_ irbackup.PositionCapturer = (*SchemaReader)(nil)
	_ ir.DiagnoseProber         = (*SchemaReader)(nil)
	_ ir.HealthReporter         = (*SchemaReader)(nil)
	_ ir.HeartbeatWriter        = (*SchemaReader)(nil)
	_ ir.MultiDatabaseScoper    = (*SchemaReader)(nil)
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
	_ ir.TableIndexedNotifier    = (*SchemaWriter)(nil)

	// RowReader optional surfaces.
	_ ir.BatchedRowReader   = (*RowReader)(nil)
	_ ir.RangeBoundsQuerier = (*RowReader)(nil)
	_ ir.RowCounter         = (*RowReader)(nil)

	// RowWriter optional surfaces.
	_ ir.BulkTableDropper            = (*RowWriter)(nil)
	_ ir.CopyDurableProgressReporter = (*RowWriter)(nil)
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

	// VStream (PlanetScale / Vitess flavor) types. The snapshot-rows
	// reader's resume surfaces are the ADR-0072 crash-resume path —
	// losing any of these silently turns a resumable COPY into a
	// start-over.
	_ ir.CDCReader               = (*vstreamCDCReader)(nil)
	_ ir.ReshardReopener         = (*vstreamCDCReader)(nil)
	_ ir.CDCReader               = (*vstreamSnapshotChanges)(nil)
	_ ir.CopyCheckpointer        = (*vstreamSnapshotRows)(nil)
	_ ir.CopyDurableProgressSink = (*vstreamSnapshotRows)(nil)
	_ ir.IdempotentCopyReader    = (*vstreamSnapshotRows)(nil)
	_ ir.MaxBufferBytesSetter    = (*vstreamSnapshotRows)(nil)

	// Migration-state store.
	_ ir.MigrationStateStore = (*MigrationStateStore)(nil)
)
