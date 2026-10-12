// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// stampKeyCollationDeltaOnAttach raises the segment's format version to
// [irbackup.FormatVersionKeyCollationDelta] when the schema delta just
// attached changes a key column's collation, so a pre-v0.159.1 binary —
// whose diff reads that entry as no change and replays the window onto
// the old key, merging rows the source keeps apart (Bug 304) — refuses the
// segment at its version ceiling instead of restoring it at exit 0.
//
// Called by both delta writers: [IncrementalBackup.attachWindowEndSchema]
// (`backup incremental`) and [BackupStream.refreshSchemaAndAttachDelta]
// (`backup stream` rollovers). They are the only two sites that set
// [irbackup.Manifest.SchemaDelta]; the fill lane and the broker read it,
// and smart compaction rewrites a manifest under its recorded version.
//
// Safe after the window's chunks sealed: an encrypted segment is already
// at the injective-AAD tier, and none of the version folds change between
// that tier and this one. Both callers run before [irbackup.ComputeBackupID].
func stampKeyCollationDeltaOnAttach(m *irbackup.Manifest) {
	irbackup.StampKeyCollationDelta(m, migcore.DeltaChangesKeyCollation(m.SchemaDelta))
}
