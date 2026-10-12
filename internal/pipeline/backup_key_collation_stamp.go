// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"sluicesync.dev/sluice/internal/ir"
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

// stampKeyCollationHistoryOnDrain is the stamp for the one segment that
// commits WITHOUT a schema delta although its window may hold DDL: the
// `backup stream` ctx-cancel drain-commit ([BackupStream.runRollover]), which
// skips the schema refresh because the source read cannot run on the
// cancelled context. Measured 2026-10-12 (review finding 4, MySQL 8.0): a
// window that changed a UNIQUE key's collation (`utf8mb4_0900_ai_ci` →
// `utf8mb4_bin`) and then inserted 'A' beside 'a' drain-committed at
// FormatVersion 1 with no SchemaDelta — so a v0.158.x binary, which has no
// key-collation guard, would replay 'A' onto the old `_ci` key and merge it
// at exit 0: the Bug 304 harm through a door the delta writers' stamp does
// not reach (TestSchemaDeltaWritersStampKeyCollation rosters writers of a
// delta; this path writes none).
//
// The evidence is the window's own SchemaHistory: each [ir.SchemaSnapshot]
// the change stream recorded, compared with before (the parent's schema,
// which the drain commits unchanged) for a key column whose collation the
// stream saw change ([migcore.TableChangesKeyCollation]). An entry that
// does not decode stamps too — the conservative direction: an older binary
// that cannot be shown the change must not read the segment.
//
// What it does not change: this binary still restores such a segment by
// replaying the window onto the old key, and its applier guard, armed from
// the same SchemaHistory, refuses the merge loudly (KEY-COLLATION-MERGE,
// measured) — filed as KEY-COLLATION-DRAIN-NO-DELTA.
func stampKeyCollationHistoryOnDrain(m *irbackup.Manifest, before *ir.Schema) {
	if m == nil || before == nil {
		return
	}
	for _, h := range m.SchemaHistory {
		if h == nil {
			continue
		}
		after, err := ir.UnmarshalTable(h.TableJSON)
		if err != nil || after == nil {
			irbackup.StampKeyCollationDelta(m, true)
			return
		}
		for _, b := range before.Tables {
			if b == nil || b.Name != h.Table || (b.Schema != "" && h.Schema != "" && b.Schema != h.Schema) {
				continue
			}
			if migcore.TableChangesKeyCollation(b, after) {
				irbackup.StampKeyCollationDelta(m, true)
				return
			}
		}
	}
}
