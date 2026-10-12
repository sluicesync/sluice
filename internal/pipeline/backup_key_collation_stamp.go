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

// stampKeyCollationHistory stamps a CDC segment from its window's own
// SchemaHistory: every [ir.SchemaSnapshot] the change stream recorded is
// compared with before (the schema the segment's parent recorded) for a key
// column whose collation the stream saw change. Called on EVERY CDC commit —
// the `backup stream` rollover and its ctx-cancel drain-commit, and
// `backup incremental` — because the delta writers' stamp
// ([stampKeyCollationDeltaOnAttach]) sees only the window's END shape:
//
//   - the drain-commit writes no delta at all (it skips the schema refresh,
//     which cannot run on the cancelled context). Measured 2026-10-12 (review
//     finding 4, MySQL 8.0): a window that changed a UNIQUE key `0900_ai_ci`
//     → `_bin` and then inserted 'A' beside 'a' committed at FormatVersion 1;
//   - a window that changes a key's collation AND BACK (A → B → A) diffs
//     empty at its end, so no delta writer stamps it, yet the rows written
//     under B replay onto the A key on an older binary and merge (review
//     2026-10-12 #2, finding 2a).
//
// A pre-v0.159.1 binary reading such a segment has no guard (before
// v0.159.0) or no delta to apply, and merges at exit 0 — the Bug 304 harm.
//
// [migcore.KeyCollationChange] decides per table. A change stream that
// records NO collation for a character key column (VStream) cannot show the
// change either way; the segment is stamped then too, conservatively —
// review 2026-10-12 #2, finding 2b: otherwise a VStream segment never could
// be. So a VStream segment whose window touched a table with a character
// key is readable only by v0.159.1+. An entry that does not decode stamps
// as well (an older binary that cannot be shown the change must not read
// the segment).
//
// What it does not change: this binary still restores a drain segment by
// replaying the window onto the old key, and its applier guard, armed from
// the same SchemaHistory, refuses the merge loudly (KEY-COLLATION-MERGE,
// measured) — filed as KEY-COLLATION-DRAIN-NO-DELTA.
func stampKeyCollationHistory(m *irbackup.Manifest, before *ir.Schema) {
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
			if changed, unknown := migcore.KeyCollationChange(b, after); changed || unknown {
				irbackup.StampKeyCollationDelta(m, true)
				return
			}
		}
	}
}
