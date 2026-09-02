// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// TestNearestAncestorPosition pins the resume rule for a chain extension
// whose parent link recorded no EndPosition (a quiet or DDL-only window):
// resume from the nearest ancestor that did. Before v0.138.0 such a parent
// fell into the legacy "start from the source's current position" branch,
// which silently skipped every change between the link's real position and
// now — the arm that made a stalled foreign VStream resume SILENT rather
// than slow (audit 2026-09-01 SLM-2, measured on the cluster rig).
func TestNearestAncestorPosition(t *testing.T) {
	t.Parallel()
	pos := func(lsn string) ir.Position {
		return ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"` + lsn + `"}`}
	}
	full := &irbackup.Manifest{BackupID: "full1", Kind: irbackup.BackupKindFull, EndPosition: pos("0/100")}
	inc1 := &irbackup.Manifest{BackupID: "inc1", Kind: irbackup.BackupKindIncremental, ParentBackupID: "full1", EndPosition: pos("0/200")}
	quiet := &irbackup.Manifest{BackupID: "quiet", Kind: irbackup.BackupKindIncremental, ParentBackupID: "inc1"}
	quiet2 := &irbackup.Manifest{BackupID: "quiet2", Kind: irbackup.BackupKindIncremental, ParentBackupID: "quiet"}
	legacyFull := &irbackup.Manifest{BackupID: "legacy", Kind: irbackup.BackupKindFull}
	quietOnLegacy := &irbackup.Manifest{BackupID: "q3", Kind: irbackup.BackupKindIncremental, ParentBackupID: "legacy"}

	chainOf := func(ms ...*irbackup.Manifest) []lineage.SegmentRecord {
		out := make([]lineage.SegmentRecord, 0, len(ms))
		for _, m := range ms {
			out = append(out, lineage.SegmentRecord{ManifestRecord: lineage.ManifestRecord{Manifest: m}})
		}
		return out
	}

	for _, tc := range []struct {
		name         string
		chain        []lineage.SegmentRecord
		parent       string
		wantPos      ir.Position
		wantAncestor string
		wantOK       bool
	}{
		{"one quiet link resumes from the link before it", chainOf(full, inc1, quiet), "quiet", pos("0/200"), "inc1", true},
		{"two quiet links in a row walk back past both", chainOf(full, inc1, quiet, quiet2), "quiet2", pos("0/200"), "inc1", true},
		{"a quiet link directly on the full resumes from the full's end", chainOf(full, quiet), "quiet", pos("0/100"), "full1", true},
		{"a quiet link on a legacy full with no position has nowhere to go", chainOf(legacyFull, quietOnLegacy), "q3", ir.Position{}, "", false},
		{"a parent that is not in the chain has nowhere to go", chainOf(full, inc1), "ghost", ir.Position{}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, anc, ok := nearestAncestorPosition(tc.chain, tc.parent)
			if ok != tc.wantOK || anc != tc.wantAncestor || got != tc.wantPos {
				t.Fatalf("nearestAncestorPosition(%s) = (%+v, %q, %v), want (%+v, %q, %v)", tc.parent, got, anc, ok, tc.wantPos, tc.wantAncestor, tc.wantOK)
			}
		})
	}
}
