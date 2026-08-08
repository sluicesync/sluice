// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The false-refusal direction of the DDL-only-window premise
// (2026-08-07 invariant sweep, audit-backlog "PG/DDL-only-window
// empty-EndPosition"). [irbackup.Manifest.SchemaHistoryAnchors] and the
// restore/broker completeness guards all rest on one sentence — "a
// legitimate DDL-only window never presents a schema anchor at a
// position-bearing EndPosition" — and every existing pin checked the
// direction where the guard FIRES. This file checks the direction where it
// must not: a window that carried a schema snapshot and no rows.

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// ddlWindowSchemas returns the before/after source schema pair a DDL-only
// window observes (one ADD COLUMN), plus the after-shape table the reader's
// snapshot carries.
func ddlWindowSchemas() (before, after *ir.Schema) {
	before = &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	after = &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "nickname", Type: ir.Text{}},
		},
	}}}
	return before, after
}

// writeDDLWindowParent seeds a store with a completed full manifest the
// incremental can chain off, and returns it.
func writeDDLWindowParent(t *testing.T, store *blobcodec.LocalStore, schema *ir.Schema) *irbackup.Manifest {
	t.Helper()
	parent := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 8, 7, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        schema,
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
		PartialState:  irbackup.BackupStateComplete,
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)
	return parent
}

// findIncremental returns the single incremental manifest in store.
func findIncremental(t *testing.T, store *blobcodec.LocalStore) *irbackup.Manifest {
	t.Helper()
	records, err := lineage.ListAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatalf("ListAllManifestsViaWalk: %v", err)
	}
	var incr *irbackup.Manifest
	for _, r := range records {
		if r.Manifest.Kind == irbackup.BackupKindIncremental {
			incr = r.Manifest
		}
	}
	if incr == nil {
		t.Fatal("no incremental manifest written")
	}
	return incr
}

// TestIncrementalWindow_SchemaSnapshotDoesNotMoveEndPosition is the
// writer-side half of the DDL-only-window premise, in the direction nobody
// had checked.
//
// THE DEFECT IT PINS. A schema snapshot rides the manifest envelope, not the
// chunk's JSONL stream (ADR-0049 Chunk D) — but the capture loop opened a
// chunk writer on it anyway, counted it, and let it move EndPosition. A
// window whose only position-bearing event was a snapshot therefore committed
// a chunk carrying ZERO records, at an EndPosition equal to its own schema
// anchor, and `assertDataWindowEndPositionInvariant` refused the backup as "a
// data-bearing incremental window (1 change chunks)" and blamed the CDC
// reader. Loud, wrong, and unreachable for restore to fix: the same manifest
// shape is exactly what `chain_restore`'s `reachedEnd` guard cannot satisfy,
// because lastApplied comes from chunk RECORDS and no record carries that
// position. Either way it is a legitimate window turned into a failure.
//
// SCOPE, stated so it is not read as broader than it is: this drives the
// `IncrementalBackup` capture lane. The `BackupStream` rollover lane is the
// sibling and it is covered by TestRolloverWindow_SchemaSnapshotDoesNotMoveEndPosition
// below — both are graded, because the two loops carry the same code twice.
func TestIncrementalWindow_SchemaSnapshotDoesNotMoveEndPosition(t *testing.T) {
	before, after := ddlWindowSchemas()
	snapPos := ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/110"}`}
	rowPos := ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/120"}`}
	commitPos := ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/130"}`}

	snapshot := ir.SchemaSnapshot{Position: snapPos, Table: "users", IR: after.Tables[0]}
	row := ir.Insert{Position: rowPos, Table: "users", Row: ir.Row{"id": int64(1)}}
	commit := ir.TxCommit{Position: commitPos}

	for _, tc := range []struct {
		name    string
		changes []ir.Change
		// wantEnd is the EndPosition the window must record: the last
		// change the CHUNK STREAM recorded, or empty when it recorded none.
		wantEnd ir.Position
		// wantRecords is the number of change records the chunk list carries.
		wantRecords int64
	}{
		{
			// The window this test exists for: a DDL with no DML behind it.
			// Nothing reaches the chunk stream, so nothing may move
			// EndPosition — and an empty EndPosition is exactly what the
			// restore-side guards document a DDL-only window as producing.
			name:        "snapshot only",
			changes:     []ir.Change{snapshot},
			wantEnd:     ir.Position{},
			wantRecords: 0,
		},
		{
			// The snapshot is LAST. Before the fix this recorded
			// EndPosition = the anchor and was refused; the rows are real,
			// so it is a data window that must end at its last row.
			name:        "rows then a trailing snapshot",
			changes:     []ir.Change{row, commit, snapshot},
			wantEnd:     commitPos,
			wantRecords: 2,
		},
		{
			// The ordinary shape — a snapshot ahead of the rows it
			// introduces. Unchanged by the fix; here so a regression that
			// stopped advancing EndPosition at all is caught too.
			name:        "snapshot then rows",
			changes:     []ir.Change{snapshot, row, commit},
			wantEnd:     commitPos,
			wantRecords: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatalf("NewLocalStore: %v", err)
			}
			parent := writeDDLWindowParent(t, store, before)
			src := &fakeCDCEngine{
				name:              "postgres",
				schemaSequence:    []*ir.Schema{after},
				cdcChanges:        tc.changes,
				cdcExpectedFromOK: true,
			}
			now := time.Date(2026, 8, 7, 11, 0, 0, 0, time.UTC)
			b := &IncrementalBackup{
				Source: src, SourceDSN: "src", Store: store, ParentRef: parent.BackupID,
				Window: 5 * time.Minute, ChunkChanges: 10, SluiceVersion: "test",
				Now: func() time.Time { return now }, clockNow: func() time.Time { return now },
			}
			if err := b.Run(context.Background()); err != nil {
				t.Fatalf("IncrementalBackup.Run refused a legitimate window: %v", err)
			}

			incr := findIncremental(t, store)
			if incr.EndPosition != tc.wantEnd {
				t.Errorf("EndPosition = %+v; want %+v", incr.EndPosition, tc.wantEnd)
			}
			if got := manifestChangeRecordCount(incr); got != tc.wantRecords {
				t.Errorf("change records = %d; want %d", got, tc.wantRecords)
			}
			// The property every restore-side guard rests on, asserted on
			// the manifest the WRITER actually produced rather than on a
			// hand-built one: a position-bearing EndPosition is never a
			// schema anchor.
			posBearing := incr.EndPosition.Engine != "" || incr.EndPosition.Token != ""
			if posBearing && incr.SchemaHistoryAnchors(incr.EndPosition) {
				t.Errorf("the writer produced a manifest whose position-bearing EndPosition %+v IS a schema anchor — "+
					"chain_restore's reachedEnd guard can never be satisfied for it and the broker's 0-chunk guard would refuse it",
					incr.EndPosition)
			}
			// Anti-vacuity: the snapshot must actually have been recorded
			// somewhere, or this test proves nothing about snapshots.
			if len(incr.SchemaHistory) != 1 {
				t.Fatalf("SchemaHistory entries = %d; want 1 (the snapshot must ride the envelope, not vanish)", len(incr.SchemaHistory))
			}
			if incr.SchemaHistory[0].AnchorPosition != snapPos {
				t.Errorf("SchemaHistory anchor = %+v; want the snapshot's own position %+v",
					incr.SchemaHistory[0].AnchorPosition, snapPos)
			}
		})
	}
}

// TestRolloverWindow_SchemaSnapshotDoesNotMoveEndPosition is the
// `backup stream` rollover lane's half of the same property. The two capture
// loops carry the same `endPos = pos` code twice, and the sibling-sweep rule
// says a fix to one is not a fix to the class until the other is graded.
func TestRolloverWindow_SchemaSnapshotDoesNotMoveEndPosition(t *testing.T) {
	before, after := ddlWindowSchemas()
	snapPos := ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/110"}`}

	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	parent := writeDDLWindowParent(t, store, before)

	src := &fakeCDCEngine{
		name:           "postgres",
		schemaSequence: []*ir.Schema{after},
		cdcChanges: []ir.Change{
			ir.SchemaSnapshot{Position: snapPos, Table: "users", IR: after.Tables[0]},
		},
		cdcExpectedFromOK: true,
	}
	now := time.Date(2026, 8, 7, 11, 0, 0, 0, time.UTC)
	stream := &BackupStream{
		Source:          src,
		SourceDSN:       "src",
		Store:           store,
		ParentRef:       parent.BackupID,
		RolloverWindow:  5 * time.Minute, // pinned clock: never fires
		ChunkChanges:    10,
		SluiceVersion:   "test",
		Now:             func() time.Time { return now },
		clockNow:        func() time.Time { return now },
		pidHostFn:       func() (int, string) { return 12345, "test-host" },
		streamStatePath: DefaultStreamStateFilename,
	}
	if err := stream.Run(context.Background()); err != nil {
		t.Fatalf("BackupStream.Run refused a legitimate DDL-only rollover: %v", err)
	}

	incr := findIncremental(t, store)
	if len(incr.SchemaHistory) != 1 {
		t.Fatalf("SchemaHistory entries = %d; want 1 (anti-vacuity: the rollover must have seen the snapshot)", len(incr.SchemaHistory))
	}
	if got := manifestChangeRecordCount(incr); got != 0 {
		t.Errorf("change records = %d; want 0 (the snapshot rides the envelope)", got)
	}
	if incr.SchemaHistoryAnchors(incr.EndPosition) {
		t.Errorf("the rollover recorded EndPosition %+v equal to its schema anchor — "+
			"assertDataWindowEndPositionInvariant refuses that shape the moment the window also carries rows",
			incr.EndPosition)
	}
	// The rollover lane substitutes StartPosition for an EndPosition no
	// recorded change set, so the chain keeps a valid resume cursor.
	if incr.EndPosition != parent.EndPosition {
		t.Errorf("EndPosition = %+v; want the parent's %+v (a snapshot-only rollover advances nothing but must not blank the cursor)",
			incr.EndPosition, parent.EndPosition)
	}
}
