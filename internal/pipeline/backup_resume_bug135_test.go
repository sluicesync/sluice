// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The Bug 135 class pin: resuming an interrupted backup MUST produce a
// correct artifact even when the resume run's row stream delivers the
// SAME rows in a DIFFERENT order than the interrupted run did.
//
// The reader has never guaranteed repeatable order ([buildSelect] has
// no ORDER BY — scan order is whatever the heap delivers), and the
// retired per-chunk resume reuse assumed it anyway: it kept prior
// chunk N verbatim and discarded N×chunkRows rows from the NEW stream,
// silently producing duplicate + missing rows whenever the two orders
// diverged. v0.99.34's serial single-connection world kept the orders
// accidentally stable; the ADR-0084 parallel sweep broke the accident
// reliably (the v0.99.35 battle-test caught it: 1.5M rows emitted /
// 800k distinct, exit 0). The deterministic test fakes are exactly why
// the original resume pin missed it — identical fake order made the
// reuse indistinguishable from a re-stream. This pin makes the order
// divergence explicit.

package pipeline

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestBackup_ResumeAfterScanOrderChange_NoDuplicatesNoHoles(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	schema := &ir.Schema{
		Tables: []*ir.Table{{
			Name:    "events",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}},
	}
	rowsAsc := make([]ir.Row, 0, 7)
	rowsDesc := make([]ir.Row, 0, 7)
	for i := 1; i <= 7; i++ {
		rowsAsc = append(rowsAsc, ir.Row{"id": int64(i)})
		rowsDesc = append(rowsDesc, ir.Row{"id": int64(8 - i)})
	}

	// Run 1 streams ids 1..7 ascending; chunk-rows=2 → chunks of
	// (1,2)(3,4)(5,6)(7).
	b1 := &Backup{
		Source:    newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{"events": rowsAsc}),
		SourceDSN: "src", Store: store, ChunkRows: 2,
	}
	if err := b1.Run(context.Background()); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	full, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if len(full.Tables) != 1 || len(full.Tables[0].Chunks) != 4 {
		t.Fatalf("premise: want 4 chunks, got %+v", full.Tables)
	}

	// Forge the killed-mid-table state: the manifest records only
	// chunks 0 and 1 (rows 1..4 in run 1's order), Partial=true. The
	// chunk FILES for 2 and 3 stay on the store, as they would after a
	// hard kill that died between chunk upload and manifest checkpoint.
	partial := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		SourceEngine:  "postgres",
		Schema:        schema,
		PartialState:  ir.BackupStateInProgress,
		Tables: []*ir.TableManifest{{
			Name:     "events",
			RowCount: 4,
			Partial:  true,
			Chunks:   []*ir.ChunkInfo{full.Tables[0].Chunks[0], full.Tables[0].Chunks[1]},
		}},
	}
	if err := writeManifest(context.Background(), store, partial); err != nil {
		t.Fatalf("writeManifest partial: %v", err)
	}

	// Run 2 (the resume) streams the SAME seven rows DESCENDING —
	// modeling the non-repeatable scan order that fires in production.
	// Pre-fix, the per-chunk reuse kept chunks (1,2)(3,4) and discarded
	// the new stream's first four rows (7,6,5,4), appending (3,2,1):
	// duplicates {1,2,3}, holes {5,6,7}, exit 0.
	b2 := &Backup{
		Source:    newBackupRecorderEngine("postgres", schema, map[string][]ir.Row{"events": rowsDesc}),
		SourceDSN: "src", Store: store, ChunkRows: 2,
	}
	if err := b2.Run(context.Background()); err != nil {
		t.Fatalf("resume Run: %v", err)
	}

	// Restore the artifact into a recording target and assert the id
	// MULTISET is exactly {1..7} — no duplicates, no holes.
	target := newRestoreRecorderEngine("postgres")
	r := &Restore{Target: target, TargetDSN: "dst", Store: store}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	_, restored := target.snapshot()
	got := map[int64]int{}
	for _, row := range restored["events"] {
		id, ok := row["id"].(int64)
		if !ok {
			t.Fatalf("restored row id has unexpected type: %#v", row["id"])
		}
		got[id]++
	}
	for i := int64(1); i <= 7; i++ {
		if got[i] != 1 {
			t.Errorf("id %d restored %d times; want exactly 1 (Bug 135: duplicate/missing rows after order-divergent resume)", i, got[i])
		}
	}
	if len(restored["events"]) != 7 {
		t.Errorf("restored %d rows; want 7", len(restored["events"]))
	}
}
