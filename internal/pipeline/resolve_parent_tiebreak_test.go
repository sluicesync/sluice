// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// TestResolveParent_CreatedAtTieDoesNotBranchChain pins the ADR-0046
// crash-matrix `pre-commit-write` flake (v0.67.0 blocker): after a
// crash forces a stream restart, resolveParent must return the chain
// TAIL incremental, not whichever manifest happens to have the maximum
// CreatedAt. CreatedAt is wall-clock with platform-dependent
// resolution and is NOT unique nor strictly monotonic with chain
// order — back-to-back small rollovers routinely share a millisecond.
// The pre-fix `max(CreatedAt)` selection (strict `.After()`) returned
// the SECOND-TO-LAST link on a tie, so the next incremental's
// ParentBackupID pointed at the wrong link and buildLineageChain
// correctly refused the branched lineage:
//
//	segment 0 incremental "…" parent "<incr1>" does not chain off
//	preceding link "<incr2>" — branching/mis-stitched lineage
//
// The lineage records incrementals in append (chain) order; the
// terminal one is the chain head a restart must continue from.
func TestResolveParent_CreatedAtTieDoesNotBranchChain(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}

	tie := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)

	full := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		CreatedAt:     tie.Add(-2 * time.Second),
		SourceEngine:  "postgres",
		Schema:        schema,
		Kind:          ir.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/100"}`},
		PartialState:  ir.BackupStateComplete,
	}
	full.BackupID = ir.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}

	// incr1 chains off the full.
	incr1 := &ir.Manifest{
		FormatVersion:  ir.BackupFormatVersion,
		CreatedAt:      tie, // SAME instant as incr2 — the timing tie.
		SourceEngine:   "postgres",
		Schema:         schema,
		Kind:           ir.BackupKindIncremental,
		ParentBackupID: full.BackupID,
		StartPosition:  full.EndPosition,
		EndPosition:    ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/200"}`},
		PartialState:   ir.BackupStateComplete,
	}
	incr1.BackupID = ir.ComputeBackupID(incr1)
	// incr2 chains off incr1 — it is the chain TAIL a restart must
	// continue from. Lower lexical path than incr1 so the buggy
	// strict-`.After()` max-scan (which keeps the FIRST element on a
	// tie) would wrongly return incr1.
	incr2 := &ir.Manifest{
		FormatVersion:  ir.BackupFormatVersion,
		CreatedAt:      tie, // tie with incr1.
		SourceEngine:   "postgres",
		Schema:         schema,
		Kind:           ir.BackupKindIncremental,
		ParentBackupID: incr1.BackupID,
		StartPosition:  incr1.EndPosition,
		EndPosition:    ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/300"}`},
		PartialState:   ir.BackupStateComplete,
	}
	incr2.BackupID = ir.ComputeBackupID(incr2)

	// Write in chain order; the manifest path encodes a strictly
	// increasing unix-millis so List() returns them chain-ordered
	// (incr-0001 = incr1, incr-0002 = incr2).
	if err := writeManifestAt(context.Background(), store, "manifests/incr-0000000000001-aaaaaaaa.json", incr1); err != nil {
		t.Fatalf("write incr1: %v", err)
	}
	if err := writeManifestAt(context.Background(), store, "manifests/incr-0000000000002-bbbbbbbb.json", incr2); err != nil {
		t.Fatalf("write incr2: %v", err)
	}

	// Seed lineage.json in chain (append) order so an inconsistent
	// resolveParent pick would branch it.
	updateLineageForManifestBestEffort(context.Background(), store, full, ManifestFileName, DefaultCodec)
	updateLineageForManifestBestEffort(context.Background(), store, incr1, "manifests/incr-0000000000001-aaaaaaaa.json", DefaultCodec)
	updateLineageForManifestBestEffort(context.Background(), store, incr2, "manifests/incr-0000000000002-bbbbbbbb.json", DefaultCodec)

	src := &fakeCDCEngine{name: "postgres", schemaSequence: []*ir.Schema{schema}}

	t.Run("BackupStream.resolveParent returns the chain tail", func(t *testing.T) {
		b := &BackupStream{Source: src, SourceDSN: "x", Store: store}
		seg, codec, err := openSegmentStore(context.Background(), store, b.Codec)
		if err != nil {
			t.Fatalf("openSegmentStore: %v", err)
		}
		b.segStore, b.segCodec = seg, codec
		got, _, err := b.resolveParent(context.Background())
		if err != nil {
			t.Fatalf("resolveParent: %v", err)
		}
		if got.BackupID != incr2.BackupID {
			t.Fatalf("resolveParent picked %q (parent=%q); want chain TAIL incr2 %q. "+
				"Picking the non-tail link branches the lineage and makes the "+
				"next incremental's ParentBackupID point off-chain.",
				got.BackupID, got.ParentBackupID, incr2.BackupID)
		}
	})

	t.Run("IncrementalBackup.resolveParent returns the chain tail", func(t *testing.T) {
		b := &IncrementalBackup{Source: src, SourceDSN: "x", Store: store}
		seg, codec, err := openSegmentStore(context.Background(), store, b.Codec)
		if err != nil {
			t.Fatalf("openSegmentStore: %v", err)
		}
		b.segStore, b.segCodec = seg, codec
		got, _, err := b.resolveParent(context.Background())
		if err != nil {
			t.Fatalf("resolveParent: %v", err)
		}
		if got.BackupID != incr2.BackupID {
			t.Fatalf("resolveParent picked %q; want chain TAIL incr2 %q", got.BackupID, incr2.BackupID)
		}
	})
}
