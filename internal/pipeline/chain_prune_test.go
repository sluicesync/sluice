// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// TestPruneLineage_KeepIncrementalsDropsOldest: 1 full + 5
// incrementals in a one-segment lineage, keep 2, the 3 oldest get
// pruned; the lineage's open segment retains 2.
func TestPruneLineage_KeepIncrementalsDropsOldest(t *testing.T) {
	store := newMemStore()
	seedLineageChain(t, store, 5)

	res, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 2})
	if err != nil {
		t.Fatalf("PruneChain: %v", err)
	}
	if len(res.Pruned) != 3 {
		t.Errorf("Pruned count = %d; want 3", len(res.Pruned))
	}
	cat, ok, err := loadLineageCatalog(context.Background(), store)
	if err != nil || !ok {
		t.Fatalf("post-prune loadLineageCatalog: ok=%v err=%v", ok, err)
	}
	if len(cat.Segments) != 1 || len(cat.Segments[0].Incrementals) != 2 {
		t.Errorf("post-prune segment = %+v; want 1 segment with 2 incrementals", cat.Segments)
	}
}

// TestPruneLineage_KeepDuration drops incrementals older than the
// threshold.
func TestPruneLineage_KeepDuration(t *testing.T) {
	store := newMemStore()
	base := seedLineageChain(t, store, 5)
	now := func() time.Time { return base.Add(5*time.Hour + time.Minute) }

	res, err := PruneChain(context.Background(), store, PruneOpts{KeepDuration: 2 * time.Hour, Now: now})
	if err != nil {
		t.Fatalf("PruneChain: %v", err)
	}
	// Incrementals at base+1h..base+5h; now=base+5h1m; keep < 2h old →
	// keep base+4h and base+5h (2 newest), drop the 3 oldest.
	if len(res.Pruned) != 3 {
		t.Errorf("Pruned = %d; want 3 (older-than-2h)", len(res.Pruned))
	}
}

// TestPruneLineage_KeepAllNoOp: keep >= count → nothing pruned.
func TestPruneLineage_KeepAllNoOp(t *testing.T) {
	store := newMemStore()
	seedLineageChain(t, store, 3)
	res, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 10})
	if err != nil {
		t.Fatalf("PruneChain: %v", err)
	}
	if len(res.Pruned) != 0 {
		t.Errorf("Pruned = %d; want 0", len(res.Pruned))
	}
	cat, _, _ := loadLineageCatalog(context.Background(), store)
	if len(cat.Segments[0].Incrementals) != 3 {
		t.Errorf("incrementals = %d; want 3 (unchanged)", len(cat.Segments[0].Incrementals))
	}
}

// TestPruneLineage_DryRunNoSideEffects reports the would-prune set
// without mutating the lineage or deleting chunks.
func TestPruneLineage_DryRunNoSideEffects(t *testing.T) {
	store := newMemStore()
	seedLineageChain(t, store, 4)
	res, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 1, DryRun: true})
	if err != nil {
		t.Fatalf("PruneChain dry-run: %v", err)
	}
	if len(res.Pruned) != 3 || res.ChunksDeleted != 0 {
		t.Errorf("dry-run Pruned=%d ChunksDeleted=%d; want 3,0", len(res.Pruned), res.ChunksDeleted)
	}
	cat, _, _ := loadLineageCatalog(context.Background(), store)
	if len(cat.Segments[0].Incrementals) != 4 {
		t.Errorf("post-dry-run incrementals = %d; want 4 (unchanged)", len(cat.Segments[0].Incrementals))
	}
}

func TestPruneLineage_RefusesBothFlags(t *testing.T) {
	store := newMemStore()
	seedLineageChain(t, store, 2)
	_, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 1, KeepDuration: time.Hour})
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("err = %v; want mutual-exclusion", err)
	}
}

func TestPruneLineage_RefusesNeitherFlag(t *testing.T) {
	store := newMemStore()
	seedLineageChain(t, store, 2)
	_, err := PruneChain(context.Background(), store, PruneOpts{})
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("err = %v; want at-least-one", err)
	}
}

func TestPruneLineage_RefusesWhenCatalogAbsent(t *testing.T) {
	store := newMemStore()
	mustWriteManifest(t, store, ManifestFileName, &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion, SourceEngine: "postgres",
		BackupID: "full000", Kind: ir.BackupKindFull, CreatedAt: time.Now().UTC(),
	})
	_, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 1})
	if err == nil || !strings.Contains(err.Error(), "lineage.json not found") {
		t.Errorf("err = %v; want lineage.json-not-found refusal", err)
	}
}

// TestPruneLineage_MultiSegmentDropsLeadingWholeSegment: a 2-segment
// lineage (seg0 capped w/ 2 incrs, seg1 open w/ 2 incrs); keep 2 →
// the whole seg0 (full + its 2 incrs) is dropped, seg1's full is the
// new restore base; restore-after-prune stays correct (the segment
// full is a self-contained snapshot).
func TestPruneLineage_MultiSegmentDropsLeadingWholeSegment(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)

	// Contiguous chain: each link's StartPosition == the preceding
	// link's EndPosition, and seg1.full.Start == seg0.lastIncr.End
	// (the inter-segment boundary the rotation FSM guarantees).
	f0 := seedFull(t, store, "", "0/000", "0/100", now)
	i01 := seedIncr(t, store, "", "incr01", f0.BackupID, "0/100", "0/200", now.Add(time.Hour))
	i02 := seedIncr(t, store, "", "incr02", i01.BackupID, "0/200", "0/300", now.Add(2*time.Hour))
	f1 := seedFull(t, store, "seg-1", "0/300", "0/400", now.Add(3*time.Hour))
	i11 := seedIncr(t, store, "seg-1", "incr11", f1.BackupID, "0/400", "0/500", now.Add(4*time.Hour))
	i12 := seedIncr(t, store, "seg-1", "incr12", i11.BackupID, "0/500", "0/600", now.Add(5*time.Hour))

	capt := now.Add(3 * time.Hour)
	cat := &LineageCatalog{
		FormatVersion: 1, SourceEngine: "postgres",
		Segments: []LineageSegment{
			{
				SegmentID: f0.BackupID, Dir: "", FullManifestPath: ManifestFileName,
				Incrementals:  []string{"manifests/incr-01.json", "manifests/incr-02.json"},
				StartPosition: f0.EndPosition, EndPosition: i02.EndPosition,
				CappedAt: &capt, CapReason: rotationReasonAge, Codec: CodecGzip,
			},
			{
				SegmentID: f1.BackupID, Dir: "seg-1", FullManifestPath: ManifestFileName,
				Incrementals:  []string{"manifests/incr-11.json", "manifests/incr-12.json"},
				StartPosition: f1.EndPosition, EndPosition: i12.EndPosition, Codec: CodecGzip,
			},
		},
	}
	if err := writeLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}

	res, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 2})
	if err != nil {
		t.Fatalf("PruneChain: %v", err)
	}
	if res.SegmentsDropped != 1 {
		t.Errorf("SegmentsDropped = %d; want 1 (whole seg0)", res.SegmentsDropped)
	}
	got, _, _ := loadLineageCatalog(context.Background(), store)
	if len(got.Segments) != 1 || got.Segments[0].Dir != "seg-1" {
		t.Fatalf("post-prune segments = %+v; want only seg-1", got.Segments)
	}
	if got.RestorableFromSegment != 0 {
		t.Errorf("RestorableFromSegment = %d; want 0 (re-based to seg-1)", got.RestorableFromSegment)
	}
	// seg0 full + its chunks are gone; seg-1 full survives.
	if ex, _ := store.Exists(context.Background(), ManifestFileName); ex {
		t.Error("seg0 root full not deleted after whole-segment prune")
	}
	if ex, _ := store.Exists(context.Background(), "seg-1/manifest.json"); !ex {
		t.Error("seg-1 full must survive (it is the new restore base)")
	}
	// Restore-after-prune correctness: the surviving lineage still
	// validates (the kept segment full is a contiguous base).
	if _, err := buildLineageChain(context.Background(), store, nil); err != nil {
		t.Errorf("restore-after-prune buildLineageChain: %v; want valid", err)
	}
	_ = i02
}

// --- seed helpers ---

func seedFull(t *testing.T, root ir.BackupStore, dir, startLSN, lsn string, created time.Time) *ir.Manifest {
	t.Helper()
	m := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion, SourceEngine: "postgres",
		Kind: ir.BackupKindFull, CreatedAt: created,
		StartPosition: ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"` + startLSN + `"}`},
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"` + lsn + `"}`},
		PartialState:  ir.BackupStateComplete,
	}
	m.BackupID = ir.ComputeBackupID(m)
	if err := writeManifestAt(context.Background(), newPrefixedStore(root, dir), ManifestFileName, m); err != nil {
		t.Fatalf("seed full: %v", err)
	}
	return m
}

func seedIncr(t *testing.T, root ir.BackupStore, dir, _id, parent, startLSN, lsn string, created time.Time) *ir.Manifest {
	t.Helper()
	m := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion, SourceEngine: "postgres",
		Kind: ir.BackupKindIncremental, CreatedAt: created, ParentBackupID: parent,
		StartPosition: ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"` + startLSN + `"}`},
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"` + lsn + `"}`},
		PartialState:  ir.BackupStateComplete,
	}
	m.BackupID = ir.ComputeBackupID(m)
	p := "manifests/incr-" + strings.TrimPrefix(_id, "incr") + ".json"
	if err := writeManifestAt(context.Background(), newPrefixedStore(root, dir), p, m); err != nil {
		t.Fatalf("seed incr: %v", err)
	}
	return m
}

// seedLineageChain writes a one-segment lineage (full + N
// incrementals) via the production lineage hooks so lineage.json is
// well-formed. Returns the base time (incrementals at base+1h..+Nh).
func seedLineageChain(t *testing.T, store ir.BackupStore, incrementals int) time.Time {
	t.Helper()
	base := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	full := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion, SourceEngine: "postgres",
		Kind: ir.BackupKindFull, CreatedAt: base,
		EndPosition:  ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/100"}`},
		PartialState: ir.BackupStateComplete,
	}
	full.BackupID = ir.ComputeBackupID(full)
	mustWriteManifest(t, store, ManifestFileName, full)
	updateLineageForManifestBestEffort(context.Background(), store, full, ManifestFileName, CodecGzip)
	parent := full.BackupID
	for i := 1; i <= incrementals; i++ {
		m := &ir.Manifest{
			FormatVersion: ir.BackupFormatVersion, SourceEngine: "postgres",
			Kind: ir.BackupKindIncremental, ParentBackupID: parent,
			CreatedAt:     base.Add(time.Duration(i) * time.Hour),
			StartPosition: ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/100"}`},
			EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"s","lsn":"0/100"}`},
			PartialState:  ir.BackupStateComplete,
		}
		m.BackupID = ir.ComputeBackupID(m)
		path := "manifests/incr-000" + string(rune('0'+i)) + ".json"
		mustWriteManifest(t, store, path, m)
		updateLineageForManifestBestEffort(context.Background(), store, m, path, CodecGzip)
		parent = m.BackupID
	}
	return base
}
