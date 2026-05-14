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

// TestPruneChain_KeepIncrementalsDropsOldest pins the basic
// keep-N-most-recent flow: 1 full + 5 incrementals, keep 2, the 3
// oldest get pruned.
func TestPruneChain_KeepIncrementalsDropsOldest(t *testing.T) {
	store := newMemStore()
	seedChain(t, store, 5)

	res, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 2})
	if err != nil {
		t.Fatalf("PruneChain: %v", err)
	}
	if len(res.Pruned) != 3 {
		t.Errorf("Pruned count = %d; want 3", len(res.Pruned))
	}
	if len(res.Kept) != 3 { // full + 2 incrs
		t.Errorf("Kept count = %d; want 3 (full + 2 incrementals)", len(res.Kept))
	}
	cat, ok, err := loadChainCatalog(context.Background(), store)
	if err != nil || !ok {
		t.Fatalf("post-prune loadChainCatalog: ok=%v err=%v", ok, err)
	}
	if len(cat.Entries) != 3 {
		t.Errorf("post-prune catalog entries = %d; want 3", len(cat.Entries))
	}
}

// TestPruneChain_KeepDurationDropsOlderThanThreshold covers the
// time-based mode. Setup: 5 incrementals at 1-hour intervals; keep
// any within last 2h → the 3 oldest get dropped.
func TestPruneChain_KeepDurationDropsOlderThanThreshold(t *testing.T) {
	store := newMemStore()
	seedChain(t, store, 5)

	// Pin "now" to 5 hours after the chain's last incremental.
	cat, _, _ := loadChainCatalog(context.Background(), store)
	lastTime := cat.Entries[len(cat.Entries)-1].CreatedAt
	now := func() time.Time { return lastTime.Add(time.Hour) }

	res, err := PruneChain(context.Background(), store, PruneOpts{
		KeepDuration: 2 * time.Hour,
		Now:          now,
	})
	if err != nil {
		t.Fatalf("PruneChain: %v", err)
	}
	// 5 incrementals at 1h intervals, threshold = now - 2h.
	// now is lastTime + 1h. Threshold = lastTime - 1h. So incrementals
	// whose CreatedAt < (lastTime - 1h) get dropped — that's the 3
	// oldest (0h, 1h, 2h from start, which are 4h/3h/2h before
	// lastTime, all earlier than threshold's 1h-before-last).
	if len(res.Pruned) != 3 {
		t.Errorf("Pruned count = %d; want 3 (older-than-2h)", len(res.Pruned))
	}
}

// TestPruneChain_KeepAllPreservesEverything covers the no-op case:
// KeepIncrementals >= incremental count → nothing to do.
func TestPruneChain_KeepAllPreservesEverything(t *testing.T) {
	store := newMemStore()
	seedChain(t, store, 3)

	res, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 10})
	if err != nil {
		t.Fatalf("PruneChain: %v", err)
	}
	if len(res.Pruned) != 0 {
		t.Errorf("Pruned count = %d; want 0", len(res.Pruned))
	}
	// Catalog should be unchanged.
	cat, _, _ := loadChainCatalog(context.Background(), store)
	if len(cat.Entries) != 4 { // full + 3 incrementals
		t.Errorf("catalog entries = %d; want 4", len(cat.Entries))
	}
}

// TestPruneChain_DryRunNoSideEffects covers the dry-run mode: the
// would-prune list is returned but nothing on disk changes.
func TestPruneChain_DryRunNoSideEffects(t *testing.T) {
	store := newMemStore()
	seedChain(t, store, 4)

	res, err := PruneChain(context.Background(), store, PruneOpts{
		KeepIncrementals: 1,
		DryRun:           true,
	})
	if err != nil {
		t.Fatalf("PruneChain dry-run: %v", err)
	}
	if len(res.Pruned) != 3 {
		t.Errorf("dry-run Pruned count = %d; want 3", len(res.Pruned))
	}
	if res.ChunksDeleted != 0 {
		t.Errorf("dry-run ChunksDeleted = %d; want 0", res.ChunksDeleted)
	}
	// Catalog untouched.
	cat, _, _ := loadChainCatalog(context.Background(), store)
	if len(cat.Entries) != 5 {
		t.Errorf("post-dry-run catalog entries = %d; want 5 (unchanged)", len(cat.Entries))
	}
}

// TestPruneChain_RefusesBothFlags covers the mutual-exclusion gate.
func TestPruneChain_RefusesBothFlags(t *testing.T) {
	store := newMemStore()
	seedChain(t, store, 2)

	_, err := PruneChain(context.Background(), store, PruneOpts{
		KeepIncrementals: 1,
		KeepDuration:     time.Hour,
	})
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("expected mutual-exclusion error; got %v", err)
	}
}

// TestPruneChain_RefusesNeitherFlag covers the validation gate.
func TestPruneChain_RefusesNeitherFlag(t *testing.T) {
	store := newMemStore()
	seedChain(t, store, 2)

	_, err := PruneChain(context.Background(), store, PruneOpts{})
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("expected at-least-one error; got %v", err)
	}
}

// TestPruneChain_RefusesWhenCatalogAbsent ensures the operator-
// actionable refusal fires on a chain without chain.json — the
// expected next step is `--rebuild-catalog`.
func TestPruneChain_RefusesWhenCatalogAbsent(t *testing.T) {
	store := newMemStore()
	// Put a full manifest but no chain.json.
	mustWriteManifest(t, store, ManifestFileName, &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		SourceEngine:  "postgres",
		BackupID:      "full000",
		Kind:          ir.BackupKindFull,
		CreatedAt:     time.Now().UTC(),
	})

	_, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 1})
	if err == nil || !strings.Contains(err.Error(), "chain.json not found") {
		t.Errorf("expected chain.json-not-found refusal; got %v", err)
	}
}

// TestPruneChain_RefusesWhenChainWouldBreak covers the orphan-parent
// guard: if the operator's keep-set would orphan a kept incremental
// (parent in drop set), the prune refuses with the actionable hint.
//
// Constructed shape: 1 full → incr1 → incr2 where incr2's parent is
// incr1 (chain link). Keep only incr2 (drop full + incr1). The full
// is always kept by PruneChain, but incr1 dropping leaves incr2
// orphaned. The refusal fires.
//
// We have to handcraft this because seedChain wires parent links
// in chain order; this test manually breaks the parent chain by
// renaming an incremental's parent.
func TestPruneChain_RefusesWhenChainWouldBreak(t *testing.T) {
	store := newMemStore()
	now := time.Now().UTC()
	full := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		SourceEngine:  "postgres",
		BackupID:      "full000",
		Kind:          ir.BackupKindFull,
		CreatedAt:     now,
	}
	// 5 incrementals all chaining off full000 (parent = full000) so
	// the parent-link check sees full000 in the drop set... wait —
	// the full is ALWAYS kept. Need to break differently: make
	// incr5's parent = incr3 (skip incr4). If we keep incr5 but drop
	// incr3 + incr4, the refusal must fire (incr5's parent incr3 is
	// dropped).
	incrs := make([]*ir.Manifest, 5)
	for i := range incrs {
		parent := full.BackupID
		if i > 0 {
			parent = incrs[i-1].BackupID
		}
		if i == 4 {
			parent = incrs[2].BackupID // incr5.parent = incr3 (skip incr4)
		}
		incrs[i] = &ir.Manifest{
			FormatVersion:  ir.BackupFormatVersion,
			SourceEngine:   "postgres",
			BackupID:       "incr" + string(rune('0'+i+1)),
			Kind:           ir.BackupKindIncremental,
			ParentBackupID: parent,
			CreatedAt:      now.Add(time.Duration(i+1) * time.Hour),
		}
	}
	mustWriteManifest(t, store, ManifestFileName, full)
	for i, m := range incrs {
		path := "manifests/incr-" + string(rune('1'+i)) + ".json"
		mustWriteManifest(t, store, path, m)
		if err := updateChainCatalog(context.Background(), store, m, path, 1); err != nil {
			t.Fatalf("seed catalog: %v", err)
		}
	}

	// Keep last 2 → would drop incrs 1-3. incr5's parent is incr3,
	// which is in the drop set → refusal.
	_, err := PruneChain(context.Background(), store, PruneOpts{KeepIncrementals: 2})
	if err == nil || !strings.Contains(err.Error(), "refusing to break chain") {
		t.Errorf("expected chain-break refusal; got %v", err)
	}
}

// seedChain helper: write a full + N incrementals to store via the
// production chain.json hooks so the catalog is well-formed.
// Each incremental's parent points at the prior one. Used by every
// happy-path test.
func seedChain(t *testing.T, store ir.BackupStore, incrementals int) {
	t.Helper()
	now := time.Now().UTC()
	full := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		SourceEngine:  "postgres",
		BackupID:      "full000",
		Kind:          ir.BackupKindFull,
		CreatedAt:     now,
	}
	mustWriteManifest(t, store, ManifestFileName, full)
	if err := updateChainCatalog(context.Background(), store, full, ManifestFileName, 1); err != nil {
		t.Fatalf("seed full catalog: %v", err)
	}
	parent := full.BackupID
	for i := 1; i <= incrementals; i++ {
		id := "incr" + string(rune('0'+i))
		path := "manifests/incr-" + string(rune('0'+i)) + ".json"
		m := &ir.Manifest{
			FormatVersion:  ir.BackupFormatVersion,
			SourceEngine:   "postgres",
			BackupID:       id,
			Kind:           ir.BackupKindIncremental,
			ParentBackupID: parent,
			CreatedAt:      now.Add(time.Duration(i) * time.Hour),
		}
		mustWriteManifest(t, store, path, m)
		if err := updateChainCatalog(context.Background(), store, m, path, 0); err != nil {
			t.Fatalf("seed incr catalog: %v", err)
		}
		parent = id
	}
}
