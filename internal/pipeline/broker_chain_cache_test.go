// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// countingGetStore counts every Get the broker chain code issues.
// Used to prove the cache's idle-tick GET count is constant in chain
// length (repo-audit M2.4: an idle tick was O(chain) GETs).
type countingGetStore struct {
	ir.BackupStore
	gets int
}

func (s *countingGetStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	s.gets++
	return s.BackupStore.Get(ctx, path)
}

// seedLinearLineage writes a one-segment lineage with n chained
// incrementals plus its lineage.json, returning the manifests in
// chain order (full first).
func seedLinearLineage(t *testing.T, store ir.BackupStore, n int) []*ir.Manifest {
	t.Helper()
	full := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	manifests := []*ir.Manifest{full}
	incrs := make([]*ir.Manifest, 0, n)
	prev := full
	for i := 0; i < n; i++ {
		m := makeManifest(t, ir.BackupKindIncremental, prev, fmt.Sprintf("0/%d", 200+i*100))
		incrs = append(incrs, m)
		manifests = append(manifests, m)
		prev = m
	}
	seg := seedSegment(t, store, "", full, incrs, CodecGzip)
	cat := &LineageCatalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []LineageSegment{seg}}
	if err := writeLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatalf("writeLineageCatalog: %v", err)
	}
	return manifests
}

// TestBrokerChainCache_IdleTickGETsConstant pins the M2.4 acceptance
// criterion: once warmed, an idle broker tick costs exactly 2 store
// GETs (lineage.json + tail manifest) REGARDLESS of chain length —
// not one GET per manifest. Both sizes assert the same constant, so
// a regression back to O(chain) fails loudly on the 50-link case.
func TestBrokerChainCache_IdleTickGETsConstant(t *testing.T) {
	for _, chainLinks := range []int{5, 50} {
		t.Run(fmt.Sprintf("links=%d", chainLinks), func(t *testing.T) {
			mem := newMemStore()
			manifests := seedLinearLineage(t, mem, chainLinks-1)
			store := &countingGetStore{BackupStore: mem}
			var cache brokerChainCache

			// Warm walk: O(chain) GETs, by design.
			chain, err := cache.get(context.Background(), store)
			if err != nil {
				t.Fatalf("warm get: %v", err)
			}
			if len(chain) != chainLinks {
				t.Fatalf("warm chain len = %d; want %d", len(chain), chainLinks)
			}
			warmGets := store.gets
			if warmGets < chainLinks {
				t.Fatalf("warm get issued %d GETs; want >= %d (one per manifest)", warmGets, chainLinks)
			}

			// Three idle ticks: each must be exactly 2 GETs and serve
			// the identical chain.
			for tick := 1; tick <= 3; tick++ {
				before := store.gets
				chain, err = cache.get(context.Background(), store)
				if err != nil {
					t.Fatalf("idle tick %d: %v", tick, err)
				}
				if got := store.gets - before; got != 2 {
					t.Errorf("idle tick %d issued %d GETs; want exactly 2 (catalog + tail manifest)", tick, got)
				}
				if len(chain) != chainLinks {
					t.Errorf("idle tick %d chain len = %d; want %d", tick, len(chain), chainLinks)
				}
				wantTail := manifestBackupID(manifests[len(manifests)-1])
				if got := manifestBackupID(chain[len(chain)-1].manifest); got != wantTail {
					t.Errorf("idle tick %d tail = %s; want %s", tick, got, wantTail)
				}
			}
		})
	}
}

// TestBrokerChainCache_AppendInvalidates: appending a new incremental
// through the production lineage hook (manifest write + catalog
// update) must invalidate the cache and surface the new link on the
// next get — a stale chain here would make the broker skip the
// incremental's data.
func TestBrokerChainCache_AppendInvalidates(t *testing.T) {
	ctx := context.Background()
	mem := newMemStore()
	manifests := seedLinearLineage(t, mem, 2)
	var cache brokerChainCache
	if _, err := cache.get(ctx, mem); err != nil {
		t.Fatalf("warm get: %v", err)
	}

	// Append incr-3 the way the stream does: durable manifest first,
	// then the lineage.json catalog update.
	tail := manifests[len(manifests)-1]
	next := makeManifest(t, ir.BackupKindIncremental, tail, "0/900")
	const nextPath = "manifests/incr-0003.json"
	mustWriteManifest(t, mem, nextPath, next)
	if err := updateLineageForManifest(ctx, mem, next, nextPath, CodecGzip); err != nil {
		t.Fatalf("updateLineageForManifest: %v", err)
	}

	chain, err := cache.get(ctx, mem)
	if err != nil {
		t.Fatalf("post-append get: %v", err)
	}
	if len(chain) != 4 {
		t.Fatalf("post-append chain len = %d; want 4", len(chain))
	}
	if got := manifestBackupID(chain[3].manifest); got != manifestBackupID(next) {
		t.Errorf("post-append tail = %s; want the appended incremental %s", got, manifestBackupID(next))
	}
}

// TestBrokerChainCache_TailCheckpointRewriteInvalidates pins the case
// the catalog identity alone would miss: the per-chunk checkpoint
// re-writes the OPEN incremental's manifest at the same path, and its
// companion lineage.json update is best-effort (it can fail and leave
// the catalog byte-identical). The tail-manifest identity GET must
// catch the rewrite so the broker sees the new chunk — never a stale
// chunk list.
func TestBrokerChainCache_TailCheckpointRewriteInvalidates(t *testing.T) {
	ctx := context.Background()
	mem := newMemStore()
	manifests := seedLinearLineage(t, mem, 2)
	var cache brokerChainCache
	chain, err := cache.get(ctx, mem)
	if err != nil {
		t.Fatalf("warm get: %v", err)
	}
	if got := len(chain[2].manifest.ChangeChunks); got != 0 {
		t.Fatalf("pre-checkpoint tail chunks = %d; want 0", got)
	}

	// Checkpoint-rewrite the tail manifest in place (one new chunk),
	// deliberately WITHOUT touching lineage.json — the failed
	// best-effort catalog-update shape.
	tail := manifests[2]
	tail.ChangeChunks = append(tail.ChangeChunks, &ir.ChunkInfo{File: "changes/c1.bin", RowCount: 3})
	mustWriteManifest(t, mem, "manifests/incr-0001.json", tail)

	chain, err = cache.get(ctx, mem)
	if err != nil {
		t.Fatalf("post-checkpoint get: %v", err)
	}
	if got := len(chain[2].manifest.ChangeChunks); got != 1 {
		t.Errorf("post-checkpoint tail chunks = %d; want 1 (stale cached chain served)", got)
	}
}

// TestBrokerChainCache_RotationInvalidates: rotation's segment-append
// COMMIT rewrites lineage.json; the next get must walk the new
// segment instead of serving the pre-rotation chain.
func TestBrokerChainCache_RotationInvalidates(t *testing.T) {
	ctx := context.Background()
	mem := newMemStore()
	f0 := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	i0 := makeManifest(t, ir.BackupKindIncremental, f0, "0/200")
	s0 := seedSegment(t, mem, "", f0, []*ir.Manifest{i0}, CodecGzip)
	cat := &LineageCatalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []LineageSegment{s0}}
	if err := writeLineageCatalog(ctx, mem, cat); err != nil {
		t.Fatal(err)
	}
	var cache brokerChainCache
	chain, err := cache.get(ctx, mem)
	if err != nil {
		t.Fatalf("warm get: %v", err)
	}
	if len(chain) != 2 {
		t.Fatalf("pre-rotation chain len = %d; want 2", len(chain))
	}

	// Rotate: cap segment 0, append segment 1 with its own full,
	// commit via the catalog rewrite (the FSM's linearization point).
	f1 := makeManifest(t, ir.BackupKindFull, nil, "0/300")
	f1.StartPosition = i0.EndPosition
	f1.BackupID = ir.ComputeBackupID(f1)
	s1 := seedSegment(t, mem, "seg-1", f1, nil, CodecZstd)
	capt := time.Now().UTC()
	s0.CappedAt, s0.CapReason = &capt, rotationReasonAge
	cat.Segments = []LineageSegment{s0, s1}
	if err := writeLineageCatalog(ctx, mem, cat); err != nil {
		t.Fatal(err)
	}

	chain, err = cache.get(ctx, mem)
	if err != nil {
		t.Fatalf("post-rotation get: %v", err)
	}
	if len(chain) != 3 {
		t.Fatalf("post-rotation chain len = %d; want 3 (f0,i0,f1)", len(chain))
	}
	if got := manifestBackupID(chain[2].manifest); got != manifestBackupID(f1) {
		t.Errorf("post-rotation tail = %s; want the new segment full %s", got, manifestBackupID(f1))
	}
}

// TestBrokerChainCache_PruneFloorAdvanceInvalidates: prune rewrites
// lineage.json advancing RestorableFromSegment; the cached
// pre-prune chain (which still includes the dropped segment) must not
// be served.
func TestBrokerChainCache_PruneFloorAdvanceInvalidates(t *testing.T) {
	ctx := context.Background()
	mem := newMemStore()
	f0 := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	i0 := makeManifest(t, ir.BackupKindIncremental, f0, "0/200")
	s0 := seedSegment(t, mem, "", f0, []*ir.Manifest{i0}, CodecGzip)
	f1 := makeManifest(t, ir.BackupKindFull, nil, "0/300")
	f1.StartPosition = i0.EndPosition
	f1.BackupID = ir.ComputeBackupID(f1)
	i1 := makeManifest(t, ir.BackupKindIncremental, f1, "0/400")
	s1 := seedSegment(t, mem, "seg-1", f1, []*ir.Manifest{i1}, CodecZstd)
	capt := time.Now().UTC()
	s0.CappedAt, s0.CapReason = &capt, rotationReasonAge
	cat := &LineageCatalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []LineageSegment{s0, s1}}
	if err := writeLineageCatalog(ctx, mem, cat); err != nil {
		t.Fatal(err)
	}
	var cache brokerChainCache
	chain, err := cache.get(ctx, mem)
	if err != nil {
		t.Fatalf("warm get: %v", err)
	}
	if len(chain) != 4 {
		t.Fatalf("pre-prune chain len = %d; want 4", len(chain))
	}

	cat.RestorableFromSegment = 1
	if err := writeLineageCatalog(ctx, mem, cat); err != nil {
		t.Fatal(err)
	}
	chain, err = cache.get(ctx, mem)
	if err != nil {
		t.Fatalf("post-prune get: %v", err)
	}
	if len(chain) != 2 {
		t.Fatalf("post-prune chain len = %d; want 2 (f1,i1 only)", len(chain))
	}
	if got := manifestBackupID(chain[0].manifest); got != manifestBackupID(f1) {
		t.Errorf("post-prune head = %s; want segment-1 full %s", got, manifestBackupID(f1))
	}
}

// TestBrokerChainCache_LegacyCatalogAbsentBypassesCache: the
// catalog-less legacy shape is walk-discovered and has no cheap head
// object, so the cache must never hold a chain for it — every get is
// a full walk and a newly-walked manifest appears immediately.
func TestBrokerChainCache_LegacyCatalogAbsentBypassesCache(t *testing.T) {
	ctx := context.Background()
	mem := newMemStore()
	full := makeManifest(t, ir.BackupKindFull, nil, "0/100")
	mustWriteManifest(t, mem, ManifestFileName, full)
	i0 := makeManifest(t, ir.BackupKindIncremental, full, "0/200")
	mustWriteManifest(t, mem, "manifests/incr-0001.json", i0)

	var cache brokerChainCache
	chain, err := cache.get(ctx, mem)
	if err != nil {
		t.Fatalf("legacy get: %v", err)
	}
	if len(chain) != 2 {
		t.Fatalf("legacy chain len = %d; want 2", len(chain))
	}
	if cache.chain != nil {
		t.Error("cache holds a chain for a catalog-less store; want bypass (no cheap identity)")
	}

	// A new incremental written under the legacy layout must show up
	// on the very next get (no catalog write happens here).
	i1 := makeManifest(t, ir.BackupKindIncremental, i0, "0/300")
	mustWriteManifest(t, mem, "manifests/incr-0002.json", i1)
	chain, err = cache.get(ctx, mem)
	if err != nil {
		t.Fatalf("legacy get after append: %v", err)
	}
	if len(chain) != 3 {
		t.Errorf("legacy chain len after append = %d; want 3", len(chain))
	}
}

// TestBrokerChainCache_CorruptCatalogIsLoudNotStale: once the catalog
// turns unreadable, get must surface buildLineageChain's loud refusal
// — serving the previously-cached chain would mask DR-data corruption
// behind a healthy-looking broker.
func TestBrokerChainCache_CorruptCatalogIsLoudNotStale(t *testing.T) {
	ctx := context.Background()
	mem := newMemStore()
	seedLinearLineage(t, mem, 2)
	var cache brokerChainCache
	if _, err := cache.get(ctx, mem); err != nil {
		t.Fatalf("warm get: %v", err)
	}

	mem.data[LineageCatalogFileName] = []byte("{not json")
	if _, err := cache.get(ctx, mem); err == nil {
		t.Fatal("get after catalog corruption returned nil error; want the loud lineage refusal, never the stale cached chain")
	}
}
