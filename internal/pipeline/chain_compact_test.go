// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// TestCompactChain_TwoSegmentGroup_Merges: a 2-segment in-window group
// merges into 1 segment whose StartPosition + EndPosition span the
// pair, with both source dirs swept post-commit.
func TestCompactChain_TwoSegmentGroup_Merges(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	seedTwoSegmentLineage(t, store, now, time.Hour)

	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow:  2 * time.Hour,
		Now:          func() time.Time { return now.Add(10 * time.Hour) },
		newSegmentID: func() string { return "merged-A" },
	})
	if err != nil {
		t.Fatalf("CompactChain: %v", err)
	}
	if res.GroupsConsidered != 1 || res.GroupsMerged != 1 || res.SegmentsRemoved != 1 {
		t.Errorf("GroupsConsidered=%d GroupsMerged=%d SegmentsRemoved=%d; want 1,1,1",
			res.GroupsConsidered, res.GroupsMerged, res.SegmentsRemoved)
	}
	if res.BytesBefore != res.BytesAfter {
		t.Errorf("BytesBefore=%d BytesAfter=%d; naive compact moves bytes, never rewrites them",
			res.BytesBefore, res.BytesAfter)
	}

	cat, ok, err := loadLineageCatalog(context.Background(), store)
	if err != nil || !ok {
		t.Fatalf("post-compact loadLineageCatalog: ok=%v err=%v", ok, err)
	}
	if len(cat.Segments) != 1 {
		t.Fatalf("post-compact segments = %d; want 1 merged segment", len(cat.Segments))
	}
	merged := cat.Segments[0]
	if merged.SegmentID != "merged-A" {
		t.Errorf("merged.SegmentID = %q; want merged-A", merged.SegmentID)
	}
	if merged.Dir != mergedSegmentDirPrefix+"merged-A" {
		t.Errorf("merged.Dir = %q; want %s", merged.Dir, mergedSegmentDirPrefix+"merged-A")
	}
	if merged.CapReason != compactedCapReason {
		t.Errorf("merged.CapReason = %q; want %s", merged.CapReason, compactedCapReason)
	}
	if len(merged.Incrementals) != 4 {
		t.Errorf("merged.Incrementals = %d; want 4 (2 per source × 2 sources)", len(merged.Incrementals))
	}
	// Merged segment's full + incrementals are addressable under the
	// merged dir.
	mergedStore := merged.store(store)
	if ex, _ := mergedStore.Exists(context.Background(), ManifestFileName); !ex {
		t.Error("merged segment's full manifest is missing under its dir")
	}
	for _, ip := range merged.Incrementals {
		if ex, _ := mergedStore.Exists(context.Background(), ip); !ex {
			t.Errorf("merged segment incremental %q is missing under its dir", ip)
		}
	}
	// Source dirs are swept (post-commit cleanup).
	if ex, _ := store.Exists(context.Background(), ManifestFileName); ex {
		t.Error("source-0 root manifest survives compact; should have been swept")
	}
	if paths, _ := store.List(context.Background(), "seg-1/"); len(paths) != 0 {
		t.Errorf("source-1 dir survives compact: %v", paths)
	}
	// No leftover staging dir.
	if paths, _ := store.List(context.Background(), compactStagingDirPrefix); len(paths) != 0 {
		t.Errorf("staging dir survives compact: %v", paths)
	}
}

// TestCompactChain_ThreeSegmentGroup_Merges: 3 in-window segments
// collapse to 1.
func TestCompactChain_ThreeSegmentGroup_Merges(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	seedNSegmentLineage(t, store, now, time.Hour, 3)

	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow:  2 * time.Hour,
		Now:          func() time.Time { return now.Add(20 * time.Hour) },
		newSegmentID: func() string { return "merged-3" },
	})
	if err != nil {
		t.Fatalf("CompactChain: %v", err)
	}
	if res.GroupsMerged != 1 || res.SegmentsRemoved != 2 {
		t.Errorf("GroupsMerged=%d SegmentsRemoved=%d; want 1,2", res.GroupsMerged, res.SegmentsRemoved)
	}
	cat, _, _ := loadLineageCatalog(context.Background(), store)
	if len(cat.Segments) != 1 {
		t.Fatalf("post-compact segments = %d; want 1", len(cat.Segments))
	}
}

// TestCompactChain_GroupOutsideWindow_Untouched: a 2-segment lineage
// with a gap LARGER than the merge window is left alone.
func TestCompactChain_GroupOutsideWindow_Untouched(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	seedTwoSegmentLineage(t, store, now, 5*time.Hour) // gap = 5h

	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow:  time.Hour, // tighter than the 5h gap
		Now:          func() time.Time { return now.Add(20 * time.Hour) },
		newSegmentID: func() string { return "should-not-mint" },
	})
	if err != nil {
		t.Fatalf("CompactChain: %v", err)
	}
	if res.GroupsMerged != 0 || res.SegmentsRemoved != 0 {
		t.Errorf("out-of-window groups merged: %+v", res)
	}
	cat, _, _ := loadLineageCatalog(context.Background(), store)
	if len(cat.Segments) != 2 {
		t.Errorf("segments = %d; want 2 (untouched)", len(cat.Segments))
	}
}

// TestCompactChain_MixedInOutWindow: 4 segments with gaps [1h,5h,1h];
// the middle gap exceeds the window so we get two groups: {seg0,seg1}
// and {seg2,seg3}. Both eligible to merge.
func TestCompactChain_MixedInOutWindow(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	gaps := []time.Duration{time.Hour, 5 * time.Hour, time.Hour}
	seedSegmentsWithGaps(t, store, now, gaps)

	ids := []string{"mergeA", "mergeB"}
	idx := 0
	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow:  2 * time.Hour,
		Now:          func() time.Time { return now.Add(50 * time.Hour) },
		newSegmentID: func() string { id := ids[idx]; idx++; return id },
	})
	if err != nil {
		t.Fatalf("CompactChain: %v", err)
	}
	if res.GroupsConsidered != 2 || res.GroupsMerged != 2 || res.SegmentsRemoved != 2 {
		t.Errorf("GroupsConsidered=%d GroupsMerged=%d SegmentsRemoved=%d; want 2,2,2",
			res.GroupsConsidered, res.GroupsMerged, res.SegmentsRemoved)
	}
	cat, _, _ := loadLineageCatalog(context.Background(), store)
	if len(cat.Segments) != 2 {
		t.Errorf("post-compact segments = %d; want 2 merged segments", len(cat.Segments))
	}
}

// TestCompactChain_KeysetBoundary_RefusesLoudly: a 2-segment group
// whose fulls bind to DIFFERENT encryption keysets refuses loudly per
// the loud-failure tenet.
func TestCompactChain_KeysetBoundary_RefusesLoudly(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	seedTwoSegmentLineageWithEncryption(
		t, store, now,
		&irbackup.ChainEncryption{Algorithm: "AES-256-GCM", Mode: "per-chain", KEKMode: "passphrase-argon2id", KEKRef: "", Argon2id: &irbackup.Argon2idParams{Salt: []byte("salt-A")}},
		&irbackup.ChainEncryption{Algorithm: "AES-256-GCM", Mode: "per-chain", KEKMode: "passphrase-argon2id", KEKRef: "", Argon2id: &irbackup.Argon2idParams{Salt: []byte("salt-B")}},
	)

	_, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow: 2 * time.Hour,
	})
	if err == nil {
		t.Fatal("CompactChain with mixed keysets: nil error; want loud refusal")
	}
	msg := err.Error()
	if !strings.Contains(msg, "merge window straddles encryption keyset boundaries") {
		t.Errorf("err = %q; want the documented keyset-boundary refusal message", msg)
	}
	if !strings.Contains(msg, "re-key the chain first") {
		t.Errorf("err = %q; want the recovery hint", msg)
	}
	// Refusal is BEFORE any mutation: catalog should still hold both
	// source segments unchanged.
	cat, _, _ := loadLineageCatalog(context.Background(), store)
	if len(cat.Segments) != 2 {
		t.Errorf("post-refusal segments = %d; want 2 (no mutation)", len(cat.Segments))
	}
}

// TestCompactChain_CodecBoundary_RefusesLoudly: a 2-segment group with
// different codecs refuses loudly.
func TestCompactChain_CodecBoundary_RefusesLoudly(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	seedTwoSegmentLineageWithCodecs(t, store, now, CodecGzip, CodecZstd)

	_, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow: 2 * time.Hour,
	})
	if err == nil {
		t.Fatal("CompactChain with mixed codecs: nil error; want loud refusal")
	}
	if !errors.Is(err, errMergeGroupCodecMismatch) {
		t.Errorf("err = %v; want errMergeGroupCodecMismatch wrapper", err)
	}
}

// TestCompactChain_PositionGap_RefusesLoudly: a 2-segment group where
// seg[i+1].StartPosition != seg[i].EndPosition refuses loudly to
// avoid silent event loss.
func TestCompactChain_PositionGap_RefusesLoudly(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	seedTwoSegmentLineageWithGap(t, store, now)

	_, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow: 2 * time.Hour,
	})
	if err == nil {
		t.Fatal("CompactChain across a position gap: nil error; want loud refusal")
	}
	if !strings.Contains(err.Error(), "position gap") {
		t.Errorf("err = %q; want position-gap refusal", err.Error())
	}
}

// TestCompactChain_RotatedOverlap_Merges is the unit-level Bug 95 pin:
// a chain rotated in the ADR-0067 shape (each non-root segment keeps the
// (P_N, S] overlap and records IncrementalCoverageStart = P_N) is
// CONTIGUOUS and so MERGES — where the pre-ADR-0067 gap shape
// (TestCompactChain_PositionGap_RefusesLoudly) correctly refused. Pins
// that the contiguity check keys off IncrementalCoverageStart, and that
// the merged lineage still walks (restore-build) cleanly.
func TestCompactChain_RotatedOverlap_Merges(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	seedSegmentsWithGapsOpts(t, store, now, []time.Duration{time.Hour}, segmentSeedOpts{rotatedOverlap: true})

	res, err := CompactChain(context.Background(), store, CompactOpts{MergeWindow: 2 * time.Hour})
	if err != nil {
		t.Fatalf("CompactChain on a rotated-overlap (contiguous) chain: %v; want a clean merge (ADR-0067)", err)
	}
	if res.GroupsMerged != 1 {
		t.Fatalf("GroupsMerged = %d; want 1 (the rotated chain is contiguous and compactable)", res.GroupsMerged)
	}
	// The post-compact lineage must still build (the restore-walk): the
	// merged segment's full is the oldest (non-rotated) source's, the
	// former inter-segment boundary is now an exact intra-segment one.
	if _, err := buildLineageChain(context.Background(), store, nil); err != nil {
		t.Errorf("post-compact buildLineageChain: %v", err)
	}
}

// TestCompactChain_OnlyFull_NoOp: a chain with no incrementals (a
// one-segment lineage) is a no-op, no error.
func TestCompactChain_OnlyFull_NoOp(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	seedNSegmentLineage(t, store, now, time.Hour, 1)

	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow: 2 * time.Hour,
	})
	if err != nil {
		t.Fatalf("CompactChain on one-segment lineage: %v", err)
	}
	if res.GroupsMerged != 0 {
		t.Errorf("GroupsMerged = %d; want 0 (single-segment lineage is a no-op)", res.GroupsMerged)
	}
}

// TestCompactChain_AllOutOfWindow_NoOp: every gap exceeds the window.
func TestCompactChain_AllOutOfWindow_NoOp(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	gaps := []time.Duration{5 * time.Hour, 5 * time.Hour}
	seedSegmentsWithGaps(t, store, now, gaps)

	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow: time.Hour,
	})
	if err != nil {
		t.Fatalf("CompactChain: %v", err)
	}
	if res.GroupsMerged != 0 {
		t.Errorf("GroupsMerged = %d; want 0", res.GroupsMerged)
	}
}

// TestCompactChain_DryRun_NoSideEffects: --dry-run reports the plan
// without touching storage or the catalog.
func TestCompactChain_DryRun_NoSideEffects(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	seedTwoSegmentLineage(t, store, now, time.Hour)

	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow:  2 * time.Hour,
		DryRun:       true,
		Now:          func() time.Time { return now.Add(10 * time.Hour) },
		newSegmentID: func() string { return "would-merge" },
	})
	if err != nil {
		t.Fatalf("CompactChain dry-run: %v", err)
	}
	if res.GroupsMerged != 1 || res.SegmentsRemoved != 1 {
		t.Errorf("dry-run plan GroupsMerged=%d SegmentsRemoved=%d; want 1,1", res.GroupsMerged, res.SegmentsRemoved)
	}
	if len(res.Plan) != 1 || res.Plan[0].MergedSegmentID != "would-merge" {
		t.Errorf("dry-run Plan = %+v; want one entry with MergedSegmentID=would-merge", res.Plan)
	}
	// Catalog unchanged.
	cat, _, _ := loadLineageCatalog(context.Background(), store)
	if len(cat.Segments) != 2 {
		t.Errorf("post-dry-run segments = %d; want 2 (no mutation)", len(cat.Segments))
	}
	// No staging or final merged dirs.
	if paths, _ := store.List(context.Background(), compactStagingDirPrefix); len(paths) != 0 {
		t.Errorf("dry-run left staging dir: %v", paths)
	}
	if paths, _ := store.List(context.Background(), mergedSegmentDirPrefix); len(paths) != 0 {
		t.Errorf("dry-run left merged dir: %v", paths)
	}
}

// TestCompactChain_RequiresMergeWindow: zero / negative MergeWindow
// refuses.
func TestCompactChain_RequiresMergeWindow(t *testing.T) {
	store := newMemStore()
	_, err := CompactChain(context.Background(), store, CompactOpts{})
	if err == nil || !strings.Contains(err.Error(), "merge-window") {
		t.Errorf("err = %v; want --merge-window refusal", err)
	}
}

// TestCompactChain_RefusesWhenCatalogAbsent: matches prune's posture.
func TestCompactChain_RefusesWhenCatalogAbsent(t *testing.T) {
	store := newMemStore()
	_, err := CompactChain(context.Background(), store, CompactOpts{MergeWindow: time.Hour})
	if err == nil || !strings.Contains(err.Error(), "lineage.json not found") {
		t.Errorf("err = %v; want lineage.json-not-found refusal", err)
	}
}

// TestCompactChain_RestoreShape_PostCompact: the post-compact lineage
// remains structurally valid for restore — buildLineageChain must
// succeed (the same way prune's analogous pin asserts).
func TestCompactChain_RestoreShape_PostCompact(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	seedTwoSegmentLineage(t, store, now, time.Hour)

	if _, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow:  2 * time.Hour,
		Now:          func() time.Time { return now.Add(10 * time.Hour) },
		newSegmentID: func() string { return "merged-shape" },
	}); err != nil {
		t.Fatalf("CompactChain: %v", err)
	}
	if _, err := buildLineageChain(context.Background(), store, nil); err != nil {
		t.Errorf("post-compact buildLineageChain: %v; want valid lineage", err)
	}
}

// TestCompactChain_StaleStagingCleanup: a leftover .compact-staging-*
// from an earlier crashed run gets swept on the next CompactChain.
func TestCompactChain_StaleStagingCleanup(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	seedTwoSegmentLineage(t, store, now, time.Hour)

	// Plant garbage from a crashed prior run.
	stale := compactStagingDirPrefix + "stale-1/garbage.txt"
	if err := store.Put(context.Background(), stale, bytes.NewReader([]byte("junk"))); err != nil {
		t.Fatalf("seed stale staging: %v", err)
	}

	if _, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow:  2 * time.Hour,
		Now:          func() time.Time { return now.Add(10 * time.Hour) },
		newSegmentID: func() string { return "merged-stale" },
	}); err != nil {
		t.Fatalf("CompactChain: %v", err)
	}
	if ex, _ := store.Exists(context.Background(), stale); ex {
		t.Error("stale staging file survives the cleanup pass")
	}
}

// --- seed helpers ---

func seedTwoSegmentLineage(t *testing.T, store irbackup.Store, now time.Time, gap time.Duration) {
	t.Helper()
	seedSegmentsWithGaps(t, store, now, []time.Duration{gap})
}

func seedNSegmentLineage(t *testing.T, store irbackup.Store, now time.Time, gap time.Duration, n int) {
	t.Helper()
	if n < 1 {
		t.Fatalf("n must be >= 1; got %d", n)
	}
	gaps := make([]time.Duration, n-1)
	for i := range gaps {
		gaps[i] = gap
	}
	seedSegmentsWithGaps(t, store, now, gaps)
}

// seedSegmentsWithGaps writes a multi-segment lineage:
//   - Each segment is at `now + cumulative_gap`.
//   - Each segment has 2 incrementals (positions stepped tightly so
//     contiguity holds across the boundary).
//   - Codec uniform (CodecGzip across all segments).
//   - No ChainEncryption on the full manifests (plaintext keyset).
//
// Returns nothing; the caller asserts on the on-disk state.
func seedSegmentsWithGaps(t *testing.T, store irbackup.Store, base time.Time, gaps []time.Duration) {
	t.Helper()
	seedSegmentsWithGapsOpts(t, store, base, gaps, segmentSeedOpts{})
}

type segmentSeedOpts struct {
	// codecsPerSegment, when len > 0, overrides the per-segment codec.
	codecsPerSegment []Codec
	// encPerSegment, when len > 0, attaches ChainEncryption to each
	// segment's full.
	encPerSegment []*irbackup.ChainEncryption
	// gapBetweenBoundaries, when true, deliberately advances each
	// segment's StartPosition past the prior's EndPosition (creating
	// the contiguity-violation shape).
	gapBetweenBoundaries bool
	// rotatedOverlap, when true, produces the ADR-0067 rotation shape for
	// each non-root segment: the full is anchored at S = prior.End + 50
	// (a gap, like gapBetweenBoundaries), BUT the segment's incrementals
	// KEEP the (P_N, S] overlap — they start at P_N (== prior.End) — and
	// the segment records IncrementalCoverageStart = P_N. Such a lineage
	// is contiguous (prior.End == cur.IncrementalCoverageStart) and so
	// compactable, unlike the gapBetweenBoundaries shape.
	rotatedOverlap bool
}

func seedSegmentsWithGapsOpts(t *testing.T, store irbackup.Store, base time.Time, gaps []time.Duration, opts segmentSeedOpts) {
	t.Helper()
	n := len(gaps) + 1
	cumulative := time.Duration(0)
	prevEndLSN := uint64(100)
	cappedAt := time.Time{} // overwritten per loop

	// First, build segments and write their conventional files. Then
	// hand-author the lineage.json describing the multi-segment shape.
	cat := &LineageCatalog{
		FormatVersion: 1,
		SourceEngine:  "postgres",
		CreatedAt:     base,
		UpdatedAt:     base,
	}

	for i := 0; i < n; i++ {
		segCreatedAt := base.Add(cumulative)
		dir := ""
		if i > 0 {
			dir = fmt.Sprintf("seg-%d", i)
		}
		codec := CodecGzip
		if i < len(opts.codecsPerSegment) {
			codec = opts.codecsPerSegment[i]
		}
		var chainEnc *irbackup.ChainEncryption
		if i < len(opts.encPerSegment) {
			chainEnc = opts.encPerSegment[i]
		}

		// Segment's full anchor + incremental-coverage start. Default:
		// both at the prior segment's EndPosition (contiguous, no
		// overlap). gapBetweenBoundaries advances BOTH past P_N (an
		// uncompactable gap — the (P_N, S] window lives only in the
		// full). rotatedOverlap (ADR-0067) anchors the full at S = P_N+50
		// but KEEPS the incrementals starting at P_N (the overlap).
		pN := prevEndLSN
		fullAnchorLSN := pN
		incrStartLSN := pN
		switch {
		case i > 0 && opts.gapBetweenBoundaries:
			fullAnchorLSN, incrStartLSN = pN+50, pN+50
		case i > 0 && opts.rotatedOverlap:
			fullAnchorLSN, incrStartLSN = pN+50, pN
		}
		// Full is a snapshot at fullAnchorLSN: Start == End == fullAnchorLSN.
		full := compactSeedFullManifest(t, segCreatedAt, fullAnchorLSN, chainEnc)
		segStore := newPrefixedStore(store, dir)
		if err := writeManifestAt(context.Background(), segStore, ManifestFileName, full); err != nil {
			t.Fatalf("seed full %d: %v", i, err)
		}
		// Two incrementals stepping LSN forward by 50 each.
		incrPaths := []string{}
		curLSN := incrStartLSN
		for j := 1; j <= 2; j++ {
			incrCreatedAt := segCreatedAt.Add(time.Duration(j) * 10 * time.Minute)
			nextLSN := curLSN + 50
			ip := fmt.Sprintf("manifests/incr-%05d-seg%d-%d.json", j, i, j)
			im := compactSeedIncrementalManifest(t, incrCreatedAt, curLSN, nextLSN)
			// Seed a tiny change chunk file the manifest references,
			// so segmentByteTotal has something to read.
			chunkPath := fmt.Sprintf("chunks/_changes/seg%d-incr%d.jsonl.gz", i, j)
			body := []byte(fmt.Sprintf("seg%d-incr%d-body", i, j))
			if err := segStore.Put(context.Background(), chunkPath, bytes.NewReader(body)); err != nil {
				t.Fatalf("seed change chunk: %v", err)
			}
			im.ChangeChunks = []*irbackup.ChunkInfo{{File: chunkPath, RowCount: 1, SHA256: ""}}
			if err := writeManifestAt(context.Background(), segStore, ip, im); err != nil {
				t.Fatalf("seed incr (%d,%d): %v", i, j, err)
			}
			incrPaths = append(incrPaths, ip)
			curLSN = nextLSN
		}
		prevEndLSN = curLSN

		segEntry := LineageSegment{
			SegmentID:        full.BackupID,
			Dir:              dir,
			FullManifestPath: ManifestFileName,
			Incrementals:     incrPaths,
			StartPosition:    full.EndPosition,
			EndPosition:      ir.Position{Engine: "postgres", Token: lsnToken(curLSN)},
			Codec:            codec,
		}
		if i > 0 && opts.rotatedOverlap {
			// ADR-0067: incrementals kept the (P_N, S] overlap; record
			// P_N as the earliest incremental coverage so the lineage is
			// contiguous with the prior segment (prior.End == this).
			segEntry.IncrementalCoverageStart = ir.Position{Engine: "postgres", Token: lsnToken(pN)}
		}
		// Cap every segment except the last (the lineage's open
		// segment). For compact tests we mostly want closed segments
		// — but a never-rotated last segment is fine; the eligibility
		// floor allows compact across any retained range.
		if i < n-1 {
			cappedAt = segCreatedAt.Add(gaps[i] - time.Minute)
			segEntry.CappedAt = &cappedAt
			segEntry.CapReason = rotationReasonAge
		}
		cat.Segments = append(cat.Segments, segEntry)

		if i < len(gaps) {
			cumulative += gaps[i]
		}
	}
	if err := writeLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatalf("write lineage catalog: %v", err)
	}
}

func compactSeedFullManifest(t *testing.T, createdAt time.Time, lsn uint64, enc *irbackup.ChainEncryption) *irbackup.Manifest {
	t.Helper()
	m := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		SourceEngine:  "postgres",
		CreatedAt:     createdAt,
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: lsnToken(lsn)},
		PartialState:  irbackup.BackupStateComplete,
	}
	if enc != nil {
		m.ChainEncryption = enc
	}
	m.BackupID = irbackup.ComputeBackupID(m)
	return m
}

func compactSeedIncrementalManifest(t *testing.T, createdAt time.Time, startLSN, endLSN uint64) *irbackup.Manifest {
	t.Helper()
	m := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		SourceEngine:  "postgres",
		CreatedAt:     createdAt,
		Kind:          irbackup.BackupKindIncremental,
		StartPosition: ir.Position{Engine: "postgres", Token: lsnToken(startLSN)},
		EndPosition:   ir.Position{Engine: "postgres", Token: lsnToken(endLSN)},
		PartialState:  irbackup.BackupStateComplete,
	}
	m.BackupID = irbackup.ComputeBackupID(m)
	return m
}

func lsnToken(lsn uint64) string {
	return fmt.Sprintf(`{"slot":"s","lsn":"0/%X"}`, lsn)
}

func seedTwoSegmentLineageWithEncryption(t *testing.T, store irbackup.Store, now time.Time, encA, encB *irbackup.ChainEncryption) {
	t.Helper()
	seedSegmentsWithGapsOpts(t, store, now, []time.Duration{time.Hour}, segmentSeedOpts{
		encPerSegment: []*irbackup.ChainEncryption{encA, encB},
	})
}

func seedTwoSegmentLineageWithCodecs(t *testing.T, store irbackup.Store, now time.Time, codecA, codecB Codec) {
	t.Helper()
	seedSegmentsWithGapsOpts(t, store, now, []time.Duration{time.Hour}, segmentSeedOpts{
		codecsPerSegment: []Codec{codecA, codecB},
	})
}

func seedTwoSegmentLineageWithGap(t *testing.T, store irbackup.Store, now time.Time) {
	t.Helper()
	seedSegmentsWithGapsOpts(t, store, now, []time.Duration{time.Hour}, segmentSeedOpts{
		gapBetweenBoundaries: true,
	})
}
