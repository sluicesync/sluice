// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
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

	cat, ok, err := lineage.LoadLineageCatalog(context.Background(), store)
	if err != nil || !ok {
		t.Fatalf("post-compact lineage.LoadLineageCatalog: ok=%v err=%v", ok, err)
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
	mergedStore := merged.Store(store)
	if ex, _ := mergedStore.Exists(context.Background(), lineage.ManifestFileName); !ex {
		t.Error("merged segment's full manifest is missing under its dir")
	}
	for _, ip := range merged.Incrementals {
		if ex, _ := mergedStore.Exists(context.Background(), ip); !ex {
			t.Errorf("merged segment incremental %q is missing under its dir", ip)
		}
	}
	// Source dirs are swept (post-commit cleanup).
	if ex, _ := store.Exists(context.Background(), lineage.ManifestFileName); ex {
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
	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
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
	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
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
	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
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
	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if len(cat.Segments) != 2 {
		t.Errorf("post-refusal segments = %d; want 2 (no mutation)", len(cat.Segments))
	}
}

// TestCompactChain_CodecBoundary_RefusesLoudly: a 2-segment group with
// different codecs refuses loudly.
func TestCompactChain_CodecBoundary_RefusesLoudly(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	seedTwoSegmentLineageWithCodecs(t, store, now, blobcodec.CodecGzip, blobcodec.CodecZstd)

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

// TestCompactChain_PositionGap_SplitsNotRefuses (ADR-0087, replaces the
// pre-ADR-0087 TestCompactChain_PositionGap_RefusesLoudly): a 2-segment
// lineage where seg[1] is the stamp-less rotation-born Bug-139 shape
// (seg[1] resolves to its full anchor S, a gap past seg[0]'s EndPosition)
// is no longer a whole-run refusal. Compact SPLITS at the gap: seg[0] is
// its own size-1 group (a no-op), seg[1] is its own size-1 group, nothing
// merges, no error, and the run succeeds.
func TestCompactChain_PositionGap_SplitsNotRefuses(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	// seg1 is the trailing zero-incremental rotation-born OPEN segment.
	seedSegmentsWithGapsOpts(t, store, now, []time.Duration{time.Hour}, segmentSeedOpts{
		zeroIncrStampless: map[int]bool{1: true},
		openLastSegment:   true,
	})

	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow: 2 * time.Hour,
	})
	if err != nil {
		t.Fatalf("CompactChain across a coverage gap: %v; want a clean split (ADR-0087), not a refusal", err)
	}
	if res.GroupsMerged != 0 {
		t.Errorf("GroupsMerged = %d; want 0 (the gap splits seg0 and seg1 into separate size-1 groups)", res.GroupsMerged)
	}
	// The lineage is untouched (both segments survive, nothing merged).
	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if len(cat.Segments) != 2 {
		t.Errorf("post-compact segments = %d; want 2 (no merge across the gap)", len(cat.Segments))
	}
}

// TestCompactChain_Bug139_GapSplit is the table-driven Bug-139 pin
// (ADR-0087): every lineage shape that carries a coverage-gap boundary
// (a stamp-less rotation-born segment) compacts by SPLITTING at the gap
// rather than refusing the whole run, and the merged content/plan are
// correct. It pins the class — trailing OPEN, trailing CAPPED, mid-chain
// stamp-less-with-incrementals, and multiple gaps — not one shape.
func TestCompactChain_Bug139_GapSplit(t *testing.T) {
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		// gaps between consecutive segments (len == segments-1).
		gaps []time.Duration
		opts segmentSeedOpts
		// wantMerged is the expected GroupsMerged.
		wantMerged int
		// wantSegmentsAfter is the expected post-compact segment count.
		wantSegmentsAfter int
	}{
		{
			// The exact idle-stop shape: a trailing zero-incremental
			// rotation-born OPEN segment. seg0+seg1+seg2 would window
			// together, but seg2 (the stamp-less open segment) splits off:
			// seg0+seg1 merge (2→1), seg2 stays its own group.
			name: "trailing zero-incr rotation-born OPEN",
			gaps: []time.Duration{time.Hour, time.Hour},
			opts: segmentSeedOpts{
				zeroIncrStampless: map[int]bool{2: true},
				openLastSegment:   true,
			},
			wantMerged:        1,
			wantSegmentsAfter: 2, // merged(seg0,seg1) + seg2
		},
		{
			// Same, but the trailing stamp-less segment is CAPPED (a
			// later session capped it without committing into it). Same
			// split outcome.
			name: "trailing zero-incr rotation-born CAPPED",
			gaps: []time.Duration{time.Hour, time.Hour},
			opts: segmentSeedOpts{
				zeroIncrStampless: map[int]bool{2: true},
			},
			wantMerged:        1,
			wantSegmentsAfter: 2,
		},
		{
			// Two stamp-less gap boundaries in one window group: seg1 and
			// seg3 are both stamp-less rotation-born. The gaps sit BEFORE
			// seg1 and BEFORE seg3, so the window splits as
			// {seg0} | {seg1, seg2} | {seg3}: seg1→seg2 is itself
			// contiguous, so that middle group merges (2→1); seg0 and seg3
			// stay size-1. One merge, three post-compact segments.
			name: "multiple gap boundaries in one window",
			gaps: []time.Duration{time.Hour, time.Hour, time.Hour},
			opts: segmentSeedOpts{
				zeroIncrStampless: map[int]bool{1: true, 3: true},
				openLastSegment:   true,
			},
			wantMerged:        1,
			wantSegmentsAfter: 3,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newMemStore()
			seedSegmentsWithGapsOpts(t, store, now, tc.gaps, tc.opts)
			res, err := CompactChain(context.Background(), store, CompactOpts{
				MergeWindow: 24 * time.Hour, // window captures every segment
				Now:         func() time.Time { return now.Add(100 * time.Hour) },
			})
			if err != nil {
				t.Fatalf("CompactChain: %v; want a clean split (ADR-0087)", err)
			}
			if res.GroupsMerged != tc.wantMerged {
				t.Errorf("GroupsMerged = %d; want %d", res.GroupsMerged, tc.wantMerged)
			}
			cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
			if len(cat.Segments) != tc.wantSegmentsAfter {
				t.Errorf("post-compact segments = %d; want %d", len(cat.Segments), tc.wantSegmentsAfter)
			}
			// The post-compact lineage must still restore-walk cleanly.
			if _, err := lineage.BuildLineageChain(context.Background(), store, nil); err != nil {
				t.Errorf("post-compact lineage.BuildLineageChain: %v", err)
			}
		})
	}
}

// TestCompactChain_Bug139_MidChainStampless_SplitsAroundBoundary: the
// bd0c5e94 rig shape — a mid-chain stamp-less segment that DOES carry
// incrementals (a later resumed session committed rollovers into it and
// even capped it), but those incrementals start at S (the full anchor),
// not P_N, so no IncrementalCoverageStart was ever stamped. The boundary
// before it is a gap; compact splits into two merged groups around it.
//
// seg0 ─┬─ contiguous ─ seg1 ── GAP ── seg2 ─ contiguous ─ seg3
// expect: {seg0,seg1} merge, {seg2,seg3} merge → 2 merged groups, 2 segments.
func TestCompactChain_Bug139_MidChainStampless_SplitsAroundBoundary(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	// seg2 is rotation-born + has incrementals but no stamp: model it with
	// gapBetweenBoundaries on index 2 only. gapBetweenBoundaries advances
	// BOTH the full anchor AND the incrementals past P_N and records no
	// stamp — exactly the "incrementals start at S, no stamp" shape.
	seedSegmentsWithGapsOpts(t, store, now, []time.Duration{time.Hour, time.Hour, time.Hour}, segmentSeedOpts{
		gapAtSegment: map[int]bool{2: true},
	})
	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow: 24 * time.Hour,
		Now:         func() time.Time { return now.Add(100 * time.Hour) },
	})
	if err != nil {
		t.Fatalf("CompactChain: %v; want a clean split into two merged groups", err)
	}
	if res.GroupsMerged != 2 {
		t.Errorf("GroupsMerged = %d; want 2 (one merged group each side of the gap)", res.GroupsMerged)
	}
	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if len(cat.Segments) != 2 {
		t.Errorf("post-compact segments = %d; want 2", len(cat.Segments))
	}
	if _, err := lineage.BuildLineageChain(context.Background(), store, nil); err != nil {
		t.Errorf("post-compact lineage.BuildLineageChain: %v", err)
	}
}

// TestCompactChain_FullyContiguousRotated_MergesExactly: a fully
// contiguous (stamped) rotated 4-segment chain merges 4→1 exactly as
// pre-ADR-0087 — no split, no WARN. This is the no-regression guard for
// the ADR-0067 happy path.
func TestCompactChain_FullyContiguousRotated_MergesExactly(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	seedSegmentsWithGapsOpts(t, store, now, []time.Duration{time.Hour, time.Hour, time.Hour}, segmentSeedOpts{
		rotatedOverlap: true,
	})
	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow: 24 * time.Hour,
		Now:         func() time.Time { return now.Add(100 * time.Hour) },
	})
	if err != nil {
		t.Fatalf("CompactChain on a fully-contiguous rotated chain: %v", err)
	}
	if res.GroupsMerged != 1 || res.SegmentsRemoved != 3 {
		t.Errorf("GroupsMerged=%d SegmentsRemoved=%d; want 1,3 (4→1 exact merge)",
			res.GroupsMerged, res.SegmentsRemoved)
	}
	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if len(cat.Segments) != 1 {
		t.Errorf("post-compact segments = %d; want 1", len(cat.Segments))
	}
}

// TestCompactChain_Bug139_DryRunSplits: --dry-run produces the same
// subdivided plan (no merge across the gap) without touching storage.
func TestCompactChain_Bug139_DryRunSplits(t *testing.T) {
	store := newMemStore()
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	seedSegmentsWithGapsOpts(t, store, now, []time.Duration{time.Hour, time.Hour}, segmentSeedOpts{
		zeroIncrStampless: map[int]bool{2: true},
		openLastSegment:   true,
	})
	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow: 24 * time.Hour,
		DryRun:      true,
	})
	if err != nil {
		t.Fatalf("CompactChain dry-run: %v", err)
	}
	if res.GroupsMerged != 1 {
		t.Errorf("dry-run GroupsMerged = %d; want 1 (seg0+seg1 merge; seg2 split off)", res.GroupsMerged)
	}
	// The plan reflects the subdivision: one size-2 mergeable group and
	// one size-1 (skipped) group for the stamp-less segment.
	var size1, size2 int
	for _, g := range res.Plan {
		switch len(g.SourceSegmentIDs) {
		case 1:
			size1++
		case 2:
			size2++
		}
	}
	if size2 != 1 || size1 != 1 {
		t.Errorf("dry-run plan groups: size2=%d size1=%d; want 1 size-2 (merge) + 1 size-1 (split-off)", size2, size1)
	}
	// Dry-run touched nothing: both segments survive on disk.
	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if len(cat.Segments) != 3 {
		t.Errorf("post-dry-run segments = %d; want 3 (untouched)", len(cat.Segments))
	}
}

// TestCompactChain_RotatedOverlap_Merges is the unit-level Bug 95 pin:
// a chain rotated in the ADR-0067 shape (each non-root segment keeps the
// (P_N, S] overlap and records IncrementalCoverageStart = P_N) is
// CONTIGUOUS and so MERGES — where the pre-ADR-0087 gap shape
// (TestCompactChain_PositionGap_SplitsNotRefuses) now splits instead.
// Pins that the contiguity check keys off IncrementalCoverageStart, and
// that the merged lineage still walks (restore-build) cleanly.
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
	if _, err := lineage.BuildLineageChain(context.Background(), store, nil); err != nil {
		t.Errorf("post-compact lineage.BuildLineageChain: %v", err)
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
	cat, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
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
// remains structurally valid for restore — lineage.BuildLineageChain must
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
	if _, err := lineage.BuildLineageChain(context.Background(), store, nil); err != nil {
		t.Errorf("post-compact lineage.BuildLineageChain: %v; want valid lineage", err)
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

// failingDeleteStore wraps memStore, failing Delete for any path with
// a seeded prefix — models a store where the orphan-sweep deletes
// persistently fail (permissions, bucket policy) while everything
// else works.
type failingDeleteStore struct {
	*memStore
	failPrefixes []string
}

func (s *failingDeleteStore) Delete(ctx context.Context, path string) error {
	for _, p := range s.failPrefixes {
		if strings.HasPrefix(path, p) {
			return fmt.Errorf("seeded delete failure for %s", path)
		}
	}
	return s.memStore.Delete(ctx, path)
}

// TestCompactChain_OrphanSweepDeleteFailure_Warns pins the Q-1 fix:
// per-file Delete failures inside the post-commit orphan sweep must
// reach the sweep WARN (naming the failing path) — never vanish
// silently — while the compaction itself still succeeds (the sweep is
// best-effort by contract; the catalog swap already committed).
func TestCompactChain_OrphanSweepDeleteFailure_Warns(t *testing.T) {
	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	// Fail the root segment's manifest delete AND every delete under the
	// second source's sub-dir — exercising BOTH sweep helpers.
	store := &failingDeleteStore{
		memStore:     newMemStore(),
		failPrefixes: []string{lineage.ManifestFileName, "seg-1/"},
	}
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	seedTwoSegmentLineage(t, store, now, time.Hour)

	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow:  2 * time.Hour,
		Now:          func() time.Time { return now.Add(10 * time.Hour) },
		newSegmentID: func() string { return "merged-warn" },
	})
	if err != nil {
		t.Fatalf("CompactChain must stay best-effort on sweep failures; got %v", err)
	}
	if res.GroupsMerged != 1 {
		t.Errorf("GroupsMerged = %d; want 1 (sweep failure must not block the merge)", res.GroupsMerged)
	}

	logs := logBuf.String()
	if !strings.Contains(logs, "orphan sweep failed") {
		t.Fatalf("no orphan-sweep WARN emitted; logs:\n%s", logs)
	}
	if !strings.Contains(logs, lineage.ManifestFileName) {
		t.Errorf("WARN does not name the failing root manifest path %q; logs:\n%s", lineage.ManifestFileName, logs)
	}
	if !strings.Contains(logs, "seg-1/") {
		t.Errorf("WARN does not name the failing seg-1/ paths; logs:\n%s", logs)
	}
}

// TestSweepSegmentSubdir_CollectsDeleteFailures pins the helper-level
// contract directly: every delete is attempted (best-effort) and each
// failure is joined into the returned error, naming its path.
func TestSweepSegmentSubdir_CollectsDeleteFailures(t *testing.T) {
	mem := newMemStore()
	for _, p := range []string{"seg-x/manifest.json", "seg-x/fail-me.json", "seg-x/ok.json"} {
		if err := mem.Put(context.Background(), p, bytes.NewReader([]byte("x"))); err != nil {
			t.Fatalf("seed %s: %v", p, err)
		}
	}
	store := &failingDeleteStore{memStore: mem, failPrefixes: []string{"seg-x/fail-me.json"}}

	err := sweepSegmentSubdir(context.Background(), store, "seg-x")
	if err == nil {
		t.Fatal("sweepSegmentSubdir returned nil despite a failing delete")
	}
	if !strings.Contains(err.Error(), "seg-x/fail-me.json") {
		t.Errorf("error does not name the failing path: %v", err)
	}
	// The other files were still attempted and deleted (best-effort).
	for _, p := range []string{"seg-x/manifest.json", "seg-x/ok.json"} {
		if ex, _ := mem.Exists(context.Background(), p); ex {
			t.Errorf("%s survives the sweep; deletes after a failure must still be attempted", p)
		}
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
	codecsPerSegment []blobcodec.Codec
	// encPerSegment, when len > 0, attaches ChainEncryption to each
	// segment's full.
	encPerSegment []*irbackup.ChainEncryption
	// gapBetweenBoundaries, when true, deliberately advances each
	// segment's StartPosition past the prior's EndPosition (creating
	// the contiguity-violation shape).
	gapBetweenBoundaries bool
	// gapAtSegment marks the SPECIFIC non-root segment indices that take
	// the gap shape (full anchor AND incrementals advanced past P_N, no
	// stamp) — models a mid-chain rotation-born segment that received
	// incrementals starting at S but was never stamped (the bd0c5e94 rig
	// shape). Unlike gapBetweenBoundaries (every boundary), this gaps only
	// the listed segments.
	gapAtSegment map[int]bool
	// rotatedOverlap, when true, produces the ADR-0067 rotation shape for
	// each non-root segment: the full is anchored at S = prior.End + 50
	// (a gap, like gapBetweenBoundaries), BUT the segment's incrementals
	// KEEP the (P_N, S] overlap — they start at P_N (== prior.End) — and
	// the segment records IncrementalCoverageStart = P_N. Such a lineage
	// is contiguous (prior.End == cur.IncrementalCoverageStart) and so
	// compactable, unlike the gapBetweenBoundaries shape.
	rotatedOverlap bool

	// zeroIncrStampless, when non-nil, marks segment indices that take
	// the exact Bug-139 shape: rotation-born (full anchored at S =
	// prior.End + 50), ZERO incrementals, and NO IncrementalCoverageStart
	// stamp — so the segment resolves to its full anchor S, a gap past the
	// prior segment's P_N. Such a segment's EndPosition stays at S (== its
	// StartPosition). Only valid on non-root segments (i > 0).
	zeroIncrStampless map[int]bool

	// openLastSegment, when true, leaves the LAST segment uncapped (the
	// lineage's open segment). Used with zeroIncrStampless to model the
	// trailing idle-stop OPEN segment.
	openLastSegment bool
}

func seedSegmentsWithGapsOpts(t *testing.T, store irbackup.Store, base time.Time, gaps []time.Duration, opts segmentSeedOpts) {
	t.Helper()
	n := len(gaps) + 1
	cumulative := time.Duration(0)
	prevEndLSN := uint64(100)
	cappedAt := time.Time{} // overwritten per loop

	// First, build segments and write their conventional files. Then
	// hand-author the lineage.json describing the multi-segment shape.
	cat := &lineage.Catalog{
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
		codec := blobcodec.CodecGzip
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
		stampless := i > 0 && opts.zeroIncrStampless[i]
		switch {
		case i > 0 && (opts.gapBetweenBoundaries || opts.gapAtSegment[i]):
			fullAnchorLSN, incrStartLSN = pN+50, pN+50
		case i > 0 && opts.rotatedOverlap:
			fullAnchorLSN, incrStartLSN = pN+50, pN
		case stampless:
			// Bug-139 shape: rotation-born full anchored at S = P_N+50,
			// but the segment never received an incremental in its
			// creating session — so no stamp and no incrementals.
			fullAnchorLSN, incrStartLSN = pN+50, pN+50
		}
		// Full is a snapshot at fullAnchorLSN: Start == End == fullAnchorLSN.
		full := compactSeedFullManifest(t, segCreatedAt, fullAnchorLSN, chainEnc)
		segStore := lineage.NewPrefixedStore(store, dir)
		if err := lineage.WriteManifestAt(context.Background(), segStore, lineage.ManifestFileName, full); err != nil {
			t.Fatalf("seed full %d: %v", i, err)
		}
		// Two incrementals stepping LSN forward by 50 each — unless this is
		// the stamp-less zero-incremental Bug-139 segment, which has none.
		incrPaths := []string{}
		curLSN := incrStartLSN
		if !stampless {
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
				if err := lineage.WriteManifestAt(context.Background(), segStore, ip, im); err != nil {
					t.Fatalf("seed incr (%d,%d): %v", i, j, err)
				}
				incrPaths = append(incrPaths, ip)
				curLSN = nextLSN
			}
		}
		prevEndLSN = curLSN

		segEntry := lineage.Segment{
			SegmentID:        full.BackupID,
			Dir:              dir,
			FullManifestPath: lineage.ManifestFileName,
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
		// floor allows compact across any retained range. openLastSegment
		// leaves the trailing segment uncapped (the idle-stop OPEN shape).
		if i < n-1 || (i == n-1 && !opts.openLastSegment && stampless) {
			cappedAt = segCreatedAt.Add(time.Hour)
			if i < len(gaps) {
				cappedAt = segCreatedAt.Add(gaps[i] - time.Minute)
			}
			segEntry.CappedAt = &cappedAt
			segEntry.CapReason = rotationReasonAge
		}
		cat.Segments = append(cat.Segments, segEntry)

		if i < len(gaps) {
			cumulative += gaps[i]
		}
	}
	if err := lineage.WriteLineageCatalog(context.Background(), store, cat); err != nil {
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

func seedTwoSegmentLineageWithCodecs(t *testing.T, store irbackup.Store, now time.Time, codecA, codecB blobcodec.Codec) {
	t.Helper()
	seedSegmentsWithGapsOpts(t, store, now, []time.Duration{time.Hour}, segmentSeedOpts{
		codecsPerSegment: []blobcodec.Codec{codecA, codecB},
	})
}
