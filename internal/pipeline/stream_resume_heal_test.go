// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRotationBoundaryResumeStart is the unit pin for the ADR-0087
// Bug-139 resume heal. A resume that lands on a rotation-born OPEN
// segment with ZERO recorded incrementals (the creating session stopped
// or crashed at the rotation boundary before committing its first
// incremental) must resume from the prior segment's EndPosition (P_N),
// not the open segment's full anchor (S) — so the first incremental
// stamps IncrementalCoverageStart = P_N and the lineage becomes
// born-contiguous and compactable. Every negative case keeps today's
// behaviour (ok=false → caller resumes from S).
func TestRotationBoundaryResumeStart(t *testing.T) {
	now := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)

	// pNTokenAfterSeg builds the EndPosition token the seed helper assigns
	// to a 2-incremental segment whose incrementals start at startLSN.
	// seedSegmentsWithGapsOpts steps each incremental +50; the root's
	// incrementals start at 100, so the root ends at 200.
	openShape := func(t *testing.T) (ir.Position, ir.Position) {
		t.Helper()
		store := newMemStore()
		// seg0 (root, contiguous) + seg1 = trailing zero-incremental
		// rotation-born OPEN segment.
		seedSegmentsWithGapsOpts(t, store, now, []time.Duration{time.Hour}, segmentSeedOpts{
			zeroIncrStampless: map[int]bool{1: true},
			openLastSegment:   true,
		})
		cat, _, _ := loadLineageCatalog(context.Background(), store)
		open := cat.Segments[1]
		prior := cat.Segments[0]
		// The caller passes startPos = parent.EndPosition; for the
		// stamp-less open segment the resolved parent is its full, whose
		// EndPosition == the segment's StartPosition (S).
		healed, _, ok := rotationBoundaryResumeStart(context.Background(), store, open.StartPosition)
		if !ok {
			t.Fatalf("rotationBoundaryResumeStart: ok=false; want true for the Bug-139 open shape")
		}
		return healed, prior.EndPosition
	}

	t.Run("rotation-born zero-incr open resumes from P_N", func(t *testing.T) {
		healed, priorEnd := openShape(t)
		if healed != priorEnd {
			t.Errorf("healed startPos = %+v; want prior.EndPosition (P_N) %+v", healed, priorEnd)
		}
	})

	// The downstream payoff: a first incremental committed at the healed
	// startPos (P_N) makes updateLineageForManifest stamp the open
	// segment's IncrementalCoverageStart = P_N, healing the gap so the
	// next compact merges the boundary.
	t.Run("first incremental at P_N stamps IncrementalCoverageStart", func(t *testing.T) {
		store := newMemStore()
		seedSegmentsWithGapsOpts(t, store, now, []time.Duration{time.Hour}, segmentSeedOpts{
			zeroIncrStampless: map[int]bool{1: true},
			openLastSegment:   true,
		})
		cat, _, _ := loadLineageCatalog(context.Background(), store)
		open := cat.Segments[1]
		pN := cat.Segments[0].EndPosition

		healed, _, ok := rotationBoundaryResumeStart(context.Background(), store, open.StartPosition)
		if !ok || healed != pN {
			t.Fatalf("resume heal: ok=%v healed=%+v; want ok + P_N %+v", ok, healed, pN)
		}

		// Commit a first incremental starting at the healed position (P_N),
		// chained off the open segment's full, ending a step past S.
		first := compactSeedIncrementalManifest(t, now.Add(2*time.Hour), 0, 0)
		first.StartPosition = healed
		first.EndPosition = ir.Position{Engine: open.StartPosition.Engine, Token: open.StartPosition.Token + "-end"}
		first.BackupID = ""
		ip := "manifests/incr-00001-heal.json"
		segStore := open.store(store)
		if err := writeManifestAt(context.Background(), segStore, ip, first); err != nil {
			t.Fatal(err)
		}
		if err := updateLineageForManifest(context.Background(), store, first, ip, open.Codec); err != nil {
			t.Fatal(err)
		}

		got, _, _ := loadLineageCatalog(context.Background(), store)
		stamp := got.Segments[1].IncrementalCoverageStart
		if stamp != pN {
			t.Errorf("IncrementalCoverageStart = %+v; want P_N %+v (the segment is now born-contiguous)", stamp, pN)
		}
	})

	t.Run("segment 0 (no prior) keeps S", func(t *testing.T) {
		store := newMemStore()
		seedSegmentsWithGapsOpts(t, store, now, nil, segmentSeedOpts{}) // single-segment lineage
		cat, _, _ := loadLineageCatalog(context.Background(), store)
		_, _, ok := rotationBoundaryResumeStart(context.Background(), store, cat.Segments[0].StartPosition)
		if ok {
			t.Error("ok=true for a single-segment lineage; want false (no prior segment)")
		}
	})

	t.Run("open segment with incrementals keeps S", func(t *testing.T) {
		store := newMemStore()
		// seg1 is a normal rotated segment WITH incrementals (the open
		// segment has a committed rollover) — not the Bug-139 shape.
		seedSegmentsWithGapsOpts(t, store, now, []time.Duration{time.Hour}, segmentSeedOpts{
			rotatedOverlap: true,
		})
		cat, _, _ := loadLineageCatalog(context.Background(), store)
		open := cat.Segments[1]
		_, _, ok := rotationBoundaryResumeStart(context.Background(), store, open.StartPosition)
		if ok {
			t.Error("ok=true for an open segment that already has incrementals; want false")
		}
	})

	t.Run("parent not the full keeps S", func(t *testing.T) {
		store := newMemStore()
		seedSegmentsWithGapsOpts(t, store, now, []time.Duration{time.Hour}, segmentSeedOpts{
			zeroIncrStampless: map[int]bool{1: true},
			openLastSegment:   true,
		})
		cat, _, _ := loadLineageCatalog(context.Background(), store)
		// A startPos that does NOT equal the open segment's StartPosition
		// (e.g. the prior segment's End) means the resolved parent is not
		// the segment's full → no heal.
		notFull := cat.Segments[0].EndPosition
		_, _, ok := rotationBoundaryResumeStart(context.Background(), store, notFull)
		if ok {
			t.Error("ok=true when startPos != open.StartPosition; want false")
		}
	})

	t.Run("empty prior end keeps S", func(t *testing.T) {
		store := newMemStore()
		seedSegmentsWithGapsOpts(t, store, now, []time.Duration{time.Hour}, segmentSeedOpts{
			zeroIncrStampless: map[int]bool{1: true},
			openLastSegment:   true,
		})
		// Corrupt the catalog so the prior segment's EndPosition is empty.
		cat, _, _ := loadLineageCatalog(context.Background(), store)
		cat.Segments[0].EndPosition = ir.Position{}
		if err := writeLineageCatalog(context.Background(), store, cat); err != nil {
			t.Fatal(err)
		}
		_, _, ok := rotationBoundaryResumeStart(context.Background(), store, cat.Segments[1].StartPosition)
		if ok {
			t.Error("ok=true with an empty prior EndPosition; want false")
		}
	})
}
