// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// ADR-0067 unit pins: contiguous-segment rotation handoff. A
// rotation-opened segment keeps the (P_N, S] overlap in its incrementals
// and records IncrementalCoverageStart = P_N, so the lineage is
// born-contiguous and compactable. These tests pin the resolver, the
// relaxed within-segment full->first-incremental boundary, and the
// rotated-overlap restore-walk.

func pgPos(lsn string) ir.Position {
	return ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"` + lsn + `"}`}
}

func TestIncrementalCoverageStartOrStart(t *testing.T) {
	seg := LineageSegment{StartPosition: pgPos("0/300")}
	if got := seg.incrementalCoverageStartOrStart(); got != seg.StartPosition {
		t.Errorf("unset: got %+v; want StartPosition %+v", got, seg.StartPosition)
	}
	seg.IncrementalCoverageStart = pgPos("0/200")
	if got := seg.incrementalCoverageStartOrStart(); got != seg.IncrementalCoverageStart {
		t.Errorf("set: got %+v; want IncrementalCoverageStart %+v", got, seg.IncrementalCoverageStart)
	}
}

// TestValidateFirstIncrementalBoundary is the table pin for the relaxed
// within-segment full->first-incremental boundary (ADR-0067). It
// exercises every branch: legacy-empty, non-rotated exact/mismatch,
// rotated overlap (with + without comparator), tampered first
// incremental, and a forward gap.
func TestValidateFirstIncrementalBoundary(t *testing.T) {
	cmp := &fakeMonotonicEngine{order: map[string]int{
		pgPos("0/100").Token: 100,
		pgPos("0/200").Token: 200,
		pgPos("0/300").Token: 300,
		pgPos("0/400").Token: 400,
	}}
	mysqlPN := ir.Position{Engine: "mysql", Token: "PN"}
	tests := []struct {
		name       string
		cmp        ir.PositionMonotonicChecker
		fullEnd    ir.Position
		recorded   ir.Position // raw IncrementalCoverageStart
		firstStart ir.Position
		wantErr    string // substring; "" = expect success
	}{
		{"legacy empty full skips", cmp, ir.Position{}, ir.Position{}, pgPos("0/200"), ""},
		{"non-rotated exact ok", cmp, pgPos("0/300"), ir.Position{}, pgPos("0/300"), ""},
		{"non-rotated mismatch refuses", cmp, pgPos("0/300"), ir.Position{}, pgPos("0/200"), "boundary mismatch"},
		{"rotated overlap ok (cmp)", cmp, pgPos("0/300"), pgPos("0/200"), pgPos("0/200"), ""},
		{"rotated tampered first incr refuses", cmp, pgPos("0/300"), pgPos("0/200"), pgPos("0/400"), "boundary mismatch"},
		{"rotated forward gap refuses (cmp)", cmp, pgPos("0/300"), pgPos("0/400"), pgPos("0/400"), "boundary mismatch"},
		{"rotated overlap ok (no cmp, same engine)", nil, pgPos("0/300"), pgPos("0/200"), pgPos("0/200"), ""},
		{"rotated no-cmp engine mismatch refuses", nil, pgPos("0/300"), mysqlPN, mysqlPN, "engine"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateFirstIncrementalBoundary(tc.cmp, tc.fullEnd, tc.recorded, tc.firstStart, "seg test")
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("got err %v; want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got err %v; want substring %q", err, tc.wantErr)
			}
		})
	}
}

// rotatedSecondSegment builds a 2-segment lineage in the ADR-0067 shape:
// seg0 (root) ends at P_N; seg1 is rotation-opened with its full anchored
// at S (> P_N), IncrementalCoverageStart == P_N, and its first
// incremental starting at P_N (the kept overlap). firstIncrStart lets a
// caller inject a forward-gap (start AHEAD of the full) for the refusal
// pin. Returns the store + a comparator ranking the LSNs.
func rotatedSecondSegment(t *testing.T, firstIncrLSN, coverageLSN string) (irbackup.BackupStore, ir.PositionMonotonicChecker) {
	t.Helper()
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	f0 := makeManifest(t, irbackup.BackupKindFull, nil, "0/100")
	i0 := makeManifest(t, irbackup.BackupKindIncremental, f0, "0/200") // P_N = 0/200
	s0 := seedSegment(t, store, "", f0, []*irbackup.Manifest{i0}, CodecGzip)

	// seg1 full anchored at S = 0/300 (> P_N).
	f1 := makeManifest(t, irbackup.BackupKindFull, nil, "0/300")
	// seg1 first incremental starts at firstIncrLSN (P_N for the overlap
	// case) and chains off the full.
	i1 := makeManifest(t, irbackup.BackupKindIncremental, f1, "0/350")
	i1.StartPosition = pgPos(firstIncrLSN)
	i1.BackupID = irbackup.ComputeBackupID(i1)
	s1 := seedSegment(t, store, "seg-1", f1, []*irbackup.Manifest{i1}, CodecNone)
	// ADR-0067: record the kept-overlap coverage start.
	s1.IncrementalCoverageStart = pgPos(coverageLSN)

	capt := time.Now().UTC()
	s0.CappedAt, s0.CapReason = &capt, rotationReasonChainLength
	cat := &LineageCatalog{FormatVersion: 1, SourceEngine: "postgres", Segments: []LineageSegment{s0, s1}}
	if err := writeLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}
	cmp := &fakeMonotonicEngine{order: map[string]int{
		pgPos("0/100").Token: 100,
		pgPos("0/200").Token: 200,
		pgPos("0/300").Token: 300,
		pgPos("0/350").Token: 350,
		pgPos("0/400").Token: 400,
	}}
	return store, cmp
}

// TestBuildLineageChain_RotatedOverlap_OK: a rotated segment whose first
// incremental starts at P_N (< the full anchor S) restores cleanly — the
// lineage is contiguous (prior.End == seg1.IncrementalCoverageStart) and
// the within-segment overlap is tolerated.
func TestBuildLineageChain_RotatedOverlap_OK(t *testing.T) {
	store, cmp := rotatedSecondSegment(t, "0/200", "0/200") // first incr at P_N
	chain, err := buildLineageChain(context.Background(), store, cmp)
	if err != nil {
		t.Fatalf("buildLineageChain: %v", err)
	}
	if len(chain) != 4 { // f0, i0, f1, i1
		t.Fatalf("chain len = %d; want 4", len(chain))
	}
}

// TestBuildLineageChain_RotatedForwardGap_Refuses: a rotated segment
// whose first incremental starts AHEAD of the full's anchor leaves a
// forward gap and is refused loudly (no silent partial — DR data).
func TestBuildLineageChain_RotatedForwardGap_Refuses(t *testing.T) {
	store, cmp := rotatedSecondSegment(t, "0/400", "0/400") // first incr AHEAD of S=0/300
	_, err := buildLineageChain(context.Background(), store, cmp)
	if err == nil || !strings.Contains(err.Error(), "boundary mismatch") {
		t.Fatalf("got err %v; want forward-gap boundary refusal", err)
	}
}
