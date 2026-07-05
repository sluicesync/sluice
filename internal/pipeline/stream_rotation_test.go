// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// fakeMonotonicEngine is a fakeCDCEngine that also implements
// [ir.PositionMonotonicChecker] so the S>=P_N hard-fail can be
// exercised deterministically (the ADR-0046 falsification test).
type fakeMonotonicEngine struct {
	fakeCDCEngine
	// order maps a token to its rank; PrecedesOrEqual(a,b) is
	// rank[a] <= rank[b]. An unknown token errors (cannot prove
	// monotonic → FSM hard-aborts).
	order map[string]int
}

func (e *fakeMonotonicEngine) PrecedesOrEqual(a, b ir.Position) (bool, error) {
	ra, oka := e.order[a.Token]
	rb, okb := e.order[b.Token]
	if !oka || !okb {
		return false, fmt.Errorf("fake: unknown token (a=%q ok=%v, b=%q ok=%v)", a.Token, oka, b.Token, okb)
	}
	return ra <= rb, nil
}

// TestAssertAnchorMonotonic_HardFailFires is THE injected-violation
// proof (ADR-0046 gotcha #1): when the new segment anchor S regresses
// before the prior segment end P_N, assertAnchorMonotonic returns a
// loud error so performRotation aborts-and-stays-open (never gaps).
func TestAssertAnchorMonotonic_HardFailFires(t *testing.T) {
	eng := &fakeMonotonicEngine{
		fakeCDCEngine: fakeCDCEngine{name: "postgres"},
		order:         map[string]int{"P_N": 100, "S_BAD": 50, "S_OK": 100, "S_AHEAD": 200},
	}
	pN := ir.Position{Engine: "postgres", Token: "P_N"}

	badS := ir.Position{Engine: "postgres", Token: "S_BAD"}
	err := assertAnchorMonotonic(eng, pN, badS)
	if err == nil || !strings.Contains(err.Error(), "S>=P_N hard-fail") || !strings.Contains(err.Error(), "regressed") {
		t.Fatalf("S<P_N: err = %v; want loud S>=P_N hard-fail", err)
	}
	if err := assertAnchorMonotonic(eng, pN, ir.Position{Engine: "postgres", Token: "S_OK"}); err != nil {
		t.Errorf("S==P_N: err = %v; want nil (contiguous boundary is valid)", err)
	}
	if err := assertAnchorMonotonic(eng, pN, ir.Position{Engine: "postgres", Token: "S_AHEAD"}); err != nil {
		t.Errorf("S>P_N: err = %v; want nil", err)
	}
}

// TestAssertAnchorMonotonic_EmptyAndEngineMismatch: the non-empty /
// engine-match guards always fire, even without a comparator.
func TestAssertAnchorMonotonic_EmptyAndEngineMismatch(t *testing.T) {
	plain := &fakeCDCEngine{name: "postgres"} // no PositionMonotonicChecker
	pN := ir.Position{Engine: "postgres", Token: "P_N"}
	if err := assertAnchorMonotonic(plain, pN, ir.Position{}); err == nil ||
		!strings.Contains(err.Error(), "anchor is empty") {
		t.Errorf("empty S: err = %v; want empty-anchor refusal", err)
	}
	if err := assertAnchorMonotonic(plain, pN, ir.Position{Engine: "mysql", Token: "x"}); err == nil ||
		!strings.Contains(err.Error(), "anchor engine") {
		t.Errorf("engine mismatch: err = %v; want engine-mismatch refusal", err)
	}
	if err := assertAnchorMonotonic(plain, pN, ir.Position{Engine: "postgres", Token: "S"}); err != nil {
		t.Errorf("no-comparator same-engine: err = %v; want nil (structural guarantee)", err)
	}
}

// TestAssertAnchorMonotonic_CannotProveIsHardFail: a comparator that
// errors (unknown token — cannot prove monotonic) is a hard-fail.
func TestAssertAnchorMonotonic_CannotProveIsHardFail(t *testing.T) {
	eng := &fakeMonotonicEngine{
		fakeCDCEngine: fakeCDCEngine{name: "postgres"},
		order:         map[string]int{"P_N": 1},
	}
	err := assertAnchorMonotonic(eng,
		ir.Position{Engine: "postgres", Token: "P_N"},
		ir.Position{Engine: "postgres", Token: "UNKNOWN"})
	if err == nil || !strings.Contains(err.Error(), "cannot prove monotonic") {
		t.Fatalf("unknown token: err = %v; want cannot-prove hard-fail", err)
	}
}

// TestShouldRotate_LengthFires: length threshold fires without
// touching the lineage (no I/O), preferred over age.
func TestShouldRotate_LengthFires(t *testing.T) {
	b := &BackupStream{RetainRotateAtChainLength: 3, Store: newMemStore()}
	if r := b.shouldRotate(context.Background(), 3, time.Now()); r != rotationReasonChainLength {
		t.Errorf("seq=3 len=3 → %q; want %q", r, rotationReasonChainLength)
	}
	if r := b.shouldRotate(context.Background(), 2, time.Now()); r != "" {
		t.Errorf("seq=2 len=3 → %q; want empty", r)
	}
}

// TestShouldRotate_AgeFromOpenSegmentFull: age measured from the OPEN
// segment's full CreatedAt (stable across stream restarts).
func TestShouldRotate_AgeFromOpenSegmentFull(t *testing.T) {
	store := newMemStore()
	created := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion, CreatedAt: created,
		SourceEngine: "postgres", Kind: irbackup.BackupKindFull,
		EndPosition:  ir.Position{Engine: "postgres", Token: "0/100"},
		PartialState: irbackup.BackupStateComplete,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	mustWriteManifest(t, store, lineage.ManifestFileName, full)
	lineage.UpdateLineageForManifestBestEffort(context.Background(), store, full, lineage.ManifestFileName, blobcodec.CodecGzip)

	b := &BackupStream{RetainRotateAt: time.Hour, Store: store}
	if r := b.shouldRotate(context.Background(), 0, created.Add(2*time.Hour)); r != rotationReasonAge {
		t.Errorf("age 2h thresh 1h → %q; want %q", r, rotationReasonAge)
	}
	if r := b.shouldRotate(context.Background(), 0, created.Add(30*time.Minute)); r != "" {
		t.Errorf("age 30m thresh 1h → %q; want empty", r)
	}
	none := &BackupStream{Store: store}
	if r := none.shouldRotate(context.Background(), 1_000_000, created.Add(99*time.Hour)); r != "" {
		t.Errorf("no thresholds → %q; want empty", r)
	}
}

// TestRecoverRotationState_PreCommitDiscards: provisional segment NOT
// in the lineage is ≤COMMIT — discard it, clear the marker, prior
// open segment intact.
func TestRecoverRotationState_PreCommitDiscards(t *testing.T) {
	store := newMemStore()
	cat := &lineage.Catalog{
		FormatVersion: 1, SourceEngine: "postgres",
		Segments: []lineage.Segment{{SegmentID: "s0", Dir: "", FullManifestPath: lineage.ManifestFileName, Codec: blobcodec.CodecGzip}},
	}
	if err := lineage.WriteLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}
	st := &rotationState{Phase: rotationPhaseBulkCopy, Reason: rotationReasonAge, ProvisionalDir: "seg-999"}
	if err := writeRotationState(context.Background(), store, st); err != nil {
		t.Fatal(err)
	}
	_ = store.Put(context.Background(), "seg-999/manifest.json", strings.NewReader("{}"))

	if err := recoverRotationState(context.Background(), store); err != nil {
		t.Fatalf("recoverRotationState: %v", err)
	}
	if ex, _ := store.Exists(context.Background(), RotationStateFileName); ex {
		t.Error("rotation_state.json not cleared after ≤COMMIT recovery")
	}
	if ex, _ := store.Exists(context.Background(), "seg-999/manifest.json"); ex {
		t.Error("provisional segment not discarded after ≤COMMIT recovery")
	}
	got, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if len(got.Segments) != 1 {
		t.Errorf("segments = %d; want 1 (prior segment intact)", len(got.Segments))
	}
}

// TestRecoverRotationState_PostCommitKeeps: provisional segment IS in
// the lineage is >COMMIT — new segment authoritative, marker cleared.
func TestRecoverRotationState_PostCommitKeeps(t *testing.T) {
	store := newMemStore()
	capped := time.Now().UTC()
	cat := &lineage.Catalog{
		FormatVersion: 1, SourceEngine: "postgres",
		Segments: []lineage.Segment{
			{
				SegmentID: "s0", Dir: "", FullManifestPath: lineage.ManifestFileName,
				CappedAt: &capped, CapReason: rotationReasonAge, Codec: blobcodec.CodecGzip,
			},
			{SegmentID: "s1", Dir: "seg-777", FullManifestPath: lineage.ManifestFileName, Codec: blobcodec.CodecGzip},
		},
	}
	if err := lineage.WriteLineageCatalog(context.Background(), store, cat); err != nil {
		t.Fatal(err)
	}
	st := &rotationState{Phase: rotationPhaseCommit, Reason: rotationReasonAge, ProvisionalDir: "seg-777"}
	if err := writeRotationState(context.Background(), store, st); err != nil {
		t.Fatal(err)
	}
	if err := recoverRotationState(context.Background(), store); err != nil {
		t.Fatalf("recoverRotationState: %v", err)
	}
	if ex, _ := store.Exists(context.Background(), RotationStateFileName); ex {
		t.Error("rotation_state.json not cleared after >COMMIT recovery")
	}
	got, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if len(got.Segments) != 2 || got.Segments[0].Open() {
		t.Errorf("post-COMMIT lineage = %+v; want 2 segments, seg0 capped", got.Segments)
	}
}

// TestRecoverRotationState_AbsentAndCorrupt: a missing marker is a
// no-op; a corrupt marker is treated as ≤COMMIT (cleared).
func TestRecoverRotationState_AbsentAndCorrupt(t *testing.T) {
	store := newMemStore()
	if err := recoverRotationState(context.Background(), store); err != nil {
		t.Fatalf("absent marker: %v", err)
	}
	store.data[RotationStateFileName] = []byte("{not json")
	if err := recoverRotationState(context.Background(), store); err != nil {
		t.Fatalf("corrupt marker: %v", err)
	}
	if ex, _ := store.Exists(context.Background(), RotationStateFileName); ex {
		t.Error("corrupt marker not cleared (should be treated as ≤COMMIT)")
	}
}
