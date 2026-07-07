// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// TestRecordEndPosition_PrefersFinalizerOverOpenTimePosition pins the
// load-bearing half of the VStream empty-EndPosition fix: when the
// snapshot carries a FinalizePositionFn (the VStream case, whose terminal
// VGTID is only known AFTER the concurrent COPY pump drains — ADR-0071),
// recordEndPosition records the FINALIZED position, not the zero-at-open
// snapshotPos. Recording snapshotPos here instead would stamp an empty
// EndPosition and break chain-resume off a VStream full backup.
func TestRecordEndPosition_PrefersFinalizerOverOpenTimePosition(t *testing.T) {
	b := &Backup{}
	manifest := &irbackup.Manifest{SourceEngine: "mysql"}

	// The open-time position is the ZERO value the VStream constructor
	// returns; the finalizer supplies the real post-sweep VGTID.
	openTime := &ir.Position{} // {Engine:"", Token:""}
	finalized := ir.Position{
		Engine: "mysql",
		Token:  `[{"keyspace":"test","shard":"-","gtid":"MySQL56/abc-1-9"}]`,
	}
	finalizerCalls := 0
	snap := &irbackup.Snapshot{
		Position: *openTime,
		FinalizePositionFn: func(context.Context) (ir.Position, error) {
			finalizerCalls++
			return finalized, nil
		},
	}

	if err := b.recordEndPosition(context.Background(), manifest, false, ir.Position{}, openTime, snap); err != nil {
		t.Fatalf("recordEndPosition: %v", err)
	}
	if finalizerCalls != 1 {
		t.Fatalf("finalizer called %d times; want exactly 1", finalizerCalls)
	}
	if manifest.EndPosition != finalized {
		t.Fatalf("EndPosition = %+v; want the FINALIZED position %+v (open-time zero must not win)", manifest.EndPosition, finalized)
	}
	if manifest.EndPosition.Token == "" {
		t.Fatal("EndPosition token is EMPTY — the VStream chain-root regression is unfixed")
	}
}

// TestRecordEndPosition_NilFinalizerRecordsOpenTimePosition pins the
// byte-identical guarantee for the engines whose open-time position is
// authoritative (Postgres exported-snapshot LSN, vanilla MySQL in-tx
// GTID): FinalizePositionFn nil → record the open-time snapshotPos
// verbatim, exactly as before the fix.
func TestRecordEndPosition_NilFinalizerRecordsOpenTimePosition(t *testing.T) {
	b := &Backup{}
	manifest := &irbackup.Manifest{SourceEngine: "postgres"}

	openTime := &ir.Position{Engine: "postgres", Token: "0/16B3F80"}
	snap := &irbackup.Snapshot{Position: *openTime} // FinalizePositionFn nil

	if err := b.recordEndPosition(context.Background(), manifest, false, ir.Position{}, openTime, snap); err != nil {
		t.Fatalf("recordEndPosition: %v", err)
	}
	if manifest.EndPosition != *openTime {
		t.Fatalf("EndPosition = %+v; want the open-time position %+v recorded unchanged", manifest.EndPosition, *openTime)
	}
}

// TestRecordEndPosition_FinalizerErrorSurfaces pins that a finalizer
// error is wrapped and returned (loud failure), never swallowed into a
// silently-empty EndPosition.
func TestRecordEndPosition_FinalizerErrorSurfaces(t *testing.T) {
	b := &Backup{}
	manifest := &irbackup.Manifest{SourceEngine: "mysql"}
	openTime := &ir.Position{}
	sentinel := errors.New("copy-complete barrier canceled")
	snap := &irbackup.Snapshot{
		FinalizePositionFn: func(context.Context) (ir.Position, error) {
			return ir.Position{}, sentinel
		},
	}

	err := b.recordEndPosition(context.Background(), manifest, false, ir.Position{}, openTime, snap)
	if err == nil {
		t.Fatal("expected a wrapped finalizer error; got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error %q does not wrap the finalizer error", err.Error())
	}
}

// TestRecordEndPosition_AnchoredResumeIgnoresFinalizer pins that the
// anchored-resume path (task #42 / ADR-0085) re-asserts the ADOPTED prior
// anchor and NEVER calls the finalizer — the finalized position of THIS
// run's read-consistency snapshot must not overwrite the adopted anchor.
func TestRecordEndPosition_AnchoredResumeIgnoresFinalizer(t *testing.T) {
	b := &Backup{}
	manifest := &irbackup.Manifest{SourceEngine: "mysql"}
	adoptedAnchor := ir.Position{Engine: "mysql", Token: `[{"keyspace":"test","shard":"-","gtid":"MySQL56/abc-1-5"}]`}

	finalizerCalls := 0
	snap := &irbackup.Snapshot{
		FinalizePositionFn: func(context.Context) (ir.Position, error) {
			finalizerCalls++
			return ir.Position{Engine: "mysql", Token: "SHOULD-NOT-WIN"}, nil
		},
	}
	openTime := &ir.Position{}

	if err := b.recordEndPosition(context.Background(), manifest, true, adoptedAnchor, openTime, snap); err != nil {
		t.Fatalf("recordEndPosition: %v", err)
	}
	if finalizerCalls != 0 {
		t.Fatalf("finalizer called %d times on an anchored resume; want 0", finalizerCalls)
	}
	if manifest.EndPosition != adoptedAnchor {
		t.Fatalf("EndPosition = %+v; want the adopted anchor %+v", manifest.EndPosition, adoptedAnchor)
	}
}
