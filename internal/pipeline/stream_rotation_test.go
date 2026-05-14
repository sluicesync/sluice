// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// TestShouldExitForRotation_ChainLengthFires covers the
// length-based threshold: when rolloverSeq reaches the configured
// value, the reason returned is rotationReasonChainLength.
func TestShouldExitForRotation_ChainLengthFires(t *testing.T) {
	b := &BackupStream{ExitAfterChainLength: 3}
	reason := b.shouldExitForRotation(context.Background(), 3, time.Now())
	if reason != rotationReasonChainLength {
		t.Errorf("rolloverSeq=3, ExitAfterChainLength=3 → reason = %q; want %q", reason, rotationReasonChainLength)
	}
}

// TestShouldExitForRotation_ChainLengthNotYet covers the under-
// threshold case (no exit).
func TestShouldExitForRotation_ChainLengthNotYet(t *testing.T) {
	b := &BackupStream{ExitAfterChainLength: 5}
	if reason := b.shouldExitForRotation(context.Background(), 2, time.Now()); reason != "" {
		t.Errorf("rolloverSeq=2, ExitAfterChainLength=5 → reason = %q; want empty", reason)
	}
}

// TestShouldExitForRotation_AgeFires covers the age-based threshold:
// when (now - chain catalog's CreatedAt) exceeds ExitAfterAge, the
// reason is rotationReasonAge.
func TestShouldExitForRotation_AgeFires(t *testing.T) {
	store := newMemStore()
	createdAt := time.Now().Add(-2 * time.Hour).UTC()
	cat := &ChainCatalog{
		FormatVersion: chainCatalogFormatVersion,
		CreatedAt:     createdAt,
		UpdatedAt:     createdAt,
		Entries: []ChainCatalogEntry{
			{BackupID: "full000", Kind: ir.BackupKindFull, ManifestPath: ManifestFileName, CreatedAt: createdAt},
		},
	}
	if err := writeChainCatalog(context.Background(), store, cat); err != nil {
		t.Fatalf("writeChainCatalog: %v", err)
	}

	b := &BackupStream{Store: store, ExitAfterAge: 1 * time.Hour}
	reason := b.shouldExitForRotation(context.Background(), 0, time.Now())
	if reason != rotationReasonAge {
		t.Errorf("chain age 2h, ExitAfterAge=1h → reason = %q; want %q", reason, rotationReasonAge)
	}
}

// TestShouldExitForRotation_AgeNotYet covers the chain-still-young
// case.
func TestShouldExitForRotation_AgeNotYet(t *testing.T) {
	store := newMemStore()
	createdAt := time.Now().Add(-30 * time.Minute).UTC()
	cat := &ChainCatalog{
		FormatVersion: chainCatalogFormatVersion,
		CreatedAt:     createdAt,
		UpdatedAt:     createdAt,
		Entries: []ChainCatalogEntry{
			{BackupID: "full000", Kind: ir.BackupKindFull, ManifestPath: ManifestFileName, CreatedAt: createdAt},
		},
	}
	if err := writeChainCatalog(context.Background(), store, cat); err != nil {
		t.Fatalf("writeChainCatalog: %v", err)
	}

	b := &BackupStream{Store: store, ExitAfterAge: 1 * time.Hour}
	if reason := b.shouldExitForRotation(context.Background(), 0, time.Now()); reason != "" {
		t.Errorf("chain age 30m, ExitAfterAge=1h → reason = %q; want empty", reason)
	}
}

// TestShouldExitForRotation_LengthPreferredOverAge covers the
// either-fires-wins semantic when both thresholds are configured: if
// length trips first, the length reason takes priority over an age
// check that would have also tripped.
func TestShouldExitForRotation_LengthPreferredOverAge(t *testing.T) {
	store := newMemStore()
	createdAt := time.Now().Add(-2 * time.Hour).UTC()
	cat := &ChainCatalog{
		FormatVersion: chainCatalogFormatVersion,
		CreatedAt:     createdAt,
		UpdatedAt:     createdAt,
		Entries: []ChainCatalogEntry{
			{BackupID: "full000", Kind: ir.BackupKindFull, ManifestPath: ManifestFileName, CreatedAt: createdAt},
		},
	}
	if err := writeChainCatalog(context.Background(), store, cat); err != nil {
		t.Fatalf("writeChainCatalog: %v", err)
	}

	b := &BackupStream{Store: store, ExitAfterAge: 1 * time.Hour, ExitAfterChainLength: 3}
	reason := b.shouldExitForRotation(context.Background(), 3, time.Now())
	if reason != rotationReasonChainLength {
		t.Errorf("both thresholds tripped → reason = %q; want %q (length checked first)", reason, rotationReasonChainLength)
	}
}

// TestShouldExitForRotation_NoneConfigured covers the default-off
// case: both thresholds zero → never fires.
func TestShouldExitForRotation_NoneConfigured(t *testing.T) {
	store := newMemStore()
	b := &BackupStream{Store: store}
	if reason := b.shouldExitForRotation(context.Background(), 1000, time.Now()); reason != "" {
		t.Errorf("no thresholds configured → reason = %q; want empty", reason)
	}
}

// TestShouldExitForRotation_CatalogAbsentSilentlySkipsAge covers the
// fall-back path: when chain.json is missing, the age check returns
// empty (conservative; the operator should resolve the catalog
// issue, not exit silently).
func TestShouldExitForRotation_CatalogAbsentSilentlySkipsAge(t *testing.T) {
	store := newMemStore()
	b := &BackupStream{Store: store, ExitAfterAge: 1 * time.Hour}
	if reason := b.shouldExitForRotation(context.Background(), 0, time.Now()); reason != "" {
		t.Errorf("catalog absent → reason = %q; want empty (conservative fall-back)", reason)
	}
}

// TestMarkChainRotatedBestEffort_WritesMarker covers the chain.json
// marking on rotation: the RotatedAt + RotationReason fields land in
// the catalog so subsequent reads see the chain is closed.
func TestMarkChainRotatedBestEffort_WritesMarker(t *testing.T) {
	store := newMemStore()
	cat := &ChainCatalog{
		FormatVersion: chainCatalogFormatVersion,
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
		Entries: []ChainCatalogEntry{
			{BackupID: "full000", Kind: ir.BackupKindFull, ManifestPath: ManifestFileName, CreatedAt: time.Now().UTC()},
		},
	}
	if err := writeChainCatalog(context.Background(), store, cat); err != nil {
		t.Fatalf("writeChainCatalog: %v", err)
	}

	rotateAt := time.Now().UTC()
	markChainRotatedBestEffort(context.Background(), store, rotationReasonAge, rotateAt)

	got, ok, err := loadChainCatalog(context.Background(), store)
	if err != nil || !ok {
		t.Fatalf("loadChainCatalog: ok=%v err=%v", ok, err)
	}
	if got.RotationReason != rotationReasonAge {
		t.Errorf("RotationReason = %q; want %q", got.RotationReason, rotationReasonAge)
	}
	if got.RotatedAt.IsZero() {
		t.Errorf("RotatedAt not set after markChainRotatedBestEffort")
	}
}

// TestMarkChainRotatedBestEffort_AbsentCatalogNoOp covers the
// chain.json-absent path: the function silently no-ops rather than
// erroring, so a stream exit with a missing catalog doesn't fail.
func TestMarkChainRotatedBestEffort_AbsentCatalogNoOp(t *testing.T) {
	store := newMemStore()
	// No panic / no error; just a WARN log entry.
	markChainRotatedBestEffort(context.Background(), store, rotationReasonAge, time.Now().UTC())
	// Catalog still absent.
	_, present, err := loadChainCatalog(context.Background(), store)
	if err != nil {
		t.Errorf("loadChainCatalog: unexpected err %v", err)
	}
	if present {
		t.Errorf("catalog should not have been created by no-op marker write")
	}
}
