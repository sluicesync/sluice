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
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// TestLoadChainTerminalPosition_FullOnly pins the simplest chain
// shape: a v0.17.2+ full with EndPosition recorded. The terminal
// manifest is the full itself.
func TestLoadChainTerminalPosition_FullOnly(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	endPos := ir.Position{
		Engine: "postgres",
		Token:  `{"slot":"sluice_slot","lsn":"1/200"}`,
	}
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 7, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          irbackup.BackupKindFull,
		EndPosition:   endPos,
		PartialState:  irbackup.BackupStateComplete,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write parent: %v", err)
	}

	got, _, err := LoadChainTerminalPosition(context.Background(), store)
	if err != nil {
		t.Fatalf("LoadChainTerminalPosition: %v", err)
	}
	if got != endPos {
		t.Errorf("position = %+v; want %+v", got, endPos)
	}
}

// TestLoadChainTerminalPosition_FullPlusIncrementals pins the chain
// shape with intermediates: terminal incremental's EndPosition is
// what gets returned, not the full's.
func TestLoadChainTerminalPosition_FullPlusIncrementals(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 7, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"1/100"}`},
		PartialState:  irbackup.BackupStateComplete,
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}

	incr1 := &irbackup.Manifest{
		FormatVersion:  irbackup.BackupFormatVersion,
		CreatedAt:      time.Date(2026, 5, 7, 11, 0, 0, 0, time.UTC),
		SourceEngine:   "postgres",
		Schema:         &ir.Schema{},
		Kind:           irbackup.BackupKindIncremental,
		ParentBackupID: full.BackupID,
		StartPosition:  full.EndPosition,
		EndPosition:    ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"1/200"}`},
		PartialState:   irbackup.BackupStateComplete,
	}
	incr1.BackupID = irbackup.ComputeBackupID(incr1)
	if err := lineage.WriteManifestAt(context.Background(), store, "manifests/incr-001.json", incr1); err != nil {
		t.Fatalf("write incr1: %v", err)
	}

	incr2 := &irbackup.Manifest{
		FormatVersion:  irbackup.BackupFormatVersion,
		CreatedAt:      time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC),
		SourceEngine:   "postgres",
		Schema:         &ir.Schema{},
		Kind:           irbackup.BackupKindIncremental,
		ParentBackupID: incr1.BackupID,
		StartPosition:  incr1.EndPosition,
		EndPosition:    ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"1/300"}`},
		PartialState:   irbackup.BackupStateComplete,
	}
	incr2.BackupID = irbackup.ComputeBackupID(incr2)
	if err := lineage.WriteManifestAt(context.Background(), store, "manifests/incr-002.json", incr2); err != nil {
		t.Fatalf("write incr2: %v", err)
	}

	got, _, err := LoadChainTerminalPosition(context.Background(), store)
	if err != nil {
		t.Fatalf("LoadChainTerminalPosition: %v", err)
	}
	if got != incr2.EndPosition {
		t.Errorf("position = %+v; want incr2 EndPosition %+v", got, incr2.EndPosition)
	}
}

// TestLoadChainTerminalPosition_EmptyEndPosition pins the loud-failure
// shape when the chain's terminal has no recorded EndPosition (a
// pre-Phase-3.3 full, or a malformed chain). The error names the
// terminal's BackupID so the operator knows which manifest to look
// at.
func TestLoadChainTerminalPosition_EmptyEndPosition(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 5, 7, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          irbackup.BackupKindFull,
		// No EndPosition.
		PartialState: irbackup.BackupStateComplete,
	}
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}

	_, _, err := LoadChainTerminalPosition(context.Background(), store)
	if err == nil {
		t.Fatal("LoadChainTerminalPosition: nil; want error on empty EndPosition")
	}
	if !strings.Contains(err.Error(), "no EndPosition") {
		t.Errorf("err = %v; want contains 'no EndPosition'", err)
	}
	if !strings.Contains(err.Error(), "v0.17.2+") {
		t.Errorf("err = %v; want contains version recovery hint", err)
	}
}

// TestLoadChainTerminalPosition_QuietWindowResumesFromStart is Bug 275
// (v0.146.0 regression cycle; the defect is pre-existing).
//
// A `backup incremental` whose window captured nothing used to write a
// terminal manifest with an EMPTY EndPosition at exit 0, and this function
// then refused the whole chain as a "pre-Phase-3.3 full backup or malformed
// chain" — about a chain written seconds earlier by the same binary, whose
// correct resume point was sitting in that same manifest's StartPosition.
// The operator met that at the moment they were reaching for the no-re-bulk
// handoff, which is precisely when they are trying to avoid a full re-copy.
//
// The two empty-EndPosition populations are distinguishable, and conflating
// them is what made a fine chain look malformed:
//
//	FULL, pre-Phase-3.3     no EndPosition, no StartPosition  -> still refuses
//	INCREMENTAL, quiet      no EndPosition, HAS StartPosition -> resumes there
//
// Writers at v0.146.0+ stamp EndPosition = StartPosition for a quiet window,
// so this arm exists for chains ALREADY ON DISK — without it they stay
// permanently unusable for the handoff.
func TestLoadChainTerminalPosition_QuietWindowResumesFromStart(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	ctx := context.Background()

	fullEnd := ir.Position{Engine: "postgres", Token: `{"lsn":"0/1600000"}`}
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          irbackup.BackupKindFull,
		EndPosition:   fullEnd,
		PartialState:  irbackup.BackupStateComplete,
	}
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}

	// The quiet incremental: StartPosition carried forward from the parent,
	// EndPosition never stamped because no position-bearing event arrived.
	incr := &irbackup.Manifest{
		FormatVersion:  irbackup.BackupFormatVersion,
		CreatedAt:      time.Date(2026, 9, 7, 10, 5, 0, 0, time.UTC),
		SourceEngine:   "postgres",
		Schema:         &ir.Schema{},
		Kind:           irbackup.BackupKindIncremental,
		ParentBackupID: lineage.ManifestBackupID(full),
		StartPosition:  fullEnd,
		PartialState:   irbackup.BackupStateComplete,
	}
	if err := lineage.WriteManifestAt(ctx, store, "manifest-incr-quiet.json", incr); err != nil {
		t.Fatalf("write incremental: %v", err)
	}

	got, _, err := LoadChainTerminalPosition(ctx, store)
	if err != nil {
		t.Fatalf("a chain whose terminal incremental captured nothing was refused: %v\n"+
			"Its StartPosition holds the resume point — the same value as the parent full's "+
			"EndPosition — so the chain is usable and the operator was told it was malformed.", err)
	}
	if got != fullEnd {
		t.Errorf("resume position = %+v; want the terminal's StartPosition %+v", got, fullEnd)
	}
}

// TestLoadChainTerminalPosition_EmptyStore pins the loud-failure
// shape on an empty store: clear "no manifests" message rather than
// silent zero-position return.
func TestLoadChainTerminalPosition_EmptyStore(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	_, _, err := LoadChainTerminalPosition(context.Background(), store)
	if err == nil {
		t.Fatal("LoadChainTerminalPosition: nil; want error")
	}
	if !strings.Contains(err.Error(), "no manifests") {
		t.Errorf("err = %v; want contains 'no manifests'", err)
	}
}
