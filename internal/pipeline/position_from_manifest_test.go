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
	// The refusal must still name WHAT is missing and WHAT to do. Its wording
	// widened in v0.147.0: it used to blame only "pre-Phase-3.3 v0.16.x", but
	// a MODERN full against a source whose reader cannot capture a backup
	// position produces the identical shape, so the text now names that cause
	// too (found by the v0.147.0 pre-tag review).
	for _, want := range []string{"EndPosition is empty", "no usable StartPosition", "fresh full backup"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v; want contains %q", err, want)
		}
	}
	// And it must still identify WHICH manifest, which is the whole reason
	// this test existed before the wording changed.
	if !strings.Contains(err.Error(), lineage.ManifestBackupID(full)) {
		t.Errorf("err = %v; want the terminal's BackupID so the operator knows which manifest", err)
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
// THIS ARM IS THE LIVE PATH, NOT A LEGACY ONE — and the sentence that used
// to stand here said the opposite. It read "writers at v0.146.0+ stamp
// EndPosition = StartPosition for a quiet window, so this arm exists for
// chains ALREADY ON DISK", which was left over from a first fix attempt that
// DID stamp the writer and was reverted (it collapsed the DDL-only window
// into the captured-nothing one). The `IncrementalBackup` lane leaves
// EndPosition empty today and permanently, so every quiet incremental takes
// this fallback. As written the claim invited the next reader to delete it as
// dead legacy code.
//
// The two backup lanes genuinely diverge here, which is worth knowing before
// anyone "fixes" that: `BackupStream`'s rollover DOES stamp
// EndPosition = startPos on an empty window (stream.go), for the same reason
// the reverted attempt did. Unifying them is a real question and is filed
// rather than settled here.
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
	if err := lineage.WriteManifestAt(ctx, store, "manifests/incr-quiet.json", incr); err != nil {
		t.Fatalf("write incremental: %v", err)
	}

	// ANTI-VACUITY FLOOR, and it is here because this test WAS vacuous.
	//
	// Its first cut wrote the incremental to the store ROOT. Incrementals are
	// only discovered under `manifests/`, so the chain came back with ONE
	// link — the full — whose EndPosition happens to equal the value being
	// asserted. Both new tests passed without ever entering the fallback, and
	// the mutation run is what exposed it: dropping the redaction marker from
	// that branch was green across the whole suite.
	//
	// Asserting on the returned position alone cannot distinguish the two
	// cases, because the full's EndPosition and the incremental's
	// StartPosition are deliberately the same value. So this captures the
	// WARN, which ONLY the fallback emits.
	logs := captureWarnLogs(t)

	got, redaction, err := LoadChainTerminalPosition(ctx, store)
	if err != nil {
		t.Fatalf("a chain whose terminal incremental captured nothing was refused: %v\n"+
			"Its StartPosition holds the resume point — the same value as the parent full's "+
			"EndPosition — so the chain is usable and the operator was told it was malformed.", err)
	}
	if !strings.Contains(logs(), "StartPosition is being used as the resume point") {
		t.Fatal("the fallback branch never ran — the terminal manifest resolved to something with an " +
			"EndPosition, so this test is asserting a value it would have gotten anyway. Check that " +
			"the incremental is written under `manifests/`; the lineage walk does not find it at the " +
			"store root, which is exactly how this test was vacuous when written.")
	}
	if got != fullEnd {
		t.Errorf("resume position = %+v; want the terminal's StartPosition %+v", got, fullEnd)
	}
	// This chain carries no redaction, so a nil marker is CORRECT here and
	// asserting otherwise would fail on the right behaviour (the first cut of
	// this line did exactly that). The marker's survival across this new path
	// is graded by the redacted variant below, which is the shape that can
	// actually regress.
	if redaction != nil {
		t.Errorf("unredacted chain returned a redaction marker: %+v", redaction)
	}
}

// TestLoadChainTerminalPosition_QuietWindowCarriesTheRedactionMarker is the
// half of the fallback that the new path added and nothing graded (F1, found
// by the v0.147.0 pre-tag review).
//
// LoadChainTerminalPosition returns the terminal manifest's redaction marker
// alongside the position, and that second return is load-bearing rather than
// a convenience: RefusePositionFromRedactedChain grades it, and without it a
// redacted chain resumed by a non-redacting sync overwrites the restore's
// redacted values with plaintext on a live target at exit 0 (audit
// 2026-09-06, filed HIGH).
//
// The existing marker test builds a terminal that HAS an EndPosition, so it
// never reaches the quiet-window branch. Mutating that branch's return to nil
// was green across the whole suite.
func TestLoadChainTerminalPosition_QuietWindowCarriesTheRedactionMarker(t *testing.T) {
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)
	ctx := context.Background()

	fullEnd := ir.Position{Engine: "postgres", Token: `{"lsn":"0/1600000"}`}
	marker := &irbackup.RedactionInfo{RuleCount: 2, Fingerprint: "deadbeef"}
	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        &ir.Schema{},
		Kind:          irbackup.BackupKindFull,
		EndPosition:   fullEnd,
		Redaction:     marker,
		PartialState:  irbackup.BackupStateComplete,
	}
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	incr := &irbackup.Manifest{
		FormatVersion:  irbackup.BackupFormatVersion,
		CreatedAt:      time.Date(2026, 9, 7, 10, 5, 0, 0, time.UTC),
		SourceEngine:   "postgres",
		Schema:         &ir.Schema{},
		Kind:           irbackup.BackupKindIncremental,
		ParentBackupID: lineage.ManifestBackupID(full),
		StartPosition:  fullEnd,
		Redaction:      marker,
		PartialState:   irbackup.BackupStateComplete,
	}
	if err := lineage.WriteManifestAt(ctx, store, "manifests/incr-quiet.json", incr); err != nil {
		t.Fatalf("write incremental: %v", err)
	}

	// Same floor as the sibling test, same reason: without it this passes by
	// reading the FULL's marker and never exercising the fallback at all.
	logs := captureWarnLogs(t)

	_, redaction, err := LoadChainTerminalPosition(ctx, store)
	if err != nil {
		t.Fatalf("LoadChainTerminalPosition: %v", err)
	}
	if !strings.Contains(logs(), "StartPosition is being used as the resume point") {
		t.Fatal("the fallback branch never ran, so this test is reading the FULL's redaction marker " +
			"and proving nothing about the new path (see the sibling test's note on `manifests/`)")
	}
	if redaction == nil {
		t.Fatal("the quiet-window fallback DROPPED the redaction marker. The caller's " +
			"redacted-chain guard has nothing to grade, so a non-redacting sync resumed off this " +
			"chain would overwrite the restore's redacted values with plaintext on a live target, " +
			"at exit 0 — the HIGH this function was hardened against one release ago.")
	}
	if redaction.RuleCount != 2 || redaction.Fingerprint != "deadbeef" {
		t.Errorf("marker = %+v; want the terminal's own (RuleCount 2, fingerprint deadbeef)", redaction)
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
