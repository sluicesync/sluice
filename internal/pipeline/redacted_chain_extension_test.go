// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The chain-extension door, end to end through the real writers.
//
// The defect these pin: `backup full --redact` writes PII-clean chunks,
// `backup incremental` / `backup stream` write change events straight off
// the CDC pump, and before v0.144.0 nothing connected the two — so the
// chain restored plaintext for every row touched after the full, at exit
// 0. The guard is only worth anything if it fires through the actual
// command path (a direct call to the predicate proves the predicate), and
// only safe if the ordinary unredacted chain still extends, which is why
// each refusal here has a control beside it.

// redactedChainFixture writes a parent full manifest into a fresh store,
// optionally carrying the redaction marker, and returns the store and the
// parent. Mirrors the fixtures the other incremental/stream unit tests
// use so the only variable is the marker.
func redactedChainFixture(t *testing.T, redacted bool) (*blobcodec.LocalStore, *irbackup.Manifest) {
	t.Helper()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	parent := &irbackup.Manifest{
		FormatVersion: irbackup.FormatVersionFor(schema),
		CreatedAt:     time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        schema,
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
		PartialState:  irbackup.BackupStateComplete,
	}
	if redacted {
		parent.Redaction = &irbackup.RedactionInfo{RuleCount: 1, Fingerprint: "0123456789abcdef"}
		irbackup.StampRedaction(parent)
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)
	return store, parent
}

// redactedChainSource is the CDC source both commands stream from: one
// transaction carrying one insert, then end-of-stream.
func redactedChainSource(schema *ir.Schema) *fakeCDCEngine {
	return &fakeCDCEngine{
		name:           "postgres",
		schemaSequence: []*ir.Schema{schema},
		cdcChanges: []ir.Change{
			ir.TxBegin{Position: posTok(110)},
			ir.Insert{Position: posTok(120), Table: "users", Row: ir.Row{"id": int64(42)}},
			ir.TxCommit{Position: posTok(130)},
		},
		cdcExpectedFromOK: true,
	}
}

func manifestCount(t *testing.T, store irbackup.Store) int {
	t.Helper()
	records, err := lineage.ListAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatalf("ListAllManifestsViaWalk: %v", err)
	}
	return len(records)
}

func assertRedactedChainRefusal(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("extending a redacted chain succeeded; it writes PLAINTEXT change events on top of a PII-clean full")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok {
		t.Fatalf("refusal carries no sluice code: %v", err)
	}
	if ce.Code != sluicecode.CodeBackupRedactedChain {
		t.Errorf("error code = %q; want %q (err: %v)", ce.Code, sluicecode.CodeBackupRedactedChain, err)
	}
}

func TestIncrementalBackup_RefusesToExtendARedactedChain(t *testing.T) {
	store, parent := redactedChainFixture(t, true)
	b := &IncrementalBackup{
		Source:        redactedChainSource(parent.Schema),
		SourceDSN:     "src",
		Store:         store,
		ParentRef:     parent.BackupID,
		Window:        5 * time.Minute,
		ChunkChanges:  10,
		SluiceVersion: "test",
		Now:           func() time.Time { return time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC) },
		clockNow:      func() time.Time { return time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC) },
	}
	assertRedactedChainRefusal(t, b.Run(context.Background()))

	// Nothing durable was added: the door is before the window opens, so
	// the chain is exactly as it was.
	if n := manifestCount(t, store); n != 1 {
		t.Errorf("manifests in store = %d; want 1 (the parent full alone — the refusal must add nothing)", n)
	}
}

// The no-false-refusal direction, and the one that would break every
// existing operator's cron if the guard were keyed on the wrong thing.
func TestIncrementalBackup_UnredactedChainStillExtends(t *testing.T) {
	store, parent := redactedChainFixture(t, false)
	b := &IncrementalBackup{
		Source:        redactedChainSource(parent.Schema),
		SourceDSN:     "src",
		Store:         store,
		ParentRef:     parent.BackupID,
		Window:        5 * time.Minute,
		ChunkChanges:  10,
		SluiceVersion: "test",
		Now:           func() time.Time { return time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC) },
		clockNow:      func() time.Time { return time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC) },
	}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("IncrementalBackup.Run on an unredacted chain: %v", err)
	}
	if n := manifestCount(t, store); n != 2 {
		t.Errorf("manifests in store = %d; want 2 (full + incremental)", n)
	}
}

// TestReadPathRefusesAMixedRedactionChain is the read-side backstop over
// a REAL two-link chain (a full plus an incremental this test actually
// streams), not a hand-built link list.
//
// The shape it pins is the one the write doors cannot reach: a chain
// whose root says "redacted" and whose incremental says nothing. A
// current release refuses to create it; an OLDER binary ignores the
// unknown marker, and a lineage can be assembled by hand out of two
// directories. Either way the archive on disk restores plaintext for
// every row the incremental touched, out of a full someone took the
// trouble to redact — so the read path refuses it too.
func TestReadPathRefusesAMixedRedactionChain(t *testing.T) {
	ctx := context.Background()
	store, parent := redactedChainFixture(t, false)
	b := &IncrementalBackup{
		Source:        redactedChainSource(parent.Schema),
		SourceDSN:     "src",
		Store:         store,
		ParentRef:     parent.BackupID,
		Window:        5 * time.Minute,
		ChunkChanges:  10,
		SluiceVersion: "test",
		Now:           func() time.Time { return time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC) },
		clockNow:      func() time.Time { return time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC) },
	}
	if err := b.Run(ctx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}
	if n := manifestCount(t, store); n != 2 {
		t.Fatalf("fixture has %d manifests; want 2 (full + incremental) — the mix needs two links to exist", n)
	}

	// The chain is coherent right now. Assert that BEFORE introducing the
	// mix, so a later failure cannot be blamed on the fixture.
	if _, err := backup.VerifyBackupCodedReport(ctx, store, backup.VerifyOptions{}); err != nil {
		t.Fatalf("fixture: verify of the unredacted chain already fails: %v", err)
	}

	// Introduce the mix the way it actually arises: the root full carries
	// the marker, the incremental does not. The BackupID is left alone —
	// the id fold is version-gated, so a marker written without the
	// version raise (a hand edit, or a future writer) leaves the recorded
	// id recomputing clean, which is exactly the case where the mixed
	// -chain refusal is the ONLY thing standing between the operator and
	// a plaintext restore.
	setRootRedactionMarker(t, store, &irbackup.RedactionInfo{RuleCount: 1, Fingerprint: "0123456789abcdef"})

	_, err := backup.VerifyBackupCodedReport(ctx, store, backup.VerifyOptions{})
	assertRedactedChainRefusal(t, err)

	// Mutation arm: remove ONLY the marker and the same chain verifies.
	// Without it, the refusal above could be any chain-shaped complaint.
	setRootRedactionMarker(t, store, nil)
	if _, err := backup.VerifyBackupCodedReport(ctx, store, backup.VerifyOptions{}); err != nil {
		t.Fatalf("mutation arm: verify of the same chain with the marker removed = %v; want clean", err)
	}
}

// setRootRedactionMarker rewrites the chain-root manifest's redaction
// marker in place, leaving every other field — schema, ids, chunk list —
// untouched.
func setRootRedactionMarker(t *testing.T, store irbackup.Store, r *irbackup.RedactionInfo) {
	t.Helper()
	ctx := context.Background()
	m, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	m.Redaction = r
	if err := lineage.WriteManifest(ctx, store, m); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
}

func TestBackupStream_RefusesToExtendARedactedChain(t *testing.T) {
	store, parent := redactedChainFixture(t, true)
	now := time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC)
	stream := &BackupStream{
		Source:             redactedChainSource(parent.Schema),
		SourceDSN:          "src",
		Store:              store,
		ParentRef:          parent.BackupID,
		RolloverWindow:     5 * time.Minute,
		RolloverMaxChanges: 10,
		RolloverMaxBytes:   1 << 40,
		ChunkChanges:       100,
		SluiceVersion:      "test",
		Now:                func() time.Time { return now },
		clockNow:           func() time.Time { return now },
		pidHostFn:          func() (int, string) { return 12345, "test-host" },
		streamStatePath:    DefaultStreamStateFilename,
	}
	assertRedactedChainRefusal(t, stream.Run(context.Background()))

	// The stream door also covers ROTATION, which takes a fresh `backup
	// full` with no redactor at all: refusing at startup makes that
	// unreachable rather than needing its own guard.
	if n := manifestCount(t, store); n != 1 {
		t.Errorf("manifests in store = %d; want 1 (the parent full alone)", n)
	}
}

func TestBackupStream_UnredactedChainStillStreams(t *testing.T) {
	store, parent := redactedChainFixture(t, false)
	now := time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC)
	stream := &BackupStream{
		Source:             redactedChainSource(parent.Schema),
		SourceDSN:          "src",
		Store:              store,
		ParentRef:          parent.BackupID,
		RolloverWindow:     5 * time.Minute,
		RolloverMaxChanges: 10,
		RolloverMaxBytes:   1 << 40,
		ChunkChanges:       100,
		SluiceVersion:      "test",
		Now:                func() time.Time { return now },
		clockNow:           func() time.Time { return now },
		pidHostFn:          func() (int, string) { return 12345, "test-host" },
		streamStatePath:    DefaultStreamStateFilename,
	}
	if err := stream.Run(context.Background()); err != nil {
		t.Fatalf("BackupStream.Run on an unredacted chain: %v", err)
	}
	if n := manifestCount(t, store); n < 2 {
		t.Errorf("manifests in store = %d; want at least 2 (full + a rollover)", n)
	}
}
