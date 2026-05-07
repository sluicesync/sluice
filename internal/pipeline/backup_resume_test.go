// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// failOnNthPutStore wraps a [LocalStore] and returns ErrInjected on the
// Nth Put call (1-indexed). Used to simulate a crash mid-backup so the
// resume path is exercised.
type failOnNthPutStore struct {
	*LocalStore

	failOn  int
	putN    int
	failErr error
}

func newFailOnNthPutStore(inner *LocalStore, failOn int) *failOnNthPutStore {
	return &failOnNthPutStore{
		LocalStore: inner,
		failOn:     failOn,
		failErr:    errors.New("injected failure for resume test"),
	}
}

func (s *failOnNthPutStore) Put(ctx context.Context, path string, r io.Reader) error {
	s.putN++
	if s.putN == s.failOn {
		return s.failErr
	}
	return s.LocalStore.Put(ctx, path, r)
}

// TestBackup_ResumeSkipsAlreadyCompletedTables runs a backup with two
// tables, kills it after the first table completes (by failing the
// next chunk Put), then re-runs and verifies the second table is
// streamed AND the first table's chunks aren't re-uploaded.
func TestBackup_ResumeSkipsAlreadyCompletedTables(t *testing.T) {
	dir := t.TempDir()
	inner, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	schema := &ir.Schema{
		Tables: []*ir.Table{
			{
				Name: "users",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
				},
			},
			{
				Name: "posts",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
				},
			},
		},
	}
	rows := map[string][]ir.Row{
		"users": {{"id": int64(1)}, {"id": int64(2)}},
		"posts": {{"id": int64(10)}, {"id": int64(20)}, {"id": int64(30)}},
	}
	src := newBackupRecorderEngine("postgres", schema, rows)

	// First run: fail on the 3rd Put. Order of Puts is:
	//   1: users chunk 0
	//   2: manifest checkpoint after users
	//   3: posts chunk 0  ← fails here
	failing := newFailOnNthPutStore(inner, 3)
	b1 := &Backup{
		Source: src, SourceDSN: "src", Store: failing,
		ChunkRows: 100, // one chunk per table
	}
	if err := b1.Run(context.Background()); err == nil {
		t.Fatal("first Run: expected injected failure; got nil")
	}

	// The manifest committed after the first table should be on disk
	// with PartialState == in_progress.
	m1, err := readManifest(context.Background(), inner)
	if err != nil {
		t.Fatalf("readManifest after partial: %v", err)
	}
	if m1.PartialState != ir.BackupStateInProgress {
		t.Errorf("PartialState = %q; want %q", m1.PartialState, ir.BackupStateInProgress)
	}
	if len(m1.Tables) != 1 || m1.Tables[0].Name != "users" {
		t.Errorf("partial manifest tables = %+v; want one entry for 'users'", m1.Tables)
	}

	// Track how many Puts the resume run does: it should NOT re-upload
	// the users chunk (one Put for posts chunk + final manifest write
	// + per-table-checkpoint manifest write = 3).
	counting := &countingPutStore{LocalStore: inner}
	b2 := &Backup{
		Source:    newBackupRecorderEngine("postgres", schema, rows),
		SourceDSN: "src",
		Store:     counting,
		ChunkRows: 100,
	}
	if err := b2.Run(context.Background()); err != nil {
		t.Fatalf("resume Run: %v", err)
	}

	// Confirm final manifest is complete with both tables.
	m2, err := readManifest(context.Background(), inner)
	if err != nil {
		t.Fatalf("readManifest after resume: %v", err)
	}
	if m2.PartialState != ir.BackupStateComplete {
		t.Errorf("final PartialState = %q; want %q", m2.PartialState, ir.BackupStateComplete)
	}
	if len(m2.Tables) != 2 {
		t.Fatalf("final tables = %d; want 2", len(m2.Tables))
	}
	// Users table chunk SHA256 should be the same hash recorded after
	// the first run (no re-upload happened).
	if m1.Tables[0].Chunks[0].SHA256 != m2.Tables[0].Chunks[0].SHA256 {
		t.Errorf("users chunk SHA256 changed across resume: %s → %s",
			m1.Tables[0].Chunks[0].SHA256, m2.Tables[0].Chunks[0].SHA256)
	}

	// The resume run should have done EXACTLY the work the second
	// table required: 1 chunk Put + 2 manifest Puts (per-table
	// checkpoint + final flip-to-complete) = 3 Puts. The users chunk
	// is not in that count — it was already on disk and the chunk-
	// matches helper short-circuited the upload.
	if counting.puts != 3 {
		t.Errorf("resume Put count = %d; want 3 (1 chunk + 2 manifest)", counting.puts)
	}

	// Verify the backup overall.
	total, mismatches, err := VerifyBackup(context.Background(), inner)
	if err != nil {
		t.Fatalf("VerifyBackup: %v", err)
	}
	if mismatches != 0 || total != 2 {
		t.Errorf("VerifyBackup = %d/%d; want 0 mismatches over 2 chunks", mismatches, total)
	}
}

// countingPutStore records how many Puts the orchestrator issued.
type countingPutStore struct {
	*LocalStore

	puts int
}

func (s *countingPutStore) Put(ctx context.Context, path string, r io.Reader) error {
	s.puts++
	return s.LocalStore.Put(ctx, path, r)
}

// TestBackup_RefusesOverwriteOfCompleteWithoutForce confirms a re-run
// against a successfully-completed prior backup refuses unless the
// operator passes ForceOverwrite.
func TestBackup_RefusesOverwriteOfCompleteWithoutForce(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	schema := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		},
	}
	rows := map[string][]ir.Row{"t": {{"id": int64(1)}}}
	src := newBackupRecorderEngine("postgres", schema, rows)

	// First run: complete.
	b1 := &Backup{Source: src, SourceDSN: "src", Store: store}
	if err := b1.Run(context.Background()); err != nil {
		t.Fatalf("first Run: %v", err)
	}

	// Second run without --force-overwrite: refused.
	b2 := &Backup{
		Source:    newBackupRecorderEngine("postgres", schema, rows),
		SourceDSN: "src", Store: store,
	}
	err := b2.Run(context.Background())
	if err == nil {
		t.Fatal("second Run: expected error refusing overwrite; got nil")
	}
	if !strings.Contains(err.Error(), "force-overwrite") {
		t.Errorf("second Run err = %v; want mention of --force-overwrite", err)
	}

	// Third run with --force-overwrite: succeeds.
	b3 := &Backup{
		Source:    newBackupRecorderEngine("postgres", schema, rows),
		SourceDSN: "src", Store: store,
		ForceOverwrite: true,
	}
	if err := b3.Run(context.Background()); err != nil {
		t.Fatalf("third Run with ForceOverwrite: %v", err)
	}
}

// TestBackup_ChunkAlreadyMatchesSkipsPut confirms the chunk-level
// skip-write fires when a chunk file is already present at the
// destination with matching SHA-256. We pre-seed the store with a
// chunk file that matches what the writer is about to produce, and
// verify the orchestrator doesn't re-Put it.
//
// (This is a unit-level assertion of the helper; the resume integration
// test above covers the end-to-end shape.)
func TestBackup_ChunkAlreadyMatchesHelper(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	want := []byte("a chunk's worth of bytes")
	// Use the same hashing path the writer uses so the comparison is
	// apples-to-apples.
	if err := store.Put(context.Background(), "chunks/x.bin", bytes.NewReader(want)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	expected, err := hashChunkBytes(context.Background(), bytes.NewReader(want))
	if err != nil {
		t.Fatalf("hashChunkBytes: %v", err)
	}

	matches, err := chunkAlreadyMatches(context.Background(), store, "chunks/x.bin", expected)
	if err != nil {
		t.Fatalf("chunkAlreadyMatches: %v", err)
	}
	if !matches {
		t.Errorf("chunkAlreadyMatches = false; want true (same bytes ⇒ same hash)")
	}

	// Mismatch: different expected hash.
	matches, err = chunkAlreadyMatches(context.Background(), store, "chunks/x.bin", "deadbeef")
	if err != nil {
		t.Fatalf("chunkAlreadyMatches mismatch: %v", err)
	}
	if matches {
		t.Errorf("chunkAlreadyMatches = true on mismatch; want false")
	}

	// Missing file.
	matches, err = chunkAlreadyMatches(context.Background(), store, "chunks/missing.bin", expected)
	if err != nil {
		t.Fatalf("chunkAlreadyMatches missing: %v", err)
	}
	if matches {
		t.Errorf("chunkAlreadyMatches = true on missing file; want false")
	}
}

// TestBackup_ResumeWithMissingPriorChunksReruns confirms that when a
// "completed" table from a prior in-progress manifest has its chunks
// deleted between runs, the resume detects the missing chunks and
// re-streams the table rather than blindly preserving the entry.
func TestBackup_ResumeWithMissingPriorChunksReruns(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)
	schema := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		},
	}
	rows := map[string][]ir.Row{"t": {{"id": int64(1)}}}

	// Stage 1: write a manifest with "in_progress" + one table entry,
	// but don't actually create the chunk file. (Simulates a partial
	// run whose chunk got deleted out from under us.)
	priorManifest := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		SourceEngine:  "postgres",
		Schema:        schema,
		PartialState:  ir.BackupStateInProgress,
		Tables: []*ir.TableManifest{
			{
				Name:     "t",
				RowCount: 1,
				Chunks: []*ir.ChunkInfo{
					{File: "chunks/t/t-0.jsonl.gz", RowCount: 1, SHA256: "abc"},
				},
			},
		},
	}
	if err := writeManifest(context.Background(), store, priorManifest); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}

	// Stage 2: re-run; the chunk is missing so the table should be
	// re-streamed.
	src := newBackupRecorderEngine("postgres", schema, rows)
	b := &Backup{Source: src, SourceDSN: "src", Store: store}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	final, _ := readManifest(context.Background(), store)
	if final.PartialState != ir.BackupStateComplete {
		t.Errorf("PartialState = %q; want complete", final.PartialState)
	}
	if len(final.Tables) != 1 {
		t.Fatalf("Tables = %+v; want 1 table", final.Tables)
	}
	gotSHA := final.Tables[0].Chunks[0].SHA256
	if gotSHA == "abc" {
		t.Errorf("chunk SHA256 = %q; want a real recomputed hash, not the placeholder from the seeded manifest", gotSHA)
	}
	if !strings.HasPrefix(fmt.Sprintf("%64s", gotSHA), strings.Repeat(" ", 0)) || len(gotSHA) != 64 {
		t.Errorf("chunk SHA256 length = %d; want 64-hex", len(gotSHA))
	}
}
