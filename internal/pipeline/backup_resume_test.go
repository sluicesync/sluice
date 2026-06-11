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

	"sluicesync.dev/sluice/internal/ir"
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

	// First run: fail on the 5th Put. Order of Puts (v0.16.1+ — Bug 34b
	// added the per-chunk checkpoint; task #42/ADR-0085 added the
	// pre-sweep in-progress manifest write) is:
	//   1: pre-sweep in-progress manifest (anchor-stamped when present)
	//   2: users chunk 0
	//   3: per-chunk checkpoint after users chunk 0 (users.Partial=true)
	//   4: per-table checkpoint after users completes (users.Partial=false)
	//   5: posts chunk 0  ← fails here
	failing := newFailOnNthPutStore(inner, 5)
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
	// ADR-0084 pre-staging: every table's entry is staged into the
	// manifest in schema order BEFORE the sweep starts, so the crashed
	// manifest carries BOTH tables — users finished (Partial=false,
	// chunks recorded) and posts not-yet-started (Partial=true, zero
	// chunks; the resume classifier re-streams it from scratch).
	if len(m1.Tables) != 2 || m1.Tables[0].Name != "users" || m1.Tables[1].Name != "posts" {
		t.Fatalf("partial manifest tables = %+v; want staged entries for [users posts]", m1.Tables)
	}
	if m1.Tables[0].Partial {
		t.Error("users entry Partial = true after natural EOF; want false")
	}
	if !m1.Tables[1].Partial || len(m1.Tables[1].Chunks) != 0 {
		t.Errorf("posts entry = %+v; want pre-staged Partial=true with zero chunks", m1.Tables[1])
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
	// table required:
	//   - 1 manifest Put for the pre-sweep in-progress manifest
	//     (task #42/ADR-0085 — written before the sweep so a crash
	//     always leaves a resumable, anchor-stamped record)
	//   - 1 chunk Put for posts chunk 0
	//   - 1 manifest Put for the per-chunk checkpoint after posts chunk 0
	//     (Bug 34b: per-chunk granularity, v0.16.1+)
	//   - 1 manifest Put for the per-table checkpoint after posts
	//   - 1 manifest Put for the final flip-to-complete
	//   - 1 chain.json Put on the final manifest write (GitHub #20,
	//     v0.47.0 — only the final write triggers the catalog because
	//     per-chunk / per-table checkpoint manifests are written with
	//     an empty BackupID, which updateChainCatalog skips)
	// = 6 Puts. The users chunk is not in that count — it was already
	// on disk from the first run and trySkipChunk short-circuited the
	// upload (no row read, no Put).
	if counting.puts != 6 {
		t.Errorf("resume Put count = %d; want 6 (1 chunk + 4 manifest checkpoints + 1 chain.json)", counting.puts)
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

// TestBackup_ResumePerChunkSkipsAlreadyUploadedChunks pins Bug 34b's
// fix: a partial-state manifest that records ChunkInfo entries for
// chunks 0..N (and those chunks are still on disk with matching
// SHA-256) causes the resume run to skip those chunks — neither
// re-uploading them (no Put against those paths) nor aborting on a
// row-stream mismatch.
//
// Setup: write the table once with --chunk-rows=2 to produce 3 chunks.
// Stash that backup state, simulate a "killed mid-table" by truncating
// the manifest to keep only chunks 0 and 1. Then re-run; verify chunks
// 0 and 1 are NOT re-uploaded (Put count proves it) and chunk 2 is.
func TestBackup_ResumeRestreamsPartialTableWithContentAddressedUploadSkip(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	schema := &ir.Schema{
		Tables: []*ir.Table{
			{
				Name: "events",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
				},
			},
		},
	}
	rows := map[string][]ir.Row{
		"events": {
			{"id": int64(1)},
			{"id": int64(2)},
			{"id": int64(3)},
			{"id": int64(4)},
			{"id": int64(5)},
		},
	}

	// First run: full backup with chunk-rows=2 → 3 chunks (2,2,1).
	src1 := newBackupRecorderEngine("postgres", schema, rows)
	b1 := &Backup{
		Source: src1, SourceDSN: "src", Store: store,
		ChunkRows: 2,
	}
	if err := b1.Run(context.Background()); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	full, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if len(full.Tables) != 1 || len(full.Tables[0].Chunks) != 3 {
		t.Fatalf("expected 3 chunks; got %+v", full.Tables)
	}

	// Stash chunks 0 and 1's SHA-256 for assertion later.
	wantChunk0SHA := full.Tables[0].Chunks[0].SHA256
	wantChunk1SHA := full.Tables[0].Chunks[1].SHA256

	// Mutate the manifest to look like a "killed mid-table" state:
	// keep chunks 0 and 1 but drop chunk 2. The chunk file for chunk 2
	// stays on disk; the resume run should re-write it (chunks 0 and 1
	// match the prior; the loop continues to chunk 2). Partial=true
	// signals to the orchestrator that this is a mid-stream entry, not
	// a fully-completed table.
	partial := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		SourceEngine:  "postgres",
		Schema:        schema,
		PartialState:  ir.BackupStateInProgress,
		Tables: []*ir.TableManifest{
			{
				Name:     "events",
				RowCount: 4, // chunks 0+1 hold 4 of the 5 rows
				Partial:  true,
				Chunks: []*ir.ChunkInfo{
					full.Tables[0].Chunks[0],
					full.Tables[0].Chunks[1],
				},
			},
		},
	}
	if err := writeManifest(context.Background(), store, partial); err != nil {
		t.Fatalf("writeManifest partial: %v", err)
	}
	// Delete chunk 2's file so the resume actually has work to do for
	// chunk 2 (and to assert chunks 0/1 aren't being re-uploaded).
	chunk2Path := full.Tables[0].Chunks[2].File
	if err := store.Delete(context.Background(), chunk2Path); err != nil {
		t.Fatalf("delete chunk 2 file: %v", err)
	}

	// Resume run: count Puts to verify chunks 0/1 are NOT re-uploaded.
	counting := &countingPutPerPathStore{LocalStore: store}
	src2 := newBackupRecorderEngine("postgres", schema, rows)
	b2 := &Backup{
		Source: src2, SourceDSN: "src", Store: counting,
		ChunkRows: 2,
	}
	if err := b2.Run(context.Background()); err != nil {
		t.Fatalf("resume Run: %v", err)
	}

	// Final manifest has 3 chunks again.
	final, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest final: %v", err)
	}
	if final.PartialState != ir.BackupStateComplete {
		t.Errorf("PartialState = %q; want complete", final.PartialState)
	}
	if len(final.Tables) != 1 || len(final.Tables[0].Chunks) != 3 {
		t.Fatalf("expected 3 chunks post-resume; got %+v", final.Tables)
	}

	// Chunks 0 and 1 must carry the SAME SHA-256 the original full
	// run recorded — proves they weren't re-streamed/re-hashed.
	if final.Tables[0].Chunks[0].SHA256 != wantChunk0SHA {
		t.Errorf("chunk 0 SHA changed across resume: %q → %q", wantChunk0SHA, final.Tables[0].Chunks[0].SHA256)
	}
	if final.Tables[0].Chunks[1].SHA256 != wantChunk1SHA {
		t.Errorf("chunk 1 SHA changed across resume: %q → %q", wantChunk1SHA, final.Tables[0].Chunks[1].SHA256)
	}

	// Per-path Put counts: chunks 0 and 1 should have 0 Puts during
	// the resume run; chunk 2 should have exactly 1.
	chunk0Path := full.Tables[0].Chunks[0].File
	chunk1Path := full.Tables[0].Chunks[1].File
	if got := counting.putsTo[chunk0Path]; got != 0 {
		t.Errorf("Put count for chunk 0 (%s) = %d; want 0 (already-uploaded skip should fire)", chunk0Path, got)
	}
	if got := counting.putsTo[chunk1Path]; got != 0 {
		t.Errorf("Put count for chunk 1 (%s) = %d; want 0", chunk1Path, got)
	}
	if got := counting.putsTo[chunk2Path]; got != 1 {
		t.Errorf("Put count for chunk 2 (%s) = %d; want 1", chunk2Path, got)
	}

	// Final overall verify still clean.
	total, mismatches, err := VerifyBackup(context.Background(), store)
	if err != nil {
		t.Fatalf("VerifyBackup: %v", err)
	}
	if mismatches != 0 || total != 3 {
		t.Errorf("VerifyBackup = %d mismatches over %d chunks; want 0 over 3", mismatches, total)
	}
}

// countingPutPerPathStore records how many Puts hit each path. Used by
// the per-chunk-skip test to verify already-uploaded chunks aren't
// re-Put on resume.
type countingPutPerPathStore struct {
	*LocalStore

	putsTo map[string]int
}

func (s *countingPutPerPathStore) Put(ctx context.Context, path string, r io.Reader) error {
	if s.putsTo == nil {
		s.putsTo = make(map[string]int)
	}
	s.putsTo[path]++
	return s.LocalStore.Put(ctx, path, r)
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
