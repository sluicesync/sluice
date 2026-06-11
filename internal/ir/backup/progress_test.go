// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"strings"
	"testing"
)

// progressBaseManifest builds the base-manifest shape a sidecar-mode
// backup writes pre-sweep: one kept-complete table plus two pre-staged
// Partial=true placeholders, sidecar reference attached.
func progressBaseManifest() *Manifest {
	return &Manifest{
		FormatVersion: FormatVersionProgressSidecar,
		SourceEngine:  "postgres",
		PartialState:  BackupStateInProgress,
		ProgressSidecar: &ProgressSidecarRef{
			File:      "manifest.progress.jsonl",
			AttemptID: "attempt-b",
		},
		Tables: []*TableManifest{
			{Name: "kept", RowCount: 3, Chunks: []*ChunkInfo{{File: "chunks/kept/kept-0.jsonl.gz", RowCount: 3, SHA256: "aa"}}},
			{Name: "users", Partial: true},
			{Schema: "app", Name: "posts", Partial: true},
		},
	}
}

// line renders one event for attempt-b. Tests that need other attempt
// IDs or malformed lines write them literally.
func line(t *testing.T, ev *ProgressEvent) string {
	t.Helper()
	b, err := ev.MarshalLine()
	if err != nil {
		t.Fatalf("MarshalLine: %v", err)
	}
	return string(b)
}

func TestReplayProgress_AppliesChunksAndCompletions(t *testing.T) {
	m := progressBaseManifest()
	sidecar := line(t, &ProgressEvent{
		AttemptID: "attempt-b", Event: ProgressEventChunk, Table: "users",
		Chunk: &ChunkInfo{File: "chunks/users/users-0.jsonl.gz", RowCount: 2, SHA256: "bb"},
	}) + line(t, &ProgressEvent{
		AttemptID: "attempt-b", Event: ProgressEventTableComplete, Table: "users", RowCount: 2,
	}) + line(t, &ProgressEvent{
		AttemptID: "attempt-b", Event: ProgressEventChunk, Schema: "app", Table: "posts",
		Chunk: &ChunkInfo{File: "chunks/app__posts/app__posts-0.jsonl.gz", RowCount: 5, SHA256: "cc"},
	})

	stats, err := ReplayProgress(m, strings.NewReader(sidecar))
	if err != nil {
		t.Fatalf("ReplayProgress: %v", err)
	}
	if stats.ChunksApplied != 2 || stats.TablesCompleted != 1 || stats.StaleLines != 0 || stats.TornTail {
		t.Errorf("stats = %+v; want 2 chunks, 1 completion, no stale, no torn tail", stats)
	}
	users := m.Tables[1]
	if users.Partial || users.RowCount != 2 || len(users.Chunks) != 1 || users.Chunks[0].SHA256 != "bb" {
		t.Errorf("users entry = %+v; want complete with the replayed chunk", users)
	}
	posts := m.Tables[2]
	if !posts.Partial || len(posts.Chunks) != 1 || posts.Chunks[0].SHA256 != "cc" {
		t.Errorf("posts entry = %+v; want still-partial with one replayed chunk (the crashed mid-table shape)", posts)
	}
	// The kept-complete entry is untouched by replay.
	if kept := m.Tables[0]; kept.Partial || len(kept.Chunks) != 1 || kept.Chunks[0].SHA256 != "aa" {
		t.Errorf("kept entry mutated by replay: %+v", kept)
	}
}

func TestReplayProgress_TornFinalLineIsToleratedLoudly(t *testing.T) {
	m := progressBaseManifest()
	sidecar := line(t, &ProgressEvent{
		AttemptID: "attempt-b", Event: ProgressEventChunk, Table: "users",
		Chunk: &ChunkInfo{File: "chunks/users/users-0.jsonl.gz", RowCount: 2, SHA256: "bb"},
	}) + `{"attempt_id":"attempt-b","event":"table_comp` // crash mid-append

	stats, err := ReplayProgress(m, strings.NewReader(sidecar))
	if err != nil {
		t.Fatalf("ReplayProgress on torn tail: %v (a torn FINAL line is expected crash debris, not corruption)", err)
	}
	if !stats.TornTail {
		t.Error("stats.TornTail = false; want true (callers log it loudly)")
	}
	if stats.ChunksApplied != 1 {
		t.Errorf("ChunksApplied = %d; want 1 (events before the torn tail still apply)", stats.ChunksApplied)
	}
	if users := m.Tables[1]; !users.Partial {
		t.Error("users flipped complete; the torn completion event must be lost, leaving the table partial (it re-streams)")
	}
}

func TestReplayProgress_MalformedMidFileIsFatal(t *testing.T) {
	m := progressBaseManifest()
	sidecar := `{"attempt_id":"attempt-b","event":"chu` + "\n" + // garbage, NOT the last line
		line(t, &ProgressEvent{AttemptID: "attempt-b", Event: ProgressEventTableComplete, Table: "users", RowCount: 2})

	if _, err := ReplayProgress(m, strings.NewReader(sidecar)); err == nil {
		t.Fatal("ReplayProgress succeeded on mid-file garbage; want loud corruption error (only a torn TAIL is tolerable)")
	} else if !strings.Contains(err.Error(), "mid-file") {
		t.Errorf("error %q should name the mid-file shape", err)
	}
}

func TestReplayProgress_StaleAttemptLinesAreSkipped(t *testing.T) {
	m := progressBaseManifest()
	// Debris from attempt-a (a previous run's crash window) followed by
	// the live attempt's event. The stale completion must NOT apply.
	sidecar := line(t, &ProgressEvent{
		AttemptID: "attempt-a", Event: ProgressEventTableComplete, Table: "users", RowCount: 99,
	}) + line(t, &ProgressEvent{
		AttemptID: "attempt-b", Event: ProgressEventChunk, Table: "users",
		Chunk: &ChunkInfo{File: "chunks/users/users-0.jsonl.gz", RowCount: 2, SHA256: "bb"},
	})

	stats, err := ReplayProgress(m, strings.NewReader(sidecar))
	if err != nil {
		t.Fatalf("ReplayProgress: %v", err)
	}
	if stats.StaleLines != 1 || stats.ChunksApplied != 1 || stats.TablesCompleted != 0 {
		t.Errorf("stats = %+v; want 1 stale skipped, 1 chunk applied, 0 completions", stats)
	}
	if users := m.Tables[1]; !users.Partial || users.RowCount == 99 {
		t.Errorf("users entry = %+v; the stale attempt's completion must not apply", users)
	}
}

func TestReplayProgress_LoudFailureLadder(t *testing.T) {
	cases := []struct {
		name    string
		event   *ProgressEvent
		wantErr string
	}{
		{
			name:    "unknown table",
			event:   &ProgressEvent{AttemptID: "attempt-b", Event: ProgressEventTableComplete, Table: "ghost", RowCount: 1},
			wantErr: "not present in the base manifest",
		},
		{
			name:    "unknown event kind (newer binary's sidecar)",
			event:   &ProgressEvent{AttemptID: "attempt-b", Event: "split_brain", Table: "users"},
			wantErr: "unknown event kind",
		},
		{
			name:    "chunk event for an already-complete table",
			event:   &ProgressEvent{AttemptID: "attempt-b", Event: ProgressEventChunk, Table: "kept", Chunk: &ChunkInfo{File: "x", SHA256: "dd"}},
			wantErr: "already complete",
		},
		{
			name:    "duplicate completion",
			event:   &ProgressEvent{AttemptID: "attempt-b", Event: ProgressEventTableComplete, Table: "kept", RowCount: 3},
			wantErr: "duplicate completion",
		},
		{
			name:    "chunk event without chunk metadata",
			event:   &ProgressEvent{AttemptID: "attempt-b", Event: ProgressEventChunk, Table: "users"},
			wantErr: "without chunk metadata",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			m := progressBaseManifest()
			_, err := ReplayProgress(m, strings.NewReader(line(t, c.event)))
			if err == nil {
				t.Fatalf("ReplayProgress succeeded; want loud error containing %q", c.wantErr)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("error %q missing %q", err, c.wantErr)
			}
		})
	}
}

func TestReplayProgress_EmptySidecarIsNoOp(t *testing.T) {
	m := progressBaseManifest()
	stats, err := ReplayProgress(m, strings.NewReader(""))
	if err != nil {
		t.Fatalf("ReplayProgress: %v", err)
	}
	if stats != (ProgressReplayStats{}) {
		t.Errorf("stats = %+v; want zero", stats)
	}
}

func TestReplayProgress_RefusesManifestWithoutSidecarRef(t *testing.T) {
	m := progressBaseManifest()
	m.ProgressSidecar = nil
	if _, err := ReplayProgress(m, strings.NewReader("")); err == nil {
		t.Fatal("ReplayProgress succeeded without a sidecar reference; want error (caller bug — replay is gated on the ref)")
	}
}
