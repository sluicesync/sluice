// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// The gate for smart compaction's post-rewrite re-read (audit 2026-08-04 C-4).
//
// Compaction stamps each rewritten chunk's SHA-256 and RowCount from its OWN
// writer's in-memory accounting and, before this fix, never read the object
// back. Every later consumer — restore, broker replay, `backup verify` at any
// depth — then compared the store's bytes against a number the writer asserted
// rather than one anything observed, on the single maintenance path that
// rewrites restore-critical bytes. Same evidence-sharing shape as roadmap item
// 129, one path over.
//
// SCOPE, stated where the gate is defined so it cannot be read as broader than
// it is: this exercises `applySmartCompactionToIncremental` — the ONLY path
// that re-encodes chunk bytes and re-stamps their SHA. The non-smart compaction
// path moves chunks verbatim (`copyFile`) carrying their ORIGINAL manifest SHA,
// so its evidence is a hash this run did not produce; `backup prune` rewrites no
// bytes at all. Both are exempt for that reason, not for lack of looking.

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// lyingPutStore is a store whose Put SILENTLY drops the tail of one named
// object — the shape a store returning success without durably holding what it
// accepted produces, and the one the writer's own Hash() can never see.
type lyingPutStore struct {
	irbackup.Store

	target string
	drop   int
}

func (s *lyingPutStore) Put(ctx context.Context, path string, r io.Reader) error {
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if path == s.target && len(body) > s.drop {
		body = body[:len(body)-s.drop]
	}
	return s.Store.Put(ctx, path, bytes.NewReader(body))
}

// seedOneChunkIncremental writes a single-chunk incremental whose events
// collapse, so smart compaction genuinely rewrites the chunk.
func seedOneChunkIncremental(t *testing.T, store irbackup.Store, chunkPath string) *irbackup.Manifest {
	t.Helper()
	var buf bytes.Buffer
	cw, err := blobcodec.NewChangeChunkWriter(&buf, nil, blobcodec.CodecGzip, nil)
	if err != nil {
		t.Fatalf("NewChangeChunkWriter: %v", err)
	}
	lsn := uint64(10)
	for i := 0; i < 8; i++ {
		if err := cw.WriteChange(ir.Insert{
			Position: pos(lsn), Schema: "public", Table: "users",
			Row: ir.Row{"id": int64(i), "name": "n"},
		}); err != nil {
			t.Fatalf("write insert: %v", err)
		}
		lsn++
	}
	for i := 0; i < 8; i++ {
		if err := cw.WriteChange(ir.Update{
			Position: pos(lsn), Schema: "public", Table: "users",
			Before: ir.Row{"id": int64(i)},
			After:  ir.Row{"id": int64(i), "name": "n2"},
		}); err != nil {
			t.Fatalf("write update: %v", err)
		}
		lsn++
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("close chunk writer: %v", err)
	}
	if err := store.Put(context.Background(), chunkPath, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("put chunk: %v", err)
	}
	return &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		SourceEngine:  "postgres",
		CreatedAt:     time.Now().UTC(),
		Kind:          irbackup.BackupKindIncremental,
		Schema:        usersSchema(),
		ChangeChunks: []*irbackup.ChunkInfo{
			{File: chunkPath, RowCount: cw.ChangeCount(), SHA256: cw.Hash()},
		},
	}
}

func TestSmartCompaction_RefusesWhenTheStoreDoesNotHoldWhatItWrote(t *testing.T) {
	ctx := context.Background()
	const chunkPath = "chunks/_changes/incr-0.jsonl.gz"

	t.Run("honest store: compaction still succeeds", func(t *testing.T) {
		store, err := blobcodec.NewLocalStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewLocalStore: %v", err)
		}
		im := seedOneChunkIncremental(t, store, chunkPath)
		res, err := applySmartCompactionToIncremental(ctx, store, im, blobcodec.CodecGzip, nil, PKStrategyPK)
		if err != nil {
			t.Fatalf("compaction refused an honest store: %v", err)
		}
		if res.eventsAfter >= res.eventsBefore {
			t.Fatalf("no collapse happened (%d → %d) — the fixture is not exercising a rewrite",
				res.eventsBefore, res.eventsAfter)
		}
		if im.ChangeChunks[0].RowCount != res.eventsAfter {
			t.Errorf("re-stamped RowCount = %d; want %d", im.ChangeChunks[0].RowCount, res.eventsAfter)
		}
	})

	t.Run("store drops the tail: compaction refuses instead of exiting 0", func(t *testing.T) {
		base, err := blobcodec.NewLocalStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewLocalStore: %v", err)
		}
		im := seedOneChunkIncremental(t, base, chunkPath)
		// Seeding used the honest store; only the REWRITE is corrupted.
		store := &lyingPutStore{Store: base, target: chunkPath, drop: 24}

		_, err = applySmartCompactionToIncremental(ctx, store, im, blobcodec.CodecGzip, nil, PKStrategyPK)
		if err == nil {
			t.Fatal("compaction reported success over a chunk the store does not hold.\n\n" +
				"The manifest now records a SHA and a RowCount the compactor's own writer computed, " +
				"and nothing ever compared them against the object. Every later consumer — restore, " +
				"broker replay, backup verify — checks the bytes against a number the writer asserted " +
				"(audit 2026-08-04 C-4).")
		}
		if !strings.Contains(err.Error(), "re-read rewritten chunk") {
			t.Errorf("refusal does not name the re-read stage, so an operator cannot tell it from a "+
				"read-side failure: %v", err)
		}
	})
}
