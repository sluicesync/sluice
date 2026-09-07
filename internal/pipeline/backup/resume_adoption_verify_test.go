// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"context"
	"testing"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// TestResumeAdoption_VerifiesChunkHashesWhenSigning closes the sibling
// the audit 2026-09-06 S-1 filing named and did not fix.
//
// THE GAP. A resumed `backup full` adopts a prior IN-PROGRESS manifest's
// completed tables, and an in-progress manifest is UNSIGNED by
// construction. So a store adversary who edits it during the interrupted
// window — replacing a chunk's bytes, dropping an entry — had the edit
// adopted and then SIGNED by the resuming run. Same class as the
// prune/compact laundering door, narrower window: it needs an
// interrupted backup rather than a scheduled job.
//
// EXISTENCE WAS NEVER THE RIGHT QUESTION, and the asymmetry was visible
// in the file: the WRITE side has verified the SHA since it existed
// (chunkAlreadyMatches overwrites a mismatched partial upload), while
// the ADOPTION side asked only whether the file was present. A chunk
// whose bytes had been replaced was therefore adopted as complete.
//
// BOTH DIRECTIONS ARE GRADED, and the second is the cost floor rather
// than an afterthought: verification runs only when the run will SIGN,
// because the harm is "adopted AND signed" and an unsigned chain has no
// signature to launder. A signing resume re-hashes every adopted chunk,
// which on a large interrupted backup is real I/O.
func TestResumeAdoption_VerifiesChunkHashesWhenSigning(t *testing.T) {
	ctx := context.Background()

	// One completed table entry whose single chunk is on the store with a
	// recorded SHA. `newMemStore` and the chunk helpers are the package's
	// own fixtures.
	build := func(t *testing.T) (irbackup.Store, *irbackup.TableManifest) {
		t.Helper()
		store := newMemStore()
		body := []byte("chunk-bytes-v1")
		key := "t/chunk-0000.jsonl.gz"
		if err := store.Put(ctx, key, bytes.NewReader(body)); err != nil {
			t.Fatalf("put chunk: %v", err)
		}
		sum, err := blobcodec.HashChunkBytes(ctx, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("hash chunk: %v", err)
		}
		return store, &irbackup.TableManifest{
			Name:   "t",
			Chunks: []*irbackup.ChunkInfo{{File: key, SHA256: sum}},
		}
	}

	t.Run("an untampered entry is adopted under both postures", func(t *testing.T) {
		// The floor. A resume that refuses to adopt a healthy prior entry
		// would re-stream every completed table on every resume, which is
		// the opposite of what the resume path is for.
		store, entry := build(t)
		for _, verify := range []bool{false, true} {
			full, err := tableManifestFullyComplete(ctx, store, entry, verify)
			if err != nil {
				t.Fatalf("verifyHashes=%v: %v", verify, err)
			}
			if !full {
				t.Errorf("verifyHashes=%v: a healthy completed table was NOT adopted; every resume would "+
					"re-stream it", verify)
			}
		}
	})

	t.Run("replaced chunk bytes are caught when the run will sign", func(t *testing.T) {
		store, entry := build(t)
		// The adversary's edit: the file is still THERE, so the
		// existence check passes; its contents are not what the manifest
		// records.
		if err := store.Put(ctx, entry.Chunks[0].File, bytes.NewReader([]byte("attacker-substituted"))); err != nil {
			t.Fatalf("overwrite chunk: %v", err)
		}
		full, err := tableManifestFullyComplete(ctx, store, entry, true)
		if err != nil {
			t.Fatalf("verify: %v", err)
		}
		if full {
			t.Fatal("a table whose chunk BYTES were replaced was adopted as complete. The resuming run " +
				"would then stamp and SIGN that content — the in-progress manifest it came from is " +
				"unsigned by construction, which is exactly why this window is exploitable.")
		}
	})

	t.Run("an unsigned run does not pay for the check", func(t *testing.T) {
		// The cost scope, asserted rather than assumed. With no signature
		// to launder there is no harm, and re-hashing every adopted chunk
		// on every resume of a large interrupted backup is real I/O. If
		// this cell ever fails, the check has silently become
		// unconditional and someone owes the cost note a re-read.
		store, entry := build(t)
		if err := store.Put(ctx, entry.Chunks[0].File, bytes.NewReader([]byte("attacker-substituted"))); err != nil {
			t.Fatalf("overwrite chunk: %v", err)
		}
		full, err := tableManifestFullyComplete(ctx, store, entry, false)
		if err != nil {
			t.Fatalf("no-verify path: %v", err)
		}
		if !full {
			t.Error("the non-signing path re-hashed the chunk; verification is scoped to signing runs " +
				"deliberately, because an unsigned chain has no signature to launder")
		}
	})

	t.Run("a dropped chunk ENTRY is caught by the row-count identity", func(t *testing.T) {
		// The shape the hash loop cannot see, because removing the entry
		// AND its file leaves a manifest that is internally consistent:
		// every listed chunk is present and matches. This release's own
		// pre-tag review found the doc naming this harm while the check
		// could not reach it — a verification whose expected value came
		// from the artifact under attack.
		//
		// TableManifest.RowCount is the independent number: the writer
		// stamps it at table completion, restore already compares against
		// it, and a dropped entry no longer reconciles.
		store, entry := build(t)
		entry.Chunks[0].RowCount = 10
		entry.RowCount = 10
		if full, err := tableManifestFullyComplete(ctx, store, entry, true); err != nil || !full {
			t.Fatalf("the healthy baseline for this cell does not adopt (full=%v err=%v)", full, err)
		}
		// The adversary's edit: drop the entry and its file together.
		if err := store.Delete(ctx, entry.Chunks[0].File); err != nil {
			t.Fatalf("delete chunk: %v", err)
		}
		entry.Chunks = nil
		full, err := tableManifestFullyComplete(ctx, store, entry, true)
		if err != nil {
			t.Fatalf("verify: %v", err)
		}
		if full {
			t.Fatal("a table whose chunk ENTRY was removed along with its file was adopted as " +
				"complete. Every listed chunk matched — because none were listed — so the hash loop " +
				"alone reports all clear and the resuming run signs a table that is missing rows.")
		}
	})

	t.Run("an older manifest with no recorded RowCount still adopts", func(t *testing.T) {
		// The floor on the check above. Manifests written before the
		// writer stamped RowCount carry zero for a perfectly good table;
		// demanding the identity there would refuse to adopt every older
		// interrupted backup and re-stream it.
		store, entry := build(t)
		entry.RowCount = 0
		entry.Chunks[0].RowCount = 0
		full, err := tableManifestFullyComplete(ctx, store, entry, true)
		if err != nil {
			t.Fatalf("verify: %v", err)
		}
		if !full {
			t.Error("a healthy table from a manifest predating RowCount was refused adoption; every " +
				"older interrupted backup would re-stream every completed table")
		}
	})

	t.Run("a missing chunk is still caught under both postures", func(t *testing.T) {
		// The pre-existing existence check must survive the change.
		store, entry := build(t)
		if err := store.Delete(ctx, entry.Chunks[0].File); err != nil {
			t.Fatalf("delete chunk: %v", err)
		}
		for _, verify := range []bool{false, true} {
			full, err := tableManifestFullyComplete(ctx, store, entry, verify)
			if err != nil {
				t.Fatalf("verifyHashes=%v: %v", verify, err)
			}
			if full {
				t.Errorf("verifyHashes=%v: a table with a MISSING chunk was adopted as complete", verify)
			}
		}
	})
}
