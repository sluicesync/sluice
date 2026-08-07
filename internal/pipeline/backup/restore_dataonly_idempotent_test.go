// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// TestRestore_DataOnly_RefusesNonIdempotentWriter is the binding test for
// the 2026-08-07 invariant-sweep correction at the head of
// [Restore.restoreChunkGroup].
//
// What the comment there used to say: a writer without
// [ir.IdempotentRowWriter] "falls back to plain WriteRows … a plain insert
// only collides on a PK the upsert would have updated to the same value".
// That argument holds only for a KEYED table. On a rotation-segment
// (DataOnly) restore the snapshot is re-applied over rows the previous
// segment already landed, so on a keyless table a plain INSERT doubles
// them and exits 0 — the silent-loss shape, inverted.
//
// The sibling gate is docsync's TestChainRestoreTargetsAreIdempotentWriters,
// which asserts no REGISTERED engine can reach this refusal. This test
// asserts the refusal exists and is loud when one does; the two together
// are the pair, and neither substitutes for the other (this package cannot
// import the engine registry, and docsync cannot reach an unexported
// pipeline path).
func TestRestore_DataOnly_RefusesNonIdempotentWriter(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "keyless",
		Columns: []*ir.Column{{Name: "v", Type: ir.Integer{Width: 64}}},
	}}}
	rows := map[string][]ir.Row{"keyless": {{"v": int64(1)}, {"v": int64(2)}}}

	src := newBackupRecorderEngine("postgres", schema, rows)
	if err := (&Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	// newRestoreRecorderEngine's writer implements RowWriter and NOT
	// IdempotentRowWriter — the shape the refusal is for.
	tgt := newRestoreRecorderEngine("postgres")
	err = (&Restore{
		Target:    tgt,
		TargetDSN: "tgt",
		Store:     store,
		DataOnly:  true,
	}).Run(context.Background())
	if err == nil {
		t.Fatalf("a DataOnly restore into a writer with no idempotent form SUCCEEDED. That is the plain-INSERT " +
			"fallback back: on a rotation-segment restore it re-inserts the previous segment's rows, which a " +
			"keyless table has no key to absorb — silently doubled data at exit 0")
	}
	for _, want := range []string{"keyless", "IdempotentRowWriter", "duplicate"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q — an operator has to be able to tell WHICH table and WHY.\ngot: %v", want, err)
		}
	}

	// And nothing was written: the refusal is resolved before the chunk
	// producer or the row channel exists, so a refused segment cannot leave
	// a partially-applied table behind.
	if _, got := tgt.snapshot(); len(got) != 0 {
		t.Errorf("the refused restore still wrote rows to %d table(s); the dispatch check must run before any row moves", len(got))
	}
}

// TestRestore_NonDataOnly_AcceptsPlainWriter is the other direction of the
// same mutation, and it is what stops the refusal above from being written
// as "DataOnly is irrelevant, always demand the idempotent surface". A
// COLD (segment-0 / single-full) restore writes into a target it just
// created and truncates first, so a plain writer is correct there and must
// keep working — sqlite is a real restore target on exactly this path.
func TestRestore_NonDataOnly_AcceptsPlainWriter(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "keyless",
		Columns: []*ir.Column{{Name: "v", Type: ir.Integer{Width: 64}}},
	}}}
	rows := map[string][]ir.Row{"keyless": {{"v": int64(1)}, {"v": int64(2)}}}

	src := newBackupRecorderEngine("postgres", schema, rows)
	if err := (&Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	tgt := newRestoreRecorderEngine("postgres")
	if err := (&Restore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("a cold restore into a plain writer must still work: %v", err)
	}
	if _, got := tgt.snapshot(); len(got["keyless"]) != 2 {
		t.Errorf("cold restore landed %d rows; want 2", len(got["keyless"]))
	}
}
