// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The fixture for this file is a GENUINELY interrupted backup, produced by
// failpointing a real three-table `Backup.Run` — not a hand-written manifest
// with the field set. That matters for the same reason item 104's
// frozen-golden hash did not: a fixture assembled from the post-change value
// proves only that the binary agrees with itself. Here the manifest, its
// staged table entries, its chunk list and its schema all come out of the
// backup writer exactly as a killed `sluice backup full` would leave them,
// and the pin then asserts what the READ paths do with it.
//
// What they did before the guard: `backup verify` rehashed every chunk the
// partial manifest listed, found them all valid, and exited 0 "all chunks
// OK"; `restore` created every table in the embedded schema, loaded the ones
// the manifest listed, logged the rest at INFO, and exited 0. Silent loss on
// the DR path.

// interruptedBackupFixture failpoints a three-table backup mid-sweep and
// returns the store carrying the resulting in-progress manifest, plus the
// schema the backup was taken from. It asserts the fixture's OWN shape first
// — an interrupted manifest that somehow came out complete, or that recorded
// every table's chunks, would make every assertion below vacuous.
func interruptedBackupFixture(t *testing.T) (*blobcodec.LocalStore, *ir.Schema) {
	t.Helper()
	ctx := context.Background()

	inner, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		{Name: "posts", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		{Name: "tags", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	rows := map[string][]ir.Row{
		"users": {{"id": int64(1)}, {"id": int64(2)}},
		"posts": {{"id": int64(10)}},
		"tags":  {{"id": int64(100)}},
	}

	// Put order: 1 = pre-sweep in-progress base manifest, 2 = users chunk 0,
	// 3 = posts chunk 0 ← killed here, so `tags` is never reached and the
	// run dies exactly as a `kill -9` of a 3-of-N-table backup would.
	b := &Backup{
		Source:    newBackupRecorderEngine("postgres", schema, rows),
		SourceDSN: "src",
		Store:     newFailOnNthPutStore(inner, 3),
		ChunkRows: 100,
	}
	if err := b.Run(ctx); err == nil {
		t.Fatal("backup Run: want the injected mid-sweep failure, got nil")
	}

	m, err := lineage.ReadManifest(ctx, inner)
	if err != nil {
		t.Fatalf("ReadManifest after the interrupted run: %v", err)
	}
	if m.PartialState != irbackup.BackupStateInProgress {
		t.Fatalf("fixture PartialState = %q; want %q — the fixture is not an interrupted backup and every "+
			"assertion below would be vacuous", m.PartialState, irbackup.BackupStateInProgress)
	}
	// Ground truth for "this backup is missing data": at least one staged
	// table entry carries no chunks at all.
	missing := 0
	for _, tm := range m.Tables {
		if len(tm.Chunks) == 0 {
			missing++
		}
	}
	if missing == 0 {
		t.Fatalf("fixture manifest records chunks for every one of its %d table(s); the interruption did not "+
			"leave a partial backup", len(m.Tables))
	}
	return inner, schema
}

// completeTheManifest is the MUTATION arm: it flips the fixture's
// PartialState to "complete" in place and leaves everything else — the same
// missing chunks, the same schema, the same ids — untouched. Every assertion
// in this file must invert under it, which is what proves the gate keys on
// the recorded state and not on some incidental property of a manifest that
// happens to come from a failed run.
func completeTheManifest(t *testing.T, store *blobcodec.LocalStore) {
	t.Helper()
	ctx := context.Background()
	m, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	m.PartialState = irbackup.BackupStateComplete
	if err := lineage.WriteManifest(ctx, store, m); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
}

// assertInterrupted requires the coded refusal AND the refusal exit status —
// a DR script branches on both, and a coded error that exited 1 is exactly
// the gap error-codes.md called out for restore through v0.104.7.
func assertInterrupted(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: want the interrupted-backup refusal, got nil (this is the silent-loss shape)", what)
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupInterrupted {
		t.Fatalf("%s: want %s, got %v", what, sluicecode.CodeBackupInterrupted, err)
	}
	if got := ce.ExitCode(); got != sluicecode.ExitRefusal {
		t.Errorf("%s: ExitCode() = %d; want %d (refusal class)", what, got, sluicecode.ExitRefusal)
	}
}

// TestInterruptedFullRefusedByEveryReadPath is the gate. Every command that
// READS data out of a manifest must refuse the interrupted one; the mutation
// arm then flips only PartialState and requires each of them to proceed.
func TestInterruptedFullRefusedByEveryReadPath(t *testing.T) {
	ctx := context.Background()

	t.Run("backup verify", func(t *testing.T) {
		store, _ := interruptedBackupFixture(t)
		_, err := VerifyBackupCodedReport(ctx, store, VerifyOptions{})
		assertInterrupted(t, "VerifyBackupCodedReport", err)

		completeTheManifest(t, store)
		rep, err := VerifyBackupCodedReport(ctx, store, VerifyOptions{})
		if err != nil {
			t.Fatalf("mutation arm: verify over the same store with partial_state=complete = %v; want a clean "+
				"scan — the refusal must key on the recorded state, not on the fixture", err)
		}
		if rep.Chunks == 0 {
			t.Fatal("mutation arm: verify scanned zero chunks; the arm proves nothing")
		}
	})

	t.Run("restore", func(t *testing.T) {
		store, schema := interruptedBackupFixture(t)
		// stubEngine panics on every Open* call, so reaching the schema
		// writer at all fails the test loudly: the refusal must land BEFORE
		// a single table is created on the target.
		r := &Restore{Store: store, Target: stubEngine{}, TargetDSN: "target-dsn"}
		assertInterrupted(t, "Restore.Run", r.Run(ctx))

		completeTheManifest(t, store)
		target := newRestoreRecorderEngine("postgres")
		r2 := &Restore{Store: store, Target: target, TargetDSN: "target-dsn"}
		if err := r2.Run(ctx); err != nil {
			t.Fatalf("mutation arm: restore of the same store with partial_state=complete = %v; want success", err)
		}
		if len(target.rows["users"]) == 0 {
			t.Fatalf("mutation arm: restore wrote no rows for users; the arm proves nothing (schema had %d tables)",
				len(schema.Tables))
		}
	})

	t.Run("export-as-parquet", func(t *testing.T) {
		store, _ := interruptedBackupFixture(t)
		out, err := blobcodec.NewLocalStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewLocalStore: %v", err)
		}
		e := &ParquetExport{Store: store, Output: out}
		assertInterrupted(t, "ParquetExport.Run", e.Run(ctx))

		completeTheManifest(t, store)
		out2, err := blobcodec.NewLocalStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewLocalStore: %v", err)
		}
		e2 := &ParquetExport{Store: store, Output: out2}
		if err := e2.Run(ctx); err != nil {
			t.Fatalf("mutation arm: export of the same store with partial_state=complete = %v; want success", err)
		}
	})
}

// TestRefuseInterruptedManifestClass pins the predicate's own boundaries,
// which the end-to-end gate above cannot reach: an EMPTY PartialState is a
// Phase-1 (pre-v0.16.x) manifest and must NOT start refusing — those are
// immutable complete backups, the same reading [priorResumableTables] takes —
// and the refusal must fire on a link ANYWHERE in a chain, not only at its
// root. The table below enumerates both `BackupState*` constants plus the
// empty legacy value, which is the whole set today; nothing MECHANICALLY
// forces a third constant into it, so a reader adding one should treat this
// table as the place the read-side decision for it belongs.
func TestRefuseInterruptedManifestClass(t *testing.T) {
	states := []struct {
		name       string
		state      string
		wantRefuse bool
	}{
		{"in-progress", irbackup.BackupStateInProgress, true},
		{"complete", irbackup.BackupStateComplete, false},
		{"empty-legacy-phase-1", "", false},
	}
	for _, s := range states {
		t.Run(s.name, func(t *testing.T) {
			m := &irbackup.Manifest{PartialState: s.state, Kind: irbackup.BackupKindFull}
			err := refuseInterruptedManifest(m, "manifest.json")
			if s.wantRefuse {
				assertInterrupted(t, "refuseInterruptedManifest", err)
				return
			}
			if err != nil {
				t.Fatalf("partial_state=%q: got %v; want nil — only %q is an interrupted run",
					s.state, err, irbackup.BackupStateInProgress)
			}
		})
	}

	t.Run("nil manifest", func(t *testing.T) {
		if err := refuseInterruptedManifest(nil, "manifest.json"); err != nil {
			t.Fatalf("nil manifest: got %v; want nil (validateManifestStructure owns that refusal)", err)
		}
	})

	// Chain position: the link-walking form must refuse whichever link
	// carries the state, and must name THAT link's path.
	t.Run("any link in a chain", func(t *testing.T) {
		for _, at := range []int{0, 1, 2} {
			links := make([]lineage.SegmentRecord, 3)
			for i := range links {
				links[i].Manifest = &irbackup.Manifest{PartialState: irbackup.BackupStateComplete}
				links[i].Path = []string{"manifest.json", "manifests/incr-1.json", "manifests/incr-2.json"}[i]
			}
			links[at].Manifest.PartialState = irbackup.BackupStateInProgress
			err := refuseInterruptedManifests(links)
			assertInterrupted(t, "refuseInterruptedManifests", err)
			if want := links[at].Path; !strings.Contains(err.Error(), want) {
				t.Errorf("link %d: refusal %q does not name the offending manifest %q", at, err, want)
			}
		}
		clean := []lineage.SegmentRecord{
			{ManifestRecord: lineage.ManifestRecord{Manifest: &irbackup.Manifest{PartialState: irbackup.BackupStateComplete}}},
			{ManifestRecord: lineage.ManifestRecord{Manifest: &irbackup.Manifest{}}},
		}
		if err := refuseInterruptedManifests(clean); err != nil {
			t.Fatalf("an all-complete chain must pass: %v", err)
		}
	})
}
