// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// F3 pins: the layer-2 row-count backstop must not be disable-able by an
// attacker who zeroes a manifest's recorded RowCount. On an unsigned
// backup, zeroing a table's (or a chunk's) RowCount used to skip the
// `RowCount > 0` check and let a populated table look empty; restore now
// refuses loudly with SLUICE-E-BACKUP-INCOMPLETE.

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func TestRestore_ZeroedRowCount_Refused(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	rows := map[string][]ir.Row{"users": {{"id": int64(1)}, {"id": int64(2)}}}

	// run backs up two rows, applies tamper to the read-back manifest, then
	// asserts restore refuses with the coded incomplete-backup error.
	run := func(t *testing.T, tamper func(m *irbackup.Manifest)) {
		t.Helper()
		ctx := context.Background()
		dir := t.TempDir()
		store, _ := blobcodec.NewLocalStore(dir)

		src := newBackupRecorderEngine("mysql", schema, rows)
		if err := (&backup.Backup{Source: src, SourceDSN: "src", Store: store}).Run(ctx); err != nil {
			t.Fatalf("Backup.Run: %v", err)
		}
		manifest, err := lineage.ReadManifest(ctx, store)
		if err != nil {
			t.Fatalf("read manifest: %v", err)
		}
		if len(manifest.Tables) == 0 || len(manifest.Tables[0].Chunks) == 0 {
			t.Fatalf("expected one table with >=1 chunk; got %+v", manifest.Tables)
		}
		tamper(manifest)
		if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, manifest); err != nil {
			t.Fatalf("rewrite manifest: %v", err)
		}

		tgt := newRestoreRecorderEngine("mysql")
		err = (&backup.Restore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(ctx)
		assertCoded(t, err, sluicecode.CodeBackupIncomplete)
	}

	t.Run("zeroed TABLE RowCount (chunks decode rows)", func(t *testing.T) {
		// Chunk RowCount stays correct, so the per-chunk check passes; the
		// TABLE-level backstop must catch the zeroed table count.
		run(t, func(m *irbackup.Manifest) { m.Tables[0].RowCount = 0 })
	})

	t.Run("zeroed CHUNK RowCount (chunk decodes rows)", func(t *testing.T) {
		// Table RowCount stays correct; the per-chunk backstop must catch the
		// zeroed chunk count that would otherwise disable the per-chunk check.
		run(t, func(m *irbackup.Manifest) { m.Tables[0].Chunks[0].RowCount = 0 })
	})

	t.Run("EMPTIED table chunk list (RowCount kept) — Bug 183", func(t *testing.T) {
		// The empty-list mirror of the zeroed-count case: the adversary
		// deletes the table's chunk entries but leaves RowCount > 0. Without
		// the guard the empty-table early-return silently restores it empty;
		// a populated table (RowCount > 0) with NO chunks must refuse.
		run(t, func(m *irbackup.Manifest) { m.Tables[0].Chunks = nil })
	})
}
