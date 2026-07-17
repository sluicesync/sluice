// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Audit 2026-07-11 L-3 pin: the BackupID recompute check (chain restore
// step 2.7b) must also guard the SINGLE-FULL restore path. Pre-fix only
// the chain walk ran verifyBackupIDs, so a lone full whose BackupID-covered
// field (created_at / source_engine / kind / EndPosition) was edited
// without recomputing the id restored with rc=0; it now refuses with
// SLUICE-E-BACKUP-MANIFEST-INVALID before any data lands. A legacy full
// with an EMPTY BackupID (pre-Phase-3 writers recorded none) still
// restores clean — there is no recorded value to verify.

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

func TestRestore_SingleFull_TamperedBackupID(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
	rows := map[string][]ir.Row{"users": {{"id": int64(1)}, {"id": int64(2)}}}

	// run backs up two rows, applies tamper to the read-back manifest, then
	// re-runs the PUBLIC single-manifest restore path and returns its error.
	run := func(t *testing.T, tamper func(m *irbackup.Manifest)) error {
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
		tamper(manifest)
		if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, manifest); err != nil {
			t.Fatalf("rewrite manifest: %v", err)
		}

		tgt := newRestoreRecorderEngine("mysql")
		return (&backup.Restore{Target: tgt, TargetDSN: "tgt", Store: store}).Run(ctx)
	}

	t.Run("untampered full restores clean", func(t *testing.T) {
		if err := run(t, func(*irbackup.Manifest) {}); err != nil {
			t.Fatalf("untampered single full refused: %v", err)
		}
	})

	t.Run("edited created_at without recomputing BackupID — REFUSED", func(t *testing.T) {
		err := run(t, func(m *irbackup.Manifest) { m.CreatedAt = m.CreatedAt.Add(time.Hour) })
		assertCoded(t, err, sluicecode.CodeBackupManifestInvalid)
	})

	t.Run("legacy full with empty BackupID restores clean (nothing recorded to verify)", func(t *testing.T) {
		if err := run(t, func(m *irbackup.Manifest) { m.BackupID = "" }); err != nil {
			t.Fatalf("legacy empty-BackupID full refused: %v", err)
		}
	})
}
