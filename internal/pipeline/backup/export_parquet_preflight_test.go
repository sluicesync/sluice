// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// completeBackupFixture runs a real two-table `Backup.Run` to completion
// and returns the store. Like interruptedBackupFixture it is produced by
// the writer rather than hand-assembled, so what the read paths are tested
// against is what a backup actually leaves on disk.
func completeBackupFixture(t *testing.T) *blobcodec.LocalStore {
	t.Helper()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		{Name: "posts", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
	rows := map[string][]ir.Row{
		"users": {{"id": int64(1)}, {"id": int64(2)}},
		"posts": {{"id": int64(10)}},
	}
	b := &Backup{
		Source:    newBackupRecorderEngine("postgres", schema, rows),
		SourceDSN: "src",
		Store:     store,
		ChunkRows: 100,
	}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("backup Run: %v", err)
	}
	return store
}

// TestExportParquetRunsTheBackupIDRecompute is the behavioural half of
// ADR-0183's third piece (the structural half is
// TestManifestConsumersRouteThroughSharedPreflights).
//
// `export-as-parquet` ran the signature check and the interrupted-manifest
// check and NEITHER recompute, on any chain shape — the same defect Bug
// 218 closed for `backup verify`, one command further out. So a manifest
// whose recorded BackupID no longer matched its content exported a full
// Parquet set with rc=0 while restore refused it with rc=3 and zero rows.
//
// The tamper is the LAZY EDIT the check exists for: change a field
// ComputeBackupID covers (created_at) and forget to re-stamp the id.
// The mutation arm re-stamps it and requires the export to proceed, so
// the refusal is keyed on the mismatch and not on the edit.
func TestExportParquetRunsTheBackupIDRecompute(t *testing.T) {
	ctx := context.Background()
	store := completeBackupFixture(t)

	m, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	recordedID := m.BackupID
	if recordedID == "" {
		t.Fatal("fixture manifest records no BackupID; the pin would be vacuous (the recompute skips an empty id)")
	}
	m.CreatedAt = m.CreatedAt.Add(-48 * 60 * 60 * 1e9) // two days earlier
	if irbackup.ComputeBackupID(m) == recordedID {
		t.Fatal("the edit did not change the recomputed BackupID; the tamper is not a tamper")
	}
	if err := lineage.WriteManifest(ctx, store, m); err != nil {
		t.Fatal(err)
	}

	out, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	err = (&ParquetExport{Store: store, Output: out}).Run(ctx)
	if err == nil {
		t.Fatal("export-as-parquet exported a manifest whose recorded BackupID does not match its content (rc=0 over a chain restore refuses)")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupManifestInvalid {
		t.Fatalf("want %s, got %v", sluicecode.CodeBackupManifestInvalid, err)
	}

	// MUTATION ARM: re-stamp the id so the manifest is coherent again.
	// Everything else — the earlier created_at, the same chunks, the same
	// schema — stays edited, so a green export here proves the refusal
	// keyed on the MISMATCH rather than on the edit.
	m.BackupID = irbackup.ComputeBackupID(m)
	if err := lineage.WriteManifest(ctx, store, m); err != nil {
		t.Fatal(err)
	}
	out2, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := (&ParquetExport{Store: store, Output: out2}).Run(ctx); err != nil {
		t.Fatalf("mutation arm: export of the re-stamped manifest = %v; want success", err)
	}
	if exists, _ := out2.Exists(ctx, ParquetIndexFileName); !exists {
		t.Fatal("mutation arm: export wrote no parquet index; the arm proves nothing")
	}
}
