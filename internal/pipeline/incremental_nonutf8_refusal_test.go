// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// TestIncrementalBackup_NonUTF8StringRefusesTheWindow pins the capture end
// of the codec refusal (blobcodec/backup_value_utf8.go) through the real
// `backup incremental` orchestrator: a CDC source that hands the backup a
// non-UTF-8 string — the GC-37 (j) shape, a MySQL latin1 `é` as the raw
// byte 0xE9 — must fail the window loudly, naming the table and column,
// and must not commit an incremental manifest. Before the refusal the same
// window completed and a chain restore landed `caf�` at exit 0. The
// independent expected value is the source's value itself: nothing that
// could reproduce it was written, so nothing may claim the window.
func TestIncrementalBackup_NonUTF8StringRefusesTheWindow(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "name", Type: ir.Text{}},
		},
	}}}
	parent := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Schema:        schema,
		Kind:          irbackup.BackupKindFull,
		EndPosition:   ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`},
		PartialState:  irbackup.BackupStateComplete,
	}
	parent.BackupID = irbackup.ComputeBackupID(parent)
	writeParentFullManifest(t, store, parent)

	src := &fakeCDCEngine{
		name:           "postgres",
		schemaSequence: []*ir.Schema{schema},
		cdcChanges: []ir.Change{
			ir.TxBegin{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/110"}`}},
			ir.Insert{
				Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/120"}`},
				Table:    "users",
				Row:      ir.Row{"id": int64(1), "name": "caf\xe9"},
			},
			ir.TxCommit{Position: ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/130"}`}},
		},
		cdcExpectedFromOK: true,
	}
	now := time.Date(2026, 9, 25, 11, 0, 0, 0, time.UTC)
	b := &IncrementalBackup{
		Source:        src,
		SourceDSN:     "src",
		Store:         store,
		ParentRef:     parent.BackupID,
		Window:        5 * time.Minute,
		ChunkChanges:  10,
		SluiceVersion: "test",
		Now:           func() time.Time { return now },
		clockNow:      func() time.Time { return now },
	}

	err = b.Run(context.Background())
	if err == nil {
		t.Fatal("backup incremental completed a window holding a non-UTF-8 string; its chunk would restore U+FFFD")
	}
	for _, want := range []string{"BACKUP-VALUE-NOT-UTF8", `"users"`, `"name"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal missing %s: %v", want, err)
		}
	}
	records, err := lineage.ListAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatalf("list manifests: %v", err)
	}
	for _, r := range records {
		if r.Manifest.Kind == irbackup.BackupKindIncremental && r.Manifest.PartialState == irbackup.BackupStateComplete {
			t.Errorf("a refused window committed a complete incremental manifest at %s", r.Path)
		}
	}
}
