// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"sluicesync.dev/sluice/internal/engines"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// TestBackupFull_SQLiteTextHoldingRawBytesRefuses pins the capture-time
// refusal on a REAL engine, with no reader defect involved: a SQLite TEXT
// value written from raw bytes is what the source holds, the SQLite reader
// hands it over faithfully as a Go string, and the JSON chunk codec would
// have sealed `caf�` at exit 0 (measured by the pre-land review of
// 230e8153 on the release before). The independent expected value is the
// source's own bytes, read back from the file: nothing the backup could have
// written reproduces them, so it must refuse — leave the manifest in
// progress — and a re-run after the value is repaired at the source must
// resume and complete.
func TestBackupFull_SQLiteTextHoldingRawBytesRefuses(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "src.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, stmt := range []string{
		`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)`,
		`INSERT INTO notes VALUES (1, 'ok'), (2, CAST(x'636166e9' AS TEXT)), (3, 'fine')`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	var srcHex string
	if err := db.QueryRowContext(ctx, `SELECT hex(body) FROM notes WHERE id = 2`).Scan(&srcHex); err != nil {
		t.Fatal(err)
	}
	if srcHex != "636166E9" {
		t.Fatalf("fixture: source holds %s; want 636166E9 (the premise is raw bytes in a TEXT column)", srcHex)
	}

	eng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	err = (&backup.Backup{Source: eng, SourceDSN: path, Store: store}).Run(ctx)
	if err == nil {
		t.Fatal("backup full of a TEXT value holding bytes that are not UTF-8 completed; its chunk would restore U+FFFD")
	}
	for _, want := range []string{"BACKUP-VALUE-NOT-UTF8", `"notes"`, `"body"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal missing %s: %v", want, err)
		}
	}
	m, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("read manifest after the refusal: %v", err)
	}
	if m == nil {
		t.Fatal("no manifest after the refused backup; want the in-progress one a resume reads")
	}
	if m.PartialState != irbackup.BackupStateInProgress {
		t.Errorf("manifest partial_state = %q after a refused backup; want %q (never complete)", m.PartialState, irbackup.BackupStateInProgress)
	}

	// Repair at the source, as the refusal says; the re-run resumes and completes.
	if _, err := db.ExecContext(ctx, `UPDATE notes SET body = 'café' WHERE id = 2`); err != nil {
		t.Fatal(err)
	}
	if err := (&backup.Backup{Source: eng, SourceDSN: path, Store: store}).Run(ctx); err != nil {
		t.Fatalf("re-run after repairing the source: %v", err)
	}
	m, err = lineage.ReadManifest(ctx, store)
	if err != nil || m == nil {
		t.Fatalf("read manifest after the re-run: %v (nil=%v)", err, m == nil)
	}
	if m.PartialState != irbackup.BackupStateComplete {
		t.Errorf("re-run manifest partial_state = %q; want %q", m.PartialState, irbackup.BackupStateComplete)
	}
}
