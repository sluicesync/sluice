//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-37 (j): a charset replay on a source with no written-charset record
// (MariaDB's default binlog_row_metadata=NO_LOG), through the backup capture
// lanes.
//
// The reader's charset-DDL guard refuses a replay only when the stream
// reaches the ALTER — after the rows before it. MEASURED (third review): a
// `backup stream` rollover that closed between those rows and the ALTER
// committed 9 replayed change records to the chain before the refusal, and
// two `backup incremental` runs split the same way committed the first
// run's misdecoded rows and exited 0. The fourth review's lane check
// (refuseUnrecordedCharsetReplay) now refuses such a window BEFORE it
// commits: the source schema read at window start and end shows the charset
// change, and the reader reports the rows were decoded by the new charset
// without crossing the ALTER.
//
// The independent evidence is the chain itself (what committed) and the
// refusal's code, hint and marker — which of the two checks refused.

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// charsetReplayChain boots MariaDB NO_LOG, creates a utf8mb4 table, takes a
// full backup, then records rows in utf8mb4 and converts the column to
// latin1 — the history a later capture replays. It returns the source, the
// engine, the store and the full.
func charsetReplayChain(t *testing.T, table string, rows int) (string, ir.Engine, irbackup.Store, *irbackup.Manifest, func()) {
	t.Helper()
	src, cleanup := startMariaDBBinlog(t)
	eng, ok := engines.Get("mariadb")
	if !ok {
		cleanup()
		t.Fatal("mariadb engine not registered")
	}
	ctx := context.Background()
	applyDDLMySQL(t, src, "CREATE TABLE "+table+" (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) CHARACTER SET utf8mb4 NULL)")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		cleanup()
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := (&backup.Backup{Source: eng, SourceDSN: src, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
		cleanup()
		t.Fatalf("Backup.Run: %v", err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		cleanup()
		t.Fatalf("ReadManifest: %v", err)
	}
	var history strings.Builder
	for i := 1; i <= rows; i++ {
		history.WriteString("INSERT INTO " + table + " VALUES (" + string(rune('0'+i)) + ", _utf8mb4 X'C3A9');\n")
	}
	history.WriteString("ALTER TABLE " + table + " MODIFY v VARCHAR(16) CHARACTER SET latin1 NULL;")
	applyDDLMySQL(t, src, history.String())
	return src, eng, store, full, cleanup
}

// committedChangeRecords counts the change records the chain's incrementals
// hold.
func committedChangeRecords(t *testing.T, store irbackup.Store) int64 {
	t.Helper()
	records, err := lineage.ListAllSegmentManifests(context.Background(), store)
	if err != nil {
		t.Fatalf("ListAllSegmentManifests: %v", err)
	}
	var n int64
	for _, rec := range records {
		if rec.Manifest.Kind == irbackup.BackupKindIncremental {
			n += manifestChangeRecordCount(rec.Manifest)
		}
	}
	return n
}

// assertLaneCharsetRefusal requires the lane check's refusal: the replay
// code, the fresh-full hint, and its own marker (not the reader's guard).
func assertLaneCharsetRefusal(t *testing.T, what string, err error) {
	t.Helper()
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch {
		t.Fatalf("%s: err = %v; want %s", what, err, sluicecode.CodeCDCSchemaReplayMismatch)
	}
	if ce.Hint != backupReplayMismatchHint {
		t.Errorf("%s: hint = %q; want %q", what, ce.Hint, backupReplayMismatchHint)
	}
	if !strings.Contains(err.Error(), "Refusing before the window commits") || !strings.Contains(err.Error(), "(utf8mb4 → latin1)") {
		t.Errorf("%s: refused, but not by the lane's window check: %v", what, err)
	}
}

// TestBackupStream_CharsetDDLReplay_RefusesBeforeCommit_MariaDBNoLog: one
// rollover per transaction, so the first closes before the stream reaches
// the ALTER. It must refuse and commit nothing.
func TestBackupStream_CharsetDDLReplay_RefusesBeforeCommit_MariaDBNoLog(t *testing.T) {
	src, eng, store, full, cleanup := charsetReplayChain(t, "cr", 3)
	defer cleanup()
	runCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	err := (&BackupStream{
		Source: eng, SourceDSN: src, Store: store, ParentRef: full.BackupID,
		RolloverWindow: 30 * time.Second, RolloverMaxChanges: 1, RolloverMaxBytes: 1 << 30,
		ChunkChanges: 100, SluiceVersion: "test",
	}).Run(runCtx)
	assertLaneCharsetRefusal(t, "backup stream", err)
	if n := committedChangeRecords(t, store); n != 0 {
		t.Errorf("rollovers committed %d replayed change records before the refusal; want 0 (third review measured 9)", n)
	}
}

// TestIncrementalBackup_CharsetDDLReplay_WindowEndsBeforeALTER_Refuses_MariaDBNoLog
// is the measured two-run shape: the first `backup incremental` closes
// after the first replayed row, before the ALTER. Before this check it
// committed that row misdecoded and exited 0, and the second run crossed
// the ALTER with nothing to compare. Now the first run refuses and commits
// nothing.
func TestIncrementalBackup_CharsetDDLReplay_WindowEndsBeforeALTER_Refuses_MariaDBNoLog(t *testing.T) {
	src, eng, store, full, cleanup := charsetReplayChain(t, "cw", 3)
	defer cleanup()
	runCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := (&IncrementalBackup{
		Source: eng, SourceDSN: src, Store: store, ParentRef: full.BackupID,
		Window: 20 * time.Second, MaxChanges: 1, SluiceVersion: "test",
	}).Run(runCtx)
	assertLaneCharsetRefusal(t, "first backup incremental", err)
	if n := committedChangeRecords(t, store); n != 0 {
		t.Errorf("the first run committed %d change records; want 0", n)
	}
}

// TestIncrementalBackup_CharsetDDLReplay_RefusalNamesFreshFull_MariaDBNoLog
// is the run that reaches the ALTER: the reader's own guard refuses, and the
// lane re-hints it to a fresh full.
func TestIncrementalBackup_CharsetDDLReplay_RefusalNamesFreshFull_MariaDBNoLog(t *testing.T) {
	src, eng, store, full, cleanup := charsetReplayChain(t, "ci", 1)
	defer cleanup()
	runCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := (&IncrementalBackup{
		Source: eng, SourceDSN: src, Store: store, ParentRef: full.BackupID,
		Window: 20 * time.Second, SluiceVersion: "test",
	}).Run(runCtx)
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch {
		t.Fatalf("backup incremental: err = %v; want %s", err, sluicecode.CodeCDCSchemaReplayMismatch)
	}
	if ce.Hint != backupReplayMismatchHint {
		t.Errorf("the one-shot lane's refusal hint = %q; want %q", ce.Hint, backupReplayMismatchHint)
	}
}

// TestIncrementalBackup_CharsetDDLLive_NoRefusal_MariaDBNoLog: the lane
// check's other direction. A run that decodes a row live (the reader's shape
// loaded before the ALTER ran), then crosses the ALTER, then decodes a row
// in the new charset, sees the charset change in its window and must not
// refuse.
func TestIncrementalBackup_CharsetDDLLive_NoRefusal_MariaDBNoLog(t *testing.T) {
	src, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	eng, ok := engines.Get("mariadb")
	if !ok {
		t.Fatal("mariadb engine not registered")
	}
	ctx := context.Background()
	applyDDLMySQL(t, src, `CREATE TABLE cl (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) CHARACTER SET utf8mb4 NULL)`)
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := (&backup.Backup{Source: eng, SourceDSN: src, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	runCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (&IncrementalBackup{
			Source: eng, SourceDSN: src, Store: store, ParentRef: full.BackupID,
			Window: 15 * time.Second, SluiceVersion: "test",
		}).Run(runCtx)
	}()
	time.Sleep(4 * time.Second)
	applyDDLMySQL(t, src, `INSERT INTO cl VALUES (1, _utf8mb4 X'C3A9');`)
	time.Sleep(3 * time.Second)
	applyDDLMySQL(t, src, `ALTER TABLE cl MODIFY v VARCHAR(16) CHARACTER SET latin1 NULL;`)
	time.Sleep(2 * time.Second)
	applyDDLMySQL(t, src, `INSERT INTO cl VALUES (2, _latin1 X'E9');`)
	if err := <-done; err != nil {
		t.Fatalf("a live incremental across a charset ALTER: err = %v; want it to commit", err)
	}
	if n := committedChangeRecords(t, store); n == 0 {
		t.Error("the live run committed no changes; the rows did not reach the window, so it proved nothing")
	}
}
