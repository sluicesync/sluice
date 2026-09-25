//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-37 (j) third review, item 4: on a source with no written-charset record
// (MariaDB's default binlog_row_metadata=NO_LOG), the charset-DDL guard
// refuses a replay only when the stream reaches the ALTER — after the rows
// before it. This measures what that means for a backup chain: a `backup
// stream` rollover that closes between those rows and the ALTER COMMITS them
// to the chain, decoded by the post-DDL charset, before the refusal. So the
// refusal a backup lane surfaces must send the operator to a fresh full
// backup, not to `sync --restart-from-scratch`.
//
// The independent evidence is the chain itself: committed incremental
// manifests recording the replayed changes, present after the refusal.

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func TestBackupStream_CharsetDDLReplay_CommittedRolloversThenRefusal_MariaDBNoLog(t *testing.T) {
	src, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	eng, ok := engines.Get("mariadb")
	if !ok {
		t.Fatal("mariadb engine not registered")
	}
	ctx := context.Background()
	applyDDLMySQL(t, src, `CREATE TABLE cr (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) CHARACTER SET utf8mb4 NULL)`)
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

	// Recorded in utf8mb4, then the column converted: the stream replays
	// the rows by the post-DDL latin1, one rollover per transaction.
	applyDDLMySQL(t, src, `INSERT INTO cr VALUES (1, _utf8mb4 X'C3A9');
		INSERT INTO cr VALUES (2, _utf8mb4 X'C3A9');
		INSERT INTO cr VALUES (3, _utf8mb4 X'C3A9');
		ALTER TABLE cr MODIFY v VARCHAR(16) CHARACTER SET latin1 NULL;`)

	runCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	err = (&BackupStream{
		Source: eng, SourceDSN: src, Store: store, ParentRef: full.BackupID,
		RolloverWindow: 30 * time.Second, RolloverMaxChanges: 1, RolloverMaxBytes: 1 << 30,
		ChunkChanges: 100, SluiceVersion: "test",
	}).Run(runCtx)

	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch {
		t.Fatalf("backup stream: err = %v; want the charset-DDL guard's %s", err, sluicecode.CodeCDCSchemaReplayMismatch)
	}
	if ce.Hint != backupReplayMismatchHint {
		t.Errorf("the backup lane's refusal hint = %q; want %q — a sync flag is no remedy for rollovers already committed", ce.Hint, backupReplayMismatchHint)
	}

	records, err := lineage.ListAllSegmentManifests(ctx, store)
	if err != nil {
		t.Fatalf("ListAllSegmentManifests: %v", err)
	}
	var committedChanges int64
	for _, rec := range records {
		if rec.Manifest.Kind == irbackup.BackupKindIncremental {
			committedChanges += manifestChangeRecordCount(rec.Manifest)
		}
	}
	if committedChanges == 0 {
		t.Fatal("no rollover committed a replayed change before the refusal; the premise of item 4 did not reproduce")
	}
	t.Logf("MEASURED: rollovers committed %d replayed change record(s) to the chain before the stream reached the ALTER and refused", committedChanges)
}

// TestIncrementalBackup_CharsetDDLReplay_RefusalNamesFreshFull_MariaDBNoLog
// is the one-shot lane's sibling: its own window is not committed on the
// refusal, but an earlier incremental that closed before the ALTER may have
// committed the replayed rows, so its refusal names a fresh full too.
func TestIncrementalBackup_CharsetDDLReplay_RefusalNamesFreshFull_MariaDBNoLog(t *testing.T) {
	src, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	eng, ok := engines.Get("mariadb")
	if !ok {
		t.Fatal("mariadb engine not registered")
	}
	ctx := context.Background()
	applyDDLMySQL(t, src, `CREATE TABLE ci (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) CHARACTER SET utf8mb4 NULL)`)
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
	applyDDLMySQL(t, src, `INSERT INTO ci VALUES (1, _utf8mb4 X'C3A9');
		ALTER TABLE ci MODIFY v VARCHAR(16) CHARACTER SET latin1 NULL;`)
	runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	err = (&IncrementalBackup{
		Source: eng, SourceDSN: src, Store: store, ParentRef: full.BackupID,
		Window: 20 * time.Second, SluiceVersion: "test",
	}).Run(runCtx)
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCSchemaReplayMismatch {
		t.Fatalf("backup incremental: err = %v; want the charset-DDL guard's %s", err, sluicecode.CodeCDCSchemaReplayMismatch)
	}
	if ce.Hint != backupReplayMismatchHint {
		t.Errorf("the one-shot lane's refusal hint = %q; want %q", ce.Hint, backupReplayMismatchHint)
	}
}
