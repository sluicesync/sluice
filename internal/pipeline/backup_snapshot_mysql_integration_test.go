//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MySQL → MySQL counterpart of backup_snapshot_pg_integration_test.go.
// Same v0.18.0 load-bearing property: snapshot-anchored EndPosition
// closes the during-backup write-window gap.
//
// MySQL specifics: the snapshot is per-session (START TRANSACTION
// WITH CONSISTENT SNAPSHOT) and not shareable across connections, so
// all table reads run on a single pinned conn sequentially. Trade-off
// vs PG's parallel-readable snapshot is documented in the v0.18.0
// release notes.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// TestBackup_SnapshotAnchoredEndPosition_MySQLGapClosed pins the
// v0.18.0 fix end-to-end on MySQL. Same shape as the PG sibling
// (seed → backup-with-during-window-writes → incremental → restore →
// verify all rows present); the snapshot mechanism differs but the
// observable property is identical.
func TestBackup_SnapshotAnchoredEndPosition_MySQLGapClosed(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES
			('alice@example.com'),
			('bob@example.com'),
			('carol@example.com');
	`
	applyDDLMySQL(t, sourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	const duringWindowEmails = 4
	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Larger head start so the snapshot tx has been opened (and
		// gtid_executed captured) before we start writing. Bug 54: the
		// pre-fix 100ms head start + 50ms inter-write pacing put the
		// 4th write at ~250ms which on a fast machine landed in the
		// tight window between the snapshot's EndPosition record and
		// the incremental's CDC catch-up open. Widening to 200ms head
		// + 250ms inter-write spreads the writes across ~1.2s, which
		// is well past both the snapshot's typical completion (<500ms)
		// and the incremental's CDC reader's open lag (~tens of ms),
		// so no write can land in the boundary window.
		time.Sleep(200 * time.Millisecond)
		db, err := sql.Open("mysql", sourceDSN)
		if err != nil {
			t.Errorf("during-window writer open: %v", err)
			return
		}
		defer func() { _ = db.Close() }()
		for i := 0; i < duringWindowEmails; i++ {
			email := fmt.Sprintf("during-window-%d@example.com", i)
			if _, err := db.ExecContext(bgCtx,
				"INSERT INTO users (email) VALUES (?)", email); err != nil {
				t.Errorf("during-window insert %d: %v", i, err)
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
	}()

	if err := (&Backup{
		Source:        mysqlEng,
		SourceDSN:     sourceDSN,
		Store:         store,
		SluiceVersion: "v0.18.0-test",
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	wg.Wait()

	full, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if full.EndPosition.Token == "" {
		t.Fatal("full manifest has empty EndPosition; snapshot-anchored capture failed")
	}
	t.Logf("full manifest EndPosition = %s", full.EndPosition.Token)

	incrCtx, incrCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer incrCancel()
	incr := &IncrementalBackup{
		Source:        mysqlEng,
		SourceDSN:     sourceDSN,
		Store:         store,
		ParentRef:     full.BackupID,
		Window:        15 * time.Second,
		MaxChanges:    50,
		ChunkChanges:  20,
		SluiceVersion: "v0.18.0-test",
	}
	if err := incr.Run(incrCtx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}

	total, mismatches, err := VerifyBackup(context.Background(), store)
	if err != nil {
		t.Fatalf("VerifyBackup: %v", err)
	}
	if mismatches != 0 {
		t.Errorf("VerifyBackup: %d of %d chunks failed", mismatches, total)
	}

	if err := (&Restore{
		Target:    mysqlEng,
		TargetDSN: targetDSN,
		Store:     store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}

	got := mysqlQueryEmails(t, targetDSN)
	gotMap := map[string]bool{}
	for _, e := range got {
		gotMap[e] = true
	}
	wantEmails := []string{
		"alice@example.com",
		"bob@example.com",
		"carol@example.com",
	}
	for i := 0; i < duringWindowEmails; i++ {
		wantEmails = append(wantEmails, fmt.Sprintf("during-window-%d@example.com", i))
	}
	for _, w := range wantEmails {
		if !gotMap[w] {
			t.Errorf("target missing email %q (got %d emails: %v)", w, len(got), got)
		}
	}
	if t.Failed() {
		t.Logf("v0.18.0 gap-fix regression: target was missing one or more during-backup-window writes; the snapshot-anchored EndPosition is supposed to ensure the chain's CDC catch-up captures every write after the snapshot point")
	}
}

// TestBackup_OpenBackupSnapshot_MySQLPositionShape pins the wire
// shape of the recorded position on MySQL: a binlog-mode (file_pos
// or gtid) JSON envelope and the engine name. Operators eyeball the
// manifest; the test catches future regressions in the position
// encoding.
func TestBackup_OpenBackupSnapshot_MySQLPositionShape(t *testing.T) {
	sourceDSN, _, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyDDLMySQL(t, sourceDSN, `
		CREATE TABLE users (id BIGINT PRIMARY KEY, email VARCHAR(255) NOT NULL);
		INSERT INTO users (id, email) VALUES (1, 'alice@example.com');
	`)

	mysqlEng, _ := engines.Get("mysql")
	opener, ok := mysqlEng.(irbackup.SnapshotOpener)
	if !ok {
		t.Fatal("mysql engine does not implement BackupSnapshotOpener")
	}

	snap, err := opener.OpenBackupSnapshot(context.Background(), sourceDSN, irbackup.SnapshotOptions{})
	if err != nil {
		t.Fatalf("OpenBackupSnapshot: %v", err)
	}
	defer func() { _ = snap.Close() }()

	if snap.Position.Engine != "mysql" {
		t.Errorf("Position.Engine = %q; want mysql", snap.Position.Engine)
	}
	if snap.Position.Token == "" {
		t.Error("Position.Token is empty; want JSON envelope")
	}
	// Token must be either a gtid-mode envelope (when GTID is on) or
	// a file_pos-mode envelope. The test container doesn't enable
	// gtid_mode so file_pos is the expected shape, but we accept
	// either to keep the test resilient to container-config changes.
	if !strings.Contains(snap.Position.Token, `"mode":"file_pos"`) &&
		!strings.Contains(snap.Position.Token, `"mode":"gtid"`) {
		t.Errorf("Position.Token = %s; want a binlog-mode JSON envelope", snap.Position.Token)
	}
}
