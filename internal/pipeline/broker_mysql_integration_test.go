//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MySQL counterpart of broker_pg_integration_test.go: covers
// Acceptance Criterion 2 (end-to-end MySQL → MySQL broker) using the
// same two-goroutine pattern (stream + broker in the test process).

package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"

	_ "github.com/orware/sluice/internal/engines/mysql"
)

// TestSyncFromBackup_MySQL_HappyPath drives the same shape as the PG
// happy-path test against MySQL: full → stream + broker pair, observe
// target catch up.
func TestSyncFromBackup_MySQL_HappyPath(t *testing.T) {
	sourceDSN, brokerTargetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('alice@example.com');
	`
	applyDDLMySQL(t, sourceDSN, seedDDL)

	mysqlEng, _ := engines.Get("mysql")
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	// Take the full + patch in EndPosition so the stream chain has a
	// resume point. Mirrors the existing MySQL incremental tests.
	if err := (&Backup{
		Source: mysqlEng, SourceDSN: sourceDSN, Store: store, SluiceVersion: "test",
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	binlogFile, binlogPos := readMySQLBinlogPos(t, sourceDSN)
	full, _ := readManifest(context.Background(), store)
	full.Kind = ir.BackupKindFull
	full.EndPosition = ir.Position{
		Engine: "mysql",
		Token:  fmt.Sprintf(`{"mode":"file_pos","file":%q,"pos":%d}`, binlogFile, binlogPos),
	}
	full.BackupID = ir.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("rewrite full manifest: %v", err)
	}

	// Pre-restore the full into the broker target so the broker has
	// the schema + the alice row.
	if err := (&Restore{
		Target: mysqlEng, TargetDSN: brokerTargetDSN, Store: store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("seed restore: %v", err)
	}

	// Drive 5 inserts on the source.
	for i := 0; i < 5; i++ {
		applyDDLMySQL(t, sourceDSN, fmt.Sprintf(
			`INSERT INTO users (email) VALUES ('user%d@example.com');`, i))
	}

	// Take an incremental that captures them. MaxChanges is generous
	// because MySQL's binlog emits ~3 events per autocommit INSERT
	// (BEGIN QueryEvent → WRITE_ROWS_EVENTv2 → XID/TxCommit) AND the
	// session's first connection-time interaction commonly flushes a
	// short empty BEGIN/COMMIT pair into the binlog (observed in the
	// v0.20.0 CI failure for this test: 5 INSERTs produced 17 events,
	// the first 2 of which were a spurious empty transaction). A cap
	// of 10 stopped after only 3 INSERTs, leaving user3/user4 missing
	// from the chain. 50 is a comfortable headroom that doesn't slow
	// the test perceptibly. ChunkChanges follows the same logic.
	incrCtx, incrCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer incrCancel()
	if err := (&IncrementalBackup{
		Source: mysqlEng, SourceDSN: sourceDSN, Store: store,
		ParentRef: full.BackupID, Window: 10 * time.Second, MaxChanges: 50,
		ChunkChanges: 50, SluiceVersion: "test",
	}).Run(incrCtx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}

	// Run the broker.
	broker := &SyncFromBackup{
		Target:        mysqlEng,
		TargetDSN:     brokerTargetDSN,
		Store:         store,
		ChainURL:      "test://mysql-broker",
		StreamID:      "mysql-broker",
		PollInterval:  2 * time.Second,
		AtChainID:     full.BackupID,
		SluiceVersion: "test",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	brokerErr := make(chan error, 1)
	go func() { brokerErr <- broker.Run(ctx) }()

	// Wait for catch-up.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		got := mysqlQueryEmails(t, brokerTargetDSN)
		if len(got) >= 6 { // alice + 5 users
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	got := mysqlQueryEmails(t, brokerTargetDSN)
	if len(got) < 6 {
		t.Fatalf("broker did not catch up: target emails = %v; want >= 6", got)
	}

	cancel()
	select {
	case err := <-brokerErr:
		if err != nil {
			t.Errorf("broker.Run = %v; want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("broker did not exit within 10s of cancel")
	}
}
