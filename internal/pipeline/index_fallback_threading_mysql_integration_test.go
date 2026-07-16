//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for the audit-MED-A1 widening of the ADR-0148
// deploy-request index-build fallback beyond migrate: `restore` and the
// sync cold-start now thread [ir.IndexBuildFallback] onto the REAL MySQL
// SchemaWriter through their real orchestrator paths. A vanilla MySQL
// container can't produce the walled errno-3024/1105 shapes (they are
// PlanetScale control-plane behaviours), so what a real target CAN pin —
// and what these tests pin — is the never-worse half of the contract on
// each mode: an ARMED run against a healthy target is byte-identical
// (indexes build directly, the fallback is threaded but never consulted,
// the run succeeds). The walled-error routing itself is pinned against
// the real writer's CreateIndexes in
// internal/engines/mysql/schema_writer_index_fallback_test.go — the SAME
// engine method both modes call — and the threading seams are unit-pinned
// per mode (restore_index_fallback_test.go,
// streamer_coldstart_index_fallback_test.go).

package pipeline

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// countingIndexFallback records BuildIndexDDL consultations; on a healthy
// target it must stay at zero (never-worse: armed == unarmed when the
// direct build succeeds).
type countingIndexFallback struct{ calls atomic.Int64 }

func (f *countingIndexFallback) BuildIndexDDL(context.Context, string, []string, error) error {
	f.calls.Add(1)
	return nil
}

// fbtIndexExists reports whether the named secondary index exists on
// the table, via information_schema.statistics.
func fbtIndexExists(t *testing.T, dsn, table, index string) bool {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT index_name) FROM information_schema.statistics
		 WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?`,
		table, index).Scan(&n); err != nil {
		t.Fatalf("index probe: %v", err)
	}
	return n > 0
}

const indexFallbackSeedDDL = `
	CREATE TABLE fbt (
		id    BIGINT       NOT NULL AUTO_INCREMENT,
		email VARCHAR(255) NOT NULL,
		PRIMARY KEY (id),
		KEY idx_email (email)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	INSERT INTO fbt (email) VALUES ('a@example.com'), ('b@example.com'), ('c@example.com');
`

// TestRestore_ArmedIndexFallback_MySQLNeverWorse pins the restore mode:
// an armed restore into a healthy real MySQL target completes, builds
// the secondary index directly, and never consults the fallback.
func TestRestore_ArmedIndexFallback_MySQLNeverWorse(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()
	applyDDLMySQL(t, sourceDSN, indexFallbackSeedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := (&backup.Backup{
		Source: mysqlEng, SourceDSN: sourceDSN, Store: store, SluiceVersion: "test",
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	fb := &countingIndexFallback{}
	if err := (&backup.Restore{
		Target:             mysqlEng,
		TargetDSN:          targetDSN,
		Store:              store,
		IndexBuildFallback: fb,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run (armed): %v", err)
	}
	if n := countRowsMySQL(t, targetDSN, "fbt"); n != 3 {
		t.Errorf("restored rows = %d; want 3", n)
	}
	if !fbtIndexExists(t, targetDSN, "fbt", "idx_email") {
		t.Error("secondary index idx_email missing on the restored target")
	}
	if n := fb.calls.Load(); n != 0 {
		t.Errorf("fallback consulted %d times on a healthy direct build; want 0 (never-worse)", n)
	}
}

// TestSyncColdStart_ArmedIndexFallback_MySQLNeverWorse pins the sync
// cold-start mode: an armed streamer's cold start copies the rows,
// builds the secondary index directly, and never consults the fallback.
func TestSyncColdStart_ArmedIndexFallback_MySQLNeverWorse(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()
	applyDDLMySQL(t, sourceDSN, indexFallbackSeedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	fb := &countingIndexFallback{}
	streamer := &Streamer{
		Source:             mysqlEng,
		Target:             mysqlEng,
		SourceDSN:          sourceDSN,
		TargetDSN:          targetDSN,
		StreamID:           "index-fallback-coldstart",
		IndexBuildFallback: fb,
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(20 * time.Second):
			t.Errorf("streamer did not exit within 20s of cancel")
		}
	}()

	if !waitForRowCountMySQL(t, targetDSN, "fbt", 3, 60*time.Second) {
		t.Fatalf("cold start never delivered the 3 seed rows")
	}
	// The deferred index phase runs right after the copy drains; poll for
	// it rather than asserting instantly (rows land one phase earlier).
	indexDeadline := time.Now().Add(30 * time.Second)
	for !fbtIndexExists(t, targetDSN, "fbt", "idx_email") {
		if time.Now().After(indexDeadline) {
			t.Fatal("secondary index idx_email missing after the cold start")
		}
		time.Sleep(250 * time.Millisecond)
	}
	if n := fb.calls.Load(); n != 0 {
		t.Errorf("fallback consulted %d times on a healthy direct build; want 0 (never-worse)", n)
	}
}
