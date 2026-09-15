//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 163 end to end, per trigger engine: a `backup full` on a
// trigger-CDC source records the change log's anchor; N rows are written
// AFTER the full; `backup incremental` chains off it; the chain restores
// into a fresh Postgres; and the restored count equals the SOURCE's own
// count read after the writes — the independent expected value, never the
// chain's. Before item 163 the pgtrigger full recorded a pgoutput position
// the poller refused as foreign (a loud failure at the first incremental)
// and the sqlite-trigger full recorded nothing, so its incremental anchored
// "from now" and the restore was short by every post-full row — 100 of 100
// in the v0.153.1 regression cycle, the exact shape asserted here.
//
// D1 has no live access in this release: the d1-trigger engine runs the
// SAME openBackupSnapshot through the backend seam, pinned against the D1
// mock in internal/engines/sqlite-trigger/backup_snapshot_test.go.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"

	_ "modernc.org/sqlite"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// triggerChainPostFullRows is N: the rows written between the full and
// the incremental — the window the pre-fix chain lost entirely.
const triggerChainPostFullRows = 100

// TestBackupChain_PGTrigger_PostFullRowsReachTheRestore is the pgtrigger
// pin.
func TestBackupChain_PGTrigger_PostFullRowsReachTheRestore(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()
	applyDDL(t, sourceDSN, `
		CREATE TABLE events (id BIGINT PRIMARY KEY, n INTEGER NOT NULL, note TEXT);
		INSERT INTO events (id, n, note) SELECT g, g * 2, 'seed-' || g FROM generate_series(1, 50) g;
	`)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if _, err := pgtrigger.Setup(ctx, sourceDSN, pgtrigger.SetupOptions{Tables: []string{"events"}, Schema: "public"}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}
	src, ok := engines.Get(pgtrigger.EngineName)
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}

	store := runTriggerChainFull(ctx, t, src, sourceDSN, pgtrigger.EngineName, func(tok string) (int64, error) {
		return pgtrigger.AppliedLastID(tok)
	})

	// N rows AFTER the full — the window.
	applyDDL(t, sourceDSN, fmt.Sprintf(
		`INSERT INTO events (id, n, note) SELECT g, g * 3, 'post-full-' || g FROM generate_series(51, %d) g;`,
		50+triggerChainPostFullRows,
	))
	wantRows := pgQueryOne[int64](t, sourceDSN, "SELECT COUNT(*) FROM events")
	wantSum := pgQueryOne[int64](t, sourceDSN, "SELECT COALESCE(SUM(n), 0) FROM events")

	runTriggerChainIncrementalAndRestore(ctx, t, src, sourceDSN, store, targetDSN)

	gotRows := pgQueryOne[int64](t, targetDSN, "SELECT COUNT(*) FROM events")
	gotSum := pgQueryOne[int64](t, targetDSN, "SELECT COALESCE(SUM(n), 0) FROM events")
	if gotRows != wantRows || gotSum != wantSum {
		t.Fatalf("chain restore: target rows/sum = %d/%d; source (read AFTER the post-full writes) = %d/%d — the "+
			"window between the full's sweep and the incremental's anchor is missing from the chain", gotRows, gotSum, wantRows, wantSum)
	}
	if wantRows != 50+triggerChainPostFullRows {
		t.Errorf("source rows = %d; want %d (test premise broken)", wantRows, 50+triggerChainPostFullRows)
	}
}

// TestBackupChain_SQLiteTrigger_PostFullRowsReachTheRestore is the
// sqlite-trigger pin against the local executor (the d1-trigger engine
// shares the code path; see the file doc).
func TestBackupChain_SQLiteTrigger_PostFullRowsReachTheRestore(t *testing.T) {
	path := seedSQLiteTriggerSource(t) // events(id, big, blb, note) with 2 seed rows, trigger setup done
	_, targetDSN, cleanup := startPostgres(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	src, ok := engines.Get(sqlitetrigger.EngineName)
	if !ok {
		t.Fatal("sqlite-trigger engine not registered")
	}

	store := runTriggerChainFull(ctx, t, src, path, sqlitetrigger.EngineName, sqlitetrigger.AppliedLastID)

	for i := 1; i <= triggerChainPostFullRows; i++ {
		sqliteExec(t, path, `INSERT INTO events (id, big, blb, note) VALUES (?, ?, NULL, ?)`, 100+i, int64(1)<<40+int64(i), fmt.Sprintf("post-full-%d", i))
	}
	wantRows, wantSum := sqliteCountAndSum(t, path)

	runTriggerChainIncrementalAndRestore(ctx, t, src, path, store, targetDSN)

	gotRows := pgQueryOne[int64](t, targetDSN, "SELECT COUNT(*) FROM events")
	gotSum := pgQueryOne[int64](t, targetDSN, "SELECT COALESCE(SUM(big), 0) FROM events")
	if gotRows != wantRows || gotSum != wantSum {
		t.Fatalf("chain restore: target rows/sum(big) = %d/%d; source (read AFTER the post-full writes) = %d/%d — the "+
			"window between the full's sweep and the incremental's anchor is missing from the chain", gotRows, gotSum, wantRows, wantSum)
	}
	if wantRows != 2+triggerChainPostFullRows {
		t.Errorf("source rows = %d; want %d (test premise broken)", wantRows, 2+triggerChainPostFullRows)
	}
}

// runTriggerChainFull takes the full and asserts the manifest carries a
// change-log anchor under the engine's own tag that the engine's resume
// decoder reads — the fact the chain's first link resumes on.
func runTriggerChainFull(ctx context.Context, t *testing.T, src ir.Engine, sourceDSN, engineName string, decode func(string) (int64, error)) *blobcodec.LocalStore {
	t.Helper()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := (&backup.Backup{Source: src, SourceDSN: sourceDSN, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
		t.Fatalf("backup full on %s: %v", engineName, err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("read full manifest: %v", err)
	}
	if full.EndPosition.Engine != engineName {
		t.Fatalf("full EndPosition = %+v; want a %q-tagged change-log anchor (a pgoutput/empty position here is the pre-item-163 defect)", full.EndPosition, engineName)
	}
	if _, err := decode(full.EndPosition.Token); err != nil {
		t.Fatalf("full EndPosition token %q does not decode through the engine's own resume decoder: %v", full.EndPosition.Token, err)
	}
	return store
}

// runTriggerChainIncrementalAndRestore takes one incremental (closing on
// the post-full row count) and chain-restores into targetDSN, asserting
// the link starts exactly where the full ended.
func runTriggerChainIncrementalAndRestore(ctx context.Context, t *testing.T, src ir.Engine, sourceDSN string, store *blobcodec.LocalStore, targetDSN string) {
	t.Helper()
	incr := &IncrementalBackup{
		Source:        src,
		SourceDSN:     sourceDSN,
		Store:         store,
		Window:        45 * time.Second,
		MaxChanges:    triggerChainPostFullRows,
		ChunkChanges:  25,
		SluiceVersion: "test",
	}
	if err := incr.Run(ctx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}
	records, err := lineage.ListAllManifestsViaWalk(ctx, store)
	if err != nil {
		t.Fatalf("ListAllManifestsViaWalk: %v", err)
	}
	var full, link *irbackup.Manifest
	for _, r := range records {
		switch r.Manifest.Kind {
		case irbackup.BackupKindFull:
			full = r.Manifest
		case irbackup.BackupKindIncremental:
			link = r.Manifest
		}
	}
	if full == nil || link == nil {
		t.Fatalf("chain has full=%v incremental=%v; want both", full != nil, link != nil)
	}
	if link.StartPosition != full.EndPosition {
		t.Fatalf("incremental StartPosition = %+v; want the full's EndPosition %+v", link.StartPosition, full.EndPosition)
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	if err := (&backup.ChainRestore{Target: pgEng, TargetDSN: targetDSN, Store: store}).Run(ctx); err != nil {
		t.Fatalf("ChainRestore.Run: %v", err)
	}
}

// sqliteCountAndSum reads the source's own COUNT(*) and SUM(big) — the
// independent expected value for the restore.
func sqliteCountAndSum(t *testing.T, path string) (rows, sum int64) {
	t.Helper()
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*), COALESCE(SUM(big), 0) FROM events`).Scan(&rows, &sum); err != nil {
		t.Fatalf("count source: %v", err)
	}
	return rows, sum
}
