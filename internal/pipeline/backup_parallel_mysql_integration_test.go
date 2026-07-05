//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0088 MySQL coordinated parallel backup snapshot. The cross-table
// parallel sweep now engages on a vanilla MySQL source: N reader
// transactions whose consistent snapshots COINCIDE under a brief
// FLUSH TABLES WITH READ LOCK window (mydumper's mechanism), instead of
// the pre-ADR single-reader serial sweep. This file is the behavioural
// oracle for that change:
//
//   - parallel engaged (dispatch observer) + zero-loss restore parity
//     vs the same corpus;
//   - the CONSISTENCY ORACLE (the correctness gate): pause the sweep
//     after the readers are opened, INSERT into both an already-listed
//     and a not-yet-listed table, resume — the artifact must contain
//     NEITHER insert, proving the N readers share one consistent point;
//   - a serial control proving the oracle exercises the coordinated path;
//   - FTWRL-denied → serial fallback (a role without RELOAD).
//
// The Vitess/PlanetScale path is unaffected — it ignores ReaderParallelism
// and takes the VStream COPY path (its existing tests stay green).

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/mysql"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// mysqlTableChecksum returns (row count, content checksum) for one
// two-column (id, label) test table.
func mysqlTableChecksum(t *testing.T, dsn, table string) (count, sum int64) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	defer func() { _ = db.Close() }()
	q := fmt.Sprintf("SELECT COUNT(*), COALESCE(SUM(CRC32(CONCAT_WS('|', id, label))), 0) FROM %s", table)
	if err := db.QueryRow(q).Scan(&count, &sum); err != nil {
		t.Fatalf("checksum %s: %v", table, err)
	}
	return count, sum
}

// seedMySQLBackupCorpus creates nTables (mbk_NN) of (id, label) and
// fills each with rowsPerTable rows (id 1..rowsPerTable). Returns the
// table names in creation order.
func seedMySQLBackupCorpus(t *testing.T, dsn string, nTables, rowsPerTable int) []string {
	t.Helper()
	tables := make([]string, nTables)
	for i := 0; i < nTables; i++ {
		table := fmt.Sprintf("mbk_%02d", i)
		tables[i] = table
		var ddl strings.Builder
		fmt.Fprintf(&ddl, `CREATE TABLE %s (
			id    BIGINT NOT NULL,
			label VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`, table)
		for r := 1; r <= rowsPerTable; r++ {
			fmt.Fprintf(&ddl, "\nINSERT INTO %s (id, label) VALUES (%d, 'tbl-%d-row-%d');", table, r, i, r)
		}
		applyDDLMySQL(t, dsn, ddl.String())
	}
	return tables
}

// withMySQLUser swaps the user:pass credentials at the head of a
// go-sql-driver DSN (`user:pass@tcp(host:port)/db?params`) for the
// supplied ones, keeping host/db/params. Used to build a limited-role
// DSN for the FTWRL-denied fallback test.
func withMySQLUser(t *testing.T, dsn, user, pass string) string {
	t.Helper()
	at := strings.Index(dsn, "@")
	if at < 0 {
		t.Fatalf("DSN %q has no @ separating credentials", dsn)
	}
	return user + ":" + pass + dsn[at:]
}

// mysqlRowExists reports whether a row with the given id is present in
// table.
func mysqlRowExists(t *testing.T, dsn, table string, id int64) bool {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = ?", table), id).Scan(&n); err != nil {
		t.Fatalf("row-exists query %s id=%d: %v", table, id, err)
	}
	return n > 0
}

// TestBackupParallel_MySQLCoordinated_RoundTripParity pins the headline
// ADR-0088 path: a TableParallelism=4 full backup on a vanilla MySQL
// source ENGAGES the coordinated parallel sweep (observer-asserted, not
// timing-inferred), and a restore's per-table checksums match the
// source — zero-loss parity with the corpus.
func TestBackupParallel_MySQLCoordinated_RoundTripParity(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	tables := seedMySQLBackupCorpus(t, sourceDSN, 6, 80)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	gotP, gotReason := observeBackupDispatch(t)
	if err := (&backup.Backup{
		Source:           mysqlEng,
		SourceDSN:        sourceDSN,
		Store:            store,
		ChunkRows:        25, // 80 rows → 4 chunks per table
		TableParallelism: 4,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	if *gotP <= 1 {
		t.Fatalf("dispatch = serial (reason %q); want the coordinated parallel sweep engaged on a vanilla MySQL source", *gotReason)
	}

	if err := (&backup.Restore{
		Target:    mysqlEng,
		TargetDSN: targetDSN,
		Store:     store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	for _, table := range tables {
		srcN, srcSum := mysqlTableChecksum(t, sourceDSN, table)
		dstN, dstSum := mysqlTableChecksum(t, targetDSN, table)
		if srcN != dstN || srcSum != dstSum {
			t.Errorf("table %s: source (n=%d sum=%d) != target (n=%d sum=%d)", table, srcN, srcSum, dstN, dstSum)
		}
	}
}

// TestBackupParallel_MySQLConsistencyOracle is the value-fidelity oracle
// (the correctness gate, ADR-0088 test matrix item 1). Two tables;
// engage the coordinated parallel backup but PAUSE the sweep after the
// readers are opened (the backupReadersOpenedHook test seam in the mysql
// engine, which fires once all N readers' snapshots are pinned and the
// FTWRL is released). While paused, INSERT into BOTH an already-listed
// (mbk_00) and a not-yet-listed (mbk_01) table on the source; resume —
// the backup artifact (restored into the target) must contain NEITHER
// insert, because every reader's snapshot predates the writes. A broken
// FTWRL window (readers seeing independent / post-write snapshots) would
// leak one or both inserts into the artifact.
func TestBackupParallel_MySQLConsistencyOracle(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	// Two tables, 40 rows each (id 1..40). The during-pause inserts use
	// id 9001/9002, which are absent from the snapshot.
	tables := seedMySQLBackupCorpus(t, sourceDSN, 2, 40)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	// Test seam: when the coordinated readers are open + the FTWRL is
	// released, synchronously INSERT into both an already-listed and a
	// not-yet-listed table. The readers' snapshots predate these writes,
	// so the backup must not see them. Fires exactly once.
	var hookFired int
	mysql.SetBackupReadersOpenedHookForTest(func() {
		hookFired++
		writer, werr := sql.Open("mysql", sourceDSN)
		if werr != nil {
			t.Errorf("oracle writer open: %v", werr)
			return
		}
		defer func() { _ = writer.Close() }()
		for _, table := range tables {
			if _, err := writer.Exec(
				fmt.Sprintf("INSERT INTO %s (id, label) VALUES (9001, 'POST-SNAPSHOT'), (9002, 'POST-SNAPSHOT')", table),
			); err != nil {
				t.Errorf("oracle insert into %s: %v", table, err)
			}
		}
	})
	t.Cleanup(func() { mysql.SetBackupReadersOpenedHookForTest(nil) })

	gotP, gotReason := observeBackupDispatch(t)
	if err := (&backup.Backup{
		Source:           mysqlEng,
		SourceDSN:        sourceDSN,
		Store:            store,
		ChunkRows:        100,
		TableParallelism: 2,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	if *gotP <= 1 {
		t.Fatalf("dispatch = serial (reason %q); the consistency oracle requires the coordinated parallel path", *gotReason)
	}
	if hookFired != 1 {
		t.Fatalf("readers-opened hook fired %d times; want exactly 1 (the coordinated open must have run)", hookFired)
	}

	if err := (&backup.Restore{
		Target:    mysqlEng,
		TargetDSN: targetDSN,
		Store:     store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}

	// The artifact must contain neither during-pause insert in either
	// table — 40 rows each, no id 9001/9002.
	for _, table := range tables {
		n, _ := mysqlTableChecksum(t, targetDSN, table)
		if n != 40 {
			t.Errorf("table %s restored with %d rows; want 40 — a during-pause INSERT leaked into the snapshot (broken FTWRL consistency window)", table, n)
		}
		if mysqlRowExists(t, targetDSN, table, 9001) || mysqlRowExists(t, targetDSN, table, 9002) {
			t.Errorf("table %s: a POST-SNAPSHOT row (id 9001/9002) is present in the backup artifact — the N readers did NOT share one consistent point", table)
		}
	}
}

// TestBackupParallel_MySQLConsistencyOracle_SerialControl is the control
// for the oracle: with a SINGLE reader (the pre-ADR serial path), the
// readers-opened hook never fires (no coordinated open) and the
// during-window writes are likewise absent from the artifact (the single
// START TRANSACTION WITH CONSISTENT SNAPSHOT pins the view). Confirms the
// oracle is exercising the coordinated path specifically, not an artifact
// of the serial path.
func TestBackupParallel_MySQLConsistencyOracle_SerialControl(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	tables := seedMySQLBackupCorpus(t, sourceDSN, 2, 40)

	mysqlEng, _ := engines.Get("mysql")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	var hookFired int
	mysql.SetBackupReadersOpenedHookForTest(func() { hookFired++ })
	t.Cleanup(func() { mysql.SetBackupReadersOpenedHookForTest(nil) })

	gotP, _ := observeBackupDispatch(t)
	if err := (&backup.Backup{
		Source:           mysqlEng,
		SourceDSN:        sourceDSN,
		Store:            store,
		ChunkRows:        100,
		TableParallelism: 1, // serial: no coordinated open
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	if *gotP != 1 {
		t.Fatalf("dispatch parallelism = %d; want 1 (serial control)", *gotP)
	}
	if hookFired != 0 {
		t.Fatalf("readers-opened hook fired %d times on the serial path; want 0 (no coordinated open)", hookFired)
	}

	if err := (&backup.Restore{Target: mysqlEng, TargetDSN: targetDSN, Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	for _, table := range tables {
		if n, _ := mysqlTableChecksum(t, targetDSN, table); n != 40 {
			t.Errorf("table %s restored with %d rows; want 40", table, n)
		}
	}
}

// TestBackupParallel_MySQLFTWRLDeniedFallsBackToSerial pins the
// FTWRL-denied → serial fallback (ADR-0088 test matrix item 3): a source
// role WITHOUT the RELOAD privilege cannot FLUSH TABLES WITH READ LOCK,
// so the coordinated open aborts and the run falls back to the
// single-reader serial sweep. The dispatch observer reports serial (the
// not-shareable reason), the readers-opened hook never fires, the backup
// still succeeds, and the restore checksums match. No regression.
func TestBackupParallel_MySQLFTWRLDeniedFallsBackToSerial(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	tables := seedMySQLBackupCorpus(t, sourceDSN, 3, 50)

	// A role with broad SELECT but NO RELOAD (FTWRL needs RELOAD).
	applyDDLMySQL(t, sourceDSN, `
		CREATE USER 'norelo'@'%' IDENTIFIED BY 'norelopw';
		GRANT SELECT, SHOW VIEW, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'norelo'@'%';
		FLUSH PRIVILEGES;
	`)
	limitedDSN := withMySQLUser(t, sourceDSN, "norelo", "norelopw")

	mysqlEng, _ := engines.Get("mysql")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	// The hook must NOT fire — the coordinated open aborts before the
	// readers are pinned (FTWRL is rejected up front).
	var hookFired int
	mysql.SetBackupReadersOpenedHookForTest(func() { hookFired++ })
	t.Cleanup(func() { mysql.SetBackupReadersOpenedHookForTest(nil) })

	gotP, gotReason := observeBackupDispatch(t)
	if err := (&backup.Backup{
		Source:           mysqlEng,
		SourceDSN:        limitedDSN,
		Store:            store,
		ChunkRows:        20,
		TableParallelism: 4,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run (limited role): %v", err)
	}
	if *gotP != 1 {
		t.Fatalf("dispatch parallelism = %d (reason %q); want 1 — FTWRL-denied must fall back to serial", *gotP, *gotReason)
	}
	if !strings.Contains(*gotReason, "not shareable") {
		t.Errorf("serial reason = %q; want the not-shareable clause (FTWRL fallback leaves no extra readers)", *gotReason)
	}
	if hookFired != 0 {
		t.Fatalf("readers-opened hook fired %d times; want 0 (FTWRL rejected → no readers pinned)", hookFired)
	}

	// Restore with the full-privilege DSN (target is its own db) and
	// confirm zero-loss parity despite the serial fallback.
	if err := (&backup.Restore{Target: mysqlEng, TargetDSN: targetDSN, Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	for _, table := range tables {
		srcN, srcSum := mysqlTableChecksum(t, sourceDSN, table)
		dstN, dstSum := mysqlTableChecksum(t, targetDSN, table)
		if srcN != dstN || srcSum != dstSum {
			t.Errorf("table %s: source (n=%d sum=%d) != target (n=%d sum=%d)", table, srcN, srcSum, dstN, dstSum)
		}
	}
}

// TestBackupParallel_MySQLCancelResumeCompletes pins the ADR-0084 crash
// contract under the ADR-0088 coordinated parallel sweep: a run
// cancelled mid-parallel-sweep leaves a resumable in-progress manifest
// (every table staged in schema order), and a plain re-run resumes to a
// complete backup whose restore checksums match the source. Coordinated
// readers don't change the manifest shape — the ≤N-partials contract
// applies unchanged. (cancelAfterChunkPutsStore lives in the PG parallel
// integration test, same package.)
func TestBackupParallel_MySQLCancelResumeCompletes(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	const nTables, tableParallelism = 6, 4
	tables := seedMySQLBackupCorpus(t, sourceDSN, nTables, 120)

	mysqlEng, _ := engines.Get("mysql")
	inner, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	// 120 rows / 30-row chunks = 4 chunks per table, 24 total; cancel
	// once the 8th chunk lands so several tables are mid-stream.
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &cancelAfterChunkPutsStore{LocalStore: inner, n: 8, cancel: cancel}
	gotP, _ := observeBackupDispatch(t)
	err = (&backup.Backup{
		Source:           mysqlEng,
		SourceDSN:        sourceDSN,
		Store:            store,
		ChunkRows:        30,
		TableParallelism: tableParallelism,
	}).Run(runCtx)
	if err == nil {
		t.Fatal("cancelled Backup.Run returned nil; want a context error")
	}
	if *gotP <= 1 {
		t.Fatalf("dispatch parallelism = %d; want the coordinated parallel sweep engaged before the cancel", *gotP)
	}

	m, err := lineage.ReadManifest(context.Background(), inner)
	if err != nil {
		t.Fatalf("lineage.ReadManifest after cancel: %v", err)
	}
	if len(m.Tables) != nTables {
		t.Fatalf("staged tables = %d; want all %d (pre-staging invariant)", len(m.Tables), nTables)
	}
	for i, table := range m.Tables {
		if table.Name != tables[i] {
			t.Errorf("manifest.Tables[%d] = %s; want %s (schema order)", i, table.Name, tables[i])
		}
	}

	// Plain re-run (no force-overwrite) resumes to completion.
	if err := (&backup.Backup{
		Source:           mysqlEng,
		SourceDSN:        sourceDSN,
		Store:            inner,
		ChunkRows:        30,
		TableParallelism: tableParallelism,
	}).Run(context.Background()); err != nil {
		t.Fatalf("resume Backup.Run: %v", err)
	}

	if err := (&backup.Restore{Target: mysqlEng, TargetDSN: targetDSN, Store: inner}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	for _, table := range tables {
		srcN, srcSum := mysqlTableChecksum(t, sourceDSN, table)
		dstN, dstSum := mysqlTableChecksum(t, targetDSN, table)
		if srcN != dstN || srcSum != dstSum {
			t.Errorf("table %s: source (n=%d sum=%d) != target (n=%d sum=%d)", table, srcN, srcSum, dstN, dstSum)
		}
	}
}
