//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MySQL leg of the ADR-0084 class matrix: the cross-table parallel
// sweep must NOT engage on a MySQL source — its consistent snapshot is
// per-session (START TRANSACTION WITH CONSISTENT SNAPSHOT) with no
// shareable name, so parallel readers would see N independent
// snapshots. The gate refuses (observer-asserted, with the reason
// operators see in the INFO log) and the serial sweep still
// round-trips zero-loss under a --table-parallelism request.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
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

// TestBackupParallel_MySQLStaysSerial pins the gate's MySQL refusal +
// zero-loss serial round-trip: TableParallelism=4 requested, dispatch
// observer reports serial with the not-shareable reason, and a
// restore's per-table checksums match the source.
func TestBackupParallel_MySQLStaysSerial(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	tables := []string{"mbk_00", "mbk_01", "mbk_02"}
	for i, table := range tables {
		var ddl strings.Builder
		fmt.Fprintf(&ddl, `CREATE TABLE %s (
			id    BIGINT NOT NULL,
			label VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`, table)
		for r := 1; r <= 60; r++ {
			fmt.Fprintf(&ddl, "\nINSERT INTO %s (id, label) VALUES (%d, 'tbl-%d-row-%d');", table, r, i, r)
		}
		applyDDLMySQL(t, sourceDSN, ddl.String())
	}

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	gotP, gotReason := observeBackupDispatch(t)
	if err := (&Backup{
		Source:           mysqlEng,
		SourceDSN:        sourceDSN,
		Store:            store,
		ChunkRows:        25, // 60 rows → 3 chunks per table
		TableParallelism: 4,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	if *gotP != 1 {
		t.Fatalf("dispatch parallelism = %d; want 1 — MySQL's per-session snapshot must keep the sweep serial", *gotP)
	}
	if !strings.Contains(*gotReason, "not shareable") {
		t.Errorf("serial reason = %q; want the not-shareable clause", *gotReason)
	}

	if err := (&Restore{
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
