//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MySQL integration coverage for the sampled-keyset within-table
// parallel-copy strategy (ADR-0096). Before this file there was ZERO
// MySQL keyset integration coverage — the original suite was Postgres
// only, which is part of how the collation coverage-gap (string PK under
// a non-C collation) shipped: MySQL's default collation
// utf8mb4_0900_ai_ci is case- AND accent-INSENSITIVE, an even wider
// divergence from byte order than PG's en_US.utf8.
//
// The pin is the exactly-once property end-to-end: a varchar-PK table
// above the chunk threshold, copied with --bulk-parallelism>1, must land
// every row exactly once (source COUNT == target COUNT, and an
// order-independent content checksum matches). Under the old Go-side
// bytewise upper-bound clip, boundary-straddling rows whose collated
// order differs from byte order fell into NO chunk — silent loss.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// mysqlCountAndChecksum returns (count, checksum) for a table over a
// MySQL DSN. The checksum is an order-independent SUM of per-row
// 64-bit md5 prefixes (the MySQL analogue of the PG helper), so equal
// values on source and target ⇒ identical row sets regardless of order
// or which chunk copied which row.
func mysqlCountAndChecksum(t *testing.T, dsn, table, checksumExpr string) (count int64, checksum string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open for checksum: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	// CONV(SUBSTRING(MD5(x),1,16),16,10) → a 64-bit unsigned from the
	// md5 hex prefix; CAST to DECIMAL and SUM for an order-independent
	// aggregate. NULL-safe via COALESCE.
	sumQ := fmt.Sprintf(
		"SELECT COALESCE(CAST(SUM(CAST(CONV(SUBSTRING(MD5(%s),1,16),16,10) AS DECIMAL(30,0))) AS CHAR),'0') FROM %s",
		checksumExpr, table,
	)
	var sum sql.NullString
	if err := db.QueryRowContext(ctx, sumQ).Scan(&sum); err != nil {
		t.Fatalf("checksum %s: %v", table, err)
	}
	return count, sum.String
}

func assertMySQLCountAndChecksum(t *testing.T, sourceDSN, targetDSN, table, checksumExpr string) {
	t.Helper()
	srcCount, srcSum := mysqlCountAndChecksum(t, sourceDSN, table, checksumExpr)
	tgtCount, tgtSum := mysqlCountAndChecksum(t, targetDSN, table, checksumExpr)
	if srcCount != tgtCount {
		t.Errorf("%s: count source=%d target=%d (rows missing or duplicated)", table, srcCount, tgtCount)
	}
	if srcSum != tgtSum {
		t.Errorf("%s: checksum source=%s target=%s (rows missing/duplicated/corrupted)", table, srcSum, tgtSum)
	}
}

// insertMySQLStringPKRows seeds a varchar-PK table. Values come from the
// shared collated generator (case + punctuation + accent); the per-row
// numeric suffix keeps them distinct even under the case-/accent-
// insensitive utf8mb4_0900_ai_ci PK uniqueness.
func insertMySQLStringPKRows(t *testing.T, dsn, table string, vals []string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open for seed: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO "+table+" (id, label) VALUES (?, ?)")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	for i, v := range vals {
		if _, err := stmt.ExecContext(ctx, v, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("insert %q: %v", v, err)
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := db.ExecContext(ctx, "ANALYZE TABLE "+table); err != nil {
		t.Fatalf("analyze: %v", err)
	}
}

// TestMigrate_MySQL_KeysetCopy_VarcharPK_DefaultCollation is the MySQL
// counterpart to the PG string-PK collation pin, on the wider-divergence
// utf8mb4_0900_ai_ci default. A varchar-PK table above the threshold is
// copied with parallelism>1; every row must land exactly once.
func TestMigrate_MySQL_KeysetCopy_VarcharPK_DefaultCollation(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	const rowCount = 40_000
	applyMySQLDDL(t, sourceDSN, `
		CREATE TABLE names (
			id    VARCHAR(64) NOT NULL,
			label TEXT        NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)
	insertMySQLStringPKRows(t, sourceDSN, "names", makeCollatedStringValues(rowCount))

	myEng, _ := engines.Get("mysql")
	logs := captureSlog(t)
	mig := &Migrator{
		Source:              myEng,
		Target:              myEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		BulkParallelism:     4,
		BulkParallelMinRows: 10_000,
		MigrationID:         "test-keyset-mysql-varchar",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}
	assertChunkFanout(t, logs.String(), 2)
	assertMySQLCountAndChecksum(t, sourceDSN, targetDSN, "names", "CONCAT(id, '|', label)")
}

// TestMigrate_MySQL_KeysetCopy_CompositePK exercises a (varchar, int)
// composite keyset PK so the boundary tuple's second column is collated
// too.
func TestMigrate_MySQL_KeysetCopy_CompositePK(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	const rowCount = 40_000
	applyMySQLDDL(t, sourceDSN, `
		CREATE TABLE memberships (
			tenant VARCHAR(16) NOT NULL,
			seq    BIGINT      NOT NULL,
			payload TEXT       NOT NULL,
			PRIMARY KEY (tenant, seq)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)
	db, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		t.Fatalf("open seed: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tenants := []string{"a", "A", "b", "B", "_", "z", "Z", "é", "ñ", "9"}
	tx, _ := db.BeginTx(ctx, nil)
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO memberships (tenant, seq, payload) VALUES (?, ?, ?)")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	for g := 1; g <= rowCount; g++ {
		if _, err := stmt.ExecContext(ctx, tenants[g%len(tenants)], g, fmt.Sprintf("p-%d", g)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := db.ExecContext(ctx, "ANALYZE TABLE memberships"); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	myEng, _ := engines.Get("mysql")
	logs := captureSlog(t)
	mig := &Migrator{
		Source:              myEng,
		Target:              myEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		BulkParallelism:     4,
		BulkParallelMinRows: 10_000,
		MigrationID:         "test-keyset-mysql-composite",
	}
	rctx, rcancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer rcancel()
	if err := mig.Run(rctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}
	assertChunkFanout(t, logs.String(), 2)
	assertMySQLCountAndChecksum(t, sourceDSN, targetDSN, "memberships",
		"CONCAT(tenant, '|', seq, '|', payload)")
}

// TestMigrate_MySQL_KeysetCopy_DecimalPK pins the decimal PK family where
// lexical != numeric order. With the SQL upper bound MySQL compares both
// bounds numerically; the old bytewise Go clip compared decimal-as-text
// lexically and could drop boundary rows.
func TestMigrate_MySQL_KeysetCopy_DecimalPK(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	const rowCount = 40_000
	applyMySQLDDL(t, sourceDSN, `
		CREATE TABLE ledger (
			id    DECIMAL(20,4) NOT NULL,
			label TEXT          NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)
	db, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		t.Fatalf("open seed: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tx, _ := db.BeginTx(ctx, nil)
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO ledger (id, label) VALUES (?, ?)")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	for g := 1; g <= rowCount; g++ {
		// g + 0.5: text order 1,10,100,... diverges from numeric 1,2,3,...
		if _, err := stmt.ExecContext(ctx, fmt.Sprintf("%d.5000", g), fmt.Sprintf("r-%d", g)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := db.ExecContext(ctx, "ANALYZE TABLE ledger"); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	myEng, _ := engines.Get("mysql")
	logs := captureSlog(t)
	mig := &Migrator{
		Source:              myEng,
		Target:              myEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		BulkParallelism:     4,
		BulkParallelMinRows: 10_000,
		MigrationID:         "test-keyset-mysql-decimal",
	}
	rctx, rcancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer rcancel()
	if err := mig.Run(rctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}
	assertChunkFanout(t, logs.String(), 2)
	assertMySQLCountAndChecksum(t, sourceDSN, targetDSN, "ledger", "CONCAT(id, '|', label)")
}
