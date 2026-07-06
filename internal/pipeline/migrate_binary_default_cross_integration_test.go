//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine integration pins for the BINARY/VARBINARY column-DEFAULT
// round-trip on the NON-MySQL targets (the two leaks the same-engine
// MySQL→MySQL fix did not close). MySQL 8 reports a binary column's literal
// default in information_schema.COLUMN_DEFAULT as a bare hex literal (`0x…`);
// the reader tags it ir.DefaultExpression{Dialect:"hexbytes"}. The MySQL
// emitter renders it bare (`DEFAULT 0x…`), which MySQL round-trips — but the
// PG and SQLite emitters must translate that hex payload into THEIR native
// binary-literal syntax (PG BYTEA `'\x…'::bytea`, SQLite BLOB `X'…'`).
//
// Pre-fix both writers emitted the hexbytes payload verbatim: PG aborted
// CREATE TABLE (the abort site), and SQLite wrapped it as `(0x…)` which SQLite
// parses as an overflowing INTEGER — a silently-wrong default.
//
// Both tests assert (a) CREATE TABLE succeeds on the target, and (b) the
// DEFAULT-applied value is BYTE-EXACT: a DEFAULT-only inserted row on the
// target has the same stored bytes as the same insert on MySQL. Pins the
// CLASS (Bug-74 discipline): fixed BINARY exact-width, variable VARBINARY, and
// a BINARY(8) whose default is SHORTER than the width (MySQL NUL-pads to 8
// bytes; the padding rides in the hex payload, so BYTEA/BLOB must carry it).

package pipeline

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver for reading the produced .db file

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// binaryDefaultSeedDDL is the shared source shape: the MediaWiki-corpus
// BINARY(14) exact-width column, a VARBINARY(20) sibling, and a BINARY(8)
// whose default 'ab' is SHORTER than the width (MySQL NUL-pads to 8 bytes).
const binaryDefaultSeedDDL = `
	CREATE TABLE bin_defaults (
		id     INT NOT NULL,
		log_ts BINARY(14)   NOT NULL DEFAULT '19700101000000',
		tag    VARBINARY(20) NOT NULL DEFAULT 'hello',
		padded BINARY(8)    NOT NULL DEFAULT 'ab',
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`

// TestMigrate_BinaryColumnDefault_MySQLToPG pins GAP 1: MySQL BINARY/VARBINARY
// column defaults → PG BYTEA. The CREATE TABLE is the abort site (pre-fix the
// PG writer leaked the MySQL `0x…` spelling, which PG can't parse as bytea).
func TestMigrate_BinaryColumnDefault_MySQLToPG(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, mysqlSource, binaryDefaultSeedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mig := &Migrator{Source: mysqlEng, Target: pgEng, SourceDSN: mysqlSource, TargetDSN: pgTarget}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// GAP 1 abort site: pre-fix CREATE TABLE failed on PG because the
	// hexbytes payload leaked verbatim.
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (MySQL→PG) = %v; want SUCCESS (binary-default DDL must be valid PG)", err)
	}

	pg, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = pg.Close() }()

	// The columns landed as bytea (BINARY/VARBINARY → BYTEA).
	for _, col := range []string{"log_ts", "tag", "padded"} {
		var dataType string
		if err := pg.QueryRowContext(ctx, `
			SELECT data_type FROM information_schema.columns
			WHERE table_name = 'bin_defaults' AND column_name = $1`, col).Scan(&dataType); err != nil {
			t.Fatalf("read column type %q: %v", col, err)
		}
		if dataType != "bytea" {
			t.Errorf("column %q data_type = %q; want bytea", col, dataType)
		}
	}

	// Byte-exact oracle: a DEFAULT-only row on BOTH engines must store the
	// same bytes. src (MySQL HEX) == dst (PG encode(...,'hex')).
	srcHex := insertBinDefaultRowMySQL(ctx, t, mysqlSource)
	tgtHex := insertBinDefaultRowPG(ctx, t, pg)
	for col, want := range srcHex {
		if !strings.EqualFold(tgtHex[col], want) {
			t.Errorf("column %q default-applied bytes diverged: mysql=%s pg=%s", col, want, tgtHex[col])
		}
	}
	// Belt-and-suspenders against a silent all-empty read: the known bytes.
	if got := strings.ToLower(tgtHex["log_ts"]); got != "3139373030313031303030303030" {
		t.Errorf("log_ts PG bytes = %s; want hex of '19700101000000'", got)
	}
	// The NUL-padded shorter-than-width case: PG BYTEA must carry the padding
	// bytes exactly (the hex payload from MySQL is already padded to 8 bytes).
	if got := strings.ToLower(tgtHex["padded"]); got != "6162000000000000" {
		t.Errorf("padded PG bytes = %s; want 6162000000000000 ('ab' NUL-padded to BINARY(8))", got)
	}
}

// TestMigrate_BinaryColumnDefault_MySQLToSQLite pins GAP 2: MySQL
// BINARY/VARBINARY column defaults → SQLite BLOB. Pre-fix the SQLite writer
// wrapped the hexbytes payload as `(0x…)`, which SQLite parses as an
// overflowing INTEGER — a silently-wrong default.
func TestMigrate_BinaryColumnDefault_MySQLToSQLite(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyMySQLDDL(t, mysqlSource, binaryDefaultSeedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	dst := filepath.Join(t.TempDir(), "bin.db")
	mig := &Migrator{Source: mysqlEng, Target: sqliteEng, SourceDSN: mysqlSource, TargetDSN: dst}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (MySQL→SQLite) = %v; want SUCCESS (binary-default DDL must be valid SQLite)", err)
	}

	sdb, err := sql.Open("sqlite", dst)
	if err != nil {
		t.Fatalf("open sqlite target: %v", err)
	}
	defer func() { _ = sdb.Close() }()

	// Byte-exact oracle: DEFAULT-only row on both, compare bytes.
	srcHex := insertBinDefaultRowMySQL(ctx, t, mysqlSource)
	if _, err := sdb.ExecContext(ctx, "INSERT INTO bin_defaults (id) VALUES (1)"); err != nil {
		t.Fatalf("insert default row on sqlite: %v", err)
	}
	var logTS, tag, padded string
	if err := sdb.QueryRowContext(ctx,
		"SELECT hex(log_ts), hex(tag), hex(padded) FROM bin_defaults WHERE id = 1").
		Scan(&logTS, &tag, &padded); err != nil {
		t.Fatalf("read back default-applied sqlite row: %v", err)
	}
	tgtHex := map[string]string{"log_ts": logTS, "tag": tag, "padded": padded}
	for col, want := range srcHex {
		if !strings.EqualFold(tgtHex[col], want) {
			t.Errorf("column %q default-applied bytes diverged: mysql=%s sqlite=%s", col, want, tgtHex[col])
		}
	}
	if got := strings.ToLower(tgtHex["log_ts"]); got != "3139373030313031303030303030" {
		t.Errorf("log_ts SQLite bytes = %s; want hex of '19700101000000'", got)
	}
	// NUL-padded shorter-than-width: SQLite BLOB must carry the padding bytes.
	if got := strings.ToLower(tgtHex["padded"]); got != "6162000000000000" {
		t.Errorf("padded SQLite bytes = %s; want 6162000000000000 ('ab' NUL-padded to BINARY(8))", got)
	}
}

// insertBinDefaultRowMySQL inserts a row supplying only the PK (every binary
// column takes its DEFAULT) into the MySQL source and returns HEX(col)→value.
func insertBinDefaultRowMySQL(ctx context.Context, t *testing.T, dsn string) map[string]string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "INSERT INTO bin_defaults (id) VALUES (1)"); err != nil {
		t.Fatalf("insert default row on mysql: %v", err)
	}
	var logTS, tag, padded string
	if err := db.QueryRowContext(ctx,
		"SELECT HEX(log_ts), HEX(tag), HEX(padded) FROM bin_defaults WHERE id = 1").
		Scan(&logTS, &tag, &padded); err != nil {
		t.Fatalf("read back default-applied mysql row: %v", err)
	}
	return map[string]string{"log_ts": logTS, "tag": tag, "padded": padded}
}

// insertBinDefaultRowPG inserts a DEFAULT-only row into the PG target and
// returns encode(col,'hex')→value for the three binary columns.
func insertBinDefaultRowPG(ctx context.Context, t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	if _, err := db.ExecContext(ctx, "INSERT INTO bin_defaults (id) VALUES (1)"); err != nil {
		t.Fatalf("insert default row on pg: %v", err)
	}
	var logTS, tag, padded string
	if err := db.QueryRowContext(ctx,
		"SELECT encode(log_ts,'hex'), encode(tag,'hex'), encode(padded,'hex') FROM bin_defaults WHERE id = 1").
		Scan(&logTS, &tag, &padded); err != nil {
		t.Fatalf("read back default-applied pg row: %v", err)
	}
	return map[string]string{"log_ts": logTS, "tag": tag, "padded": padded}
}
