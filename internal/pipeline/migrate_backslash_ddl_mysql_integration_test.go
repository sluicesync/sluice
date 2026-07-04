//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SEC-1 review gap 2 ground truth: MySQL→MySQL round-trip of
// backslash-bearing DDL string values (column DEFAULTs incl. the trailing-\
// quote-swallow shape, column/table COMMENTs, ENUM labels + an ENUM
// DEFAULT). The reader receives DECODED values from information_schema
// (COLUMN_DEFAULT / COLUMN_COMMENT / TABLE_COMMENT; ENUM COLUMN_TYPE arrives
// escaped and parseEnumOrSet decodes it), and the writer's quoteSQLString
// re-escapes on emit — pre-fix the re-emit was unescaped, so every hop
// through sluice decoded the backslashes one more time (double-decode
// drift). The pin: target information_schema metadata is byte-identical to
// the source's, and a DEFAULT-applied row inserted on the TARGET carries the
// same values as one inserted on the source.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
)

func TestMigrate_MySQLBackslashDDLRoundTrip(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	// Doubled backslashes in the DDL text → each stored value contains ONE
	// backslash (the seed session runs under sluice's default injected
	// sql_mode, where \ is an escape introducer).
	const seedDDL = `
		CREATE TABLE bs_ddl (
			id INT NOT NULL AUTO_INCREMENT,
			v  VARCHAR(64) NOT NULL DEFAULT 'C:\\temp' COMMENT 'path\\note',
			w  VARCHAR(64) NOT NULL DEFAULT 'trail\\',
			e  ENUM('a\\b','x') NOT NULL DEFAULT 'a\\b',
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='tbl\\c';
		INSERT INTO bs_ddl (e) VALUES ('a\\b');
	`
	applyMySQLDDL(t, sourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	mig := &Migrator{Source: mysqlEng, Target: mysqlEng, SourceDSN: sourceDSN, TargetDSN: targetDSN}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (MySQL→MySQL backslash DDL): %v", err)
	}

	src := openMySQLForBackslashPin(t, sourceDSN)
	defer func() { _ = src.Close() }()
	dst := openMySQLForBackslashPin(t, targetDSN)
	defer func() { _ = dst.Close() }()

	// information_schema parity: the target's stored metadata must be
	// byte-identical to the source's — any divergence is exactly the
	// double-decode (or double-encode) drift this test exists to catch.
	type colMeta struct{ dflt, comment, ctype string }
	readMeta := func(db *sql.DB) map[string]colMeta {
		t.Helper()
		rows, err := db.QueryContext(ctx, `SELECT COLUMN_NAME, COLUMN_DEFAULT, COLUMN_COMMENT, COLUMN_TYPE
			FROM information_schema.columns
			WHERE table_schema = DATABASE() AND table_name = 'bs_ddl' AND column_name IN ('v','w','e')`)
		if err != nil {
			t.Fatalf("read column metadata: %v", err)
		}
		defer func() { _ = rows.Close() }()
		out := map[string]colMeta{}
		for rows.Next() {
			var name, comment, ctype string
			var dflt sql.NullString
			if err := rows.Scan(&name, &dflt, &comment, &ctype); err != nil {
				t.Fatalf("scan column metadata: %v", err)
			}
			out[name] = colMeta{dflt: dflt.String, comment: comment, ctype: ctype}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("column metadata rows: %v", err)
		}
		return out
	}
	srcMeta, dstMeta := readMeta(src), readMeta(dst)
	for _, col := range []string{"v", "w", "e"} {
		if srcMeta[col] != dstMeta[col] {
			t.Errorf("column %q metadata drift: src=%+v dst=%+v (backslash must round-trip byte-identically)", col, srcMeta[col], dstMeta[col])
		}
	}
	// Sanity-anchor the source shape itself so a seed regression can't turn
	// the parity check vacuous: one backslash, decoded, in the IS values.
	if want := `C:\temp`; srcMeta["v"].dflt != want {
		t.Errorf("source v default = %q; want %q (seed/IS decoding anchor)", srcMeta["v"].dflt, want)
	}
	if want := `trail\`; srcMeta["w"].dflt != want {
		t.Errorf("source w default = %q; want %q", srcMeta["w"].dflt, want)
	}

	readTableComment := func(db *sql.DB) string {
		t.Helper()
		var c string
		if err := db.QueryRowContext(ctx, `SELECT TABLE_COMMENT FROM information_schema.tables
			WHERE table_schema = DATABASE() AND table_name = 'bs_ddl'`).Scan(&c); err != nil {
			t.Fatalf("read table comment: %v", err)
		}
		return c
	}
	if sc, dc := readTableComment(src), readTableComment(dst); sc != dc || sc != `tbl\c` {
		t.Errorf("table comment src=%q dst=%q; want both %q", sc, dc, `tbl\c`)
	}

	// Value-level ground truth: the copied row and a TARGET-side
	// DEFAULT-applied row both carry single-backslash values.
	assertRow := func(db *sql.DB, label string, id int) {
		t.Helper()
		var v, w, e string
		if err := db.QueryRowContext(ctx, `SELECT v, w, e FROM bs_ddl WHERE id = ?`, id).Scan(&v, &w, &e); err != nil {
			t.Fatalf("%s: read row %d: %v", label, id, err)
		}
		if v != `C:\temp` || w != `trail\` || e != `a\b` {
			t.Errorf("%s row %d = (v=%q w=%q e=%q); want (C:\\temp, trail\\, a\\b)", label, id, v, w, e)
		}
	}
	assertRow(dst, "copied row on target", 1)
	if _, err := dst.ExecContext(ctx, `INSERT INTO bs_ddl () VALUES ()`); err != nil {
		t.Fatalf("insert DEFAULT-applied row on target: %v", err)
	}
	assertRow(dst, "DEFAULT-applied row on target", 2)
}

// openMySQLForBackslashPin opens a raw database/sql handle for the pin's
// metadata/value reads (the DSNs already carry parseTime=true).
func openMySQLForBackslashPin(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql %s: %v", dsn, err)
	}
	return db
}
