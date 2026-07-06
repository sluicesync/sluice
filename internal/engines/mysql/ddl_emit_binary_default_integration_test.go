//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration round-trip pin for the BINARY/VARBINARY column-DEFAULT
// fidelity bug. MySQL 8 reports a binary column's literal default in
// information_schema.COLUMN_DEFAULT as a bare hex literal (`0x…`); before
// the fix sluice re-emitted it string-quoted (`'0x…'`), which MySQL rejects
// with Error 1067 (Invalid default value) — blocking migration of any table
// with a BINARY-column default (surfaced by the item-52 MediaWiki corpus,
// `log_timestamp BINARY(14) NOT NULL DEFAULT '19700101000000'`).
//
// This test reads a source table whose columns carry binary defaults,
// re-creates it on a fresh target via the real SchemaReader→SchemaWriter
// path, and asserts (a) CREATE TABLE succeeds on the target (the 1067 that
// was the bug), and (b) the DEFAULT-applied value on the target is
// BYTE-IDENTICAL to the source's — the true fidelity oracle. Pins the CLASS
// (Bug-74 family discipline): fixed BINARY + variable VARBINARY, exact-width
// and NUL-padded-shorter-than-width shapes.

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestBinaryColumnDefault_RoundTrip_MySQLToMySQL(t *testing.T) {
	srcDSN, srcCleanup := newSharedDB(t, "sluice_bindefault_src")
	defer srcCleanup()
	tgtDSN, tgtCleanup := newSharedDB(t, "sluice_bindefault_tgt")
	defer tgtCleanup()

	// The MediaWiki corpus shape (BINARY(14) exact-width) + a VARBINARY
	// sibling + a BINARY(8) whose default is SHORTER than the width (MySQL
	// NUL-pads on storage — the padded value must round-trip identically).
	applyDDL(t, srcDSN, `
		CREATE TABLE bin_defaults (
			id       INT NOT NULL,
			log_ts   BINARY(14)   NOT NULL DEFAULT '19700101000000',
			tag      VARBINARY(20) NOT NULL DEFAULT 'hello',
			padded   BINARY(8)    NOT NULL DEFAULT 'ab',
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sr, err := Engine{}.OpenSchemaReader(ctx, srcDSN)
	if err != nil {
		t.Fatalf("open source reader: %v", err)
	}
	defer func() {
		if c, ok := sr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	schema, err := sr.(*SchemaReader).ReadSchema(ctx)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}

	// Confirm the reader produced the hex-literal DefaultExpression (not a
	// string DefaultLiteral) for the binary columns — the fix's read
	// boundary. This is what makes the writer emit the default bare.
	var tbl *ir.Table
	for _, tt := range schema.Tables {
		if tt.Name == "bin_defaults" {
			tbl = tt
		}
	}
	if tbl == nil {
		t.Fatalf("source schema missing bin_defaults; got %d tables", len(schema.Tables))
	}
	for _, c := range tbl.Columns {
		switch c.Name {
		case "log_ts", "tag", "padded":
			exp, ok := c.Default.(ir.DefaultExpression)
			if !ok {
				t.Errorf("column %q default = %T; want ir.DefaultExpression (hex literal)", c.Name, c.Default)
				continue
			}
			if exp.Dialect != hexLiteralDialect {
				t.Errorf("column %q default dialect = %q; want %q", c.Name, exp.Dialect, hexLiteralDialect)
			}
		}
	}

	sw, err := Engine{}.OpenSchemaWriter(ctx, tgtDSN)
	if err != nil {
		t.Fatalf("open target writer: %v", err)
	}
	defer closeIf(sw)

	// The bug: this failed with Error 1067 (Invalid default value) before
	// the fix, because the writer emitted `DEFAULT '0x3139…'`.
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints on target (the Error 1067 regression): %v", err)
	}

	// Fidelity oracle: insert a row relying on the DEFAULTs into BOTH source
	// and target, then compare the stored bytes column-by-column. src==dst
	// is the ground truth the corpus oracle asserts.
	srcVals := insertDefaultRowAndReadHex(ctx, t, srcDSN)
	tgtVals := insertDefaultRowAndReadHex(ctx, t, tgtDSN)
	for col, srcHex := range srcVals {
		if tgtVals[col] != srcHex {
			t.Errorf("column %q default-applied bytes diverged: source=%s target=%s", col, srcHex, tgtVals[col])
		}
	}
	// Belt-and-suspenders against a silent all-empty read: the known
	// expected bytes for the exact-width column.
	if got := tgtVals["log_ts"]; got != "3139373030313031303030303030" {
		t.Errorf("log_ts target bytes = %s; want hex of '19700101000000'", got)
	}
}

// insertDefaultRowAndReadHex inserts a row supplying only the primary key
// (so every other column takes its DEFAULT) and returns HEX(col)→value for
// the three binary columns.
func insertDefaultRowAndReadHex(ctx context.Context, t *testing.T, dsn string) map[string]string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "INSERT INTO bin_defaults (id) VALUES (1)"); err != nil {
		t.Fatalf("insert default row: %v", err)
	}
	var logTS, tag, padded string
	if err := db.QueryRowContext(
		ctx,
		"SELECT HEX(log_ts), HEX(tag), HEX(padded) FROM bin_defaults WHERE id = 1",
	).Scan(&logTS, &tag, &padded); err != nil {
		t.Fatalf("read back default-applied row: %v", err)
	}
	return map[string]string{"log_ts": logTS, "tag": tag, "padded": padded}
}
