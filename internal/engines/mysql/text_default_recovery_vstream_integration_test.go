//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestVStream_TextDefault_SupplementaryChars_ThroughVTGate is the live
// GC-37 (h) pin for the flavors that read the catalog through vtgate
// (PlanetScale, self-hosted Vitess). vtgate's parser rejects the DEFAULT()
// probe the vanilla and MariaDB readers use, so these flavors recover from
// SHOW CREATE TABLE — this asserts, against a real vtgate, that:
//
//   - the catalog is lossy there too (anti-vacuity);
//   - ReadSchema carries the declared text for utf8mb4 columns, a genuine
//     '?', and a genuine '?' on a utf16 column (which SHOW CREATE prints
//     quoted, so it agrees with the catalog);
//   - a utf16 default holding a supplementary character — which SHOW
//     CREATE prints as UTF-16 bytes — refuses the read, naming the column,
//     rather than carrying '?'.
//
// The independent expected value is the declared DDL, cross-checked by an
// INSERT that takes the default and is read back through the row path.
func TestVStream_TextDefault_SupplementaryChars_ThroughVTGate(t *testing.T) {
	mysqlDSN, _, keyspace, cleanup := startVTTestServer(t)
	defer cleanup()
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	shapes := []gc37hShape{
		{"vc", "VARCHAR(20) DEFAULT '😀x'", "😀x"},
		{"ch", "CHAR(4) DEFAULT '😀'", "😀"},
		{"mix", "VARCHAR(20) DEFAULT '?😀?'", "?😀?"},
		{"q", "VARCHAR(5) DEFAULT '?'", "?"},
		{"u16q", "VARCHAR(10) CHARACTER SET utf16 DEFAULT '?'", "?"},
		{"enumq", "ENUM('a','?b') DEFAULT '?b'", "?b"},
	}
	cols := []string{"id BIGINT PRIMARY KEY"}
	for _, s := range shapes {
		cols = append(cols, s.name+" "+s.decl)
	}
	exec("CREATE TABLE gc37h (" + strings.Join(cols, ", ") + ")")

	var catalog string
	if err := db.QueryRowContext(ctx, `SELECT column_default FROM information_schema.columns
		WHERE table_schema = ? AND table_name = 'gc37h' AND column_name = 'vc'`, keyspace).Scan(&catalog); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(catalog, "😀") {
		t.Fatalf("vtgate reports vc's default faithfully (%q); the SHOW CREATE recovery is not exercised — re-measure the premise", catalog)
	}

	read := func() (map[string]*ir.Column, error) {
		sr, err := Engine{Flavor: FlavorVitess}.OpenSchemaReader(ctx, mysqlDSN)
		if err != nil {
			return nil, err
		}
		defer closeIfCloser(sr)
		schema, err := sr.ReadSchema(ctx)
		if err != nil {
			return nil, err
		}
		for _, tbl := range schema.Tables {
			if tbl.Name == "gc37h" {
				out := map[string]*ir.Column{}
				for _, c := range tbl.Columns {
					out[c.Name] = c
				}
				return out, nil
			}
		}
		t.Fatal("gc37h not found")
		return nil, nil
	}
	got, err := read()
	if err != nil {
		t.Fatalf("ReadSchema through vtgate: %v", err)
	}
	exec("INSERT INTO gc37h (id) VALUES (1)")
	for _, s := range shapes {
		if want := (ir.DefaultLiteral{Value: s.want}); got[s.name].Default != want {
			t.Errorf("%s: default = %#v; want %#v", s.name, got[s.name].Default, want)
		}
		var stored string
		if err := db.QueryRowContext(ctx, "SELECT CAST("+s.name+" AS CHAR) FROM gc37h WHERE id = 1").Scan(&stored); err != nil {
			t.Fatal(err)
		}
		if stored != s.want {
			t.Errorf("%s: a defaulted INSERT stored %q; the fixture declares %q (fix the fixture)", s.name, stored, s.want)
		}
	}

	exec("CREATE TABLE gc37h_u16 (id BIGINT PRIMARY KEY, u16 VARCHAR(10) CHARACTER SET utf16 DEFAULT '😀w')")
	if _, err := read(); err == nil || !strings.Contains(err.Error(), "sluice decodes only UTF-8 there") ||
		!strings.Contains(err.Error(), "gc37h_u16.u16") {
		t.Errorf("ReadSchema with a utf16 supplementary-character default: err = %v; want the named refusal", err)
	}
}

// TestVStream_ExprText_NonASCII_ThroughVTGate is Bug 288's pin for the
// flavors that read the catalog through vtgate (PlanetScale, self-hosted
// Vitess): information_schema passes through to the tablet's mysqld, so an
// expression's non-ASCII text arrives double-encoded there too, and the
// recovery's SHOW CREATE TABLE read must pass through vtgate. The
// independent expected value is the server's own evaluation of the default
// and the generated column, read back through the row path.
func TestVStream_ExprText_NonASCII_ThroughVTGate(t *testing.T) {
	mysqlDSN, _, keyspace, cleanup := startVTTestServer(t)
	defer cleanup()
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := db.ExecContext(ctx, "CREATE TABLE xr (id BIGINT PRIMARY KEY, "+
		"d VARCHAR(40) DEFAULT (concat('é😀','中')), g VARCHAR(60) AS (concat('ß😀', id)) VIRTUAL, "+
		"b VARCHAR(40), CONSTRAINT xr_chk CHECK (b <> 'é😀'))"); err != nil {
		t.Fatal(err)
	}
	var def string
	if err := db.QueryRowContext(ctx, `SELECT column_default FROM information_schema.columns
		WHERE table_schema = ? AND table_name = 'xr' AND column_name = 'd'`, keyspace).Scan(&def); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(def, "é") {
		t.Fatalf("vtgate reports d's default faithfully (%q); the recovery is not exercised — re-measure the premise", def)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO xr (id, b) VALUES (1, 'x')"); err != nil {
		t.Fatal(err)
	}
	var gotD, gotG string
	if err := db.QueryRowContext(ctx, "SELECT d, g FROM xr WHERE id = 1").Scan(&gotD, &gotG); err != nil {
		t.Fatal(err)
	}
	if gotD != "é😀中" || gotG != "ß😀1" {
		t.Fatalf("the server evaluates d=%q g=%q; the fixture declares é😀中 / ß😀1", gotD, gotG)
	}
	sr, err := Engine{Flavor: FlavorVitess}.OpenSchemaReader(ctx, mysqlDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIfCloser(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema through vtgate: %v", err)
	}
	for _, tbl := range schema.Tables {
		if tbl.Name != "xr" {
			continue
		}
		for _, c := range tbl.Columns {
			switch c.Name {
			case "d":
				if e, ok := c.Default.(ir.DefaultExpression); !ok || e.Expr != "concat('é😀','中')" {
					t.Errorf("d default = %#v; want exactly concat('é😀','中')", c.Default)
				}
			case "g":
				if c.GeneratedExpr != "concat('ß😀',id)" {
					t.Errorf("g generated = %q; want exactly concat('ß😀',id)", c.GeneratedExpr)
				}
			}
		}
		for _, cc := range tbl.CheckConstraints {
			if cc.Name == "xr_chk" && cc.Expr != "(b <> 'é😀')" {
				t.Errorf("xr_chk = %q; want exactly (b <> 'é😀')", cc.Expr)
			}
		}
		return
	}
	t.Fatal("xr not read")
}
