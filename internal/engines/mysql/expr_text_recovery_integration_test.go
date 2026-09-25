//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// readIRTableViaEngine is [readTableViaEngine] returning the whole table, so
// CHECK constraints and functional indexes can be graded too.
func readIRTableViaEngine(t *testing.T, engineName, dsn, table string) *ir.Table {
	t.Helper()
	eng, ok := engines.Get(engineName)
	if !ok {
		t.Fatalf("engine %q not registered", engineName)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("%s: OpenSchemaReader: %v", engineName, err)
	}
	defer func() { _ = sr.(interface{ Close() error }).Close() }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("%s: ReadSchema: %v", engineName, err)
	}
	for _, tbl := range schema.Tables {
		if tbl.Name == table {
			return tbl
		}
	}
	t.Fatalf("%s: table %q not read", engineName, table)
	return nil
}

// TestExprText_NonASCII_OnARealServer is the live Bug 288 pin, on MySQL and
// every supported MariaDB LTS line, across all four catalog expression
// surfaces — an expression DEFAULT, a generated column, a CHECK and (MySQL
// only; MariaDB has none) a functional key part — each holding two-, three-
// and four-byte characters. MySQL's information_schema double-encodes them
// (é read back as Ã©) and MariaDB's writes '?' per byte for the four-byte
// one, and sluice carried either into every target. For each server:
//
//   - the catalog really does not render the text faithfully (anti-vacuity,
//     per surface: a server that stopped would leave this proving nothing);
//   - the cold-start seed (ReadSchema) carries the declared text on every
//     surface, and the CDC boundary projection (loadTableSchema) on the
//     default and the generated column;
//   - the server's own evaluation of the default and the generated column —
//     an INSERT read back through the row path, not through any catalog —
//     equals the declared text (the independent expected value);
//   - on MariaDB, the GC-2 door sees a default change at a lost character.
func TestExprText_NonASCII_OnARealServer(t *testing.T) {
	type server struct {
		name, engine string
		flavor       Flavor
		dsn          func(t *testing.T) (dsn, schema string)
	}
	servers := []server{{"mysql", "mysql", FlavorVanilla, func(t *testing.T) (string, string) {
		dsn, _ := startMySQL(t)
		return dsn, "sluice_test"
	}}}
	for _, image := range mariadbLTSImages() {
		servers = append(servers, server{image, "mariadb", FlavorMariaDB, func(t *testing.T) (string, string) {
			return newMariaDB(t, image, "bug288_expr_text"), "bug288_expr_text"
		}})
	}
	for _, sv := range servers {
		t.Run(sv.name, func(t *testing.T) {
			dsn, schema := sv.dsn(t)
			db, err := sql.Open("mysql", dsn)
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
			mariadb := sv.flavor == FlavorMariaDB
			ddl := "CREATE TABLE xr (id INT PRIMARY KEY, " +
				"d VARCHAR(40) DEFAULT (concat('é😀','中')), " +
				"g VARCHAR(60) AS (concat('ß😀', id)) VIRTUAL, " +
				"b VARCHAR(40), " +
				"CONSTRAINT xr_chk CHECK (b <> 'é😀')"
			if !mariadb {
				ddl += ", KEY xr_ix ((concat(b,'中😀')))"
			}
			exec(ddl + ")")

			// Anti-vacuity: information_schema must not render any surface
			// faithfully, or recovery is not what this exercises.
			var def, gen, clause string
			if err := db.QueryRowContext(ctx, `SELECT column_default FROM information_schema.columns
				WHERE table_schema = ? AND table_name = 'xr' AND column_name = 'd'`, schema).Scan(&def); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, `SELECT generation_expression FROM information_schema.columns
				WHERE table_schema = ? AND table_name = 'xr' AND column_name = 'g'`, schema).Scan(&gen); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, `SELECT check_clause FROM information_schema.check_constraints
				WHERE constraint_schema = ? AND constraint_name = 'xr_chk'`, schema).Scan(&clause); err != nil {
				t.Fatal(err)
			}
			for what, text := range map[string]string{"default": def, "generated": gen, "check": clause} {
				if strings.Contains(text, "😀") {
					t.Fatalf("%s: information_schema renders the %s faithfully (%q); re-measure the premise", sv.name, what, text)
				}
			}

			// The independent expected value: the server's own evaluation.
			exec("INSERT INTO xr (id, b) VALUES (1, 'x')")
			var gotD, gotG string
			if err := db.QueryRowContext(ctx, "SELECT d, g FROM xr WHERE id = 1").Scan(&gotD, &gotG); err != nil {
				t.Fatal(err)
			}
			if gotD != "é😀中" || gotG != "ß😀1" {
				t.Fatalf("the server evaluates d=%q g=%q; the fixture declares é😀中 / ß😀1 (fix the fixture)", gotD, gotG)
			}

			faithful := func(what, text string, want ...string) {
				t.Helper()
				for _, w := range want {
					if !strings.Contains(text, w) {
						t.Errorf("%s = %q; want it to carry %q", what, text, w)
					}
				}
				if strings.ContainsAny(text, "Ã?") {
					t.Errorf("%s = %q carries catalog damage", what, text)
				}
			}
			defaultText := func(v ir.DefaultValue) string {
				if e, ok := v.(ir.DefaultExpression); ok {
					return e.Expr
				}
				t.Errorf("default = %#v; want an expression", v)
				return ""
			}

			seed := readIRTableViaEngine(t, sv.engine, dsn, "xr")
			cols := map[string]*ir.Column{}
			for _, c := range seed.Columns {
				cols[c.Name] = c
			}
			faithful("seed default", defaultText(cols["d"].Default), "é😀", "中")
			faithful("seed generated", cols["g"].GeneratedExpr, "ß😀")
			var chk string
			for _, c := range seed.CheckConstraints {
				if c.Name == "xr_chk" {
					chk = c.Expr
				}
			}
			faithful("seed check", chk, "é😀")
			if !mariadb {
				var ix string
				for _, idx := range seed.Indexes {
					if idx.Name == "xr_ix" && len(idx.Columns) == 1 {
						ix = idx.Columns[0].Expression
					}
				}
				faithful("seed functional key part", ix, "中😀")
			}

			tbl, err := loadTableSchema(ctx, db, schema, "xr", sv.flavor)
			if err != nil {
				t.Fatalf("loadTableSchema: %v", err)
			}
			for _, c := range projectTableIR(tbl).Columns {
				switch c.Name {
				case "d":
					faithful("projection default", defaultText(c.Default), "é😀", "中")
				case "g":
					faithful("projection generated", c.GeneratedExpr, "ß😀")
				}
			}

			if mariadb {
				read := func() *mysqlTableFacts {
					t.Helper()
					facts, err := readTableFacts(ctx, db, sv.flavor, schema, "xr")
					if err != nil {
						t.Fatal(err)
					}
					return facts[qualifiedName(schema, "xr")]
				}
				prior := read()
				exec("ALTER TABLE xr ALTER COLUMN d SET DEFAULT (concat('é😁','中'))")
				deltas := strings.Join(diffTableFacts(prior, read(), nil), "; ")
				if !strings.Contains(deltas, "😁") {
					t.Errorf("a default change at a lost character: deltas = %q; want it seen", deltas)
				}
			}
		})
	}
}
