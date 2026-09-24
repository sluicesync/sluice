//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// gc37hShape is one column of the GC-37 (h) live matrix: its declaration
// and the text it declares — the independent expected value.
type gc37hShape struct {
	name, decl, want string
}

func gc37hShapes(mariadb bool) []gc37hShape {
	shapes := []gc37hShape{
		{"vc", "VARCHAR(20) DEFAULT '😀x'", "😀x"},
		{"ch", "CHAR(4) DEFAULT '😀'", "😀"},
		{"mix", "VARCHAR(20) DEFAULT '?😀?'", "?😀?"},
		{"bmp", "VARCHAR(20) DEFAULT 'é😀'", "é😀"},
		{"q", "VARCHAR(5) DEFAULT '?'", "?"},
		{"u16", "VARCHAR(10) CHARACTER SET utf16 DEFAULT '😀w'", "😀w"},
		{"enumq", "ENUM('a','?b') DEFAULT '?b'", "?b"},
	}
	if mariadb {
		shapes = append(shapes, gc37hShape{"tx", "TEXT DEFAULT '😀z'", "😀z"})
	}
	return shapes
}

// TestTextDefault_SupplementaryChars_OnARealServer is the live GC-37 (h)
// pin, on MySQL and every supported MariaDB LTS line. information_schema
// stores COLUMN_DEFAULT in utf8mb3, which cannot hold a character outside
// the Basic Multilingual Plane, so `VARCHAR DEFAULT '😀x'` read back as
// '?x' and every target was created with that. For each server:
//
//   - the catalog really is lossy there (anti-vacuity: a server that
//     stopped mangling would leave this test proving nothing, and a run in
//     which no server was lossy fails);
//   - the cold-start seed (the production ReadSchema) and the CDC boundary
//     projection (loadTableSchema) both carry the declared text;
//   - an INSERT that takes the default stores that text — ground truth
//     through the row path, not the DEFAULT() probe the fix uses;
//   - the GC-2 door sees a default change at a lost character ('😀x' →
//     '😁x' read '?x' both times before the fix), and not a re-declaration.
func TestTextDefault_SupplementaryChars_OnARealServer(t *testing.T) {
	lossy := 0
	defer func() {
		if !t.Failed() && lossy == 0 {
			t.Errorf("no server reported a supplementary-character default as '?'; the probe was not exercised live")
		}
	}()
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
			return newMariaDB(t, image, "gc37h_text_defaults"), "gc37h_text_defaults"
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
			shapes := gc37hShapes(sv.flavor == FlavorMariaDB)
			cols := []string{"id INT PRIMARY KEY"}
			for _, s := range shapes {
				cols = append(cols, s.name+" "+s.decl)
			}
			exec("CREATE TABLE gc37h (" + strings.Join(cols, ", ") + ")")

			var catalog string
			if err := db.QueryRowContext(ctx, `SELECT column_default FROM information_schema.columns
				WHERE table_schema = ? AND table_name = 'gc37h' AND column_name = 'vc'`, schema).Scan(&catalog); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(catalog, "😀") {
				t.Logf("%s: information_schema reports vc's default faithfully (%q); the probe is not reached here", sv.name, catalog)
			} else {
				lossy++
			}

			seed := readTableViaEngine(t, sv.engine, dsn, "gc37h")
			tbl, err := loadTableSchema(ctx, db, schema, "gc37h", sv.flavor)
			if err != nil {
				t.Fatalf("loadTableSchema: %v", err)
			}
			proj := map[string]*ir.Column{}
			for _, c := range projectTableIR(tbl).Columns {
				proj[c.Name] = c
			}
			exec("INSERT INTO gc37h (id) VALUES (1)")
			for _, s := range shapes {
				want := ir.DefaultLiteral{Value: s.want}
				if got := seed[s.name].Default; got != want {
					t.Errorf("%s: seed default = %#v; want %#v", s.name, got, want)
				}
				if got := proj[s.name].Default; got != want {
					t.Errorf("%s: CDC boundary projection default = %#v; want %#v", s.name, got, want)
				}
				var stored string
				if err := db.QueryRowContext(ctx, "SELECT HEX(CONVERT(`"+s.name+"` USING utf8mb4)) FROM gc37h WHERE id = 1").Scan(&stored); err != nil {
					t.Fatal(err)
				}
				if wantHex := strings.ToUpper(hex.EncodeToString([]byte(s.want))); stored != wantHex {
					t.Errorf("%s: a defaulted INSERT stored %s; the fixture declares %s (fix the fixture)", s.name, stored, wantHex)
				}
			}

			read := func() *mysqlTableFacts {
				t.Helper()
				facts, err := readTableFacts(ctx, db, sv.flavor, schema, "gc37h")
				if err != nil {
					t.Fatal(err)
				}
				return facts[qualifiedName(schema, "gc37h")]
			}
			step := func(name, ddl, col, newText string) {
				t.Helper()
				prior := read()
				exec(ddl)
				deltas := diffTableFacts(prior, read(), nil)
				joined := strings.Join(deltas, "; ")
				switch {
				case col == "" && len(deltas) != 0:
					t.Errorf("%s: deltas = %q; want none", name, deltas)
				case col != "" && !(strings.Contains(strings.ToUpper(joined), `ALTER COLUMN "`+strings.ToUpper(col)+`" SET DEFAULT`) &&
					strings.Contains(joined, newText)):
					t.Errorf("%s: deltas = %q; want %s's default change to %s", name, deltas, col, newText)
				}
			}
			step("VARCHAR default changed at a lost character", "ALTER TABLE gc37h ALTER COLUMN vc SET DEFAULT '😁x'", "vc", "😁x")
			step("utf16 default changed at a lost character", "ALTER TABLE gc37h ALTER COLUMN u16 SET DEFAULT '😁w'", "u16", "😁w")
			step("the same default re-declared", "ALTER TABLE gc37h ALTER COLUMN vc SET DEFAULT '😁x'", "", "")
		})
	}
}
