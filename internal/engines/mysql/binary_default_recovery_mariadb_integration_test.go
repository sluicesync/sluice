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

// gc36Shape is one column of the GC-36 live matrix: its declaration and
// the true bytes it declares (BINARY width-padded) — the independent
// expected value both catalog readers must reproduce.
type gc36Shape struct {
	name, decl, wantHex string
}

// gc36Shapes is the family matrix the unit fixture's catalog text was
// measured from: {leading, mid, trailing, width-padding} NUL × bytes
// >= 0x80 × {BINARY, VARBINARY} × the quote and backslash escapes, the
// faithful controls (valid UTF-8, a TRUE '?', ASCII), the binary charset
// spelling of VARBINARY, and a constant-expression default MariaDB folds
// into the same quoted literal.
func gc36Shapes() []gc36Shape {
	return []gc36Shape{
		{"lead_nul_hi", "BINARY(3) DEFAULT 0x00AB00", "00AB00"},
		{"mid_nul_hi", "VARBINARY(4) DEFAULT 0xAB00CD", "AB00CD"},
		{"trail_nul_hi", "VARBINARY(3) DEFAULT 0xFFFE00", "FFFE00"},
		{"bin_pad_hi", "BINARY(4) DEFAULT 0xFF", "FF000000"},
		{"quote_hi", "VARBINARY(4) DEFAULT 0x27FF27", "27FF27"},
		{"bslash_hi", "BINARY(3) DEFAULT 0x5C805C", "5C805C"},
		{"q_bs_nul_hi", "VARBINARY(6) DEFAULT 0x27FF5C00", "27FF5C00"},
		{"all_hi", "VARBINARY(4) DEFAULT 0x80818283", "80818283"},
		{"utf8_4b", "VARBINARY(8) DEFAULT 0xF09F9880", "F09F9880"},
		{"utf8_valid", "VARBINARY(4) DEFAULT 0xDEAD", "DEAD"},
		{"real_q", "VARBINARY(4) DEFAULT 0x3F3F", "3F3F"},
		{"q_and_hi", "VARBINARY(4) DEFAULT 0x3FAB3F", "3FAB3F"},
		{"ctl_hi", "VARBINARY(6) DEFAULT 0x0A0D1A09AB", "0A0D1A09AB"},
		{"ascii", "BINARY(4) DEFAULT 'ab'", "61620000"},
		{"cs_bin", "VARCHAR(4) CHARACTER SET binary DEFAULT 0xAB", "AB"},
		{"expr_folded", "VARBINARY(4) DEFAULT (0xAB)", "AB"},
	}
}

// TestMariaDBBinaryDefault_HighBytes_OnARealServer is the live GC-36 pin,
// on every supported MariaDB LTS line. MariaDB stores a quoted binary
// default in information_schema with every non-UTF-8 byte replaced by
// '?', so before the fix BINARY(3) DEFAULT 0x00AB00 projected as
// 0x003F00 and every forwarded or migrated column carried the wrong
// bytes. For each shape:
//
//   - the catalog really is lossy on this server (anti-vacuity: a server
//     that stopped mangling would leave this test proving nothing);
//   - the cold-start seed (the production ReadSchema) and the CDC boundary
//     projection (loadTableSchema) both carry the declared bytes;
//   - an INSERT that takes the default stores those bytes — ground truth
//     read through the row path, not the DEFAULT() probe the fix uses;
//   - the GC-2 door sees a default change at a lost byte (0x00AB00 →
//     0x00CD00 read '\0?\0' both times before the fix) and not a no-op.
//
// MariaDB 11.8 changed the catalog form to a lossless x'<hex>' literal,
// so those lines grade the translator's x'…' branch instead; the test
// refuses a third form, and refuses a run in which no line exercised the
// lossy form (the probe would then be graded nowhere live).
func TestMariaDBBinaryDefault_HighBytes_OnARealServer(t *testing.T) {
	lossyLines := 0
	defer func() {
		if !t.Failed() && lossyLines == 0 {
			t.Errorf("no MariaDB line reported the '?'-mangled quoted form; the DEFAULT() probe was not exercised live")
		}
	}()
	for _, image := range mariadbLTSImages() {
		t.Run(image, func(t *testing.T) {
			dsn := newMariaDB(t, image, "gc36_binary_defaults")
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
			shapes := gc36Shapes()
			cols := []string{"id INT PRIMARY KEY"}
			for _, s := range shapes {
				cols = append(cols, s.name+" "+s.decl)
			}
			exec("CREATE TABLE gc36 (" + strings.Join(cols, ", ") + ")")
			const schema = "gc36_binary_defaults"

			// Which catalog form this line reports: the '?'-mangled quoted
			// literal (graded through the DEFAULT() probe) or 11.8+'s
			// lossless x'…' (graded through the translator). Anything else
			// is a new form nothing here was written for.
			var catalogHex string
			if err := db.QueryRowContext(ctx, `SELECT HEX(column_default) FROM information_schema.columns
				WHERE table_schema = ? AND table_name = 'gc36' AND column_name = 'lead_nul_hi'`, schema).Scan(&catalogHex); err != nil {
				t.Fatal(err)
			}
			switch catalogHex {
			case "275C303F5C3027": // '\0?\0'
				lossyLines++
			case "782730306162303027": // x'00ab00'
			default:
				t.Fatalf("%s: information_schema reports lead_nul_hi's default as %s — neither the '?'-mangled "+
					"quoted form nor the x'…' literal; re-measure the premise in binary_default_recovery_mariadb.go", image, catalogHex)
			}

			seed := readTableViaEngine(t, "mariadb", dsn, "gc36")
			tbl, err := loadTableSchema(ctx, db, schema, "gc36", FlavorMariaDB)
			if err != nil {
				t.Fatalf("loadTableSchema: %v", err)
			}
			proj := map[string]*ir.Column{}
			for _, c := range projectTableIR(tbl).Columns {
				proj[c.Name] = c
			}
			exec("INSERT INTO gc36 (id) VALUES (1)")
			for _, s := range shapes {
				want := ir.DefaultExpression{Expr: "0x" + s.wantHex, Dialect: hexLiteralDialect}
				if got := seed[s.name].Default; got != want {
					t.Errorf("%s: seed default = %#v; want %#v", s.name, got, want)
				}
				if got := proj[s.name].Default; got != want {
					t.Errorf("%s: CDC boundary projection default = %#v; want %#v", s.name, got, want)
				}
				var stored string
				if err := db.QueryRowContext(ctx, "SELECT HEX(`"+s.name+"`) FROM gc36 WHERE id = 1").Scan(&stored); err != nil {
					t.Fatal(err)
				}
				if stored != s.wantHex {
					t.Errorf("%s: a defaulted INSERT stored %s; the fixture declares %s (fix the fixture)", s.name, stored, s.wantHex)
				}
			}

			read := func() *mysqlTableFacts {
				t.Helper()
				facts, err := readTableFacts(ctx, db, FlavorMariaDB, schema, "gc36")
				if err != nil {
					t.Fatal(err)
				}
				return facts[qualifiedName(schema, "gc36")]
			}
			// The door compares the catalog's own spelling of each side —
			// the probed 0x… on the lossy lines, the server's x'…' on 11.8+
			// — so a delta is graded by column and new bytes, not spelling.
			step := func(name, ddl, col, newHex string) {
				t.Helper()
				prior := read()
				exec(ddl)
				deltas := diffTableFacts(prior, read(), nil)
				joined := strings.ToUpper(strings.Join(deltas, "; "))
				switch {
				case col == "" && len(deltas) != 0:
					t.Errorf("%s: deltas = %q; want none", name, deltas)
				case col != "" && !(strings.Contains(joined, `ALTER COLUMN "`+strings.ToUpper(col)+`" SET DEFAULT`) &&
					strings.Contains(joined, newHex)):
					t.Errorf("%s: deltas = %q; want %s's default change to %s", name, deltas, col, newHex)
				}
			}
			step("BINARY default changed at a lost byte", `ALTER TABLE gc36 ALTER COLUMN lead_nul_hi SET DEFAULT 0x00CD00`,
				"lead_nul_hi", "00CD00")
			step("VARBINARY default changed at a lost byte", `ALTER TABLE gc36 ALTER COLUMN mid_nul_hi SET DEFAULT 0xAB00CE`,
				"mid_nul_hi", "AB00CE")
			step("the same default re-declared", `ALTER TABLE gc36 ALTER COLUMN lead_nul_hi SET DEFAULT 0x00CD00`, "", "")
		})
	}
}

// readTableViaEngine reads one table's columns through the named
// registered engine's production ReadSchema, keyed by column name.
func readTableViaEngine(t *testing.T, engineName, dsn, table string) map[string]*ir.Column {
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
			out := map[string]*ir.Column{}
			for _, c := range tbl.Columns {
				out[c.Name] = c
			}
			return out
		}
	}
	t.Fatalf("%s: table %s not found", engineName, table)
	return nil
}
