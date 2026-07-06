//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration round-trip pins for the ENUM/SET label escape-decoding fix.
// information_schema.columns.COLUMN_TYPE re-escapes control bytes inside an
// enum/set label with MySQL's full single-backslash escape set (`\0 \b \t \n
// \r \\`), which the pre-fix 2-escape parser passed through literally — a wrong
// allowed-value set. These tests read a real source table's escaped labels
// through the SchemaReader, assert the IR holds the RAW decoded bytes (the
// reader fix), then re-create the table on a fresh target and assert the target's
// allowed-value set is byte-identical (the writer re-escape symmetry). Pins the
// CLASS (Bug-74 discipline): each escape family + a control byte + a backslash +
// a well-behaved ASCII enum that must stay unchanged.

package mysql

import (
	"context"
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestEnumLabelEscape_RoundTrip_MySQLToMySQL(t *testing.T) {
	srcDSN, srcCleanup := newSharedDB(t, "sluice_enumescape_src")
	defer srcCleanup()
	tgtDSN, tgtCleanup := newSharedDB(t, "sluice_enumescape_tgt")
	defer tgtCleanup()

	// DDL uses a raw Go string so `\n`/`\t`/`\\` reach MySQL as the literal
	// two-char escapes MySQL then decodes on CREATE — mirroring how an operator
	// writes them and how information_schema re-escapes them on read-back.
	//   nl_tab_bs: newline + tab + a real backslash + a doubled quote
	//   ctrl:      backspace (0x08) + carriage-return (0x0D)
	//   plain:     well-behaved ASCII enum that must stay byte-identical
	//   flags:     a SET (not ENUM) with an escaped label to cover both kinds
	applyDDL(t, srcDSN, "CREATE TABLE labels (\n"+
		"  id        INT NOT NULL,\n"+
		"  nl_tab_bs ENUM('a\\nb','c\\td','e\\\\f','g''h') NOT NULL,\n"+
		"  ctrl      ENUM('x\\by','p\\rq') NOT NULL,\n"+
		"  plain     ENUM('red','green','blue') NOT NULL,\n"+
		"  flags     SET('r\\nw','x') NOT NULL,\n"+
		"  PRIMARY KEY (id)\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// The RAW bytes MySQL stored for each label — what the IR must hold.
	wantEnum := map[string][]string{
		"nl_tab_bs": {"a\nb", "c\td", `e\f`, "g'h"},
		"ctrl":      {"x\by", "p\rq"},
		"plain":     {"red", "green", "blue"},
	}
	wantSet := map[string][]string{
		"flags": {"r\nw", "x"},
	}

	srcSchema := readMySQLSchema(ctx, t, srcDSN)
	srcTbl := requireTable(t, srcSchema, "labels")

	// Reader fix: the IR must hold the decoded raw bytes, not the escaped form.
	assertLabelSets(t, "source", srcTbl, wantEnum, wantSet)

	// Recreate on a fresh target through the real writer, then read it back.
	sw, err := Engine{}.OpenSchemaWriter(ctx, tgtDSN)
	if err != nil {
		t.Fatalf("open target writer: %v", err)
	}
	defer closeIf(sw)
	if err := sw.CreateTablesWithoutConstraints(ctx, srcSchema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints on target: %v", err)
	}

	tgtSchema := readMySQLSchema(ctx, t, tgtDSN)
	tgtTbl := requireTable(t, tgtSchema, "labels")

	// Writer re-escape symmetry: the target's allowed-value set is byte-exact.
	assertLabelSets(t, "target", tgtTbl, wantEnum, wantSet)
}

// TestEnumLabelNUL_RoundTrip_MySQLToMySQL pins the headline silent-corruption
// case: a genuinely NUL-bearing enum label. COLUMN_TYPE reports it as `'…\0…'`;
// the pre-fix parser produced the literal string `…\0…` (a WRONG allowed value).
// The fix decodes the `\0` to a real 0x00, and the value round-trips byte-exact
// MySQL→MySQL (the writer emits the raw NUL, which MySQL stores). A NUL-bearing
// label to a PG text/enum target is EXPECTED to loud-fail at apply (PG text
// cannot hold a NUL) — that is correct loud-failure and is not exercised here
// (no PG target in this package).
func TestEnumLabelNUL_RoundTrip_MySQLToMySQL(t *testing.T) {
	srcDSN, srcCleanup := newSharedDB(t, "sluice_enumnul_src")
	defer srcCleanup()
	tgtDSN, tgtCleanup := newSharedDB(t, "sluice_enumnul_tgt")
	defer tgtCleanup()

	// `nul_x\0y` → the 7-byte label nul_x + 0x00 + y.
	applyDDL(t, srcDSN, "CREATE TABLE nul_labels (\n"+
		"  id  INT NOT NULL,\n"+
		"  e   ENUM('nul_x\\0y','plain') NOT NULL,\n"+
		"  PRIMARY KEY (id)\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	want := []string{"nul_x\x00y", "plain"}

	srcSchema := readMySQLSchema(ctx, t, srcDSN)
	srcTbl := requireTable(t, srcSchema, "nul_labels")
	got := enumValues(t, srcTbl, "e")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("source enum labels = %q; want %q (the \\0 must decode to a real NUL, not the literal 2 chars)", got, want)
	}

	sw, err := Engine{}.OpenSchemaWriter(ctx, tgtDSN)
	if err != nil {
		t.Fatalf("open target writer: %v", err)
	}
	defer closeIf(sw)
	if err := sw.CreateTablesWithoutConstraints(ctx, srcSchema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints on target (MySQL should accept a raw NUL in an ENUM label): %v", err)
	}

	tgtSchema := readMySQLSchema(ctx, t, tgtDSN)
	tgtTbl := requireTable(t, tgtSchema, "nul_labels")
	if gotT := enumValues(t, tgtTbl, "e"); !reflect.DeepEqual(gotT, want) {
		t.Errorf("target enum labels = %q; want %q (byte-exact NUL round-trip)", gotT, want)
	}
}

// readMySQLSchema opens a reader on dsn and returns its schema.
func readMySQLSchema(ctx context.Context, t *testing.T, dsn string) *ir.Schema {
	t.Helper()
	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("open reader: %v", err)
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
	return schema
}

func requireTable(t *testing.T, schema *ir.Schema, name string) *ir.Table {
	t.Helper()
	if tbl := findTable(schema, name); tbl != nil {
		return tbl
	}
	t.Fatalf("schema missing table %q; got %d tables", name, len(schema.Tables))
	return nil
}

func enumValues(t *testing.T, tbl *ir.Table, col string) []string {
	t.Helper()
	for _, c := range tbl.Columns {
		if c.Name != col {
			continue
		}
		switch v := c.Type.(type) {
		case ir.Enum:
			return v.Values
		case ir.Set:
			return v.Values
		default:
			t.Fatalf("column %q is %T; want ir.Enum/ir.Set", col, c.Type)
		}
	}
	t.Fatalf("table %q missing column %q", tbl.Name, col)
	return nil
}

func assertLabelSets(t *testing.T, side string, tbl *ir.Table, wantEnum, wantSet map[string][]string) {
	t.Helper()
	for col, want := range wantEnum {
		if got := enumValues(t, tbl, col); !reflect.DeepEqual(got, want) {
			t.Errorf("%s ENUM %q labels = %q; want %q", side, col, got, want)
		}
	}
	for col, want := range wantSet {
		if got := enumValues(t, tbl, col); !reflect.DeepEqual(got, want) {
			t.Errorf("%s SET %q labels = %q; want %q", side, col, got, want)
		}
	}
}
