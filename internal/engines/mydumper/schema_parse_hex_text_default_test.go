// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestParseCreateTable_HexTextDefault pins GC-37 (h) on the dump path:
// MySQL's SHOW CREATE TABLE — what mydumper writes — prints a character
// default holding a character outside the Basic Multilingual Plane as a
// 0x… literal of the column's bytes. The DDL below is what mysql:8.0.46
// printed for the declared defaults in the comments (measured 2026-09-24).
// Before the fix the column's default became the literal text "0xF09F…".
func TestParseCreateTable_HexTextDefault(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `v` varchar(20) DEFAULT 0xF09F988078,\n" + // '😀x'
		"  `c` char(4) DEFAULT 0xF09F9880,\n" + // '😀'
		"  `s` set('a','b') DEFAULT 0x612C62,\n" + // hypothetical hex spelling of an all-BMP default: still decodes
		"  `q` varchar(5) DEFAULT '?',\n" + // a genuine '?', printed quoted
		"  `b` varbinary(4) DEFAULT 0xF09F9880,\n" + // binary: stays a hex expression
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	tbl, err := parseCreateTable(ddl, "t-schema.sql")
	if err != nil {
		t.Fatalf("parseCreateTable: %v", err)
	}
	want := map[string]ir.DefaultValue{
		"v": ir.DefaultLiteral{Value: "😀x"},
		"c": ir.DefaultLiteral{Value: "😀"},
		"s": ir.DefaultLiteral{Value: "a,b"},
		"q": ir.DefaultLiteral{Value: "?"},
		"b": ir.DefaultExpression{Expr: "0xF09F9880", Dialect: "hexbytes"},
	}
	for _, c := range tbl.Columns {
		w, ok := want[c.Name]
		if !ok {
			continue
		}
		if c.Default != w {
			t.Errorf("%s: default = %#v; want %#v", c.Name, c.Default, w)
		}
	}
}

// TestParseCreateTable_HexTextDefault_Refusals pins the loud edges: an
// ENUM default outside the labels as SHOW CREATE printed them (the labels
// lose the same characters, GC-37 (i)) and a hex default that is not
// UTF-8 refuse, naming the column; and a table whose charset the reader
// refuses keeps its Bug 188 deferral — the hex default is never decoded,
// so a utf16 column's UTF-16 bytes cannot turn the deferrable charset
// refusal into a hard parse error.
func TestParseCreateTable_HexTextDefault_Refusals(t *testing.T) {
	for _, c := range []struct {
		name, col, want string
	}{
		{"enum label", "`e` enum('a','?b') DEFAULT 0xF09F988062", "cannot read this column's type faithfully"},
		{"not UTF-8", "`v` varchar(4) DEFAULT 0xFF", "not UTF-8"},
	} {
		ddl := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  " + c.col + ",\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
		_, err := parseCreateTable(ddl, "t-schema.sql")
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v; want one containing %q", c.name, err, c.want)
		}
	}

	ddl := "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  `w` varchar(10) CHARACTER SET utf16 DEFAULT 0xD83DDE000077,\n" +
		"  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
	tbl, err := parseCreateTable(ddl, "t-schema.sql")
	var charsetErr *CharsetRefusalError
	if !errors.As(err, &charsetErr) || tbl == nil {
		t.Fatalf("utf16 column: err = %v (table %v); want the deferrable CharsetRefusalError with the parsed table", err, tbl != nil)
	}
}
