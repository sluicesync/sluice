// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// Unit pins for the cross-engine collation-drop policy on the MySQL
// emitter (companion to the PG emitter's COLLATE-carry pins).
//
// Pre-policy, a PG-dialect collation ("C", "en_US") riding on the IR
// was forwarded verbatim into `COLLATE <name>` and MySQL rejected the
// CREATE TABLE mid-migration with a raw Error 1273 "Unknown
// collation". The policy drops it and WARNs (docs/type-mapping.md
// "Charsets and collations"). MySQL-dialect collations must keep the
// full MySQL → MySQL round-trip.
//
// The strip dispatches on the IR string-type family, so the matrix
// covers EVERY family — Char, Varchar (narrow AND the wide→TEXT-tier
// downmap branch, which threads collation separately), Text — × both
// dialects (the Bug 74 lesson).

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// captureCollationWarnMySQL installs a WARN-level JSON slog handler
// into a buffer for the test's duration (same shape as the PG
// package's captureCollationWarn).
func captureCollationWarnMySQL(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// TestEmitColumnType_CollationDialect_FamilyMatrix pins keep-vs-drop
// across every collation-carrying family × dialect.
func TestEmitColumnType_CollationDialect_FamilyMatrix(t *testing.T) {
	cases := []struct {
		name string
		typ  ir.Type
		want string
	}{
		// MySQL-dialect collations: kept (the MySQL → MySQL round-trip).
		{"Char keeps mysql collation", ir.Char{Length: 3, Charset: "utf8mb4", Collation: "utf8mb4_bin"}, "CHAR(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"},
		{"Varchar keeps mysql collation", ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}, "VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"},
		{"wide Varchar downmap keeps mysql collation", ir.Varchar{Length: 20000, Charset: "utf8mb4", Collation: "utf8mb4_unicode_ci"}, "MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"},
		{"Text keeps mysql collation", ir.Text{Size: ir.TextRegular, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"}, "TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"},

		// PG-dialect collations (charset-empty by the readers'
		// convention): dropped — forwarding raised Error 1273.
		{"Char drops pg collation", ir.Char{Length: 3, Collation: "C"}, "CHAR(3)"},
		{"Varchar drops pg collation", ir.Varchar{Length: 20, Collation: "en_US"}, "VARCHAR(20)"},
		{"wide Varchar downmap drops pg collation", ir.Varchar{Length: 20000, Collation: "C"}, "MEDIUMTEXT"},
		{"Text drops pg collation", ir.Text{Size: ir.TextLong, Collation: "en-x-icu"}, "LONGTEXT"},

		// No collation: unchanged either way.
		{"Text bare", ir.Text{Size: ir.TextLong}, "LONGTEXT"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := emitColumnType(tc.typ)
			if err != nil {
				t.Fatalf("emitColumnType: %v", err)
			}
			if got != tc.want {
				t.Errorf("emitColumnType = %q; want %q", got, tc.want)
			}
		})
	}
}

// TestEmitTableDef_CrossEngineCollationWarn_MySQL pins the
// once-per-table WARN for dropped PG-dialect collations, and its
// absence on a pure-MySQL table.
func TestEmitTableDef_CrossEngineCollationWarn_MySQL(t *testing.T) {
	buf := captureCollationWarnMySQL(t)
	tbl := &ir.Table{
		Name: "customers",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "region_code", Type: ir.Text{Size: ir.TextLong, Collation: "C"}, Nullable: true},
		},
	}
	def, err := emitTableDef(tbl)
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	if strings.Contains(def, "COLLATE") {
		t.Errorf("table def = %q; PG-dialect collation must not reach MySQL DDL", def)
	}
	out := buf.String()
	if got := strings.Count(out, "source collations have no"); got != 1 {
		t.Fatalf("want exactly 1 per-table WARN; got %d:\n%s", got, out)
	}
	for _, want := range []string{"customers", "region_code (C)"} {
		if !strings.Contains(out, want) {
			t.Errorf("WARN should name %q; got %q", want, out)
		}
	}

	// MySQL-dialect collations: carried, no WARN.
	buf.Reset()
	myTbl := &ir.Table{
		Name: "posts",
		Columns: []*ir.Column{
			{Name: "title", Type: ir.Varchar{Length: 100, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}, Nullable: true},
		},
	}
	def, err = emitTableDef(myTbl)
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	if !strings.Contains(def, "COLLATE utf8mb4_0900_ai_ci") {
		t.Errorf("table def = %q; want the MySQL collation carried", def)
	}
	if buf.Len() != 0 {
		t.Errorf("MySQL-dialect collation must not WARN; got %q", buf.String())
	}
}

// TestWarnDroppedForeignCollation_MySQLPerColumn pins the ALTER-path
// per-column helper.
func TestWarnDroppedForeignCollation_MySQLPerColumn(t *testing.T) {
	buf := captureCollationWarnMySQL(t)
	tbl := &ir.Table{Name: "t"}

	warnDroppedForeignCollation(tbl, "ok_mysql", ir.Varchar{Length: 4, Charset: "utf8mb4", Collation: "utf8mb4_bin"})
	warnDroppedForeignCollation(tbl, "ok_none", ir.Varchar{Length: 4})
	if buf.Len() != 0 {
		t.Fatalf("MySQL-dialect / collation-free columns must not WARN; got %q", buf.String())
	}

	warnDroppedForeignCollation(tbl, "lossy", ir.Text{Size: ir.TextLong, Collation: "C"})
	out := buf.String()
	for _, want := range []string{"lossy", `"collation":"C"`, `"table":"t"`} {
		if !strings.Contains(out, want) {
			t.Errorf("WARN should contain %q; got %q", want, out)
		}
	}
}
