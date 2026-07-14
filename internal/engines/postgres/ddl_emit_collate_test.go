// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Unit pins for the per-column COLLATE carry (restore-parity oracle
// TRIAGE finding #2) and the cross-engine collation-drop WARN policy.
//
// The COLLATE render dispatches on the IR string-type family, so the
// matrix pins EVERY family — Char, Varchar, Text (the full set of
// collation-carrying types) — × {PG-dialect collation, MySQL-dialect
// collation, no collation}, not one representative (the Bug 74
// lesson). ir.Array is excluded by construction: the PG reader's
// array-element resolution never populates an element collation (a
// documented read-side gap — see translate.ColumnCollation).

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// captureCollationWarn installs a WARN-level JSON slog handler into a
// buffer for the test's duration, restoring the previous default on
// cleanup (same shape as captureKeylessWarn).
func captureCollationWarn(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// TestEmitColumnDef_CollateCarry_FamilyMatrix pins the full family ×
// collation-shape matrix through emitColumnDef.
func TestEmitColumnDef_CollateCarry_FamilyMatrix(t *testing.T) {
	cases := []struct {
		name        string
		typ         ir.Type
		wantCollate string // "" = the def must contain no COLLATE at all
	}{
		// PG-dialect collation (explicit on the source column by
		// construction): rendered, quoted as an identifier.
		{"Char + pg collation", ir.Char{Length: 2, Collation: "C"}, ` COLLATE "C"`},
		{"Varchar + pg collation", ir.Varchar{Length: 16, Collation: "en_US"}, ` COLLATE "en_US"`},
		{"Text + pg collation", ir.Text{Size: ir.TextLong, Collation: "C"}, ` COLLATE "C"`},
		{"Text + pg icu collation", ir.Text{Size: ir.TextLong, Collation: "en-x-icu"}, ` COLLATE "en-x-icu"`},

		// Domain-typed column: the effective explicit collation the PG
		// reader resolved onto the base type re-emits on the COLUMN
		// (`"c" email_address COLLATE "C"`) — the CREATE DOMAIN
		// statement can't carry a per-column override.
		{"Domain + effective pg collation", ir.Domain{Name: "email_address", BaseType: ir.Text{Size: ir.TextLong, Collation: "C"}}, ` COLLATE "C"`},
		{"Domain without collation", ir.Domain{Name: "email_address", BaseType: ir.Text{Size: ir.TextLong}}, ""},

		// Database-default collation (reader stores nothing): no
		// clause — mirrors pg_dump's omission rule, no per-column noise.
		{"Char default collation", ir.Char{Length: 2}, ""},
		{"Varchar default collation", ir.Varchar{Length: 16}, ""},
		{"Text default collation", ir.Text{Size: ir.TextLong}, ""},

		// MySQL-dialect collation (charset-paired): dropped — PG has no
		// such collation name; forwarding it would 42704 at CREATE TABLE.
		{"Char + mysql collation dropped", ir.Char{Length: 2, Charset: "utf8mb4", Collation: "utf8mb4_bin"}, ""},
		{"Varchar + mysql collation dropped", ir.Varchar{Length: 16, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}, ""},
		{"Text + mysql collation dropped", ir.Text{Size: ir.TextLong, Charset: "latin1", Collation: "latin1_swedish_ci"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tbl := &ir.Table{Name: "t", Columns: []*ir.Column{{Name: "c", Type: tc.typ, Nullable: true}}}
			def, err := emitColumnDef(tbl, tbl.Columns[0], emitOpts{})
			if err != nil {
				t.Fatalf("emitColumnDef: %v", err)
			}
			if tc.wantCollate == "" {
				if strings.Contains(def, "COLLATE") {
					t.Errorf("def = %q; want no COLLATE clause", def)
				}
				return
			}
			if !strings.Contains(def, tc.wantCollate) {
				t.Errorf("def = %q; want it to contain %q", def, tc.wantCollate)
			}
		})
	}
}

// TestEmitColumnDef_CollatePrecedesGenerated pins PG's grammar
// position: COLLATE sits between the data type and the GENERATED
// clause (`type COLLATE "x" GENERATED ALWAYS AS (...) STORED`).
func TestEmitColumnDef_CollatePrecedesGenerated(t *testing.T) {
	tbl := &ir.Table{Name: "t"}
	col := &ir.Column{
		Name:                 "g",
		Type:                 ir.Text{Size: ir.TextLong, Collation: "C"},
		Nullable:             true,
		GeneratedExpr:        "lower(src)",
		GeneratedStored:      true,
		GeneratedExprDialect: "postgres",
	}
	def, err := emitColumnDef(tbl, col, emitOpts{})
	if err != nil {
		t.Fatalf("emitColumnDef: %v", err)
	}
	collateAt := strings.Index(def, `COLLATE "C"`)
	genAt := strings.Index(def, "GENERATED ALWAYS AS")
	if collateAt < 0 || genAt < 0 || collateAt > genAt {
		t.Errorf("def = %q; want COLLATE before GENERATED", def)
	}
}

// TestEmitTableDef_CrossEngineCollationWarn pins the once-per-table
// WARN for dropped MySQL-dialect collations: fired once, naming the
// table and every affected column + collation; NOT fired for a
// PG-dialect or collation-free table.
func TestEmitTableDef_CrossEngineCollationWarn(t *testing.T) {
	buf := captureCollationWarn(t)
	tbl := &ir.Table{
		Name: "posts",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "title", Type: ir.Varchar{Length: 200, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}, Nullable: true},
			{Name: "body", Type: ir.Text{Size: ir.TextLong, Charset: "utf8mb4", Collation: "utf8mb4_unicode_ci"}, Nullable: true},
		},
	}
	if _, err := emitTableDef("public", tbl, emitOpts{}); err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	out := buf.String()
	if got := strings.Count(out, "source collations have no"); got != 1 {
		t.Fatalf("want exactly 1 per-table WARN; got %d:\n%s", got, out)
	}
	for _, want := range []string{"posts", "title (utf8mb4_0900_ai_ci)", "body (utf8mb4_unicode_ci)"} {
		if !strings.Contains(out, want) {
			t.Errorf("WARN should name %q; got %q", want, out)
		}
	}

	// A PG-dialect collation table emits its clause with NO warn.
	buf.Reset()
	pgTbl := &ir.Table{
		Name: "customers",
		Columns: []*ir.Column{
			{Name: "region_code", Type: ir.Text{Size: ir.TextLong, Collation: "C"}},
		},
	}
	def, err := emitTableDef("public", pgTbl, emitOpts{})
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	if !strings.Contains(def, `COLLATE "C"`) {
		t.Errorf("table def = %q; want COLLATE \"C\" carried", def)
	}
	if buf.Len() != 0 {
		t.Errorf("PG-dialect collation must not WARN; got %q", buf.String())
	}
}

// TestWarnDroppedForeignCollation_PerColumn pins the ALTER-path
// per-column WARN helper: fires only for a foreign (MySQL-dialect)
// collation, naming table + column + collation.
func TestWarnDroppedForeignCollation_PerColumn(t *testing.T) {
	buf := captureCollationWarn(t)
	tbl := &ir.Table{Name: "t"}

	warnDroppedForeignCollation(tbl, "ok_pg", ir.Text{Size: ir.TextLong, Collation: "C"})
	warnDroppedForeignCollation(tbl, "ok_none", ir.Varchar{Length: 4})
	if buf.Len() != 0 {
		t.Fatalf("PG-dialect / collation-free columns must not WARN; got %q", buf.String())
	}

	warnDroppedForeignCollation(tbl, "lossy", ir.Char{Length: 4, Charset: "utf8mb4", Collation: "utf8mb4_bin"})
	out := buf.String()
	for _, want := range []string{"lossy", "utf8mb4_bin", `"table":"t"`} {
		if !strings.Contains(out, want) {
			t.Errorf("WARN should contain %q; got %q", want, out)
		}
	}
}
