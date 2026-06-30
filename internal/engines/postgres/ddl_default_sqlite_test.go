// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestTranslateSQLiteDefaultExpr pins the portable-SQLite-default → PG map
// (D1/SQLite robustness Chunk A) across the FULL mappable set — every
// "current instant" family × every accepted surface form (function-call,
// SQL keyword, parens, case, whitespace, double-quoted 'now') — AND a
// representative non-mappable set, which must return ok=false so the caller
// loud-drops instead of guessing.
func TestTranslateSQLiteDefaultExpr(t *testing.T) {
	mappable := []struct {
		in   string
		want string
	}{
		// datetime / CURRENT_TIMESTAMP family.
		{"datetime('now')", "CURRENT_TIMESTAMP"},
		{"(datetime('now'))", "CURRENT_TIMESTAMP"},
		{"DATETIME('now')", "CURRENT_TIMESTAMP"},
		{"  datetime ( 'now' )  ", "CURRENT_TIMESTAMP"},
		{`datetime("now")`, "CURRENT_TIMESTAMP"}, // double-quoted misfeature
		{`(DateTime ( "now" ))`, "CURRENT_TIMESTAMP"},
		{"CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"},
		{"current_timestamp", "CURRENT_TIMESTAMP"},
		{"(CURRENT_TIMESTAMP)", "CURRENT_TIMESTAMP"},
		// date / CURRENT_DATE family.
		{"date('now')", "CURRENT_DATE"},
		{"(date('now'))", "CURRENT_DATE"},
		{"DATE('now')", "CURRENT_DATE"},
		{`date("now")`, "CURRENT_DATE"},
		{"CURRENT_DATE", "CURRENT_DATE"},
		{"current_date", "CURRENT_DATE"},
		// time / CURRENT_TIME family.
		{"time('now')", "CURRENT_TIME"},
		{"(time('now'))", "CURRENT_TIME"},
		{"TIME('now')", "CURRENT_TIME"},
		{`time("now")`, "CURRENT_TIME"},
		{"CURRENT_TIME", "CURRENT_TIME"},
		{"current_time", "CURRENT_TIME"},
	}
	for _, tc := range mappable {
		t.Run("ok/"+tc.in, func(t *testing.T) {
			got, ok := translateSQLiteDefaultExpr(tc.in)
			if !ok {
				t.Fatalf("translateSQLiteDefaultExpr(%q) ok=false; want a portable mapping to %q", tc.in, tc.want)
			}
			if got != tc.want {
				t.Errorf("translateSQLiteDefaultExpr(%q) = %q; want %q", tc.in, got, tc.want)
			}
		})
	}

	nonMappable := []string{
		"julianday('now')",
		"strftime('%Y-%m-%d', 'now')",
		"unixepoch('now')",
		"datetime('now', '+1 day')", // modifier — not the bare "now"
		"date('now', 'localtime')",
		`"draft"`,   // double-quoted-string misfeature
		`'literal'`, // (would be DefaultLiteral upstream, but if it reaches here it's not portable-fn)
		"now()",     // not a SQLite spelling
		"42",
		"",
		"(a) + (b)", // parens that don't wrap the whole
		"randomblob(16)",
	}
	for _, in := range nonMappable {
		t.Run("notok/"+in, func(t *testing.T) {
			if got, ok := translateSQLiteDefaultExpr(in); ok {
				t.Errorf("translateSQLiteDefaultExpr(%q) = (%q, true); want ok=false (non-portable → loud drop)", in, got)
			}
		})
	}
}

// TestEmitDefault_SQLiteNonPortableWarns is the emit-level pin: a column with
// a non-portable SQLite DEFAULT emits NO DEFAULT clause (ok=false) AND the
// loud per-column warn fires naming the table, column, and dropped
// expression. The warn (not silent drop) is the load-bearing half of the
// loud-failure tenet on this path.
func TestEmitDefault_SQLiteNonPortableWarns(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	table := &ir.Table{Name: "flyway_schema_history"}
	col := &ir.Column{
		Name:    "installed_on",
		Type:    ir.Text{},
		Default: ir.DefaultExpression{Expr: "julianday('now')", Dialect: "sqlite"},
	}

	got, ok := emitDefault(table, col, emitOpts{})
	if ok || got != "" {
		t.Fatalf("emitDefault = (%q, %v); want (\"\", false) — the non-portable default must be dropped", got, ok)
	}

	logged := buf.String()
	for _, want := range []string{
		"dropped non-portable SQLite DEFAULT",
		"flyway_schema_history",
		"installed_on",
		"julianday('now')",
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("warn log %q missing %q (the drop must be LOUD and name table/column/expression)", logged, want)
		}
	}
}

// TestEmitColumnDef_SQLiteDefaultPortableAndDrop pins the column-emit
// integration of both halves: a portable SQLite default lands as a valid PG
// DEFAULT keyword, while a non-portable one yields a column with NO DEFAULT
// clause (so CREATE TABLE succeeds instead of aborting the whole migration).
func TestEmitColumnDef_SQLiteDefaultPortableAndDrop(t *testing.T) {
	table := &ir.Table{Name: "t"}

	portable := &ir.Column{
		Name:     "created_at",
		Type:     ir.Timestamp{},
		Nullable: false,
		Default:  ir.DefaultExpression{Expr: "(datetime('now'))", Dialect: "sqlite"},
	}
	def, err := emitColumnDef(table, portable, emitOpts{})
	if err != nil {
		t.Fatalf("emitColumnDef(portable): %v", err)
	}
	if !strings.Contains(def, "DEFAULT CURRENT_TIMESTAMP") {
		t.Errorf("portable column def = %q; want it to contain `DEFAULT CURRENT_TIMESTAMP`", def)
	}

	nonPortable := &ir.Column{
		Name:     "installed_on",
		Type:     ir.Text{},
		Nullable: false,
		Default:  ir.DefaultExpression{Expr: "strftime('%s','now')", Dialect: "sqlite"},
	}
	def, err = emitColumnDef(table, nonPortable, emitOpts{})
	if err != nil {
		t.Fatalf("emitColumnDef(non-portable): %v", err)
	}
	if strings.Contains(def, "DEFAULT") {
		t.Errorf("non-portable column def = %q; want NO DEFAULT clause (dropped)", def)
	}
	if !strings.Contains(def, "NOT NULL") {
		t.Errorf("non-portable column def = %q; want NOT NULL preserved (only the DEFAULT is dropped)", def)
	}
}
