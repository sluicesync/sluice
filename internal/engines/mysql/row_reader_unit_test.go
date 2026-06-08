// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestQuoteIdent(t *testing.T) {
	cases := []struct{ in, want string }{
		{"users", "`users`"},
		{"with space", "`with space`"},
		{"weird`name", "`weird``name`"},
		{"", "``"},

		// Multi-byte UTF-8 identifiers. Sluice's loud-failure tenet is
		// most at-risk from SILENT identifier corruption — the worst
		// class of bug. Bucardo's t/10-object-names.t pins these; sluice
		// previously had no equivalent. The quote-by-byte-wrap policy
		// passes UTF-8 through verbatim (the backtick is ASCII and isn't
		// part of any multi-byte sequence under UTF-8), so these MUST
		// come back byte-exact. From
		// docs/dev/notes/test-gap-mining-broader.md (#1).
		{"café", "`café`"},                 // Latin-1 supplement (2-byte sequences)
		{"jeu_d'études", "`jeu_d'études`"}, // ASCII apostrophe + multi-byte (regression — apostrophe is NOT the MySQL quote char)
		{"имя", "`имя`"},                   // Cyrillic (2-byte sequences)
		{"用户表", "`用户表`"},                   // CJK (3-byte sequences)
		{"日本語", "`日本語`"},                   // CJK kanji
		{"naïve`col", "`naïve``col`"},      // Multi-byte mixed with the MySQL quote char — must STILL double the backtick per the escape rule
	}
	for _, c := range cases {
		if got := quoteIdent(c.in); got != c.want {
			t.Errorf("quoteIdent(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

func TestBuildSelect(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
			{Name: "weird`name", Type: ir.Boolean{}},
		},
	}
	got := buildSelect(table, false)
	want := "SELECT `id`, `email`, `weird``name` FROM `users`"
	if got != want {
		t.Errorf("buildSelect:\n got  %q\n want %q", got, want)
	}
}

// TestBuildSelect_TemporalCAST pins the Vector A read-side fix: DATE,
// DATETIME and TIMESTAMP columns are read through CAST(... AS CHAR) so
// the value-decode layer receives MySQL's raw literal (zero/partial
// dates included) instead of a value the driver has already normalized
// under parseTime=true. Non-temporal columns (including TIME, which
// decodes as a string) are read directly. buildBatchedSelect shares the
// same selectColumnExpr helper, so the CAST must appear there too.
func TestBuildSelect_TemporalCAST(t *testing.T) {
	table := &ir.Table{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "d", Type: ir.Date{}},
			{Name: "dt", Type: ir.DateTime{}},
			{Name: "ts", Type: ir.Timestamp{}},
			{Name: "dur", Type: ir.Time{}},
			{Name: "label", Type: ir.Varchar{Length: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}

	wantList := "`id`, CAST(`d` AS CHAR) AS `d`, CAST(`dt` AS CHAR) AS `dt`, " +
		"CAST(`ts` AS CHAR) AS `ts`, `dur`, `label`"

	got := buildSelect(table, false)
	if want := "SELECT " + wantList + " FROM `events`"; got != want {
		t.Errorf("buildSelect:\n got  %q\n want %q", got, want)
	}

	gotBatch := buildBatchedSelect(table, 1000, false)
	if want := "SELECT " + wantList + " FROM `events` ORDER BY `id` LIMIT 1000"; gotBatch != want {
		t.Errorf("buildBatchedSelect:\n got  %q\n want %q", gotBatch, want)
	}
}

// TestBuildSelect_QualifyBySchema pins the ADR-0074 Phase 1b.2 spanning-
// snapshot variant: when qualifyBySchema is true and Table.Schema is set,
// the FROM clause is `db`.`table`; with Schema empty it stays unqualified
// (so a server-DSN reader with no per-table database doesn't emit a bare
// dot-qualified ref).
func TestBuildSelect_QualifyBySchema(t *testing.T) {
	table := &ir.Table{
		Name:   "users",
		Schema: "app_db",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
		},
	}
	got := buildSelect(table, true)
	want := "SELECT `id` FROM `app_db`.`users`"
	if got != want {
		t.Errorf("buildSelect qualify:\n got  %q\n want %q", got, want)
	}

	// Empty Schema + qualifyBySchema true → unqualified (defensive).
	table.Schema = ""
	got = buildSelect(table, true)
	want = "SELECT `id` FROM `users`"
	if got != want {
		t.Errorf("buildSelect qualify empty-schema:\n got  %q\n want %q", got, want)
	}
}
