// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestQuoteIdent(t *testing.T) {
	cases := []struct{ in, want string }{
		{"users", `"users"`},
		{"with space", `"with space"`},
		{`weird"name`, `"weird""name"`},
		{"", `""`},

		// Multi-byte UTF-8 identifiers. Sluice's loud-failure tenet is
		// most at-risk from SILENT identifier corruption — the worst
		// class of bug. Bucardo's t/10-object-names.t pins these; sluice
		// previously had no equivalent. The quote-by-byte-wrap policy
		// passes UTF-8 through verbatim (the quote char itself is ASCII
		// and isn't part of any multi-byte sequence under UTF-8), so
		// these MUST come back byte-exact. From
		// docs/dev/notes/test-gap-mining-broader.md (#1).
		{"café", `"café"`},                 // Latin-1 supplement (2-byte sequences)
		{"jeu_d'études", `"jeu_d'études"`}, // ASCII apostrophe + multi-byte (regression — embedded apostrophe is NOT the PG quote char)
		{"имя", `"имя"`},                   // Cyrillic (2-byte sequences)
		{"用户表", `"用户表"`},                   // CJK (3-byte sequences)
		{"日本語", `"日本語"`},                   // CJK kanji
		{"naïve\"col", `"naïve""col"`},     // Multi-byte mixed with the PG quote char — must STILL double the `"` per the escape rule
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
			{Name: `weird"name`, Type: ir.Boolean{}},
		},
	}
	got := buildSelect("public", table)
	want := `SELECT "id", "email", "weird""name" FROM "public"."users"`
	if got != want {
		t.Errorf("buildSelect:\n got  %q\n want %q", got, want)
	}
}

func TestBuildSelectNonDefaultSchema(t *testing.T) {
	table := &ir.Table{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}
	got := buildSelect("app", table)
	want := `SELECT "id" FROM "app"."users"`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestCountRows_PinnedReaderReturnsZeroWithoutQuerying guards the
// load-bearing invariant the ADR-0079 v1.1 design rests on: CountRows
// (which the throughput/ETA probe [kickOffRowCount] fires CONCURRENTLY
// with the in-flight copy stream on the SAME pinned reader) must NEVER
// query the pinned conn — it short-circuits to (0, nil) for both pinned
// shapes. The first v1.1 attempt regressed precisely by relaxing this and
// letting the ETA probe hit the pinned conn mid-copy. A nil querier proves
// no query is issued: if the short-circuit broke, the nil deref would
// panic.
func TestCountRows_PinnedReaderReturnsZeroWithoutQuerying(t *testing.T) {
	table := &ir.Table{
		Name:    "big",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}
	cases := []struct {
		name string
		r    *RowReader
	}{
		{
			name: "externally-owned stream reader (closer==nil)",
			r:    &RowReader{q: nil, schema: "public", closer: nil},
		},
		{
			name: "self-closing SnapshotImporter reader (snapshotPinned)",
			r:    &RowReader{q: nil, schema: "public", closer: snapshotConnCloser{}, snapshotPinned: true},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n, err := c.r.CountRows(context.Background(), table)
			if err != nil {
				t.Fatalf("CountRows: unexpected error: %v", err)
			}
			if n != 0 {
				t.Fatalf("CountRows on a pinned reader = %d; want 0 (must not query the pinned conn — that would race the ETA probe)", n)
			}
		})
	}
}

// TestEstimateRowCount_PinnedReaderNoDSNReturnsZero pins the
// EstimateRowCount degrade-path: a pinned reader with no estimatorDSN
// (e.g. a NewSnapshotRowReader minted by a delegating engine that didn't
// thread one in) yields (0, nil) → single-stream, rather than touching the
// pinned conn. A nil querier proves no on-conn query is issued.
func TestEstimateRowCount_PinnedReaderNoDSNReturnsZero(t *testing.T) {
	table := &ir.Table{
		Name:    "big",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}
	r := &RowReader{q: nil, schema: "public", snapshotPinned: true, estimatorDSN: ""}
	n, err := r.EstimateRowCount(context.Background(), table)
	if err != nil {
		t.Fatalf("EstimateRowCount: unexpected error: %v", err)
	}
	if n != 0 {
		t.Fatalf("EstimateRowCount on a pinned reader with no DSN = %d; want 0 (single-stream, no on-conn query)", n)
	}
}
