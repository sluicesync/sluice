// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// `sluice schema diff` must be able to SEE the constraint weakening the emit
// refusals exist to prevent (roadmap item 125 / audit 2026-08-04 S9).
//
// The index comparison was name-only: an index present on both sides under
// the same name compared EQUAL no matter how its definition differed. So the
// one operator-facing surface for answering "does my target still enforce
// what my source enforces?" was structurally blind to:
//
//	source UNIQUE (email(10))   target UNIQUE (email)        -- S8, admits more
//	source UNIQUE (email)       target INDEX  (email)        -- no longer unique
//	source UNIQUE (email) WHERE deleted_at IS NULL
//	                            target UNIQUE (email)        -- admits fewer
//
// Each of those is a real migration outcome sluice now refuses at emit time
// (items 118, 120) — but a schema that predates those releases, or one built
// by hand, can carry any of them today, and this is how an operator finds out.
//
// The matrix below IS that class, which is the point: the test is derived
// from the defect family rather than from the implementation, so a future
// refactor that drops an attribute from the comparison fails here.

package diff

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// idxTable builds a one-table schema carrying a single named index.
func idxTable(idx *ir.Index) *ir.Table {
	return &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "email", Type: ir.Varchar{Length: 255}},
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "deleted_at", Type: ir.Timestamp{}},
		},
		Indexes: []*ir.Index{idx},
	}
}

func uniqueOn(cols ...ir.IndexColumn) *ir.Index {
	return &ir.Index{Name: "uq_email", Unique: true, Columns: cols}
}

func TestSchemaDiff_SeesIndexDefinitionDrift(t *testing.T) {
	cases := []struct {
		name     string
		expected *ir.Index
		actual   *ir.Index
		want     bool // want a mismatch reported
		why      string
	}{
		{
			name:     "prefix length dropped (the S8 class)",
			expected: uniqueOn(ir.IndexColumn{Column: "email", Length: 10}),
			actual:   uniqueOn(ir.IndexColumn{Column: "email"}),
			want:     true,
			why: "the source forbids two rows sharing the first 10 characters of email and the target " +
				"permits them — the exact weakening items 118 and 120 refuse at emit time",
		},
		{
			name:     "prefix length differs",
			expected: uniqueOn(ir.IndexColumn{Column: "email", Length: 10}),
			actual:   uniqueOn(ir.IndexColumn{Column: "email", Length: 20}),
			want:     true,
			why:      "different prefix widths enforce different constraints",
		},
		{
			name:     "uniqueness dropped",
			expected: uniqueOn(ir.IndexColumn{Column: "email"}),
			actual:   &ir.Index{Name: "uq_email", Columns: []ir.IndexColumn{{Column: "email"}}},
			want:     true,
			why:      "a key that stopped enforcing uniqueness is the same weakening by another route",
		},
		{
			name: "partial predicate dropped",
			expected: &ir.Index{
				Name: "uq_email", Unique: true,
				Columns:   []ir.IndexColumn{{Column: "email"}},
				Predicate: "deleted_at IS NULL",
			},
			actual: uniqueOn(ir.IndexColumn{Column: "email"}),
			want:   true,
			why:    "the target became STRICTER — rows the source holds legally are refused",
		},
		{
			name:     "column set differs",
			expected: uniqueOn(ir.IndexColumn{Column: "email"}, ir.IndexColumn{Column: "id"}),
			actual:   uniqueOn(ir.IndexColumn{Column: "email"}),
			want:     true,
			why:      "a narrower key constrains different rows",
		},

		// CONTROLS. An identical index must stay clean, or the diff cries
		// wolf on every run and operators stop reading it — which is a worse
		// outcome than the blindness this closes.
		{
			name:     "identical index is clean",
			expected: uniqueOn(ir.IndexColumn{Column: "email", Length: 10}),
			actual:   uniqueOn(ir.IndexColumn{Column: "email", Length: 10}),
			want:     false,
			why:      "no drift",
		},
		{
			name: "identical partial unique is clean",
			expected: &ir.Index{
				Name: "uq_email", Unique: true,
				Columns:   []ir.IndexColumn{{Column: "email"}},
				Predicate: "deleted_at IS NULL",
			},
			actual: &ir.Index{
				Name: "uq_email", Unique: true,
				Columns:   []ir.IndexColumn{{Column: "email"}},
				Predicate: " deleted_at IS NULL ", // whitespace only
			},
			want: false,
			why:  "predicate comparison trims, as the CHECK comparison does",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exp := &ir.Schema{Tables: []*ir.Table{idxTable(tc.expected)}}
			act := &ir.Schema{Tables: []*ir.Table{idxTable(tc.actual)}}

			d := Schemas(exp, act, Options{})

			var got []IndexDiff
			for _, td := range d.TablesMismatched {
				got = append(got, td.IndexesMismatched...)
			}
			if (len(got) > 0) != tc.want {
				t.Fatalf("IndexesMismatched = %+v; want mismatch=%v\n\nwhy this matters: %s",
					got, tc.want, tc.why)
			}
			if tc.want && got[0].Name != "uq_email" {
				t.Errorf("mismatch reported for %q; want uq_email", got[0].Name)
			}
		})
	}
}

// The PRIMARY KEY participates too. indexNames() includes it in the
// missing/extra comparison, so a definition comparison that skipped it would
// be inconsistent with the pass beside it — and a prefixed PK is exactly the
// shape item 120 closed at the emitter.
func TestSchemaDiff_SeesPrimaryKeyDrift(t *testing.T) {
	mk := func(length int) *ir.Table {
		return &ir.Table{
			Name:    "users",
			Columns: []*ir.Column{{Name: "email", Type: ir.Varchar{Length: 255}}},
			PrimaryKey: &ir.Index{
				Name: "PRIMARY", Unique: true,
				Columns: []ir.IndexColumn{{Column: "email", Length: length}},
			},
		}
	}
	d := Schemas(
		&ir.Schema{Tables: []*ir.Table{mk(20)}},
		&ir.Schema{Tables: []*ir.Table{mk(0)}},
		Options{},
	)
	if !d.HasChanges() {
		t.Fatal("a PRIMARY KEY that lost its prefix length reported NO drift.\n\n" +
			"That is item 120's defect as an operator would try to detect it after the fact.")
	}
	found := false
	for _, td := range d.TablesMismatched {
		for _, id := range td.IndexesMismatched {
			if id.Name == "PRIMARY" {
				found = true
			}
		}
	}
	if !found {
		t.Error("the PK drift was not reported under IndexesMismatched")
	}
}

// The mismatch must reach HasChanges and Summary, or `sluice schema diff`
// exits 0 and prints "in sync" while holding the finding — the same
// carried-but-never-surfaced shape that hid the buffer-pool tier cap.
func TestSchemaDiff_IndexMismatchReachesExitCodeAndSummary(t *testing.T) {
	exp := &ir.Schema{Tables: []*ir.Table{idxTable(uniqueOn(ir.IndexColumn{Column: "email", Length: 10}))}}
	act := &ir.Schema{Tables: []*ir.Table{idxTable(uniqueOn(ir.IndexColumn{Column: "email"}))}}

	d := Schemas(exp, act, Options{})
	if !d.HasChanges() {
		t.Fatal("HasChanges() = false with an index mismatch present — the CLI would exit 0")
	}
	if got := d.Summary(); got == "in sync" {
		t.Fatalf("Summary() = %q with an index mismatch present", got)
	}
}

// fkTable builds a table carrying one named foreign key.
func fkTable(fk *ir.ForeignKey) *ir.Table {
	return &ir.Table{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "user_id", Type: ir.Integer{Width: 64}},
		},
		ForeignKeys: []*ir.ForeignKey{fk},
	}
}

func baseFK() *ir.ForeignKey {
	return &ir.ForeignKey{
		Name: "fk_orders_user", Columns: []string{"user_id"},
		ReferencedTable: "users", ReferencedColumns: []string{"id"},
		OnDelete: ir.FKActionCascade, OnUpdate: ir.FKActionNoAction,
	}
}

// The FK half of item 125. TableDiff carried NO foreign-key fields, so every
// FK difference compared equal on the surface an operator uses to check a
// target against its source.
func TestSchemaDiff_SeesForeignKeyDrift(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*ir.ForeignKey)
		want   bool
		why    string
	}{
		{
			name:   "referential action weakened",
			mutate: func(fk *ir.ForeignKey) { fk.OnDelete = ir.FKActionRestrict },
			want:   true,
			why:    "a source CASCADE against a target RESTRICT changes what a delete on the parent DOES",
		},
		{
			name:   "parent table re-pointed",
			mutate: func(fk *ir.ForeignKey) { fk.ReferencedTable = "accounts" },
			want:   true,
			why:    "the constraint now constrains something else entirely",
		},
		{
			name:   "referencing column changed",
			mutate: func(fk *ir.ForeignKey) { fk.Columns = []string{"id"} },
			want:   true,
			why:    "a different child column is constrained",
		},
		{
			name:   "MATCH strength changed",
			mutate: func(fk *ir.ForeignKey) { fk.Match = ir.FKMatchFull },
			want:   true,
			why: "under MATCH FULL the source rejects a partially-NULL composite key that MATCH SIMPLE " +
				"accepts — a constraint-strength difference (audit 2026-07-26 SL-7)",
		},
		{
			name:   "deferrability changed",
			mutate: func(fk *ir.ForeignKey) { fk.Deferrable, fk.InitiallyDeferred = true, true },
			want:   true,
			why:    "a target that is not deferrable rejects a transaction ordering the source accepts",
		},
		{
			name:   "identical FK is clean",
			mutate: func(*ir.ForeignKey) {},
			want:   false,
			why:    "no drift",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			act := baseFK()
			tc.mutate(act)
			d := Schemas(
				&ir.Schema{Tables: []*ir.Table{fkTable(baseFK())}},
				&ir.Schema{Tables: []*ir.Table{fkTable(act)}},
				Options{},
			)
			var got []ForeignKeyDiff
			for _, td := range d.TablesMismatched {
				got = append(got, td.ForeignKeysMismatched...)
			}
			if (len(got) > 0) != tc.want {
				t.Fatalf("ForeignKeysMismatched = %+v; want mismatch=%v\n\nwhy this matters: %s",
					got, tc.want, tc.why)
			}
		})
	}
}

// A dropped foreign key must surface as missing — the simplest and most
// likely real drift, and it was invisible too.
func TestSchemaDiff_SeesDroppedForeignKey(t *testing.T) {
	withFK := &ir.Schema{Tables: []*ir.Table{fkTable(baseFK())}}
	without := &ir.Schema{Tables: []*ir.Table{{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "user_id", Type: ir.Integer{Width: 64}},
		},
	}}}
	d := Schemas(withFK, without, Options{})
	if !d.HasChanges() {
		t.Fatal("a foreign key present on the source and absent on the target reported NO drift")
	}
	found := false
	for _, td := range d.TablesMismatched {
		for _, n := range td.ForeignKeysMissing {
			if n == "fk_orders_user" {
				found = true
			}
		}
	}
	if !found {
		t.Error("the dropped FK was not reported under ForeignKeysMissing")
	}
}

// An UNNAMED foreign key cannot be name-matched, and the diff must SAY it
// skipped one rather than reporting clean — "this comparison did not look at
// N of your foreign keys" is the coverage gap that makes a clean report
// misleading.
func TestSchemaDiff_ReportsUnnamedForeignKeysAsUncompared(t *testing.T) {
	unnamed := baseFK()
	unnamed.Name = ""
	d := Schemas(
		&ir.Schema{Tables: []*ir.Table{fkTable(unnamed)}},
		&ir.Schema{Tables: []*ir.Table{fkTable(unnamed)}},
		Options{},
	)
	// Both sides identical, so no drift — but the count must be recorded.
	td := TableDiff{}
	diffForeignKeys(&td, fkTable(unnamed), fkTable(unnamed), Options{})
	if td.ForeignKeysUnnamed != 2 {
		t.Errorf("ForeignKeysUnnamed = %d; want 2 (one per side).\n\n"+
			"An FK the comparison cannot see must be counted, not silently skipped.", td.ForeignKeysUnnamed)
	}
	_ = d
}
