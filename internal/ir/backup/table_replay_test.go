// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestTableReplayIdempotent pins the keyed-ness derivation the
// anchored-resume guard depends on (task #42, ADR-0085). The matrix
// mirrors the engines' Bug-125 effectiveUpsertKeyColumns selection —
// PK, else an all-NOT-NULL plain-column UNIQUE index — including both
// exclusions (nullable-column UNIQUE, expression index member).
func TestTableReplayIdempotent(t *testing.T) {
	cases := []struct {
		name  string
		table *ir.Table
		want  bool
	}{
		{"nil table", nil, false},
		{
			"primary key",
			&ir.Table{
				Name:       "t",
				Columns:    []*ir.Column{{Name: "id"}},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
			},
			true,
		},
		{
			"empty primary key falls through to indexes",
			&ir.Table{
				Name:       "t",
				Columns:    []*ir.Column{{Name: "id", Nullable: true}},
				PrimaryKey: &ir.Index{},
			},
			false,
		},
		{
			"non-null unique index",
			&ir.Table{
				Name:    "t",
				Columns: []*ir.Column{{Name: "email"}},
				Indexes: []*ir.Index{{Name: "uq", Unique: true, Columns: []ir.IndexColumn{{Column: "email"}}}},
			},
			true,
		},
		{
			"composite non-null unique index",
			&ir.Table{
				Name:    "t",
				Columns: []*ir.Column{{Name: "a"}, {Name: "b"}},
				Indexes: []*ir.Index{{Name: "uq", Unique: true, Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}}},
			},
			true,
		},
		{
			// A UNIQUE index over a NULLABLE column is NOT a replay key:
			// both engines allow multiple NULLs in a UNIQUE column, so
			// the replay would not reliably collide (silent duplicates).
			"nullable unique index ineligible",
			&ir.Table{
				Name:    "t",
				Columns: []*ir.Column{{Name: "email", Nullable: true}},
				Indexes: []*ir.Index{{Name: "uq", Unique: true, Columns: []ir.IndexColumn{{Column: "email"}}}},
			},
			false,
		},
		{
			// One nullable member poisons a composite key.
			"composite unique with one nullable member ineligible",
			&ir.Table{
				Name:    "t",
				Columns: []*ir.Column{{Name: "a"}, {Name: "b", Nullable: true}},
				Indexes: []*ir.Index{{Name: "uq", Unique: true, Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}}},
			},
			false,
		},
		{
			"expression unique index ineligible",
			&ir.Table{
				Name:    "t",
				Columns: []*ir.Column{{Name: "email"}},
				Indexes: []*ir.Index{{Name: "uq", Unique: true, Columns: []ir.IndexColumn{{Expression: "lower(email)"}}}},
			},
			false,
		},
		{
			"non-unique index ineligible",
			&ir.Table{
				Name:    "t",
				Columns: []*ir.Column{{Name: "email"}},
				Indexes: []*ir.Index{{Name: "ix", Columns: []ir.IndexColumn{{Column: "email"}}}},
			},
			false,
		},
		{
			"truly keyless",
			&ir.Table{Name: "t", Columns: []*ir.Column{{Name: "v", Nullable: true}}},
			false,
		},
		{
			// A PARTIAL unique index constrains only the rows its predicate
			// selects; a replayed row outside it collides with nothing
			// (measured on Postgres: TestRowWriter_ProbeReplayKey_ShapeMatrix
			// "partial_unique" duplicates).
			"partial unique index ineligible",
			&ir.Table{
				Name:    "t",
				Columns: []*ir.Column{{Name: "id"}},
				Indexes: []*ir.Index{{Name: "uq", Unique: true, Predicate: "id > 0", Columns: []ir.IndexColumn{{Column: "id"}}}},
			},
			false,
		},
		{
			// A whitespace-only predicate is no predicate.
			"blank predicate is a full index",
			&ir.Table{
				Name:    "t",
				Columns: []*ir.Column{{Name: "id"}},
				Indexes: []*ir.Index{{Name: "uq", Unique: true, Predicate: "  ", Columns: []ir.IndexColumn{{Column: "id"}}}},
			},
			true,
		},
		{
			// DEFERRABLE stays eligible on the recorded side: every target
			// lands a recorded deferrable UNIQUE as an immediate one, and a
			// PG target that keeps a deferrable PK refuses loudly at the
			// applier (see the function doc).
			"deferrable unique stays eligible",
			&ir.Table{
				Name:    "t",
				Columns: []*ir.Column{{Name: "id"}},
				Indexes: []*ir.Index{{Name: "uq", Unique: true, ConstraintBacked: true, ConstraintDeferrable: true, Columns: []ir.IndexColumn{{Column: "id"}}}},
			},
			true,
		},
		{
			"deferrable primary key stays eligible",
			&ir.Table{
				Name:       "t",
				Columns:    []*ir.Column{{Name: "id"}},
				PrimaryKey: &ir.Index{ConstraintDeferrable: true, Columns: []ir.IndexColumn{{Column: "id"}}},
			},
			true,
		},
		{
			// Mixed: an ineligible nullable UNIQUE plus an eligible one —
			// any eligible index qualifies.
			"one eligible among ineligible indexes",
			&ir.Table{
				Name:    "t",
				Columns: []*ir.Column{{Name: "a", Nullable: true}, {Name: "b"}},
				Indexes: []*ir.Index{
					{Name: "uq_a", Unique: true, Columns: []ir.IndexColumn{{Column: "a"}}},
					{Name: "uq_b", Unique: true, Columns: []ir.IndexColumn{{Column: "b"}}},
				},
			},
			true,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := TableReplayIdempotent(c.table); got != c.want {
				t.Errorf("TableReplayIdempotent = %v; want %v", got, c.want)
			}
		})
	}
}

// TestReplaySuppliedColumns pins the supplied side of the F-E1 target
// judgment: a generated column is never bound by a writer, so it is never
// supplied; everything else is, in declaration order.
func TestReplaySuppliedColumns(t *testing.T) {
	if got := ReplaySuppliedColumns(nil); got != nil {
		t.Errorf("nil table: got %v", got)
	}
	table := &ir.Table{Columns: []*ir.Column{
		{Name: "id"},
		{Name: "k", GeneratedExpr: "id * 2"},
		nil,
		{Name: "v"},
	}}
	got := ReplaySuppliedColumns(table)
	if len(got) != 2 || got[0] != "id" || got[1] != "v" {
		t.Errorf("got %v; want [id v]", got)
	}
}
