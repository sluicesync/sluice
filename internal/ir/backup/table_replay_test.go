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
