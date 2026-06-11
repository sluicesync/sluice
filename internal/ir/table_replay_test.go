// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "testing"

// TestTableReplayIdempotent pins the keyed-ness derivation the
// anchored-resume guard depends on (task #42, ADR-0085). The matrix
// mirrors the engines' Bug-125 effectiveUpsertKeyColumns selection —
// PK, else an all-NOT-NULL plain-column UNIQUE index — including both
// exclusions (nullable-column UNIQUE, expression index member).
func TestTableReplayIdempotent(t *testing.T) {
	cases := []struct {
		name  string
		table *Table
		want  bool
	}{
		{"nil table", nil, false},
		{
			"primary key",
			&Table{
				Name:       "t",
				Columns:    []*Column{{Name: "id"}},
				PrimaryKey: &Index{Columns: []IndexColumn{{Column: "id"}}},
			},
			true,
		},
		{
			"empty primary key falls through to indexes",
			&Table{
				Name:       "t",
				Columns:    []*Column{{Name: "id", Nullable: true}},
				PrimaryKey: &Index{},
			},
			false,
		},
		{
			"non-null unique index",
			&Table{
				Name:    "t",
				Columns: []*Column{{Name: "email"}},
				Indexes: []*Index{{Name: "uq", Unique: true, Columns: []IndexColumn{{Column: "email"}}}},
			},
			true,
		},
		{
			"composite non-null unique index",
			&Table{
				Name:    "t",
				Columns: []*Column{{Name: "a"}, {Name: "b"}},
				Indexes: []*Index{{Name: "uq", Unique: true, Columns: []IndexColumn{{Column: "a"}, {Column: "b"}}}},
			},
			true,
		},
		{
			// A UNIQUE index over a NULLABLE column is NOT a replay key:
			// both engines allow multiple NULLs in a UNIQUE column, so
			// the replay would not reliably collide (silent duplicates).
			"nullable unique index ineligible",
			&Table{
				Name:    "t",
				Columns: []*Column{{Name: "email", Nullable: true}},
				Indexes: []*Index{{Name: "uq", Unique: true, Columns: []IndexColumn{{Column: "email"}}}},
			},
			false,
		},
		{
			// One nullable member poisons a composite key.
			"composite unique with one nullable member ineligible",
			&Table{
				Name:    "t",
				Columns: []*Column{{Name: "a"}, {Name: "b", Nullable: true}},
				Indexes: []*Index{{Name: "uq", Unique: true, Columns: []IndexColumn{{Column: "a"}, {Column: "b"}}}},
			},
			false,
		},
		{
			"expression unique index ineligible",
			&Table{
				Name:    "t",
				Columns: []*Column{{Name: "email"}},
				Indexes: []*Index{{Name: "uq", Unique: true, Columns: []IndexColumn{{Expression: "lower(email)"}}}},
			},
			false,
		},
		{
			"non-unique index ineligible",
			&Table{
				Name:    "t",
				Columns: []*Column{{Name: "email"}},
				Indexes: []*Index{{Name: "ix", Columns: []IndexColumn{{Column: "email"}}}},
			},
			false,
		},
		{
			"truly keyless",
			&Table{Name: "t", Columns: []*Column{{Name: "v", Nullable: true}}},
			false,
		},
		{
			// Mixed: an ineligible nullable UNIQUE plus an eligible one —
			// any eligible index qualifies.
			"one eligible among ineligible indexes",
			&Table{
				Name:    "t",
				Columns: []*Column{{Name: "a", Nullable: true}, {Name: "b"}},
				Indexes: []*Index{
					{Name: "uq_a", Unique: true, Columns: []IndexColumn{{Column: "a"}}},
					{Name: "uq_b", Unique: true, Columns: []IndexColumn{{Column: "b"}}},
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
