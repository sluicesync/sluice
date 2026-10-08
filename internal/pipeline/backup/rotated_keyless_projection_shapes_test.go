// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestProjectedKeySupplied_ShapeMatrix is the v0.157.0 review's item 3: the
// Bug 297 projection answers the same engine-neutral questions as the live
// replay-key probes. A projected key the later full's rows collide on counts;
// a GENERATED key column (never supplied — the fix), a nullable UNIQUE and a
// partial UNIQUE do not. A DEFERRABLE primary key is a characterization row:
// the projection cannot judge it (it is target-dependent, and the projection
// serves `backup verify` too), so it counts here and the live probe at each
// later full refuses it on a Postgres target — the residual its doc names.
func TestProjectedKeySupplied_ShapeMatrix(t *testing.T) {
	col := func(n string, nullable bool) *ir.Column {
		return &ir.Column{Name: n, Type: ir.Integer{Width: 64}, Nullable: nullable}
	}
	gen := func(n string) *ir.Column {
		c := col(n, false)
		c.GeneratedExpr = "v + 1"
		c.GeneratedStored = true
		return c
	}
	pk := func(cols ...string) *ir.Index {
		idx := &ir.Index{Unique: true}
		for _, c := range cols {
			idx.Columns = append(idx.Columns, ir.IndexColumn{Column: c})
		}
		return idx
	}
	uniq := func(pred string, cols ...string) *ir.Index {
		idx := pk(cols...)
		idx.Name, idx.Predicate = "u_"+cols[0], pred
		return idx
	}
	for _, tc := range []struct {
		name     string
		table    *ir.Table
		supplied []string
		want     bool
	}{
		{"plain PRIMARY KEY, supplied", &ir.Table{Name: "t", Columns: []*ir.Column{col("id", false), col("v", false)}, PrimaryKey: pk("id")}, []string{"id", "v"}, true},
		{"PRIMARY KEY on a GENERATED column", &ir.Table{Name: "t", Columns: []*ir.Column{gen("id"), col("v", false)}, PrimaryKey: pk("id")}, []string{"id", "v"}, false},
		{"NOT NULL UNIQUE on a GENERATED column", &ir.Table{Name: "t", Columns: []*ir.Column{gen("g"), col("v", false)}, Indexes: []*ir.Index{uniq("", "g")}}, []string{"g", "v"}, false},
		{"NOT NULL UNIQUE, supplied", &ir.Table{Name: "t", Columns: []*ir.Column{col("u", false), col("v", false)}, Indexes: []*ir.Index{uniq("", "u")}}, []string{"u", "v"}, true},
		{"nullable UNIQUE", &ir.Table{Name: "t", Columns: []*ir.Column{col("u", true), col("v", false)}, Indexes: []*ir.Index{uniq("", "u")}}, []string{"u", "v"}, false},
		{"partial UNIQUE", &ir.Table{Name: "t", Columns: []*ir.Column{col("u", false), col("v", false)}, Indexes: []*ir.Index{uniq("v > 0", "u")}}, []string{"u", "v"}, false},
		{"PRIMARY KEY the later full does not supply", &ir.Table{Name: "t", Columns: []*ir.Column{col("id", false), col("v", false)}, PrimaryKey: pk("id")}, []string{"v"}, false},
		{"DEFERRABLE PRIMARY KEY (residual: judged by the live probe)", &ir.Table{Name: "t", Columns: []*ir.Column{col("id", false)}, PrimaryKey: func() *ir.Index {
			idx := pk("id")
			idx.ConstraintDeferrable = true
			return idx
		}()}, []string{"id"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := projectedKeySupplied(tc.table, tc.supplied); got != tc.want {
				t.Errorf("projectedKeySupplied = %v; want %v", got, tc.want)
			}
		})
	}
}
