// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"reflect"
	"testing"
)

// TestColumnIsGenerated exercises the IsGenerated helper across the
// three column shapes the IR distinguishes: plain (no expression),
// stored generated, and virtual generated.
func TestColumnIsGenerated(t *testing.T) {
	cases := []struct {
		name string
		col  Column
		want bool
	}{
		{
			name: "plain column",
			col:  Column{Name: "id", Type: Integer{Width: 64}},
			want: false,
		},
		{
			name: "stored generated",
			col: Column{
				Name:            "total",
				Type:            Integer{Width: 64},
				GeneratedExpr:   "qty * price",
				GeneratedStored: true,
			},
			want: true,
		},
		{
			name: "virtual generated",
			col: Column{
				Name:            "label",
				Type:            Varchar{Length: 64},
				GeneratedExpr:   "CONCAT(first_name, ' ', last_name)",
				GeneratedStored: false,
			},
			want: true,
		},
		{
			name: "empty expression with stored=true is not generated",
			col: Column{
				Name:            "id",
				Type:            Integer{Width: 64},
				GeneratedStored: true, // ignored: predicate is on Expr
			},
			want: false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := c.col.IsGenerated(); got != c.want {
				t.Errorf("IsGenerated() = %v; want %v", got, c.want)
			}
		})
	}
}

// TestCheckConstraint covers the struct shape and round-trip on
// Table.CheckConstraints. Both column-scoped and table-scoped CHECKs
// surface as table-level entries in the IR — engines normalize both
// forms into information_schema as table-level constraints, so the
// IR mirrors that shape.
func TestCheckConstraint(t *testing.T) {
	checks := []*CheckConstraint{
		{Name: "orders_qty_check", Expr: "qty >= 0"},
		{Name: "orders_status_check", Expr: "status IN ('open','closed','cancelled')"},
		{Name: "orders_date_check", Expr: "start_date <= end_date"},
	}
	tbl := &Table{
		Name: "orders",
		Columns: []*Column{
			{Name: "id", Type: Integer{Width: 64}},
		},
		CheckConstraints: checks,
	}

	if got := len(tbl.CheckConstraints); got != 3 {
		t.Fatalf("len(CheckConstraints) = %d; want 3", got)
	}
	if !reflect.DeepEqual(tbl.CheckConstraints, checks) {
		t.Errorf("CheckConstraints round-trip mismatch:\n got  %#v\n want %#v",
			tbl.CheckConstraints, checks)
	}
	// Spot-check fields directly so a future struct rename doesn't
	// silently neuter the assertion.
	if tbl.CheckConstraints[0].Name != "orders_qty_check" ||
		tbl.CheckConstraints[0].Expr != "qty >= 0" {
		t.Errorf("CheckConstraint[0] mismatch: %+v", tbl.CheckConstraints[0])
	}
}
