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

// TestPolicyRoundTrip pins the IR shape for the new RLS [Policy] field
// (ADR-0063 — task #52 sub-deliverables 2 + 3). The field set mirrors
// PG's `pg_policies` columns so a same-engine PG → PG migration carries
// the policy verbatim. The matrix covers Command × Permissive ×
// USING/CHECK shape, the Bug-74 class-pin discipline the ADR's test
// strategy section calls out.
func TestPolicyRoundTrip(t *testing.T) {
	policies := []*Policy{
		{
			Name:       "p_select_all",
			Command:    "ALL",
			Permissive: true,
			Roles:      []string{"public"},
			Using:      "tenant = current_setting('app.tenant', true)",
		},
		{
			Name:       "p_select_only",
			Command:    "SELECT",
			Permissive: true,
			Roles:      []string{"app_user"},
			Using:      "owner = current_user",
		},
		{
			Name:       "p_insert_check",
			Command:    "INSERT",
			Permissive: true,
			Roles:      []string{"public"},
			Check:      "owner = current_user",
		},
		{
			Name:       "p_update_both",
			Command:    "UPDATE",
			Permissive: true,
			Roles:      []string{"public"},
			Using:      "owner = current_user",
			Check:      "owner = current_user",
		},
		{
			Name:       "p_delete_restrictive",
			Command:    "DELETE",
			Permissive: false,
			Roles:      []string{"app_user", "admin"},
			Using:      "is_archived = false",
		},
	}
	tbl := &Table{
		Name:       "tenants",
		Columns:    []*Column{{Name: "id", Type: Integer{Width: 64}}},
		RLSEnabled: true,
		RLSForced:  true,
		Policies:   policies,
	}

	if !tbl.RLSEnabled || !tbl.RLSForced {
		t.Errorf("RLS flags round-trip mismatch: enabled=%v forced=%v",
			tbl.RLSEnabled, tbl.RLSForced)
	}
	if got := len(tbl.Policies); got != len(policies) {
		t.Fatalf("len(Policies) = %d; want %d", got, len(policies))
	}
	if !reflect.DeepEqual(tbl.Policies, policies) {
		t.Errorf("Policies round-trip mismatch:\n got  %#v\n want %#v",
			tbl.Policies, policies)
	}
	// Spot-check each row covers a distinct cell of the matrix; if a
	// future field rename silently neutered this, every cell would
	// still pass DeepEqual but lose its operator-visible meaning.
	if tbl.Policies[0].Command != "ALL" || tbl.Policies[0].Check != "" {
		t.Errorf("p_select_all: cmd=%q check=%q (want ALL/empty)",
			tbl.Policies[0].Command, tbl.Policies[0].Check)
	}
	if tbl.Policies[2].Using != "" || tbl.Policies[2].Check == "" {
		t.Errorf("p_insert_check: USING should be empty, CHECK non-empty; got using=%q check=%q",
			tbl.Policies[2].Using, tbl.Policies[2].Check)
	}
	if tbl.Policies[4].Permissive {
		t.Errorf("p_delete_restrictive: Permissive should be false; got true")
	}
	if len(tbl.Policies[4].Roles) != 2 {
		t.Errorf("p_delete_restrictive: Roles len = %d; want 2", len(tbl.Policies[4].Roles))
	}
}

// TestPolicyZeroValue confirms an empty Policy slice + RLS-off flags
// are the legitimate "no RLS" representation. Reader-side code keys
// off len(Policies) == 0 and !RLSEnabled to skip emit on tables that
// never enabled RLS — the common case on most PG schemas.
func TestPolicyZeroValue(t *testing.T) {
	tbl := &Table{Name: "plain", Columns: []*Column{{Name: "id", Type: Integer{Width: 64}}}}
	if tbl.RLSEnabled || tbl.RLSForced {
		t.Errorf("zero-value Table: RLS flags should be false; got enabled=%v forced=%v",
			tbl.RLSEnabled, tbl.RLSForced)
	}
	if len(tbl.Policies) != 0 {
		t.Errorf("zero-value Table: len(Policies) = %d; want 0", len(tbl.Policies))
	}
}
