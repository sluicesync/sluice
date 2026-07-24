// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the C3 UNIQUE-constraint attribute fidelity WARN
// (roadmap "UNIQUE-constraint attribute fidelity"): the version-gated
// catalog select expressions (the read must not 42703 on servers that
// predate the column), the attribute-naming matrix, and the WARN's
// fire/no-fire contract. The real-PG WARN-once-per-constraint behavior
// is pinned by the integration twin
// (schema_reader_unique_attrs_integration_test.go); the PG-18
// `conperiod` cell is covered HERE only — the rig images are PG 16.

package postgres

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestUniqueConstraintAttrExprs_VersionMatrix(t *testing.T) {
	// NULLS NOT DISTINCT lives on pg_index (indnullsnotdistinct), NOT
	// pg_constraint — a real PG 16 read 42703'd the roadmap entry's
	// assumed `connullsnotdistinct` column, which never existed.
	const (
		nndCol    = "ix.indnullsnotdistinct"
		periodCol = "COALESCE(ucon.conperiod, false)"
	)
	cases := []struct {
		name            string
		version         int
		wantNND, wantPd string
	}{
		{"PG 13 — neither column exists", 130011, "false", "false"},
		{"PG 14 — neither column exists", 140006, "false", "false"},
		{"PG 15.0 — connullsnotdistinct arrives", 150000, nndCol, "false"},
		{"PG 16 (the rig image)", 160004, nndCol, "false"},
		{"PG 17 — conperiod NOT yet (temporal constraints were reverted pre-17-GA)", 170002, nndCol, "false"},
		{"PG 18.0 — conperiod arrives", 180000, nndCol, periodCol},
		{"PG 19 — both", 190001, nndCol, periodCol},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotNND, gotPd := uniqueConstraintAttrExprs(c.version)
			if gotNND != c.wantNND {
				t.Errorf("nullsNotDistinctExpr(%d) = %q; want %q", c.version, gotNND, c.wantNND)
			}
			if gotPd != c.wantPd {
				t.Errorf("periodExpr(%d) = %q; want %q", c.version, gotPd, c.wantPd)
			}
		})
	}
}

// TestWeakenedUniqueAttrs pins the attribute-naming matrix: every
// family member (DEFERRABLE / NULLS NOT DISTINCT / WITHOUT OVERLAPS)
// is named when set, a plain constraint names nothing, and the flags
// are inert without ConstraintBacked (they are attributes OF the
// owning constraint; a bare unique index cannot carry them).
func TestWeakenedUniqueAttrs(t *testing.T) {
	cases := []struct {
		name string
		idx  ir.Index
		want []string // substring per expected entry, in order
	}{
		{
			name: "plain constraint-backed UNIQUE — no attrs",
			idx:  ir.Index{Name: "u", Unique: true, ConstraintBacked: true},
		},
		{
			name: "not constraint-backed — flags inert (defensive)",
			idx:  ir.Index{Name: "u", Unique: true, ConstraintNullsNotDistinct: true, ConstraintDeferrable: true},
		},
		{
			name: "NULLS NOT DISTINCT",
			idx:  ir.Index{Name: "u", Unique: true, ConstraintBacked: true, ConstraintNullsNotDistinct: true},
			want: []string{"NULLS NOT DISTINCT"},
		},
		{
			name: "DEFERRABLE",
			idx:  ir.Index{Name: "u", Unique: true, ConstraintBacked: true, ConstraintDeferrable: true},
			want: []string{"DEFERRABLE"},
		},
		{
			name: "WITHOUT OVERLAPS",
			idx:  ir.Index{Name: "u", Unique: true, ConstraintBacked: true, ConstraintWithoutOverlaps: true},
			want: []string{"WITHOUT OVERLAPS"},
		},
		{
			name: "all three name all three",
			idx: ir.Index{
				Name: "u", Unique: true, ConstraintBacked: true,
				ConstraintDeferrable: true, ConstraintNullsNotDistinct: true, ConstraintWithoutOverlaps: true,
			},
			want: []string{"NULLS NOT DISTINCT", "DEFERRABLE", "WITHOUT OVERLAPS"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := weakenedUniqueAttrs(&c.idx)
			if len(got) != len(c.want) {
				t.Fatalf("got %d attrs %v; want %d", len(got), got, len(c.want))
			}
			for i, w := range c.want {
				if !strings.Contains(got[i], w) {
					t.Errorf("attr[%d] = %q; want it to name %q", i, got[i], w)
				}
			}
		})
	}
}

func TestWarnWeakenedUniqueConstraint(t *testing.T) {
	ctx := context.Background()

	buf := captureSlog(t)
	warnWeakenedUniqueConstraint(ctx, "orders", &ir.Index{Name: "orders_ref_u", Unique: true, ConstraintBacked: true})
	if buf.Len() != 0 {
		t.Errorf("plain constraint-backed UNIQUE must not WARN:\n%s", buf.String())
	}

	buf.Reset()
	warnWeakenedUniqueConstraint(ctx, "orders", &ir.Index{
		Name: "orders_ref_nnd", Unique: true,
		ConstraintBacked: true, ConstraintNullsNotDistinct: true,
	})
	out := buf.String()
	if !strings.Contains(out, "orders_ref_nnd") || !strings.Contains(out, "NULLS NOT DISTINCT") {
		t.Errorf("WARN must name the constraint and the dropped attribute:\n%s", out)
	}
	if !strings.Contains(out, "duplicate NULLs") {
		t.Errorf("WARN must state the weaker landing (admits duplicate NULLs):\n%s", out)
	}
	if !strings.Contains(out, "level=WARN") {
		t.Errorf("must be a WARN-level line:\n%s", out)
	}
}
