// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import "testing"

// TestFKParentOnlyExpr_VersionMatrix pins the version gate on the
// partition-FK-clone filter (roadmap item 78a). The predicate must be present
// from PG 11 — the first version with pg_constraint.conparentid, and the first
// that can create the clones at all — and must be a constant TRUE below it,
// because referencing a nonexistent column would 42703 the ENTIRE foreign-key
// read on a PG 10 source. That matters even though partitioned sources are
// refused (Bug 100): this query runs against every PG source, partitioned or
// not, so the gate protects a live supported version rather than a
// hypothetical one.
func TestFKParentOnlyExpr_VersionMatrix(t *testing.T) {
	cases := []struct {
		name    string
		version int
		want    string
	}{
		{"PG 10 — column absent, must not be referenced", 100000, "true"},
		{"PG 10.23 point release", 100023, "true"},
		{"PG 11 — first version with conparentid", 110000, "con.conparentid = 0"},
		{"PG 16", 160006, "con.conparentid = 0"},
		{"PG 18", 180004, "con.conparentid = 0"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := fkParentOnlyExpr(c.version); got != c.want {
				t.Errorf("fkParentOnlyExpr(%d) = %q; want %q", c.version, got, c.want)
			}
		})
	}
}
