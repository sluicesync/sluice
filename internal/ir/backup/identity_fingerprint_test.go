// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The [ir.Column.Identity] fingerprint-exclusion gate (gap census
// 2026-09-22 S4/S5), written the way on_update_fingerprint_test.go is: a
// schema WITH the field must hash EQUAL to the same schema WITHOUT it.
// The Postgres reader sets Identity on every identity column, so a hash
// that saw it would have minted a new epoch for nearly every Postgres
// chain (roadmap item 104 repeating). The anti-vacuity floor is
// TestSchemaHash_StillSeesRealSchemaChanges in the sibling file, which
// proves the hash still sees column changes at all.

package backup

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func schemaWithIdentity(id *ir.IdentityOptions) *ir.Schema {
	return &ir.Schema{
		Tables: []*ir.Table{{
			Schema: "app",
			Name:   "orders",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}, Default: ir.DefaultNone{}, Identity: id},
				{Name: "note", Type: ir.Text{}, Default: ir.DefaultNone{}},
			},
		}},
	}
}

func TestSchemaHash_ExcludesIdentityOptions(t *testing.T) {
	without, err := ComputeSchemaHash(schemaWithIdentity(nil))
	if err != nil {
		t.Fatalf("ComputeSchemaHash(without): %v", err)
	}
	for name, id := range map[string]*ir.IdentityOptions{
		"BY DEFAULT with factory options": {Start: 1, Increment: 1, MinValue: 1, MaxValue: 9223372036854775807, Cache: 1},
		"ALWAYS, re-optioned":             {Always: true, Start: 3, Increment: 10, MinValue: 1, MaxValue: 1000, Cache: 50, Cycle: true},
	} {
		withField, err := ComputeSchemaHash(schemaWithIdentity(id))
		if err != nil {
			t.Fatalf("ComputeSchemaHash(%s): %v", name, err)
		}
		if withField != without {
			t.Errorf("%s moved the schema fingerprint (%s vs %s): every Postgres chain carrying an identity column "+
				"would be repartitioned into a new epoch and refuse to restore on any earlier release. Exclude the "+
				"field in canonicalColumnsForHash.", name, withField, without)
		}
	}
}

// The exclusion must not mutate the caller's schema: the manifest records
// Identity exactly as the reader produced it, and only the FINGERPRINT is
// canonical.
func TestSchemaHash_IdentityExclusionDoesNotMutateTheCaller(t *testing.T) {
	id := &ir.IdentityOptions{Always: true, Increment: 10}
	s := schemaWithIdentity(id)
	if _, err := ComputeSchemaHash(s); err != nil {
		t.Fatalf("ComputeSchemaHash: %v", err)
	}
	if s.Tables[0].Columns[0].Identity != id {
		t.Error("ComputeSchemaHash mutated the caller's Column.Identity")
	}
}
