// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// orderTestTable builds a table carrying every non-semantic collection
// with ≥2 members, so reversing each collection exercises every
// sortedByName call site in canonicalSchemaForHash.
func orderTestTable() *ir.Table {
	return &ir.Table{
		Schema: "public",
		Name:   "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{Name: "users_pkey", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{
			{Name: "users_created_at_idx", Columns: []ir.IndexColumn{{Column: "created_at"}}},
			{Name: "users_email_idx", Columns: []ir.IndexColumn{{Column: "email"}}},
		},
		ForeignKeys: []*ir.ForeignKey{
			{Name: "fk_a", Columns: []string{"a_id"}},
			{Name: "fk_b", Columns: []string{"b_id"}},
		},
		CheckConstraints: []*ir.CheckConstraint{
			{Name: "chk_a", Expr: "a > 0"},
			{Name: "chk_b", Expr: "b > 0"},
		},
		ExcludeConstraints: []*ir.ExcludeConstraint{
			{Name: "excl_a", Definition: "EXCLUDE USING gist (a WITH =)"},
			{Name: "excl_b", Definition: "EXCLUDE USING gist (b WITH =)"},
		},
		Policies: []*ir.Policy{
			{Name: "pol_a", Command: "SELECT"},
			{Name: "pol_b", Command: "INSERT"},
		},
	}
}

func reverseInPlace[T any](s []*T) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// TestComputeSchemaHash_OrderInsensitiveCollections pins task #41's
// hash half: two semantically-identical schemas whose non-semantic
// collections (indexes, FKs, checks, excludes, policies) arrive in
// different orders MUST hash identically — catalog reads historically
// drained these through randomized map iteration, and an
// order-sensitive fingerprint made identical schemas look tampered.
// Column order stays semantic and must keep changing the hash.
func TestComputeSchemaHash_OrderInsensitiveCollections(t *testing.T) {
	a := &ir.Schema{Tables: []*ir.Table{orderTestTable()}}
	b := &ir.Schema{Tables: []*ir.Table{orderTestTable()}}
	reverseInPlace(b.Tables[0].Indexes)
	reverseInPlace(b.Tables[0].ForeignKeys)
	reverseInPlace(b.Tables[0].CheckConstraints)
	reverseInPlace(b.Tables[0].ExcludeConstraints)
	reverseInPlace(b.Tables[0].Policies)

	ha, err := ComputeSchemaHash(a)
	if err != nil {
		t.Fatalf("hash a: %v", err)
	}
	hb, err := ComputeSchemaHash(b)
	if err != nil {
		t.Fatalf("hash b: %v", err)
	}
	if ha != hb {
		t.Errorf("hashes differ across collection reorders:\n a=%s\n b=%s", ha, hb)
	}

	// Hashing must not mutate the input — manifests record schemas
	// exactly as read; only the fingerprint is canonical.
	if b.Tables[0].Indexes[0].Name != "users_email_idx" {
		t.Error("ComputeSchemaHash mutated the input schema's index order")
	}

	// Column order IS semantic: swapping it must change the hash.
	c := &ir.Schema{Tables: []*ir.Table{orderTestTable()}}
	c.Tables[0].Columns[0], c.Tables[0].Columns[1] = c.Tables[0].Columns[1], c.Tables[0].Columns[0]
	hc, err := ComputeSchemaHash(c)
	if err != nil {
		t.Fatalf("hash c: %v", err)
	}
	if hc == ha {
		t.Error("column reorder did NOT change the hash; column order is semantic and must be fingerprinted")
	}

	// A real difference must still change the hash.
	d := &ir.Schema{Tables: []*ir.Table{orderTestTable()}}
	d.Tables[0].Indexes[0].Unique = true
	hd, err := ComputeSchemaHash(d)
	if err != nil {
		t.Fatalf("hash d: %v", err)
	}
	if hd == ha {
		t.Error("index property change did NOT change the hash")
	}
}
