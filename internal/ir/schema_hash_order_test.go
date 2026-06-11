// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "testing"

// orderTestTable builds a table carrying every non-semantic collection
// with ≥2 members, so reversing each collection exercises every
// sortedByName call site in canonicalSchemaForHash.
func orderTestTable() *Table {
	return &Table{
		Schema: "public",
		Name:   "users",
		Columns: []*Column{
			{Name: "id", Type: Integer{Width: 64}},
			{Name: "email", Type: Varchar{Length: 255}},
		},
		PrimaryKey: &Index{Name: "users_pkey", Unique: true, Columns: []IndexColumn{{Column: "id"}}},
		Indexes: []*Index{
			{Name: "users_created_at_idx", Columns: []IndexColumn{{Column: "created_at"}}},
			{Name: "users_email_idx", Columns: []IndexColumn{{Column: "email"}}},
		},
		ForeignKeys: []*ForeignKey{
			{Name: "fk_a", Columns: []string{"a_id"}},
			{Name: "fk_b", Columns: []string{"b_id"}},
		},
		CheckConstraints: []*CheckConstraint{
			{Name: "chk_a", Expr: "a > 0"},
			{Name: "chk_b", Expr: "b > 0"},
		},
		ExcludeConstraints: []*ExcludeConstraint{
			{Name: "excl_a", Definition: "EXCLUDE USING gist (a WITH =)"},
			{Name: "excl_b", Definition: "EXCLUDE USING gist (b WITH =)"},
		},
		Policies: []*Policy{
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
	a := &Schema{Tables: []*Table{orderTestTable()}}
	b := &Schema{Tables: []*Table{orderTestTable()}}
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
	c := &Schema{Tables: []*Table{orderTestTable()}}
	c.Tables[0].Columns[0], c.Tables[0].Columns[1] = c.Tables[0].Columns[1], c.Tables[0].Columns[0]
	hc, err := ComputeSchemaHash(c)
	if err != nil {
		t.Fatalf("hash c: %v", err)
	}
	if hc == ha {
		t.Error("column reorder did NOT change the hash; column order is semantic and must be fingerprinted")
	}

	// A real difference must still change the hash.
	d := &Schema{Tables: []*Table{orderTestTable()}}
	d.Tables[0].Indexes[0].Unique = true
	hd, err := ComputeSchemaHash(d)
	if err != nil {
		t.Fatalf("hash d: %v", err)
	}
	if hd == ha {
		t.Error("index property change did NOT change the hash")
	}
}
