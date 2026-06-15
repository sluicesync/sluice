// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "testing"

// TestStableID_ExcludedFromSchemaSignature pins ADR-0091 F7b's CRITICAL
// invariant: StableID is METADATA, not a schema attribute. A seed
// (StableID=0) and the first CDC snapshot (StableID=attnum) for an
// otherwise-identical column MUST share a decode signature — otherwise
// the seed→firstCDC diff would spuriously resnapshot / be treated as a
// delta on every PG stream.
func TestStableID_ExcludedFromSchemaSignature(t *testing.T) {
	seed := &Table{
		Name: "widgets",
		Columns: []*Column{
			{Name: "id", Type: Integer{Width: 32}, StableID: 0},
			{Name: "name", Type: Text{Size: TextLong}, StableID: 0},
		},
	}
	cdc := &Table{
		Name: "widgets",
		Columns: []*Column{
			{Name: "id", Type: Integer{Width: 32}, StableID: 1},
			{Name: "name", Type: Text{Size: TextLong}, StableID: 2},
		},
	}
	if !SchemaSignatureOf(seed).Equal(SchemaSignatureOf(cdc)) {
		t.Fatalf("signature differs only by StableID — StableID must NOT affect the decode contract")
	}
	// And a column differing in StableID alone shares a signature even
	// when the IDs are both non-zero but different (rename-pre vs rename
	// target attnum reuse cannot perturb decode).
	a := &Table{Name: "t", Columns: []*Column{{Name: "c", Type: UUID{}, StableID: 7}}}
	b := &Table{Name: "t", Columns: []*Column{{Name: "c", Type: UUID{}, StableID: 99}}}
	if !SchemaSignatureOf(a).Equal(SchemaSignatureOf(b)) {
		t.Fatalf("two non-zero StableIDs perturbed the signature — must be ignored")
	}
}

// TestStableID_NotSerialized pins that StableID does not round-trip
// through the schema-history / backup codec. It is only ever compared
// between two live CDC projections within one stream; a persisted value
// would be meaningless on resume, so the wire shape deliberately omits
// it. (This also keeps the persisted-snapshot byte shape unchanged from
// pre-F7b, so existing schema-history rows decode identically.)
func TestStableID_NotSerialized(t *testing.T) {
	in := &Table{
		Name: "widgets",
		Columns: []*Column{
			{Name: "id", Type: Integer{Width: 32}, StableID: 42},
		},
	}
	b, err := MarshalTable(in)
	if err != nil {
		t.Fatalf("MarshalTable: %v", err)
	}
	out, err := UnmarshalTable(b)
	if err != nil {
		t.Fatalf("UnmarshalTable: %v", err)
	}
	if got := out.Columns[0].StableID; got != 0 {
		t.Fatalf("StableID survived the codec (got %d); it must NOT be serialized", got)
	}
}
