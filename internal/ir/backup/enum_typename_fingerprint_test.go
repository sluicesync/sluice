// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// [ir.Enum.TypeName] must ride the wire and stay OUT of the schema
// fingerprint (roadmap item 121) — the same split [ir.Macaddr.Width] needed,
// for a field that is non-zero far more often.
//
// The Postgres reader sets TypeName for EVERY enum column, so folding it into
// the fingerprint would move the hash of close to every Postgres chain in
// existence and mint an epoch those chains cannot restore across. It must
// still reach a restore, though, or the writer synthesizes a name and
// re-creates `post_status` as `posts_status_enum` — which is precisely what
// the field exists to prevent, and what a manifest round trip was silently
// doing before this item.
//
// The equality assertions below are with-field vs without-field, which a
// self-consistent binary cannot satisfy by agreeing with itself — the item-104
// trap. The byte-level test is the half that makes "the fingerprint did not
// move" a statement about EARLIER RELEASES: it proves the hashed bytes carry
// no enum_type_name key at all, which is exactly what a binary whose envelope
// had no such field emitted.

package backup

import (
	"encoding/json"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// schemaWithEnumTypeName builds a schema whose enum columns carry the given
// type name — scalar, array element, and domain base, so the fingerprint
// walk's recursion is exercised at every nesting site it claims to reach.
func schemaWithEnumTypeName(name string) *ir.Schema {
	vals := []string{"draft", "live"}
	return &ir.Schema{Tables: []*ir.Table{{
		Schema: "public", Name: "posts",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "status", Type: ir.Enum{Values: vals, TypeName: name}},
			{Name: "history", Type: ir.Array{Element: ir.Enum{Values: vals, TypeName: name}}},
			{Name: "checked", Type: ir.Domain{
				Name: "checked_status", BaseType: ir.Enum{Values: vals, TypeName: name},
			}},
		},
	}}}
}

func TestSchemaHash_ExcludesEnumTypeName(t *testing.T) {
	bare, err := ComputeSchemaHash(schemaWithEnumTypeName(""))
	if err != nil {
		t.Fatalf("ComputeSchemaHash (no type name): %v", err)
	}
	for _, name := range []string{"post_status", "posts_status_enum", "some_other_name"} {
		got, err := ComputeSchemaHash(schemaWithEnumTypeName(name))
		if err != nil {
			t.Fatalf("ComputeSchemaHash(%q): %v", name, err)
		}
		if got != bare {
			t.Errorf("enum type name %q moved the schema fingerprint (%s != %s).\n\n"+
				"The PG reader sets a type name for EVERY enum column, so a fingerprint that folds it in "+
				"repartitions close to every Postgres chain in existence and makes them unrestorable "+
				"across the boundary — the item-104 trap, at greater scale.", name, got, bare)
		}
	}
}

// TestSchemaHash_EnumTypeNameAbsentFromTheHashedBytes is the cross-version
// half: equality above proves the names hash alike, this proves the bytes are
// the ones a binary WITHOUT the field would have written.
func TestSchemaHash_EnumTypeNameAbsentFromTheHashedBytes(t *testing.T) {
	for _, name := range []string{"", "post_status"} {
		b, err := json.Marshal(canonicalSchemaForHash(schemaWithEnumTypeName(name)))
		if err != nil {
			t.Fatalf("marshal canonical (%q): %v", name, err)
		}
		if strings.Contains(string(b), "enum_type_name") || strings.Contains(string(b), name) && name != "" {
			t.Errorf("the bytes hashed for a schema with enum type name %q carry it, so they are NOT the "+
				"bytes an older binary produced for the same schema:\n%s", name, b)
		}
	}
}

// TestSchemaHash_EnumExclusionDoesNotMutateTheCaller: the exclusion must be a
// COPY. The manifest records the schema verbatim, so a fingerprint helper that
// cleared the type name in place would make restore synthesize a name —
// reintroducing the very defect this item closes, by way of the hash.
func TestSchemaHash_EnumExclusionDoesNotMutateTheCaller(t *testing.T) {
	s := schemaWithEnumTypeName("post_status")
	if _, err := ComputeSchemaHash(s); err != nil {
		t.Fatalf("ComputeSchemaHash: %v", err)
	}
	cols := s.Tables[0].Columns
	if got := cols[1].Type.(ir.Enum).TypeName; got != "post_status" {
		t.Errorf("hashing CLEARED the scalar column's enum type name (got %q)", got)
	}
	if got := cols[2].Type.(ir.Array).Element.(ir.Enum).TypeName; got != "post_status" {
		t.Errorf("hashing CLEARED the array element's enum type name (got %q)", got)
	}
	if got := cols[3].Type.(ir.Domain).BaseType.(ir.Enum).TypeName; got != "post_status" {
		t.Errorf("hashing CLEARED the domain base's enum type name (got %q)", got)
	}
}

// TestEnumTypeName_RidesTheWire is the other half of the split, and the one
// the defect actually was: excluding the name from the hash is only correct
// because a restore still receives it.
func TestEnumTypeName_RidesTheWire(t *testing.T) {
	src := schemaWithEnumTypeName("post_status").Tables[0]
	b, err := ir.MarshalTable(src)
	if err != nil {
		t.Fatalf("MarshalTable: %v", err)
	}
	if !strings.Contains(string(b), "post_status") {
		t.Fatalf("the encoded table does not carry the enum type name — a restore would synthesize "+
			"one and rename the type:\n%s", b)
	}
	back, err := ir.UnmarshalTable(b)
	if err != nil {
		t.Fatalf("UnmarshalTable: %v", err)
	}
	for i, get := range []func(*ir.Table) string{
		func(t *ir.Table) string { return t.Columns[1].Type.(ir.Enum).TypeName },
		func(t *ir.Table) string { return t.Columns[2].Type.(ir.Array).Element.(ir.Enum).TypeName },
		func(t *ir.Table) string { return t.Columns[3].Type.(ir.Domain).BaseType.(ir.Enum).TypeName },
	} {
		if got := get(back); got != "post_status" {
			t.Errorf("nesting site %d lost the enum type name on the round trip (got %q)", i, got)
		}
	}
}

// TestSchemaHash_StillSeesEnumValueChanges is the anti-vacuity floor. An
// exclusion that accidentally swallowed the whole Enum would make every
// assertion above pass while the fingerprint stopped noticing a real schema
// change — the shape where a gate certifies its own blind spot.
func TestSchemaHash_StillSeesEnumValueChanges(t *testing.T) {
	base, err := ComputeSchemaHash(schemaWithEnumTypeName("post_status"))
	if err != nil {
		t.Fatalf("ComputeSchemaHash: %v", err)
	}
	changed := schemaWithEnumTypeName("post_status")
	changed.Tables[0].Columns[1].Type = ir.Enum{
		Values: []string{"draft", "live", "archived"}, TypeName: "post_status",
	}
	got, err := ComputeSchemaHash(changed)
	if err != nil {
		t.Fatalf("ComputeSchemaHash (extra value): %v", err)
	}
	if got == base {
		t.Fatal("adding an enum VALUE did not move the schema fingerprint.\n\n" +
			"The exclusion is supposed to drop the type NAME only. If it drops the whole Enum, every " +
			"other assertion in this file passes while the fingerprint stops seeing real schema changes.")
	}
}
