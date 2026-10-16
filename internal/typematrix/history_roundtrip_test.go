// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package typematrix

import (
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestSchemaHistoryRoundTrip_EveryTypeFamily holds the schema-history codec
// to the two properties [ir.SameColumnType] rests on, for every family ×
// shape the readers emit ([Cases]) plus the empty-slice spellings a reader
// can build (TYPE-WIRE-EMPTY-SLICE-SIGNATURE):
//
//   - a version read back from history has the SAME signature as the table
//     it was written from — the comparison a warm resume's
//     PrimeSchemaHistoryCache makes against the stream's next boundary, which
//     before the fix called a domain without a CHECK a schema change on every
//     restart;
//   - the codec is LOSSLESS for every non-empty field of these cases. It is
//     NOT lossless in general — it drops [ir.Enum.Collation] and rewrites
//     invalid UTF-8 — which is why [ir.SameColumnType] normalizes empty lists
//     explicitly instead of comparing round trips (the first cut did, and
//     merged those real differences; found by the re-review).
//
// And the control: a real change still differs, including the ones the codec
// cannot tell apart.
func TestSchemaHistoryRoundTrip_EveryTypeFamily(t *testing.T) {
	cases := Cases()
	cases = append(
		cases,
		Case{"domain/no checks, empty slice (the postgres schema reader's spelling)", ir.Domain{Name: "d", BaseType: ir.Varchar{Length: 30}, Checks: []ir.DomainCheck{}}},
		Case{"domain/with a check", ir.Domain{Name: "d", BaseType: ir.Varchar{Length: 30}, Checks: []ir.DomainCheck{{Name: "c", Body: "VALUE <> ''"}}}},
		Case{"enum/empty values slice", ir.Enum{Values: []string{}}},
		Case{"set/empty values slice", ir.Set{Values: []string{}}},
		Case{"extension/empty modifiers slice", ir.ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{}}},
		Case{"array/of a domain without checks", ir.Array{Element: ir.Domain{Name: "d", BaseType: ir.Text{}, Checks: []ir.DomainCheck{}}}},
	)
	if len(cases) < 40 {
		t.Fatalf("only %d cases; the matrix walk is vacuous", len(cases))
	}
	for _, c := range cases {
		corrupt := strings.Contains(c.Name, "corrupt")
		tbl := &ir.Table{Schema: "public", Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}, {Name: "c", Type: c.Type}}}
		b, err := ir.MarshalTable(tbl)
		if err != nil {
			if !corrupt {
				t.Errorf("%s: marshal: %v", c.Name, err)
			}
			continue
		}
		rt, err := ir.UnmarshalTable(b)
		if err != nil || rt == nil {
			if !corrupt {
				t.Errorf("%s: unmarshal: %v", c.Name, err)
			}
			continue
		}
		if !ir.SchemaSignatureOf(tbl).Equal(ir.SchemaSignatureOf(rt)) {
			t.Errorf("%s: the version read back from history has a different signature (%#v, written %#v): a warm resume would call it a schema change", c.Name, rt.Columns[1].Type, c.Type)
		}
		if !hasEmptySlice(c.Type) && !corrupt && !reflect.DeepEqual(rt.Columns[1].Type, c.Type) {
			t.Errorf("%s: the codec is lossy: wrote %#v, read %#v — SameColumnType's wire comparison would merge types that differ in that field", c.Name, c.Type, rt.Columns[1].Type)
		}
	}

	// The control: a real change still differs on both sides of the codec.
	for _, pair := range [][2]ir.Type{
		{ir.Varchar{Length: 10}, ir.Varchar{Length: 20}},
		{ir.Domain{Name: "d", BaseType: ir.Text{}, Checks: []ir.DomainCheck{}}, ir.Domain{Name: "d", BaseType: ir.Text{}, Checks: []ir.DomainCheck{{Name: "c", Body: "true"}}}},
		{ir.Enum{Values: []string{}}, ir.Enum{Values: []string{"a"}}},
		{ir.Varchar{Length: 10, Collation: "C", Determinism: ir.CollationDeterministic}, ir.Varchar{Length: 10, Collation: "C"}},
		// The codec drops Enum.Collation: a collation-only MySQL ENUM change
		// must still surface (audit 2026-07-19 E1/E3), bare and inside an array.
		{ir.Enum{Values: []string{"a", "b"}, Collation: "utf8mb4_0900_ai_ci"}, ir.Enum{Values: []string{"a", "b"}, Collation: "utf8mb4_bin"}},
		{ir.Array{Element: ir.Enum{Values: []string{"a"}, Collation: "utf8mb4_0900_ai_ci"}}, ir.Array{Element: ir.Enum{Values: []string{"a"}, Collation: "utf8mb4_bin"}}},
		// JSON rewrites invalid UTF-8 to U+FFFD: labels and CHECK bodies that
		// differ only in invalid bytes (a SQL_ASCII Postgres) must differ.
		{ir.Enum{Values: []string{"a\xff"}}, ir.Enum{Values: []string{"a\xfe"}}},
		{ir.Domain{Name: "d", BaseType: ir.Text{}, Checks: []ir.DomainCheck{{Body: "VALUE <> 'a\xff'"}}}, ir.Domain{Name: "d", BaseType: ir.Text{}, Checks: []ir.DomainCheck{{Body: "VALUE <> 'a\xfe'"}}}},
		// The decision, stated: a version read back from history (no collation)
		// still differs from the live MySQL boundary that carries one.
		{ir.Enum{Values: []string{"a"}}, ir.Enum{Values: []string{"a"}, Collation: "utf8mb4_bin"}},
	} {
		if ir.SameColumnType(pair[0], pair[1]) {
			t.Errorf("SameColumnType(%#v, %#v) = true; a real change must differ", pair[0], pair[1])
		}
	}
}

// hasEmptySlice reports whether t (or a type inside it) spells an empty
// list as an empty non-nil slice, the one difference the codec erases.
func hasEmptySlice(t ir.Type) bool {
	switch v := t.(type) {
	case ir.Domain:
		return (v.Checks != nil && len(v.Checks) == 0) || hasEmptySlice(v.BaseType)
	case ir.Enum:
		return v.Values != nil && len(v.Values) == 0
	case ir.Set:
		return v.Values != nil && len(v.Values) == 0
	case ir.ExtensionType:
		return v.Modifiers != nil && len(v.Modifiers) == 0
	case ir.Array:
		return hasEmptySlice(v.Element)
	}
	return false
}

// TestSameColumnType_EveryListFieldIsNormalized derives its universe from
// [ir.AllTypes]: for every IR type with a slice field, a value whose slices
// are all empty must compare equal to the same value with them nil, and a
// value with one element in a slice must still differ. A new type (or a new
// slice field) that [ir.SameColumnType]'s normalizer does not handle fails
// here by name. Floor: the four families known to carry a list today.
func TestSameColumnType_EveryListFieldIsNormalized(t *testing.T) {
	withLists := 0
	for _, zero := range ir.AllTypes() {
		rt := reflect.TypeOf(zero)
		if rt.Kind() != reflect.Struct {
			continue
		}
		empty := reflect.New(rt).Elem()
		one := reflect.New(rt).Elem()
		has := false
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			if f.Type.Kind() != reflect.Slice || !f.IsExported() {
				continue
			}
			has = true
			empty.Field(i).Set(reflect.MakeSlice(f.Type, 0, 0))
			one.Field(i).Set(reflect.MakeSlice(f.Type, 1, 1))
		}
		if !has {
			continue
		}
		withLists++
		e, n, o := empty.Interface().(ir.Type), zero, one.Interface().(ir.Type)
		if !ir.SameColumnType(e, n) {
			t.Errorf("%s: empty lists and nil lists compare as different types; add the type to ir.normalizeEmptyLists", rt.Name())
		}
		if !ir.SameColumnType(ir.Array{Element: e}, ir.Array{Element: n}) {
			t.Errorf("%s: inside an array, empty and nil lists compare as different types", rt.Name())
		}
		if ir.SameColumnType(o, n) {
			t.Errorf("%s: a one-element list compares equal to a nil one; the normalizer is merging a real difference", rt.Name())
		}
	}
	if withLists < 4 {
		t.Fatalf("found %d IR types with a list field; floor 4 (Domain, Enum, Set, ExtensionType) — the walk is vacuous", withLists)
	}
}
