// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"strings"
	"testing"
)

// TestTypesImplementType is a compile-time-and-runtime guarantee that
// every IR type satisfies the sealed Type interface and reports the
// expected Tier. Adding a new type should require adding a row here.
func TestTypesImplementType(t *testing.T) {
	cases := []struct {
		name string
		typ  Type
		tier Tier
	}{
		// Core types.
		{"Boolean", Boolean{}, TierCore},
		{"Integer", Integer{Width: 32}, TierCore},
		{"Decimal", Decimal{Precision: 10, Scale: 2}, TierCore},
		{"Float", Float{Precision: FloatDouble}, TierCore},
		{"Char", Char{Length: 10}, TierCore},
		{"Varchar", Varchar{Length: 255}, TierCore},
		{"Text", Text{Size: TextLong}, TierCore},
		{"Binary", Binary{Length: 16}, TierCore},
		{"Varbinary", Varbinary{Length: 64}, TierCore},
		{"Blob", Blob{Size: BlobLong}, TierCore},
		{"Date", Date{}, TierCore},
		{"Time", Time{Precision: 6}, TierCore},
		{"DateTime", DateTime{Precision: 6}, TierCore},
		{"Timestamp", Timestamp{Precision: 6, WithTimeZone: true}, TierCore},
		{"JSON", JSON{Binary: true}, TierCore},
		// Extension types.
		{"Enum", Enum{Values: []string{"a", "b"}}, TierExtension},
		{"Set", Set{Values: []string{"x", "y"}}, TierExtension},
		{"UUID", UUID{}, TierExtension},
		{"Array", Array{Element: Integer{Width: 32}}, TierExtension},
		{"Geometry", Geometry{Subtype: GeometryPoint}, TierExtension},
		{"Inet", Inet{}, TierExtension},
		{"Cidr", Cidr{}, TierExtension},
		{"Macaddr", Macaddr{}, TierExtension},
		// ADR-0032 PG extension passthrough variant.
		{"ExtensionType", ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{384}}, TierExtension},
		// ADR-0047 verbatim (uncatalogued) PG extension type.
		{"VerbatimType", VerbatimType{Definition: "ltree"}, TierExtension},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := c.typ.Tier(); got != c.tier {
				t.Errorf("Tier() = %v; want %v", got, c.tier)
			}
			if s := c.typ.String(); s == "" {
				t.Error("String() returned empty")
			}
		})
	}
}

// TestKindOf checks that KindOf identifies extension types and rejects
// core ones.
func TestKindOf(t *testing.T) {
	if _, ok := KindOf(Integer{Width: 32}); ok {
		t.Error("KindOf(Integer) reported as extension")
	}
	cases := []struct {
		name string
		typ  Type
		want ExtensionKind
	}{
		{"Enum", Enum{}, ExtEnum},
		{"Set", Set{}, ExtSet},
		{"UUID", UUID{}, ExtUUID},
		{"Array", Array{Element: Integer{}}, ExtArray},
		{"Geometry", Geometry{}, ExtGeometry},
		{"Inet", Inet{}, ExtInet},
		{"Cidr", Cidr{}, ExtCidr},
		{"Macaddr", Macaddr{}, ExtMacaddr},
		{"ExtensionType", ExtensionType{Extension: "vector", Name: "vector"}, ExtExtensionType},
		{"VerbatimType", VerbatimType{Definition: "ltree"}, ExtVerbatimType},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, ok := KindOf(c.typ)
			if !ok {
				t.Fatalf("KindOf(%s) reported as core; want extension", c.name)
			}
			if got != c.want {
				t.Errorf("KindOf(%s) = %v; want %v", c.name, got, c.want)
			}
		})
	}
}

// TestExtensionKindEnumIsAppendOnly pins the wire-stable
// ExtensionKind values. These are part of the backup tagged-union
// enum discipline (ADR-0047): never reorder/renumber existing kinds —
// only append. A failure here means an existing kind's value moved,
// which would silently mis-decode older backups.
func TestExtensionKindEnumIsAppendOnly(t *testing.T) {
	want := map[ExtensionKind]string{
		0: "Enum",
		1: "Set",
		2: "UUID",
		3: "Array",
		4: "Geometry",
		5: "Inet",
		6: "Cidr",
		7: "Macaddr",
		8: "ExtensionType",
		9: "VerbatimType", // ADR-0047 — appended; must stay last.
	}
	for k, name := range want {
		if got := k.String(); got != name {
			t.Errorf("ExtensionKind(%d).String() = %q; want %q "+
				"(append-only discipline violated — never renumber)", k, got, name)
		}
	}
	if ExtVerbatimType != 9 {
		t.Errorf("ExtVerbatimType = %d; want 9 (must be the last appended kind)", ExtVerbatimType)
	}
}

// TestTypeSet exercises the bitset operations on TypeSet.
func TestTypeSet(t *testing.T) {
	s := NewTypeSet(ExtEnum, ExtArray)
	if !s.Has(ExtEnum) {
		t.Error("Has(ExtEnum) = false; want true")
	}
	if !s.Has(ExtArray) {
		t.Error("Has(ExtArray) = false; want true")
	}
	if s.Has(ExtUUID) {
		t.Error("Has(ExtUUID) = true; want false")
	}
	s2 := s.With(ExtUUID)
	if !s2.Has(ExtUUID) {
		t.Error("after With(ExtUUID): Has() = false")
	}
	if s.Has(ExtUUID) {
		t.Error("With() mutated original (it should return a copy)")
	}
	s3 := s2.Without(ExtEnum)
	if s3.Has(ExtEnum) {
		t.Error("after Without(ExtEnum): Has(ExtEnum) = true")
	}
}

// TestStringSamples spot-checks a few String() outputs to guard against
// accidental format regressions in golden-file consumers.
func TestStringSamples(t *testing.T) {
	cases := []struct {
		typ      Type
		contains string
	}{
		{Integer{Width: 64, Unsigned: true}, "UInt64"},
		{Integer{Width: 32, AutoIncrement: true}, "AutoIncrement"},
		{Decimal{Precision: 18, Scale: 4}, "Decimal(18,4)"},
		{Timestamp{Precision: 6, WithTimeZone: true}, "TimestampTZ(6)"},
		{Timestamp{Precision: 0}, "Timestamp(0)"},
		{Array{Element: Integer{Width: 32}}, "Array<Int32>"},
		{Enum{Values: []string{"red", "green"}}, "red,green"},
		{ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{384}}, "vector.vector(384)"},
		{ExtensionType{Extension: "hstore", Name: "hstore"}, "hstore.hstore"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.typ.String(), func(t *testing.T) {
			if !strings.Contains(c.typ.String(), c.contains) {
				t.Errorf("%q does not contain %q", c.typ.String(), c.contains)
			}
		})
	}
}
