// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package typematrix enumerates every IR type FAMILY crossed with the SHAPE
// VARIANTS that can change a DDL emitter's verdict on it — the one axis the
// column-type gates in every engine package need, held in one place.
//
// # Why it is not [ir.AllTypes], and not a copy per engine
//
// [ir.AllTypes] deliberately returns ZERO VALUES only, and says so: the
// variant-carrying fields (a decimal's scale, a bit string's varying-ness, an
// array's element, a domain's base) are the consumer's axis to enumerate. This
// is that axis, for the consumers that share it. Three engines answer "can I
// render this column type", their answers differ per shape and not only per
// family, and a per-engine copy of the shape list is precisely the Bug-74
// rot one level up — the pin matrix itself going stale on the axis that grew.
// One list, three consumers, and [Cases] is held to [ir.AllTypes] by
// TestCases_CoverEveryIRTypeFamily so a new IR type cannot land unshaped.
//
// # It is a fixture, and it is production code on purpose
//
// Same shape and same reason as internal/ir/alltypes.go: a `_test.go` file
// cannot be shared across packages, and the alternative — three copies that
// drift — is the defect this exists to prevent.
package typematrix

import (
	"fmt"
	"reflect"

	"sluicesync.dev/sluice/internal/ir"
)

// Case is one cell of the matrix: a human-readable label (used verbatim in
// failure messages, so it should say what makes the shape interesting) and the
// IR type to render.
type Case struct {
	Name string
	Type ir.Type
}

// Cases returns the family × shape matrix.
//
// Ordering is by family, following [ir.AllTypes], so a diff against that list
// reads straight down. Every entry is a shape a real schema reader can produce,
// with two deliberate exceptions kept because they are what a CORRUPT or
// hand-built IR looks like and every engine must refuse them rather than render
// something: a nil array element and a nil domain base.
func Cases() []Case {
	return []Case{
		// ---- Boolean ----
		{"boolean", ir.Boolean{}},

		// ---- Integer: width × unsigned × identity ----
		{"integer/tinyint", ir.Integer{Width: 8}},
		{"integer/smallint", ir.Integer{Width: 16}},
		{"integer/int", ir.Integer{Width: 32}},
		{"integer/bigint", ir.Integer{Width: 64}},
		{"integer/bigint unsigned", ir.Integer{Width: 64, Unsigned: true}},
		{"integer/bigint auto-increment", ir.Integer{Width: 64, AutoIncrement: true}},

		// ---- Decimal: the scale axis, including PG 15+ negative scale ----
		{"decimal/(10,2)", ir.Decimal{Precision: 10, Scale: 2}},
		{"decimal/unconstrained", ir.Decimal{Unconstrained: true}},
		{"decimal/negative scale", ir.Decimal{Precision: 10, Scale: -2}},

		// ---- Float ----
		{"float/single", ir.Float{Precision: ir.FloatSingle}},
		{"float/double", ir.Float{Precision: ir.FloatDouble}},

		// ---- Character: the zero-length marker column is the load-bearing one ----
		{"char/(10)", ir.Char{Length: 10}},
		{"char/(0) marker column", ir.Char{Length: 0}},
		{"varchar/(255)", ir.Varchar{Length: 255}},
		{"varchar/(0) marker column", ir.Varchar{Length: 0}},
		{"varchar/(20000) wider than MySQL can hold", ir.Varchar{Length: 20000}},
		{"varchar/(255) with a MySQL collation", ir.Varchar{Length: 255, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}},
		{"text/tiny", ir.Text{Size: ir.TextTiny}},
		{"text/regular", ir.Text{Size: ir.TextRegular}},
		{"text/medium", ir.Text{Size: ir.TextMedium}},
		{"text/long", ir.Text{Size: ir.TextLong}},

		// ---- Binary ----
		{"binary/(16)", ir.Binary{Length: 16}},
		{"varbinary/(255)", ir.Varbinary{Length: 255}},
		{"blob/tiny", ir.Blob{Size: ir.BlobTiny}},
		{"blob/regular", ir.Blob{Size: ir.BlobRegular}},
		{"blob/medium", ir.Blob{Size: ir.BlobMedium}},
		{"blob/long", ir.Blob{Size: ir.BlobLong}},

		// ---- Bit: fixed vs varying ----
		{"bit/(8) fixed", ir.Bit{Length: 8}},
		{"bit/(8) varying", ir.Bit{Length: 8, Varying: true}},

		// ---- Temporal: precision × time-zone awareness ----
		{"date", ir.Date{}},
		{"time", ir.Time{}},
		{"time/(6)", ir.Time{Precision: 6}},
		{"time/with time zone", ir.Time{WithTimeZone: true}},
		{"interval", ir.Interval{}},
		{"datetime", ir.DateTime{}},
		{"datetime/(6)", ir.DateTime{Precision: 6}},
		{"timestamp", ir.Timestamp{}},
		{"timestamp/(6)", ir.Timestamp{Precision: 6}},
		{"timestamp/with time zone", ir.Timestamp{WithTimeZone: true}},

		// ---- Structured ----
		{"json/text", ir.JSON{}},
		{"json/binary", ir.JSON{Binary: true}},

		// ---- Categorical ----
		{"enum/with values", ir.Enum{Values: []string{"a", "b"}}},
		{"enum/no values", ir.Enum{}},
		{"set/with values", ir.Set{Values: []string{"a", "b"}}},

		// ---- Identity / network ----
		{"uuid", ir.UUID{}},
		{"inet", ir.Inet{}},
		{"cidr", ir.Cidr{}},
		{"macaddr/unspecified width", ir.Macaddr{}},
		{"macaddr/EUI-48", ir.Macaddr{Width: ir.MacaddrEUI48}},
		{"macaddr/EUI-64", ir.Macaddr{Width: ir.MacaddrEUI64}},
		{"macaddr/impossible 7-byte width", ir.Macaddr{Width: 7}},

		// ---- Composite: the element is its own family axis ----
		{"array/of integer", ir.Array{Element: ir.Integer{Width: 32}}},
		{"array/of text", ir.Array{Element: ir.Text{}}},
		{"array/of decimal", ir.Array{Element: ir.Decimal{Precision: 10, Scale: 2}}},
		{"array/of timestamptz", ir.Array{Element: ir.Timestamp{WithTimeZone: true}}},
		{"array/of uuid", ir.Array{Element: ir.UUID{}}},
		{"array/of array", ir.Array{Element: ir.Array{Element: ir.Integer{Width: 32}}}},
		{"array/of enum", ir.Array{Element: ir.Enum{Values: []string{"a"}}}},
		{"array/of an unknown extension type", ir.Array{Element: ir.ExtensionType{Extension: "nosuchext", Name: "t"}}},
		{"array/nil element (corrupt IR)", ir.Array{}},

		// ---- Geometry ----
		{"geometry/point", ir.Geometry{Subtype: ir.GeometryPoint}},
		{"geometry/point with SRID", ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326}},
		{"geometry/geography", ir.Geometry{Subtype: ir.GeometryPoint, IsGeography: true}},

		// ---- Domain ----
		{"domain/over integer", ir.Domain{Name: "d", BaseType: ir.Integer{Width: 32}}},
		{"domain/over blob", ir.Domain{Name: "d", BaseType: ir.Blob{Size: ir.BlobRegular}}},
		{"domain/nil base (corrupt IR)", ir.Domain{Name: "d"}},

		// ---- Extension passthrough ----
		{"extension/hstore", ir.ExtensionType{Extension: "hstore", Name: "hstore"}},
		{"extension/citext", ir.ExtensionType{Extension: "citext", Name: "citext"}},
		{"extension/vector", ir.ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{384}}},
		{"extension/unknown to every catalog", ir.ExtensionType{Extension: "nosuchext", Name: "nosuchtype"}},

		// ---- Verbatim (ADR-0047) ----
		{"verbatim/citext spelling", ir.VerbatimType{Definition: "public.citext"}},
		{"verbatim/empty definition (corrupt IR)", ir.VerbatimType{}},
	}
}

// Families returns the set of concrete Go types [Cases] covers, as
// reflect.Type values — the input to the completeness gate that holds this
// matrix to [ir.AllTypes].
func Families() map[reflect.Type]bool {
	out := map[reflect.Type]bool{}
	for _, c := range Cases() {
		out[reflect.TypeOf(c.Type)] = true
	}
	return out
}

// MissingFamilies returns the [ir.AllTypes] entries no [Cases] entry has a
// shape for, formatted for a failure message. Empty means the matrix is
// complete.
func MissingFamilies() []string {
	covered := Families()
	var missing []string
	for _, t := range ir.AllTypes() {
		if !covered[reflect.TypeOf(t)] {
			missing = append(missing, fmt.Sprintf("%T", t))
		}
	}
	return missing
}
