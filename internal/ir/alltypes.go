// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// AllTypes returns one zero-value instance of every concrete [Type] in
// the IR — the enumeration source for exhaustiveness gates over the
// type universe (audit 2026-07-23 TEST-2 / G-12). A classifier or codec
// that dispatches on the type family can assert its case list covers
// every entry here, so ADDING a type without classifying it in each
// such switch fails a test instead of silently falling into a default
// arm nobody re-derived (the Bug-74 class one level up: the pin matrix
// itself rotting when the family axis grows).
//
// The list is kept exhaustive by TestAllTypes_CoversEveryIsTypeImplementor,
// which parses this package's source for `isType()` receivers — adding a
// Type without extending this list (or vice versa) fails there, so the
// registry cannot drift from the sealed interface it enumerates.
//
// Zero values only: variant-carrying fields (collation, tz-awareness,
// element types) are the CONSUMER's axis to enumerate — e.g. the
// push-down envelope pin adds `Timestamp{WithTimeZone: true}` and the
// collation variants on top of this base list.
func AllTypes() []Type {
	return []Type{
		// Core types (types.go).
		Boolean{},
		Integer{},
		Decimal{},
		Float{},
		Char{},
		Varchar{},
		Text{},
		Binary{},
		Varbinary{},
		Blob{},
		Bit{},
		Date{},
		Time{},
		Interval{},
		DateTime{},
		Timestamp{},
		JSON{},

		// Extension / engine-specific types (extension_types.go).
		Enum{},
		Domain{},
		Set{},
		UUID{},
		Array{},
		Geometry{},
		Inet{},
		Cidr{},
		Macaddr{},
		ExtensionType{},
		VerbatimType{},
	}
}
