// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"fmt"
	"strings"
)

// =====================================================================
// Extension IR types — engines opt in via Capabilities.SupportedTypes.
// =====================================================================

// ExtensionKind enumerates the extension types in the IR. It is used by
// [Capabilities.SupportedTypes] so engines can declare which extension
// types they natively support.
type ExtensionKind uint8

// Recognised ExtensionKind values, one per extension Type defined below.
const (
	ExtEnum ExtensionKind = iota
	ExtSet
	ExtUUID
	ExtArray
	ExtGeometry
	ExtInet
	ExtCidr
	ExtMacaddr
	// ExtExtensionType represents column types defined by a PG extension
	// the operator has explicitly opted into via `--enable-pg-extension`
	// (ADR-0032). The IR carries the extension+type names plus opaque
	// modifiers; engine-specific binary representation lives in the
	// catalog entry on the engine side.
	ExtExtensionType
)

func (k ExtensionKind) String() string {
	switch k {
	case ExtEnum:
		return "Enum"
	case ExtSet:
		return "Set"
	case ExtUUID:
		return "UUID"
	case ExtArray:
		return "Array"
	case ExtGeometry:
		return "Geometry"
	case ExtInet:
		return "Inet"
	case ExtCidr:
		return "Cidr"
	case ExtMacaddr:
		return "Macaddr"
	case ExtExtensionType:
		return "ExtensionType"
	default:
		return "unknown"
	}
}

// GeometrySubtype identifies the spatial-data subtype of a [Geometry] column.
type GeometrySubtype uint8

// Recognised GeometrySubtype values.
const (
	GeometryUnspecified GeometrySubtype = iota
	GeometryPoint
	GeometryLineString
	GeometryPolygon
	GeometryMultiPoint
	GeometryMultiLineString
	GeometryMultiPolygon
	GeometryCollection
)

func (s GeometrySubtype) String() string {
	switch s {
	case GeometryPoint:
		return "Point"
	case GeometryLineString:
		return "LineString"
	case GeometryPolygon:
		return "Polygon"
	case GeometryMultiPoint:
		return "MultiPoint"
	case GeometryMultiLineString:
		return "MultiLineString"
	case GeometryMultiPolygon:
		return "MultiPolygon"
	case GeometryCollection:
		return "GeometryCollection"
	case GeometryUnspecified:
		return "Geometry"
	default:
		return "unknown"
	}
}

// Enum is a categorical value drawn from a fixed set of named alternatives.
// MySQL has column-level ENUM; Postgres has CREATE TYPE ... AS ENUM types.
type Enum struct {
	Values []string
}

func (Enum) isType()    {}
func (Enum) Tier() Tier { return TierExtension }
func (e Enum) String() string {
	return fmt.Sprintf("Enum{%s}", strings.Join(e.Values, ","))
}

// Set is a value composed of zero or more elements drawn from a fixed
// allowed set. MySQL has SET natively; other engines require degradation
// (commonly to a text array or a text column with a CHECK constraint).
type Set struct {
	Values []string
}

func (Set) isType()    {}
func (Set) Tier() Tier { return TierExtension }
func (s Set) String() string {
	return fmt.Sprintf("Set{%s}", strings.Join(s.Values, ","))
}

// UUID represents a 128-bit universally-unique identifier. Postgres has
// a native uuid type; MySQL stores them in CHAR(36) or BINARY(16).
type UUID struct{}

func (UUID) isType()        {}
func (UUID) Tier() Tier     { return TierExtension }
func (UUID) String() string { return "UUID" }

// Array represents a variable-length ordered collection of values, all
// of the same element type. Native to Postgres; degraded to JSON on
// engines that lack array support.
type Array struct {
	Element Type
}

func (Array) isType()    {}
func (Array) Tier() Tier { return TierExtension }
func (a Array) String() string {
	if a.Element == nil {
		return "Array<nil>"
	}
	return fmt.Sprintf("Array<%s>", a.Element.String())
}

// Geometry represents a spatial data value. PostGIS provides this on
// Postgres; MySQL has built-in spatial types.
//
// SRID is the column's spatial reference system identifier. 0 means
// "unknown CRS" — the same default both MySQL (no SRID argument)
// and PostGIS (SRID=0) use when no coordinate system is declared.
// Schema readers may populate it from the source's metadata; the
// translate-layer mappings registry sets it from
// `target_type_options.srid` for postgis_* aliases.
type Geometry struct {
	Subtype GeometrySubtype
	SRID    int
	// IsGeography flags this column as PostGIS `geography` rather than
	// `geometry`. PG's two spatial types share a WKB/EWKB wire shape
	// and subtype enumeration, so they ride on the same IR variant;
	// the boolean lets the PG writer emit `geography(<subtype>, <srid>)`
	// vs `geometry(<subtype>, <srid>)` and lets the spatial-index
	// opclass set route correctly. Engines without a distinct
	// geography type (MySQL) ignore the flag — geography flattens to
	// geometry on those targets, losing the spherical-operator
	// semantics but preserving values.
	IsGeography bool
	// HasZ / HasM carry the PostGIS dimensional modifiers orthogonal
	// to [Subtype]. PostGIS extends the seven 2D base shapes with Z
	// (3D), M (measure), and ZM (4D) variants — e.g.
	// `geometry(POINTZ, 4326)` for elevation-aware coordinates. The
	// IR carries the dimensional flags as two booleans rather than
	// expanding [GeometrySubtype] into 28 entries; the writer
	// reconstructs the suffix on emit (postgisSubtypeName). Engines
	// that carry Z / M in the value bytes rather than the column type
	// modifier (MySQL) ignore the flags — the WKB / EWKB framing
	// preserves the dimensional coordinates regardless.
	HasZ bool
	HasM bool
}

func (Geometry) isType()    {}
func (Geometry) Tier() Tier { return TierExtension }

func (g Geometry) String() string {
	name := "Geometry"
	if g.IsGeography {
		name = "Geography"
	}
	subtype := g.Subtype.String()
	switch {
	case g.HasZ && g.HasM:
		subtype += "ZM"
	case g.HasZ:
		subtype += "Z"
	case g.HasM:
		subtype += "M"
	}
	if g.SRID == 0 {
		return fmt.Sprintf("%s[%s]", name, subtype)
	}
	return fmt.Sprintf("%s[%s,SRID=%d]", name, subtype, g.SRID)
}

// Inet represents an IPv4 or IPv6 host address (Postgres inet).
type Inet struct{}

func (Inet) isType()        {}
func (Inet) Tier() Tier     { return TierExtension }
func (Inet) String() string { return "Inet" }

// Cidr represents an IPv4 or IPv6 network specification (Postgres cidr).
type Cidr struct{}

func (Cidr) isType()        {}
func (Cidr) Tier() Tier     { return TierExtension }
func (Cidr) String() string { return "Cidr" }

// Macaddr represents a hardware (MAC) address (Postgres macaddr).
type Macaddr struct{}

func (Macaddr) isType()        {}
func (Macaddr) Tier() Tier     { return TierExtension }
func (Macaddr) String() string { return "Macaddr" }

// ExtensionType represents a column type defined by a PG extension
// (ADR-0032). The IR is engine-neutral by name (Extension + Name); the
// binary representation is opaque to the IR — the schema reader
// captures the type's modifiers and the matching engine writer emits
// the column verbatim when the same extension is enabled on the
// target.
//
// PG's extensibility introduces type variety MySQL fundamentally
// lacks; this variant is the IR's representation of "I see a column
// whose type is owned by an extension; preserve fidelity for
// same-engine targets that have the same extension installed;
// cross-engine targets get loud-failure unless an explicit operator
// translation is supplied."
//
// Modifiers carries optional type-modifier values. For pgvector this
// is the dimension count (vector(384) → Modifiers=[]int{384}). For
// PostGIS (future) this is [subtypeCode, srid].
//
// Extension is the canonical extension name registered with PG (e.g.
// "vector", "postgis", "hstore"). Name is the canonical type name
// within the extension (e.g. "vector", "geometry"). The pair is what
// the engine catalog uses to look up the column-DDL renderer.
type ExtensionType struct {
	Extension string
	Name      string
	Modifiers []int
}

func (ExtensionType) isType()    {}
func (ExtensionType) Tier() Tier { return TierExtension }

func (e ExtensionType) String() string {
	if len(e.Modifiers) == 0 {
		return fmt.Sprintf("ExtensionType[%s.%s]", e.Extension, e.Name)
	}
	mods := make([]string, len(e.Modifiers))
	for i, m := range e.Modifiers {
		mods[i] = fmt.Sprintf("%d", m)
	}
	return fmt.Sprintf("ExtensionType[%s.%s(%s)]",
		e.Extension, e.Name, strings.Join(mods, ","))
}

// KindOf reports the [ExtensionKind] of an extension Type, or returns
// false if t is a core type.
func KindOf(t Type) (ExtensionKind, bool) {
	switch t.(type) {
	case Enum:
		return ExtEnum, true
	case Set:
		return ExtSet, true
	case UUID:
		return ExtUUID, true
	case Array:
		return ExtArray, true
	case Geometry:
		return ExtGeometry, true
	case Inet:
		return ExtInet, true
	case Cidr:
		return ExtCidr, true
	case Macaddr:
		return ExtMacaddr, true
	case ExtensionType:
		return ExtExtensionType, true
	default:
		return 0, false
	}
}
