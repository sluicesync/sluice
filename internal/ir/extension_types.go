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

const (
	ExtEnum ExtensionKind = iota
	ExtSet
	ExtUUID
	ExtArray
	ExtGeometry
	ExtInet
	ExtCidr
	ExtMacaddr
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
	default:
		return "unknown"
	}
}

// GeometrySubtype identifies the spatial-data subtype of a [Geometry] column.
type GeometrySubtype uint8

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
type Geometry struct {
	Subtype GeometrySubtype
}

func (Geometry) isType()        {}
func (Geometry) Tier() Tier     { return TierExtension }
func (g Geometry) String() string { return fmt.Sprintf("Geometry[%s]", g.Subtype) }

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
	default:
		return 0, false
	}
}
