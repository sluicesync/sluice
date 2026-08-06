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
	// ExtVerbatimType represents an uncatalogued PG extension column
	// type carried verbatim for same-engine PG → PG / PG-backup paths
	// (ADR-0047). Distinct from [ExtExtensionType]: it has no catalog
	// build/emit dispatch — the writer emits its captured string
	// literally. Append-only; never reorder/renumber the kinds above
	// (the values are part of the backup tagged-union enum discipline).
	ExtVerbatimType
	// ExtDomain represents a Postgres `CREATE DOMAIN` user-defined type
	// (`pg_type.typtype = 'd'`). A domain wraps an arbitrary base type
	// and attaches operator-supplied CHECK constraints that the source
	// enforces on every value. Pre-v0.95.1 the schema reader resolved a
	// domain-typed column to its underlying base type and the CHECK was
	// silently lost on PG→PG migrate — Bug 113 (CRITICAL silent-
	// constraint-loss). v0.95.1 carries the domain name + base type +
	// captured CHECK definitions via [Domain] so the same-engine writer
	// emits `CREATE DOMAIN ... AS ... CHECK (...)` before any table
	// that references it. Cross-engine PG → MySQL emits a WARN and
	// downgrades to the base type with the CHECK inlined at the table
	// level (MySQL 8.0.16+).
	ExtDomain
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
	case ExtVerbatimType:
		return "VerbatimType"
	case ExtDomain:
		return "Domain"
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

	// TypeName is the source-side named-type identifier for engines
	// that model enums as standalone types (Postgres `CREATE TYPE ...
	// AS ENUM`). Empty for engines with column-inline enums (MySQL,
	// which has no enum type name) — writers that need a type name then
	// synthesize a deterministic one from the table+column. Carrying it
	// lets a same-engine PG → PG migration round-trip the *type name*
	// verbatim instead of renaming `post_status` to
	// `posts_status_enum` (which would break casts / shared-enum tables
	// / app code referencing the type by name).
	TypeName string

	// Collation is the MySQL-family column collation whose `=` a filtered
	// `sync --where status='active'` must reproduce client-side: a MySQL ENUM
	// compares a value against a string literal under the column's collation,
	// so a case/accent-insensitive one (`utf8mb4_0900_ai_ci`) matches `Active`
	// against `active` — a byte-exact client compare would mis-classify the
	// row-move (audit 2026-07-19 M1-5). Populated by the MySQL schema reader;
	// empty for Postgres (a PG enum compares by exact label, not a collation)
	// and after a wire round-trip (like TypeName, not carried on the wire — the
	// `--where` predicate is compiled from a fresh schema read, so it always has
	// the live collation). Consumed only by the row-predicate resolver; DDL
	// emission is unaffected (the enum inherits its column/table charset).
	//
	// The field DOES participate in structural equality (reflect.DeepEqual in
	// diffRenameColumnIR, SchemaSignature.Equal), so a collation-ONLY ENUM change
	// is surfaced as a schema delta rather than a no-op (audit 2026-07-19 E1/E3).
	// That is intentional and monotonic-safe: a collation change alters `=`
	// semantics (the whole reason this field exists), so surfacing it is correct,
	// and a field-add can only ADD advisory deltas (a spurious rename→drop+add
	// classification or an extra schema-history version), never HIDE a
	// decode-affecting change. No data is mutated on either path.
	Collation string
}

func (Enum) isType()    {}
func (Enum) Tier() Tier { return TierExtension }
func (e Enum) String() string {
	return fmt.Sprintf("Enum{%s}", strings.Join(e.Values, ","))
}

// Domain is a Postgres `CREATE DOMAIN` user-defined type — `pg_type.typtype
// = 'd'`. It wraps an arbitrary base type with operator-supplied CHECK
// constraints the source enforces on every value. Pre-v0.95.1 the
// schema reader resolved a domain-typed column to its underlying base
// type and the CHECK was silently lost on PG→PG migrate — Bug 113
// (CRITICAL silent-constraint-loss class: the operator's input-
// validation invariant disappeared with no WARN, no error, exit 0).
// v0.95.1 carries the domain name + base type + captured CHECK
// definitions so the same-engine PG writer can emit `CREATE DOMAIN
// ... AS ... CHECK (...)` before any table that references it. The
// IR type is itself a [Type], so a column whose declared type is a
// domain stores `Domain{...}` directly in [Column.Type] (the same
// shape [Enum] uses for `CREATE TYPE … AS ENUM` types — both are
// user-defined types that must be created before referencing tables).
//
// Cross-engine PG → MySQL: domains have no MySQL counterpart. The
// MySQL writer downgrades the column to the domain's base type and
// emits a WARN naming the lost CHECK constraints; operators relying
// on the DB-level invariant must either replicate it as a table-
// level CHECK on MySQL 8.0.16+ or move enforcement to the app layer
// (the WARN names both options).
type Domain struct {
	// Name is the operator-declared domain identifier (the source's
	// `pg_type.typname`). Required for same-engine round-trip; cross-
	// engine writers ignore it.
	Name string

	// BaseType is the [Type] the domain wraps (e.g. `Text{}` for the
	// canonical `CREATE DOMAIN email_address AS text CHECK (...)`).
	// Always populated — a domain without a base type is malformed.
	BaseType Type

	// Checks are the CHECK constraint definitions attached to the
	// domain, in source order. Each entry is the raw constraint body
	// suitable for re-emission (`VALUE ~ '...'`, `VALUE BETWEEN 0 AND
	// 100`, etc.). The same-engine writer emits each as `CHECK
	// (<body>)` after the base type in the `CREATE DOMAIN` statement;
	// cross-engine writers inline each as a table-level CHECK on the
	// degraded base column with a WARN. Empty for a domain that
	// happens to carry no checks (rare — most operator-declared
	// domains have at least one CHECK; without one the domain is
	// effectively a type alias).
	Checks []DomainCheck
}

// DomainCheck is one CHECK constraint attached to an [Domain]. The
// body is the raw source-dialect expression (PG dialect; cross-engine
// targets run it through the ADR-0016 translator before inlining at
// the table level). The Name carries the constraint's identifier if
// the source declared one — operator-readable on DDL diffs and
// preserved on same-engine round-trip; cross-engine writers may drop
// it since MySQL's table-level CHECK names live in a different
// namespace.
type DomainCheck struct {
	// Name is the CHECK constraint identifier (`pg_constraint.conname`).
	// Empty for anonymous CHECKs.
	Name string

	// Body is the raw constraint expression as parsed by the source,
	// with no surrounding `CHECK (...)` wrapper. E.g.
	// `VALUE ~ '^[^@]+@[^@]+\\.[^@]+$'`.
	Body string
}

func (Domain) isType()    {}
func (Domain) Tier() Tier { return TierExtension }
func (d Domain) String() string {
	base := "<nil>"
	if d.BaseType != nil {
		base = d.BaseType.String()
	}
	return fmt.Sprintf("Domain{%s AS %s, %d check(s)}", d.Name, base, len(d.Checks))
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

// InetFamily names the address family a network column CONSTRAINS its stored
// values to (roadmap item 133). It is a property of the COLUMN, not of a
// value: a column that constrains the family COERCES every value into it, and
// the coerced value is what the change stream delivers.
//
// It exists because the IR collapses two distinct engine types onto one IR
// type. MariaDB has separate INET4 and INET6 columns and both read to
// [Inet]; an INET6 column stores `10.0.0.1` as the IPv4-MAPPED
// `::ffff:10.0.0.1` and delivers it that way, while accepting (and matching)
// the bare literal server-side. Without a discriminant, a `--where
// ip = '10.0.0.1'` therefore matched during the server-evaluated cold copy
// and never again in the client-side CDC evaluator — the filtered sync froze,
// caught up, at exit 0 (live-verified on mariadb:11.4).
//
// ZERO MEANS UNSPECIFIED and consumers must FAIL CLOSED on it, exactly as
// [Macaddr.Width] does: the two Postgres readers and MariaDB's native-type
// registry are the only producers that can reach a `--where` resolver and all
// three decide, so an unspecified family on a live schema read means a NEW
// producer appeared without deciding — and the failure mode of guessing is
// silent. `parquetexport/duckdbverify/matrix.go` builds a bare `Inet{}` for
// its fixture corpus; that one never reaches a predicate resolver, which is
// the property the refusal actually rests on.
//
// The family rides the backup wire (schema_wire.go) so a manifest records
// which MariaDB type the column was, but it is DELIBERATELY EXCLUDED from the
// schema fingerprint — see the exclusion block in internal/ir/backup/chain.go,
// and roadmap item 104 for why a field that is non-zero on ordinary source
// data must never reach that hash.
type InetFamily uint8

const (
	// InetFamilyUnspecified is the zero value: the producer has not named
	// what the column constrains, so no literal spelling can be known
	// correct. Value comparisons against such a column are REFUSED.
	InetFamilyUnspecified InetFamily = iota
	// InetFamilyAny is the Postgres `inet`/`cidr` rule: the column holds
	// either family and coerces nothing — a stored `10.0.0.1` is delivered
	// `10.0.0.1` and a stored `::ffff:10.0.0.1` is delivered
	// `::ffff:10.0.0.1`. It is a DECLARED answer, not a default; that is
	// what makes the zero value mean "nobody decided".
	InetFamilyAny
	// InetFamilyIPv4 is the MariaDB `INET4` rule: the column holds only an
	// IPv4 address. The server REFUSES an IPv6 literal on insert
	// (errno 1292) and matches nothing for one in a WHERE, so a predicate
	// naming an IPv6 address is refused rather than compiled into a filter
	// that can never be true.
	InetFamilyIPv4
	// InetFamilyIPv6 is the MariaDB `INET6` rule: every value is widened
	// into IPv6, so an IPv4 address is stored and delivered as its
	// IPv4-MAPPED form (`10.0.0.1` -> `::ffff:10.0.0.1`, `0.0.0.0` ->
	// `::ffff:0.0.0.0`). This is the family that made item 133 silent.
	InetFamilyIPv6
)

// Inet represents an IPv4 or IPv6 host address (Postgres inet, MariaDB
// inet4/inet6).
type Inet struct {
	// Family is what the column constrains its values to; see [InetFamily]
	// for why the zero value must fail closed.
	Family InetFamily
}

func (Inet) isType()    {}
func (Inet) Tier() Tier { return TierExtension }

// String deliberately renders the SAME "Inet" for every family, unlike
// [Macaddr.String], which renders its two widths distinctly.
//
// The difference is principled rather than an oversight. A `macaddr` ⇄
// `macaddr8` retype changes the TARGET DDL (`MACADDR` vs `MACADDR8`), so the
// two schema-diff paths that compare rendered type strings must see it. An
// inet family does not: every family emits the same target column (Postgres
// `INET`, a MySQL-family target `VARCHAR(45)`), so a rendered-type diff that
// reported them differently would flag drift on every MariaDB-source inet
// column with nothing an operator could act on. The family is a SOURCE-side
// comparison discriminant, not a target type distinction.
func (Inet) String() string { return "Inet" }

// Cidr represents an IPv4 or IPv6 network specification (Postgres cidr).
type Cidr struct {
	// Family carries the same discriminant as [Inet.Family] and for the same
	// fail-closed reason. Postgres's `cidr` is the only type any engine maps
	// here today and it constrains nothing ([InetFamilyAny]) — the field is
	// present so a future engine with a family-constrained network type is
	// REFUSED until it decides, rather than silently inheriting "any" from a
	// premise about the current producer set.
	Family InetFamily
}

func (Cidr) isType()    {}
func (Cidr) Tier() Tier { return TierExtension }

// String renders "Cidr" for every family, for the reason [Inet.String] gives.
func (Cidr) String() string { return "Cidr" }

// MacaddrEUI48 / MacaddrEUI64 are the two widths a Postgres MAC-address
// column can have, in BYTES: `macaddr` holds a 6-byte EUI-48 and
// `macaddr8` holds an 8-byte EUI-64. Postgres has no other MAC type, so
// these two plus the unspecified zero value are the whole domain.
const (
	MacaddrEUI48 = 6
	MacaddrEUI64 = 8
)

// Macaddr represents a hardware (MAC) address (Postgres `macaddr` or
// `macaddr8`).
type Macaddr struct {
	// Width is the address width in BYTES — [MacaddrEUI48] for `macaddr`,
	// [MacaddrEUI64] for `macaddr8`.
	//
	// ZERO MEANS UNSPECIFIED, and every consumer must treat it exactly as
	// it treated the field-less type this replaced: the Postgres emitter
	// renders `MACADDR`, [String] renders "Macaddr". That is the
	// v0.99.51 zero-value rule — the width arrived after the type, so
	// every construction site that predates it (a manifest written by an
	// older binary, a translate/test fixture) keeps its old behaviour
	// rather than silently acquiring the wider one.
	//
	// Only the two Postgres readers set it: the text-keyed schema reader
	// (engines/postgres/types.go) and the OID-keyed CDC relation reader
	// (engines/postgres/cdc_relations.go). They are the only producers of
	// this type in the tree, which is what lets consumers treat an
	// unspecified width on a live read as drift rather than as "6"
	// (see rowpredicate.checkMACLiteralWidth).
	//
	// The width rides the backup wire (schema_wire.go) but is DELIBERATELY
	// EXCLUDED from the schema fingerprint — see the exclusion block in
	// internal/ir/backup/chain.go, and roadmap item 104 for why a field
	// that is non-zero on ordinary source data must never reach that hash.
	Width int
}

func (Macaddr) isType()    {}
func (Macaddr) Tier() Tier { return TierExtension }

// String renders the EUI-64 form distinctly, so the two schema-diff paths
// that compare RENDERED TYPE STRINGS (diff.typeString, diff.typeStringDrift)
// report a `macaddr` → `macaddr8` change instead of silently agreeing. The
// unspecified width renders as the historical "Macaddr", so a legacy
// manifest does not read as a type change against a freshly-read `macaddr`.
func (m Macaddr) String() string {
	if m.Width == MacaddrEUI64 {
		return "Macaddr8"
	}
	return "Macaddr"
}

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

// VerbatimType represents an uncatalogued PG extension column type
// (ADR-0047). Where [ExtensionType] models one of the seven catalogued
// extensions (ADR-0032) with a rich per-extension build/emit contract
// — typmod decode, modifier synthesis, cross-engine translators —
// VerbatimType is the deliberately narrower, lower-fidelity-but-
// faithful tier *below* that catalog: it carries the column type's
// exact `pg_catalog.format_type(atttypid, atttypmod)` string and
// nothing else. There is NO catalog dispatch by construction; the PG
// writer emits [VerbatimType.Definition] literally in the column-type
// position and values round-trip via the type's text I/O.
//
// It is produced ONLY for the two paths where semantic understanding
// is provably unnecessary (ADR-0047 §1):
//
//   - same-engine PG → PG (live sync / migrate), and
//   - PG backup whose restore target is also PG (enforced by a
//     recorded lineage-segment marker + a loud restore-time engine
//     gate — a verbatim-marked backup restored to MySQL refuses
//     loudly at preflight, never silently drops/mangles).
//
// Cross-engine targets receiving VerbatimType refuse loudly: it is
// PG-native by definition with no portable equivalent. The
// [Definition] string is opaque to the IR — engine-neutral by
// construction (it is just the PG type spelling; no MySQL analogue is
// implied, which is exactly why the cross-engine refusal is correct).
type VerbatimType struct {
	// Definition is the exact PG type spelling as returned by
	// `pg_catalog.format_type(atttypid, atttypmod)` for the source
	// column (e.g. "ltree", "cube", "public.mytype", "geometry(Point,
	// 4326)"). The PG writer emits this verbatim in the CREATE TABLE
	// column-type position. Same PG major version is the documented
	// fidelity contract (an extension's text representation is usually
	// but not guaranteed version-stable — ADR-0047 §Consequences).
	Definition string
}

func (VerbatimType) isType()    {}
func (VerbatimType) Tier() Tier { return TierExtension }

func (v VerbatimType) String() string {
	return fmt.Sprintf("VerbatimType[%s]", v.Definition)
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
	case VerbatimType:
		return ExtVerbatimType, true
	default:
		return 0, false
	}
}
