// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// columnMeta is the subset of information_schema.columns the type
// translator needs, augmented with metadata sluice resolves separately
// (enum values, array element type) so the translator can stay pure.
type columnMeta struct {
	// DataType is information_schema.columns.data_type — e.g. "integer",
	// "character varying", "ARRAY", "USER-DEFINED". Lowercase.
	DataType string

	// UDTName is information_schema.columns.udt_name — the canonical
	// underlying type name. For arrays, prefixed with underscore
	// (e.g. "_int4"). For custom types like enums, the type name.
	UDTName string

	// CharMaxLen is character_maximum_length, or nil when not applicable.
	CharMaxLen *int64

	// NumPrec is numeric_precision, or nil when not applicable.
	NumPrec *int64

	// NumScale is numeric_scale, or nil when not applicable.
	NumScale *int64

	// DTPrec is datetime_precision, or nil when not applicable.
	DTPrec *int64

	// Collation is the per-column collation name resolved from
	// pg_attribute.attcollation → pg_collation.collname. Empty when
	// the column inherits the database default. PG has no per-column
	// charset (server_encoding is database-wide) so there's no
	// matching Charset field — the IR's character-type structs leave
	// Charset empty for PG sources.
	Collation string

	// IsAutoIncrement is true for SERIAL/BIGSERIAL/IDENTITY columns.
	// The schema reader sets this based on either is_identity or a
	// nextval() default; the translator just respects it.
	IsAutoIncrement bool

	// ArrayElement is populated when DataType == "ARRAY". It carries
	// the resolved metadata of the array's element type so the
	// translator can recurse into it.
	ArrayElement *columnMeta

	// EnumValues is populated when the column's UDT is a Postgres
	// enum type. The schema reader resolves the values via pg_enum
	// before invoking the translator.
	EnumValues []string

	// GeometryInfo is populated when the column's UDT is the PostGIS
	// `geometry` type. The schema reader queries PostGIS's
	// `geometry_columns` view to recover the subtype and SRID that
	// information_schema flattens away. nil means "no geometry info
	// available" — either the column isn't geometry, or the lookup
	// returned nothing (PostGIS not installed, or the view doesn't
	// know about this column for some reason). The translator
	// degrades gracefully to GeometryUnspecified+SRID=0 in that case.
	GeometryInfo *geometryColumnInfo

	// AttTypmod is pg_attribute.atttypmod for this column. -1 means
	// "no typmod" (the PG sentinel). Used by the ADR-0032 extension
	// catalog to decode per-extension type modifiers (pgvector
	// dimension is `atttypmod - 4`; future PostGIS subtype/SRID
	// packs both into the same int32). Opaque from the rest of the
	// translator's POV.
	AttTypmod int32

	// ExtensionName + ExtensionTypeName are populated by the schema
	// reader (populateColumns) when (a) the column's data_type is
	// USER-DEFINED, (b) the operator opted into the column's
	// extension via `--enable-pg-extension`, and (c) the udt_name
	// matches a typesByName entry in pgExtensionCatalog. The
	// translator dispatches to the catalog's build function when
	// both are non-empty, emitting an [ir.ExtensionType]; otherwise
	// the existing user-defined → enum / loud-failure path runs.
	ExtensionName     string
	ExtensionTypeName string
}

// geometryColumnInfo carries PostGIS's per-column metadata as
// surfaced by the geometry_columns view (and the parallel
// geography_columns view for PostGIS `geography` columns).
type geometryColumnInfo struct {
	// Subtype is the PostGIS bareword from geometry_columns.type, e.g.
	// "POINT", "POLYGON", "GEOMETRY". Empty when no row matched (the
	// schema reader represents that as a nil GeometryInfo, but
	// callers seeing the empty string treat it the same way).
	Subtype string
	// SRID is geometry_columns.srid. PostGIS uses 0 to mean "unknown
	// CRS", which matches sluice's IR default. PostGIS's
	// geography_columns view defaults SRID to 4326 (WGS84) when the
	// column was declared without an explicit modifier; the reader
	// passes the view's value through unchanged.
	SRID int
	// IsGeography is true when this entry came from PostGIS's
	// geography_columns view (rather than geometry_columns). The
	// translator uses this to set [ir.Geometry.IsGeography] so the PG
	// writer emits `geography(...)` instead of `geometry(...)`.
	IsGeography bool
	// HasZ / HasM are populated by the schema reader from the
	// geometry_columns / geography_columns view's `coord_dimension`
	// column. PostGIS encodes dimensional variants in a two-channel
	// shape: the M-only case (POINTM, LINESTRINGM) puts an "M" suffix
	// in the view's `type` column AND records coord_dimension=3, but
	// the Z and ZM cases (POINTZ, POINTZM) leave the view's `type`
	// column as the 2D base name and signal the dimension only via
	// coord_dimension=3 / =4. The translator's parseGeometrySubtype
	// extracts what's encoded in the type string; these flags carry
	// the orthogonal signal from coord_dimension. Final
	// [ir.Geometry.HasZ] / [ir.Geometry.HasM] are the OR-merge.
	HasZ bool
	HasM bool
}

// translateType maps a Postgres column's metadata to an IR type. The
// function is pure; all required external lookups (enum values, array
// element types) must be resolved by the caller and supplied via
// columnMeta before invoking translateType.
func translateType(c columnMeta) (ir.Type, error) {
	// Arrays first — the rest of the logic dispatches on the
	// "scalar" type name, and arrays carry that name on their element.
	if c.DataType == "array" || c.DataType == "ARRAY" {
		if c.ArrayElement == nil {
			return nil, fmt.Errorf("postgres: array column with unresolved element type (udt %q)", c.UDTName)
		}
		elem, err := translateType(*c.ArrayElement)
		if err != nil {
			return nil, fmt.Errorf("postgres: array element: %w", err)
		}
		return ir.Array{Element: elem}, nil
	}

	// USER-DEFINED covers enums, composites, and domain types. We
	// support enums and PostGIS's geometry; composites/domains are
	// not modelled in the IR.
	if c.DataType == "user-defined" || c.DataType == "USER-DEFINED" {
		// ADR-0032: extension passthrough. When the schema reader
		// recognised this column's udt_name as belonging to an
		// operator-enabled extension, dispatch to the catalog's
		// build function so the IR carries [ir.ExtensionType] with
		// the per-extension Modifiers.
		if c.ExtensionName != "" {
			def, ok := pgExtensionCatalog[c.ExtensionName]
			if !ok {
				return nil, fmt.Errorf(
					"postgres: extension %q is not in the catalog "+
						"(internal error — schema reader recognised it "+
						"earlier in the pipeline)",
					c.ExtensionName)
			}
			return def.build(c.ExtensionTypeName, c.AttTypmod)
		}
		if c.EnumValues != nil {
			return ir.Enum{Values: c.EnumValues}, nil
		}
		// PostGIS geometry / geography. information_schema reports
		// both as USER-DEFINED with udt_name="geometry" / "geography";
		// subtype + SRID live in PostGIS's own geometry_columns /
		// geography_columns views, which the schema reader queries
		// separately and stashes on the columnMeta before invoking the
		// translator. When that lookup returns nothing (PostGIS not
		// installed, view doesn't know this column, or the schema
		// reader is the older unaware version), we degrade gracefully
		// to GeometryUnspecified+SRID=0. The IsGeography flag rides
		// on c.GeometryInfo so the IR's [ir.Geometry.IsGeography]
		// preserves the source's geography-vs-geometry distinction
		// for same-engine PG → PG; cross-engine targets ignore the
		// flag and flatten to their generic spatial type.
		if c.UDTName == "geometry" || c.UDTName == "geography" {
			if c.GeometryInfo == nil {
				return ir.Geometry{
					Subtype:     ir.GeometryUnspecified,
					IsGeography: c.UDTName == "geography",
				}, nil
			}
			subtype, parsedZ, parsedM := parseGeometrySubtype(c.GeometryInfo.Subtype)
			return ir.Geometry{
				Subtype:     subtype,
				SRID:        c.GeometryInfo.SRID,
				IsGeography: c.GeometryInfo.IsGeography,
				// OR-merge: the type-string parsing covers the M-only
				// case where PostGIS records the suffix in `type`; the
				// schema reader's coord_dimension capture covers the Z
				// and ZM cases where PostGIS records the dimension out
				// of band in coord_dimension. Bug 53.
				HasZ: parsedZ || c.GeometryInfo.HasZ,
				HasM: parsedM || c.GeometryInfo.HasM,
			}, nil
		}
		// ADR-0032 hint: if udt_name matches a known extension type
		// the operator didn't opt into, surface the actionable flag
		// rather than the vague "not a recognised enum" wording. The
		// extension-owning lookup runs unconditionally — the catalog
		// is small, the data point is already there from the
		// `--enable-pg-extension` allowlist machinery.
		if owningExt := extensionOwningType(c.UDTName); owningExt != "" {
			return nil, fmt.Errorf(
				"postgres: user-defined type %q is owned by extension %q; "+
					"pass --enable-pg-extension %s to enable passthrough "+
					"(ADR-0032)",
				c.UDTName, owningExt, owningExt)
		}
		return nil, fmt.Errorf("postgres: user-defined type %q is not a recognised enum", c.UDTName)
	}

	switch c.DataType {
	// ---- Boolean ----
	case "boolean":
		return ir.Boolean{}, nil

	// ---- Integer family ----
	case "smallint":
		return ir.Integer{Width: 16, AutoIncrement: c.IsAutoIncrement}, nil
	case "integer":
		return ir.Integer{Width: 32, AutoIncrement: c.IsAutoIncrement}, nil
	case "bigint":
		return ir.Integer{Width: 64, AutoIncrement: c.IsAutoIncrement}, nil

	// ---- Decimal / float ----
	case "numeric", "decimal":
		return ir.Decimal{Precision: int(int64Ptr(c.NumPrec)), Scale: int(int64Ptr(c.NumScale))}, nil
	case "real":
		return ir.Float{Precision: ir.FloatSingle}, nil
	case "double precision":
		return ir.Float{Precision: ir.FloatDouble}, nil

	// ---- Character ----
	case "character":
		return ir.Char{Length: int(int64Ptr(c.CharMaxLen)), Collation: c.Collation}, nil
	case "character varying":
		return ir.Varchar{Length: int(int64Ptr(c.CharMaxLen)), Collation: c.Collation}, nil
	case "text":
		// Postgres text is unbounded; the IR's TextLong is the
		// closest match that round-trips correctly to MySQL's LONGTEXT.
		return ir.Text{Size: ir.TextLong, Collation: c.Collation}, nil

	// ---- Binary ----
	case "bytea":
		return ir.Blob{Size: ir.BlobLong}, nil

	// ---- Temporal ----
	case "date":
		return ir.Date{}, nil
	case "time without time zone", "time":
		return ir.Time{Precision: int(int64Ptr(c.DTPrec))}, nil
	case "time with time zone":
		// The IR currently has a single Time type without a TZ flag.
		// Modelled as Time; the TZ-ness is implicit and lossy on
		// MySQL output. A future IR extension could add a TZ flag.
		return ir.Time{Precision: int(int64Ptr(c.DTPrec))}, nil
	case "timestamp without time zone", "timestamp":
		return ir.DateTime{Precision: int(int64Ptr(c.DTPrec))}, nil
	case "timestamp with time zone":
		return ir.Timestamp{Precision: int(int64Ptr(c.DTPrec)), WithTimeZone: true}, nil

	// ---- Structured ----
	case "json":
		return ir.JSON{Binary: false}, nil
	case "jsonb":
		return ir.JSON{Binary: true}, nil

	// ---- Identity / network ----
	case "uuid":
		return ir.UUID{}, nil
	case "inet":
		return ir.Inet{}, nil
	case "cidr":
		return ir.Cidr{}, nil
	case "macaddr", "macaddr8":
		return ir.Macaddr{}, nil
	}

	return nil, fmt.Errorf("postgres: unsupported data_type %q (udt %q)", c.DataType, c.UDTName)
}

// int64Ptr returns *p, or 0 if p is nil. Used to translate
// information_schema's nullable numeric columns into the IR's int
// fields without per-call nil checks.
func int64Ptr(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

// parseGeometrySubtype maps the PostGIS subtype string from
// geometry_columns.type (or geography_columns.type) to the IR's
// [ir.GeometrySubtype] value plus the orthogonal Z / M dimension
// flags carried on [ir.Geometry].
//
// PostGIS reports the same logical subtype in two distinct casings
// across views: geometry_columns uses ALL CAPS ("POINT"), but
// geography_columns uses Mixed Case ("Point") — Bug 51. The function
// upper-cases first so geography inputs dispatch correctly.
//
// PostGIS extends the seven 2D base subtypes with Z, M, and ZM
// variants ("POINTZ" / "POINTM" / "POINTZM") for 3D + measure
// dimensions — Bug 52. The function strips the dimensional suffix
// before dispatching on the base name and returns the captured flags
// so the IR's [ir.Geometry.HasZ] / [ir.Geometry.HasM] preserve the
// source fidelity. The PG writer reconstructs the suffix on emit
// (postgisSubtypeName); cross-engine MySQL ignores the flags (MySQL
// carries Z / M in the WKB bytes rather than the column type
// modifier).
//
// Unknown / unparsable strings return GeometryUnspecified with both
// flags false — degrading gracefully rather than erroring is the
// long-standing policy for forward-compat with PostGIS evolution.
func parseGeometrySubtype(s string) (subtype ir.GeometrySubtype, hasZ, hasM bool) {
	s = strings.ToUpper(s)
	// Strip dimensional suffix. Order matters — ZM must be tried before
	// Z and M as individual letters.
	switch {
	case strings.HasSuffix(s, "ZM"):
		hasZ, hasM = true, true
		s = strings.TrimSuffix(s, "ZM")
	case strings.HasSuffix(s, "Z"):
		hasZ = true
		s = strings.TrimSuffix(s, "Z")
	case strings.HasSuffix(s, "M"):
		// `GEOMETRY` ends in "Y" not "M", but the seven base names that
		// end in "M" are MULTIPOINT, MULTILINESTRING, MULTIPOLYGON
		// (which already retain their trailing T / G), POINT
		// (no M at all), LINESTRING (G), POLYGON (N), GEOMETRYCOLLECTION
		// (N) — none of the base names end in a literal "M" character.
		// So a trailing "M" unambiguously means the M-dimension flag.
		hasM = true
		s = strings.TrimSuffix(s, "M")
	}
	switch s {
	case "POINT":
		return ir.GeometryPoint, hasZ, hasM
	case "LINESTRING":
		return ir.GeometryLineString, hasZ, hasM
	case "POLYGON":
		return ir.GeometryPolygon, hasZ, hasM
	case "MULTIPOINT":
		return ir.GeometryMultiPoint, hasZ, hasM
	case "MULTILINESTRING":
		return ir.GeometryMultiLineString, hasZ, hasM
	case "MULTIPOLYGON":
		return ir.GeometryMultiPolygon, hasZ, hasM
	case "GEOMETRYCOLLECTION":
		return ir.GeometryCollection, hasZ, hasM
	case "GEOMETRY", "":
		return ir.GeometryUnspecified, hasZ, hasM
	}
	return ir.GeometryUnspecified, false, false
}
