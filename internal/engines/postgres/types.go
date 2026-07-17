// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
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

	// EnumTypeName is the source-side enum type name (udt_name /
	// pg_type.typname). Carried onto [ir.Enum.TypeName] so a
	// same-engine PG → PG migration round-trips the type name verbatim
	// instead of synthesizing `<table>_<col>_enum` (catalog Bug 19c).
	// Empty for a MySQL source (column-inline enums have no type
	// identity); the PG writer then falls back to the synthesized
	// name.
	EnumTypeName string

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

	// FormatType is pg_catalog.format_type(atttypid, atttypmod) for the
	// column — the exact PG type spelling (e.g. "ltree", "cube",
	// "public.mytype"). Populated by the schema reader for every column;
	// consumed only by the ADR-0047 verbatim tier (the catalogued /
	// enum / geometry / core paths derive their type from the other
	// fields and ignore it).
	FormatType string

	// VerbatimEligible is set by the schema reader (populateColumns)
	// when (a) data_type is USER-DEFINED, (b) the column is NOT
	// catalogued (the ADR-0032 lookup missed), and (c) the run carries
	// a same-engine-PG guarantee (ADR-0047 tier (b) — verbatimTierFor
	// returned verbatimTierVerbatim). When true AND the column has no
	// first-class IR shape (not an enum, not geometry/geography), the
	// translator emits [ir.VerbatimType] carrying [FormatType] instead
	// of the loud refusal. Enum / geometry keep their existing
	// first-class dispatch; this flag is the last carry before the
	// refusal, not a reroute of recognised shapes.
	VerbatimEligible bool

	// IsDomain + DomainName carry the Bug 113 (v0.95.2) DOMAIN-
	// detection signal from populateColumns through the per-column
	// translate step. information_schema.columns unwraps DOMAINs at
	// every column it exposes, so the existing translateType call
	// produces the BASE IR type; the populateColumns site (after
	// translateType returns) wraps that base type in ir.Domain when
	// IsDomain is set. DomainName is the operator-declared identifier
	// resolved via pg_type.typname (information_schema's udt_name
	// ALSO unwraps so it is unreliable here).
	IsDomain   bool
	DomainName string
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

	// USER-DEFINED covers enums, composites, and domain types, dispatched
	// to a dedicated helper so each family reads as its own unit and the
	// whole translator stays under the complexity ceiling.
	if c.DataType == "user-defined" || c.DataType == "USER-DEFINED" {
		return translateUserDefinedType(c)
	}

	return translateScalarType(c)
}

// translateUserDefinedType maps a Postgres information_schema
// data_type=USER-DEFINED column to an IR type: catalogued extension
// (ADR-0032), enum, PostGIS geometry/geography, the ADR-0047 verbatim
// tier, and finally the loud refusal. Split out of [translateType] so
// each type family reads as its own unit. Precondition: the caller has
// established c.DataType is USER-DEFINED.
func translateUserDefinedType(c columnMeta) (ir.Type, error) {
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
				c.ExtensionName,
			)
		}
		return def.build(c.ExtensionTypeName, c.AttTypmod)
	}
	if c.EnumValues != nil {
		return ir.Enum{Values: c.EnumValues, TypeName: c.EnumTypeName}, nil
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
	// ADR-0047 tier (b): an UNcatalogued USER-DEFINED type the run
	// is allowed to carry verbatim (provably same-engine PG → PG,
	// or a PG backup whose PG-restore-only constraint is enforced
	// by the lineage marker + restore-time engine gate). Reached
	// only after the catalogued (ExtensionName), enum (EnumValues),
	// and geometry/geography branches above all declined — so it
	// never shadows a first-class IR shape. Carries the exact
	// pg_catalog.format_type spelling; the PG writer re-emits it
	// literally and values round-trip via the type's text I/O.
	// VerbatimEligible is false for tier (c) (cross-engine / no
	// same-engine guarantee), so that path still falls through to
	// the loud refusal below — the cross-engine default is NOT
	// weakened.
	if c.VerbatimEligible {
		if c.FormatType == "" {
			return nil, fmt.Errorf(
				"postgres: user-defined type %q is eligible for "+
					"verbatim passthrough but pg_catalog.format_type "+
					"returned empty (cannot re-emit a column with no "+
					"type spelling) — this is a sluice bug; please "+
					"report it",
				c.UDTName,
			)
		}
		return ir.VerbatimType{Definition: c.FormatType}, nil
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
			c.UDTName, owningExt, owningExt,
		)
	}
	return nil, fmt.Errorf(
		"postgres: user-defined type %q is not supported (it is not a "+
			"recognised enum, a catalogued extension type, or a "+
			"verbatim-passthrough type — composite and domain types are "+
			"not modelled in the IR) — exclude the table with "+
			"--exclude-table to proceed",
		c.UDTName,
	)
}

// translateScalarType maps a Postgres scalar (non-array, non-USER-
// DEFINED) column to an IR type via a data_type switch, with the
// ADR-0051 core-verbatim allowlist as the last carry before the loud
// refusal. Split out of [translateType] to keep the dispatch switch
// under the complexity ceiling.
func translateScalarType(c columnMeta) (ir.Type, error) {
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
		// A declared typmod is the modifier ground truth and wins over
		// information_schema, which fails it two ways: it reports NULL
		// for every modifier of an ARRAY column (Bug 195's silent-loss
		// half — `numeric(10,2)[]` landed as bare `numeric[]` because
		// only the temporal family had the typmod fallback), and it
		// mis-reports a NEGATIVE scale (PG 15+ `numeric(5,-2)`) as the
		// raw 11-bit encoding (numeric_scale=2046, ground-truthed on
		// PG 17) — [numericTypmod] sign-extends correctly.
		if c.AttTypmod >= 4 {
			p, s := numericTypmod(c.AttTypmod)
			return ir.Decimal{Precision: p, Scale: s}, nil
		}
		// A bare `numeric` / `decimal` with no declared precision is
		// arbitrary-precision: information_schema reports BOTH
		// numeric_precision and numeric_scale as NULL. Collapsing that
		// NULL to 0 (the pre-fix behaviour) emitted NUMERIC(0,0) on a
		// PG target (22023, hard fail) and DECIMAL(0,0) on a MySQL
		// target (silent decimal-precision loss) — catalog Bug 69.
		// Model the unconstrained case distinctly so the emitters can
		// render the correct per-engine form.
		if c.NumPrec == nil && c.NumScale == nil {
			return ir.Decimal{Unconstrained: true}, nil
		}
		// typmod -1 with information_schema modifiers present: a
		// DOMAIN-unwrapped column (the modifier lives on the domain
		// type; the column's own atttypmod is -1) or a caller that
		// resolves modifiers without pg_attribute.
		return ir.Decimal{Precision: int(int64Ptr(c.NumPrec)), Scale: int(int64Ptr(c.NumScale))}, nil
	case "real":
		return ir.Float{Precision: ir.FloatSingle}, nil
	case "double precision":
		return ir.Float{Precision: ir.FloatDouble}, nil

	// ---- Character ----
	case "character":
		if n, declared := charLengthOf(c); declared {
			return ir.Char{Length: n, Collation: c.Collation}, nil
		}
		// Undeclared length (typmod -1): a bare `bpchar` scalar or a
		// bare `char[]` element is UNBOUNDED in PG (it accepts any
		// length — the "char(1) default" applies only to the SQL
		// keyword `character` form, which the catalog records as an
		// explicit typmod). Emitting Char{0} false-refused it at DDL
		// emit ("CHAR(0)", Bug 195); Char{1} would silently truncate.
		// Land on Text/long, the same collapse the CDC path's oidToType
		// uses for unbounded varchar.
		return ir.Text{Size: ir.TextLong, Collation: c.Collation}, nil
	case "character varying":
		if n, declared := charLengthOf(c); declared {
			return ir.Varchar{Length: n, Collation: c.Collation}, nil
		}
		// Unbounded varchar — bare `varchar` scalar or `varchar[]`
		// element (both report character_maximum_length NULL and typmod
		// -1). The IR has no "varchar with no length", so land on
		// Text/long, matching oidToType's unbounded-varchar collapse.
		// Pre-Bug-195 this produced Varchar{0}, which false-refused at
		// DDL emit as VARCHAR(0) — a MySQL marker-column idiom the PG
		// catalog can never legitimately mean (PG refuses varchar(0) at
		// CREATE TABLE, so a PG source cannot contain one).
		return ir.Text{Size: ir.TextLong, Collation: c.Collation}, nil
	case "text":
		// Postgres text is unbounded; the IR's TextLong is the
		// closest match that round-trips correctly to MySQL's LONGTEXT.
		return ir.Text{Size: ir.TextLong, Collation: c.Collation}, nil

	// ---- Binary ----
	case "bytea":
		return ir.Blob{Size: ir.BlobLong}, nil

	// ---- Bit ----
	// Fixed-width / varying bit string. information_schema reports the
	// declared bit count in character_maximum_length. Round-trips
	// MySQL BIT(N) ↔ PG bit(N) (catalog Bug 62 — keeps the PG reader
	// symmetric with the PG writer's new ir.Bit emission so a
	// sluice-written bit column reads back to the same IR type). `bit
	// varying` with no length collapses to bit(1) — PG's default.
	//
	// Deliberately NOT routed through charLengthOf: bit typmods store
	// the raw length with NO +4 offset (ground-truthed: bit(3) carries
	// atttypmod=3), so the char-family decode would mis-read them. The
	// fallback is also unreachable here — `bit(n)[]` / `varbit(n)[]`
	// array elements refuse loudly upstream as an unsupported element
	// family (builtinArrayElement has no _bit/_varbit), and scalar bit
	// columns always carry character_maximum_length.
	case "bit", "bit varying":
		n := int(int64Ptr(c.CharMaxLen))
		if n < 1 {
			n = 1
		}
		return ir.Bit{Length: n, Varying: c.DataType == "bit varying"}, nil

	// ---- Temporal ----
	case "date":
		return ir.Date{}, nil
	case "interval":
		// PG-native duration type. Reads as ir.Interval (a span), distinct
		// from ir.Time (a time-of-day). Carrying it here makes interval a
		// first-class PG type: PG→PG round-trips it, the MySQL TIME →
		// `--type-override col=interval` migrate path round-trips through
		// it, and — load-bearing — the CDC applier's target-catalog read
		// (loadColumnTypes → translateType) resolves an interval target
		// column instead of stopping the stream. A non-PG target has no
		// interval and is refused (emitColumnType / unsupportablePGtoMySQL).
		return ir.Interval{}, nil
	case "time without time zone", "time":
		p, unspec := temporalPrecisionOf(c)
		return ir.Time{Precision: p, PrecisionUnspecified: unspec}, nil
	case "time with time zone":
		// `timetz` (PG OID 1266) is a distinct wire type from plain
		// `time` (OID 1083); the tz-bearing value cannot be encoded
		// into the tz-less codec. Carry the distinction on the IR so
		// PG→PG round-trips it faithfully. Cross-engine to MySQL the
		// tz is dropped (MySQL has no tz-aware TIME) — same documented
		// policy as timestamptz→MySQL; see docs/type-mapping.md.
		// (catalog Bug 71.)
		p, unspec := temporalPrecisionOf(c)
		return ir.Time{Precision: p, WithTimeZone: true, PrecisionUnspecified: unspec}, nil
	case "timestamp without time zone", "timestamp":
		p, unspec := temporalPrecisionOf(c)
		return ir.DateTime{Precision: p, PrecisionUnspecified: unspec}, nil
	case "timestamp with time zone":
		p, unspec := temporalPrecisionOf(c)
		return ir.Timestamp{Precision: p, WithTimeZone: true, PrecisionUnspecified: unspec}, nil

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

	// ADR-0051 (consolidating catalog Bug 17): core pg_catalog types
	// lacking a rich cross-engine IR shape carry verbatim on a
	// same-engine-PG run via [ir.VerbatimType]. Sibling tier to
	// ADR-0047's USER-DEFINED uncatalogued path: both surfaces emit the
	// same IR type, share the same downstream pipeline (DDL emit, value
	// decode, cross-engine refusal), and differ only in where they
	// dispatch (USER-DEFINED branch vs core-type fallthrough).
	//
	// The allowlist is the single named decision — adding a type later
	// is a one-line additive change plus an ADR-0051 update, not a
	// scattered switch-case edit. Default fall-through ("anything
	// VerbatimEligible = verbatim") was deliberately rejected: it would
	// silently absorb any new PG core type a future major version adds
	// (loud-failure tenet).
	//
	// VerbatimEligible is set by the schema reader only on a provably-
	// same-engine-PG run (or a PG backup whose PG-restore-only marker
	// is enforced at restore). Cross-engine leaves it false, so the
	// generic fallthrough refusal below still fires — the cross-engine
	// loud-refuse default is NOT weakened. The cross-engine refusal at
	// `internal/pipeline/cross_engine_supportable.go` rejects
	// ir.VerbatimType regardless, so the safety is doubly enforced.
	if coreVerbatimEligibleTypes[c.DataType] && c.VerbatimEligible {
		if c.FormatType == "" {
			return nil, fmt.Errorf(
				"postgres: core type %q is eligible for verbatim "+
					"passthrough but pg_catalog.format_type returned "+
					"empty — this is a sluice bug; please report it",
				c.DataType,
			)
		}
		return ir.VerbatimType{Definition: c.FormatType}, nil
	}

	return nil, fmt.Errorf("postgres: unsupported data_type %q (udt %q)", c.DataType, c.UDTName)
}

// coreVerbatimEligibleTypes is the ADR-0051 allowlist of core
// pg_catalog types that carry verbatim on a same-engine-PG run when
// no first-class IR shape is appropriate. Consumed by translateType
// just before the generic fallthrough loud refusal.
//
// Stage 1 (this ADR): FTS family (tsvector/tsquery — catalog Bug 17
// representative, consolidated here), range family, and the PG14+
// multirange family — all share opaque text-I/O semantics with no
// locale / dialect quirks, no portable MySQL form (cross-engine stays
// loud-refuse via [ir.VerbatimType]'s default in
// `cross_engine_supportable.go`).
//
// Stage 2 (promoted 2026-05-30 in ADR-0070 after the per-type
// round-trip pins shipped in v0.90.0): xml, money, pg_lsn,
// txid_snapshot, pg_snapshot. Each pin asserts the three-outcome
// shape (refuse-loudly / preserve / SILENT-TYPE-LOSS) and now hits
// the "preserve" branch with this promotion. Cross-engine stays
// loud-refuse via [ir.VerbatimType] in `cross_engine_supportable.go`.
// Do NOT add a Stage 3 entry without the same evidence: per-type
// integration pin + ADR update + cross-engine refusal verified.
var coreVerbatimEligibleTypes = map[string]bool{
	// FTS family (catalog Bug 17 — pre-existing tsvector/tsquery
	// carve-out, consolidated into this allowlist by ADR-0051).
	"tsvector": true,
	"tsquery":  true,

	// Range family. Common in partition bounds, scheduling, analytics
	// (GitLab, Rails, Django). No portable MySQL form.
	"int4range": true,
	"int8range": true,
	"numrange":  true,
	"tsrange":   true,
	"tstzrange": true,
	"daterange": true,

	// Multirange family (PG 14+). Same shape and rationale as ranges —
	// pin the class, not the representative.
	"int4multirange": true,
	"int8multirange": true,
	"nummultirange":  true,
	"tsmultirange":   true,
	"tstzmultirange": true,
	"datemultirange": true,

	// Stage 2 (ADR-0070, promoted 2026-05-30). Per-type round-trip
	// pins live in `internal/pipeline/migrate_pg_*_type_integration_test.go`.
	"xml":           true,
	"money":         true,
	"pg_lsn":        true,
	"txid_snapshot": true,
	"pg_snapshot":   true,
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

// charLengthOf resolves the declared length for a character-family
// column (varchar / bpchar). information_schema's
// character_maximum_length is the scalar ground truth, but it is NULL
// for every modifier of an ARRAY column — so array-element metas
// (whose synthesized columnMeta carries only the array column's
// atttypmod) fall back to the typmod, which PG applies to the elements
// (`varchar(20)[]` carries atttypmod=24) stored as N+4 (VARHDRSZ).
// declared=false is the bare form (typmod -1): unbounded varchar /
// bpchar, NOT length zero — the caller maps it to an unbounded IR type
// instead of the VARCHAR(0)/CHAR(0) false refusal (Bug 195). The
// char-family counterpart of [temporalPrecisionOf]'s typmod fallback;
// the numeric family's lives inline in translateScalarType via
// [numericTypmod].
func charLengthOf(c columnMeta) (length int, declared bool) {
	if c.CharMaxLen != nil {
		return int(*c.CharMaxLen), true
	}
	if c.AttTypmod >= 4 {
		return int(c.AttTypmod - 4), true
	}
	return 0, false
}

// temporalPrecisionOf resolves the (precision, unspecified) pair for a
// temporal column (restore-parity TRIAGE #3, the temporal counterpart
// of catalog Bug 69's unconstrained numeric). pg_attribute.atttypmod
// is the ground truth for declaredness: -1 is the bare form (`time` /
// `timestamp` / `timestamptz` with no precision) — information_schema
// reports datetime_precision=6 (the engine DEFAULT) for that case, so
// it cannot distinguish bare from an explicit (6), and reading it
// verbatim materialized the default into the catalog. When a typmod
// IS declared, datetime_precision carries it for scalar columns; for
// array-element metas (whose synthesized columnMeta has no
// datetime_precision) the typmod itself is the precision — PG stores
// a temporal typmod as the raw precision value.
func temporalPrecisionOf(c columnMeta) (precision int, unspecified bool) {
	if c.AttTypmod < 0 {
		return 0, true
	}
	if c.DTPrec != nil {
		return int(*c.DTPrec), false
	}
	return int(c.AttTypmod), false
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
