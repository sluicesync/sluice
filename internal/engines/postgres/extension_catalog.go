// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// PG → PG extension passthrough catalog (ADR-0032). The framework
// maps an operator-allowlisted extension name (`--enable-pg-extension
// EXT`) to the type-emission rules sluice needs to recognise and
// round-trip the extension's column types and indexes. Adding a new
// extension is "add a catalog entry," not "extend interfaces" —
// the engine surface ([ir.ExtensionAware]) and the IR variant
// ([ir.ExtensionType]) stay engine-neutral while per-extension
// knowledge stays local to this file.
//
// v0.26.0 ships pgvector as the first concrete entry. pg_trgm /
// hstore / citext / postgis follow as catalog-only additions in
// subsequent point releases per the v1 shortlist pinned in
// docs/research/pg-extensions-deployment-frequency.md.

// extensionDef captures everything sluice needs to recognise and
// re-emit one PG extension's type-passthrough surface.
//
// Each entry covers:
//
//   - typesByName: the (schema, typname) pairs the extension owns.
//     The schema reader uses this to recognise columns whose type is
//     owned by an enabled extension and emit them as
//     [ir.ExtensionType] rather than the existing "user-defined →
//     enum/loud-failure" path. Maps to a builder that assembles the
//     per-column [ir.ExtensionType] (including the column-specific
//     Modifiers — pgvector dimension, postgis subtype/SRID).
//
//   - emitColumn: the writer's column-DDL renderer for column types
//     this extension owns. Receives the IR's ExtensionType (which
//     carries the Modifiers vector) and produces the exact bareword
//     PG accepts in a CREATE TABLE column-type position
//     (e.g. "vector(384)" for pgvector with dimension 384).
//
//   - indexAccessMethods: the GIN / GIST / IVFFLAT / HNSW method
//     names this extension introduces. The schema writer's index
//     emit consults this set when an index's recorded access method
//     isn't a core PG one — a method name that isn't in any
//     enabled-extension's catalog falls through to the default
//     emit path (which today drops the method, falling back to
//     btree). Used to validate that operator-typo'd index methods
//     don't sneak through silently.
//
// The map key is the canonical PG extension name (the value of
// pg_extension.extname).
type extensionDef struct {
	// build assembles an [ir.ExtensionType] for a column whose PG
	// catalog metadata maps to this extension. fullTypeName is the
	// information_schema.columns udt_name for the column; typmod is
	// the pg_attribute.atttypmod value (used by pgvector's dimension
	// decoder). Per-extension implementations interpret typmod
	// differently — opaque from the catalog's POV.
	build func(udtName string, typmod int32) (ir.ExtensionType, error)

	// typesByName is the set of canonical type names this extension
	// owns. Used by the schema reader's catalog-query path to filter
	// the column scan down to extension-owned types in a single round
	// trip rather than per-column lookup.
	typesByName map[string]struct{}

	// emitColumn renders the PG column-type DDL for a column whose
	// IR type is [ir.ExtensionType] with Extension matching the
	// catalog key. Returns an error for malformed Modifiers (e.g.
	// pgvector with a non-positive dimension) so the caller surfaces
	// the operator-actionable message rather than emitting invalid
	// DDL.
	emitColumn func(t ir.ExtensionType) (string, error)

	// indexAccessMethods is the set of access-method names this
	// extension introduces that aren't core-PG (i.e. not btree, hash,
	// gin, gist, spgist, brin). The schema writer's index emitter
	// validates an index's method against this set when the IR's
	// IndexKind is Unspecified and the recorded Method is non-empty.
	// Empty for extensions that don't introduce new index methods
	// (hstore, citext, pg_trgm — pg_trgm rides on core gin / gist).
	indexAccessMethods map[string]struct{}

	// indexOperatorClasses is the set of operator-class names this
	// extension introduces. Used by the schema reader's index-population
	// path (alongside indexAccessMethods) to recognise opclasses that
	// are extension-owned even when the access method itself is core PG
	// — pg_trgm's `gin_trgm_ops` and `gist_trgm_ops` ride on core gin /
	// gist, so the reader must preserve them via [ir.IndexColumn.OperatorClass]
	// to make the same-engine emit round-trip cleanly.
	//
	// For extensions where the AM is itself extension-owned (pgvector's
	// ivfflat / hnsw) the opclass already round-trips through the
	// idx.Method != "" branch; the per-opclass set still gets populated
	// for symmetry and for the cross-engine refusal path that asks
	// "is this opclass extension-owned?" without re-deriving from the AM.
	indexOperatorClasses map[string]struct{}

	// hintTypeNames is the set of (schema, typname) pairs the extension
	// owns for the operator-actionable-hint path ONLY — types that have
	// a first-class IR representation outside [ir.ExtensionType] and
	// therefore are NOT dispatched through the catalog's build/emit
	// machinery. The schema reader uses this set via [extensionOwningType]
	// to surface "pass --enable-pg-extension X" hints when the operator
	// has a column of an extension-owned type but didn't enable the
	// extension; the IR path for the type stays unchanged.
	//
	// Today's only entry is `postgis`'s `geometry` (and `geography`,
	// reserved): PG's `ir.Geometry` is the IR type, not `ir.ExtensionType`,
	// because PostGIS support pre-dates ADR-0032's extension catalog
	// (the geometry/SRID path landed in v0.28.0 / ADR-0035). Same-engine
	// PG → PG geometry round-trips via the existing PostGIS-aware emit
	// path (postgis.go::postgisSubtypeName + the writer's HasPostGIS
	// gate); the catalog entry adds the opclass + actionable-hint
	// surface without rerouting the IR type.
	hintTypeNames map[string]struct{}

	// defaultExprFunctions: bareword names of functions this extension
	// owns that legitimately appear in column DEFAULT / GENERATED
	// expressions (e.g. "uuid_generate_v4", "digest"). Empty for
	// type/index-only extensions. Catalog-driven so the ADR-0044
	// Tier-3 schema-read gate, the preflight presence-check, and the
	// cross-engine policy are not scattered conditionals — every
	// site that needs "does this expr reference an extension-owned
	// function?" consults this one set.
	//
	// CRITICAL (load-bearing correctness guard): core-PG functions
	// MUST NOT appear here. `gen_random_uuid()` is core PostgreSQL
	// 13+, not pgcrypto-owned on any supported modern PG — listing it
	// would make the gate refuse valid core-PG schemas. The catalog
	// barewords are deliberately extension-specific; the scanner
	// (scanExtensionFunctionInExpr) only ever gates names that appear
	// in some entry's defaultExprFunctions, so a name absent from
	// every set sails through unconditionally (ADR-0044 §Context
	// "core-vs-extension subtlety").
	defaultExprFunctions map[string]struct{}
}

// pgExtensionCatalog is the registry of recognised PG extensions
// available for `--enable-pg-extension` passthrough. Adding an
// extension is an entry here — no interface changes, no per-call-site
// switch updates. Keys are the canonical pg_extension.extname value.
var pgExtensionCatalog = map[string]extensionDef{
	"vector":    pgVectorDef,
	"pg_trgm":   pgTrgmDef,
	"hstore":    pgHstoreDef,
	"citext":    pgCiTextDef,
	"postgis":   pgPostGISDef,
	"pgcrypto":  pgCryptoDef,
	"uuid-ossp": pgUUIDOSSPDef,
}

// crossEngineDefaultTranslatedExtensions names the PG extensions
// whose cross-engine MySQL translation has a defensible, lossless
// default (per research doc § 5). When the operator passes
// `--enable-pg-extension EXT` against a non-PG target, the engine-
// name gate in [validateEnabledPGExtensions] consults this set: the
// flag is allowed when EXT has a default translator, refused
// otherwise (preserving the loud-failure default for vector /
// pg_trgm / postgis where the cross-engine mapping is ambiguous or
// missing).
//
// Today's entries:
//   - hstore → MySQL JSON (text "k"=>"v" → {"k":"v"} at value-write
//     time; handled in mysql/row_writer.go::prepareValue + emit in
//     mysql/ddl_emit.go::emitColumnType).
//   - citext → MySQL VARCHAR with case-insensitive collation
//     (utf8mb4_0900_ai_ci); identity value translation (citext is
//     just text under a custom collation on the PG side).
//
// The map is exported for use by the pipeline's cross-engine
// preflight (validateEnabledPGExtensions). New entries here must
// also be wired into the MySQL emitter and (where applicable) the
// MySQL writer's value translator.
var crossEngineDefaultTranslatedExtensions = map[string]struct{}{
	"hstore": {},
	"citext": {},
}

// HasCrossEngineDefaultTranslator reports whether the named
// extension has a default cross-engine translator. Used by the
// pipeline package via the engine's optional [ir.ExtensionAware]
// surface (kept exported on the package so it can be referenced
// from outside without re-importing the catalog map).
func HasCrossEngineDefaultTranslator(name string) bool {
	_, ok := crossEngineDefaultTranslatedExtensions[name]
	return ok
}

// recognisedPGExtensionNames returns the sorted list of extension
// names the catalog knows about. Used by [ir.ExtensionAware]
// implementations to format operator-actionable error messages when
// `--enable-pg-extension EXT` names an unknown extension.
func recognisedPGExtensionNames() []string {
	names := make([]string, 0, len(pgExtensionCatalog))
	for name := range pgExtensionCatalog {
		names = append(names, name)
	}
	// Cheap insertion sort — the catalog is small.
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j-1], names[j] = names[j], names[j-1]
		}
	}
	return names
}

// pgVectorDef is the catalog entry for the `pgvector` extension
// (ADR-0032 v0.26.0). pgvector defines:
//
//   - One scalar type: `vector` (an N-dimensional float32 vector).
//     Optional type modifier is the dimension count;
//     `vector(384)` constrains the column to 384-dimensional values,
//     bare `vector` accepts any dimension.
//
//   - Two new index access methods: `ivfflat` and `hnsw`. Both are
//     approximate-nearest-neighbour structures over the vector type.
//     Operator classes determine the distance metric:
//
//   - vector_l2_ops      — Euclidean distance
//
//   - vector_ip_ops      — inner product
//
//   - vector_cosine_ops  — cosine distance
//
//   - vector_l1_ops      — L1 (Manhattan) distance (pgvector 0.7+)
//
//   - bit_hamming_ops    — bit-vector Hamming distance (pgvector 0.7+)
//
//   - bit_jaccard_ops    — bit-vector Jaccard distance (pgvector 0.7+)
//
//     v1 doesn't emit operator-class modifiers on indexes (the
//     verbatim `USING ivfflat (col)` form covers the load-bearing
//     pattern); the metadata is captured so a future schema-reader
//     pass can preserve them.
//
// The dimension modifier sits in pg_attribute.atttypmod: pgvector
// stores `dimension + 4` there (a small offset the extension picks).
// Per-typmod decode happens in [pgVectorDimFromTypmod].
var pgVectorDef = extensionDef{
	typesByName: map[string]struct{}{
		"vector": {},
	},
	build: func(udtName string, typmod int32) (ir.ExtensionType, error) {
		if udtName != "vector" {
			return ir.ExtensionType{}, fmt.Errorf(
				"postgres: pgvector catalog: unexpected udt_name %q (want \"vector\")", udtName)
		}
		mods := []int{}
		if dim, ok := pgVectorDimFromTypmod(typmod); ok {
			mods = []int{dim}
		}
		return ir.ExtensionType{
			Extension: "vector",
			Name:      "vector",
			Modifiers: mods,
		}, nil
	},
	emitColumn: func(t ir.ExtensionType) (string, error) {
		if t.Extension != "vector" || t.Name != "vector" {
			return "", fmt.Errorf(
				"postgres: pgvector emit: unexpected (extension=%q name=%q); "+
					"want (vector, vector)", t.Extension, t.Name)
		}
		switch len(t.Modifiers) {
		case 0:
			// Bare `vector` — no dimension constraint. Legal pgvector
			// shape; columns can hold any dimension.
			return "vector", nil
		case 1:
			dim := t.Modifiers[0]
			if dim <= 0 {
				return "", fmt.Errorf(
					"postgres: pgvector emit: dimension must be > 0 (got %d)", dim)
			}
			return fmt.Sprintf("vector(%d)", dim), nil
		default:
			return "", fmt.Errorf(
				"postgres: pgvector emit: expected 0 or 1 Modifiers (got %d)",
				len(t.Modifiers))
		}
	},
	indexAccessMethods: map[string]struct{}{
		"ivfflat": {},
		"hnsw":    {},
	},
	indexOperatorClasses: map[string]struct{}{
		"vector_l2_ops":     {},
		"vector_ip_ops":     {},
		"vector_cosine_ops": {},
		"vector_l1_ops":     {},
		"bit_hamming_ops":   {},
		"bit_jaccard_ops":   {},
	},
}

// pgVectorDimFromTypmod decodes the dimension count out of
// pg_attribute.atttypmod for a pgvector column. The reference is
// `vector_typmod_in` in pgvector/src/vector.c: the typmod IS the
// dimension verbatim — no offset. atttypmod = -1 is the PG
// "no modifier" sentinel and means a bare `vector` column with no
// dimension constraint.
//
// (An earlier version of this helper assumed a `dimension + 4`
// offset; that was a misread of the pgvector source — Bug 47 surfaced
// it when the source IR's Modifiers came back empty for a `vector(3)`
// column whose atttypmod the catalog query observed as 3.)
func pgVectorDimFromTypmod(typmod int32) (dim int, ok bool) {
	if typmod <= 0 {
		return 0, false
	}
	return int(typmod), true
}

// pgTrgmDef is the catalog entry for the `pg_trgm` extension
// (ADR-0032 Tier 2 lite). pg_trgm is "operator-class only" — it does
// not introduce any new column types. Its surface to sluice is:
//
//   - Two operator classes — `gin_trgm_ops` and `gist_trgm_ops` —
//     attached to indexes over existing `text` / `varchar` columns.
//     The schema reader preserves these via [ir.IndexColumn.OperatorClass]
//     so the same-engine writer can re-emit `<col> <opclass>` after
//     the column reference (which the writer already does).
//
//   - No new index access methods. pg_trgm rides on core PG `gin` and
//     `gist`, so [indexAccessMethods] is empty; the access-method
//     passthrough path that pgvector's ivfflat / hnsw use does not
//     fire here.
//
//   - No column types. [typesByName] is empty; [build] returns a
//     sentinel error since it is never reached on the schema-read
//     path (the catalog's lookup is gated on typesByName), and
//     [emitColumn] returns a sentinel error since [ir.ExtensionType]
//     columns never carry "extension = pg_trgm" in a well-formed IR.
//     Both sentinels exist so a hand-constructed IR or a future
//     framework extension surfaces the operator-actionable refusal
//     loudly rather than producing invalid DDL.
//
// The cross-engine refusal (PG → MySQL with a pg_trgm-indexed column)
// rides on the [ir.IndexColumn.OperatorClass] non-empty signal — see
// `internal/pipeline/cross_engine_supportable.go`. Sluice never
// populates OperatorClass for core-PG opclasses (Bug 47 design), so
// the field's presence is an honest "extension-owned opclass" marker.
var pgTrgmDef = extensionDef{
	typesByName: map[string]struct{}{},
	build: func(udtName string, _ int32) (ir.ExtensionType, error) {
		return ir.ExtensionType{}, fmt.Errorf(
			"postgres: pg_trgm catalog: build called with udt_name %q, "+
				"but pg_trgm declares no column types (operator-class only)",
			udtName)
	},
	emitColumn: func(t ir.ExtensionType) (string, error) {
		return "", fmt.Errorf(
			"postgres: pg_trgm catalog: emitColumn called for "+
				"(extension=%q name=%q), but pg_trgm declares no column types",
			t.Extension, t.Name)
	},
	indexAccessMethods: map[string]struct{}{},
	indexOperatorClasses: map[string]struct{}{
		"gin_trgm_ops":  {},
		"gist_trgm_ops": {},
	},
}

// pgHstoreDef is the catalog entry for the `hstore` extension
// (ADR-0032 Tier 1). hstore defines:
//
//   - One scalar type: `hstore` (a key/value-pair store within a
//     single column). No type modifiers — hstore values are
//     untyped key/value maps with no per-column length/cardinality
//     constraint.
//
//   - GIN / GiST operator classes (`gin_hstore_ops`,
//     `gist_hstore_ops`) for index-backed key lookup. These are
//     out-of-v1-scope per the research doc; sluice does not declare
//     them in [indexOperatorClasses] for the hstore catalog entry.
//     A future PR can add the opclass round-trip if operator demand
//     surfaces — the pattern is mechanical (mirror pg_trgm).
//
// Same-engine PG → PG passthrough is byte-for-byte: the schema
// reader emits `ir.ExtensionType{Extension: "hstore", Name:
// "hstore"}`, the writer emits the bareword `hstore` in the
// CREATE TABLE column position, and the value decoder + driver
// round-trip hstore's text form (e.g. `"a"=>"1", "b"=>"2"`)
// verbatim.
//
// Cross-engine PG → MySQL maps hstore to MySQL JSON. The hstore
// wire format is PG-specific (the `=>` arrow syntax) so the MySQL
// writer's prepareValue parses the source text into a JSON object
// at write-time. See `internal/engines/mysql/row_writer.go::
// prepareValue` for the conversion path and
// `crossEngineDefaultTranslatedExtensions` for the policy declaration.
var pgHstoreDef = extensionDef{
	typesByName: map[string]struct{}{
		"hstore": {},
	},
	build: func(udtName string, _ int32) (ir.ExtensionType, error) {
		if udtName != "hstore" {
			return ir.ExtensionType{}, fmt.Errorf(
				"postgres: hstore catalog: unexpected udt_name %q (want \"hstore\")", udtName)
		}
		return ir.ExtensionType{
			Extension: "hstore",
			Name:      "hstore",
		}, nil
	},
	emitColumn: func(t ir.ExtensionType) (string, error) {
		if t.Extension != "hstore" || t.Name != "hstore" {
			return "", fmt.Errorf(
				"postgres: hstore emit: unexpected (extension=%q name=%q); "+
					"want (hstore, hstore)", t.Extension, t.Name)
		}
		if len(t.Modifiers) != 0 {
			return "", fmt.Errorf(
				"postgres: hstore emit: hstore has no type modifiers (got %d)",
				len(t.Modifiers))
		}
		return "hstore", nil
	},
	indexAccessMethods:   map[string]struct{}{},
	indexOperatorClasses: map[string]struct{}{},
}

// pgCiTextDef is the catalog entry for the `citext` extension
// (ADR-0032 Tier 1). citext defines:
//
//   - One scalar type: `citext` (case-insensitive text). citext
//     is `text` with a custom collation; queries against citext
//     columns are case-insensitive by default (no explicit
//     LOWER() / UPPER() wrapping required).
//
//   - No new index access methods. citext rides on core PG btree /
//     gin / gist when indexed (same as plain text); no operator
//     classes specific to citext that sluice needs to round-trip.
//
// Same-engine PG → PG passthrough is byte-for-byte: the schema
// reader emits `ir.ExtensionType{Extension: "citext", Name:
// "citext"}`, the writer emits the bareword `citext` in the
// CREATE TABLE column position, and values round-trip as plain
// strings (the case-insensitive comparison is a server-side
// property of the type, not a wire-format concern).
//
// Cross-engine PG → MySQL maps citext to MySQL VARCHAR with the
// case-insensitive collation `utf8mb4_0900_ai_ci`. Value
// translation is identity (no encoding conversion). The default
// length on the MySQL side is 255; operators wanting a different
// length use `--type-override` per column. See
// `internal/engines/mysql/ddl_emit.go::emitColumnType` for the
// emit path and `crossEngineDefaultTranslatedExtensions` for the
// policy declaration.
var pgCiTextDef = extensionDef{
	typesByName: map[string]struct{}{
		"citext": {},
	},
	build: func(udtName string, _ int32) (ir.ExtensionType, error) {
		if udtName != "citext" {
			return ir.ExtensionType{}, fmt.Errorf(
				"postgres: citext catalog: unexpected udt_name %q (want \"citext\")", udtName)
		}
		return ir.ExtensionType{
			Extension: "citext",
			Name:      "citext",
		}, nil
	},
	emitColumn: func(t ir.ExtensionType) (string, error) {
		if t.Extension != "citext" || t.Name != "citext" {
			return "", fmt.Errorf(
				"postgres: citext emit: unexpected (extension=%q name=%q); "+
					"want (citext, citext)", t.Extension, t.Name)
		}
		if len(t.Modifiers) != 0 {
			return "", fmt.Errorf(
				"postgres: citext emit: citext has no type modifiers (got %d)",
				len(t.Modifiers))
		}
		return "citext", nil
	},
	indexAccessMethods:   map[string]struct{}{},
	indexOperatorClasses: map[string]struct{}{},
}

// pgPostGISDef is the catalog entry for the `postgis` extension
// (ADR-0032 Tier 2, the final v1-shortlist entry). PostGIS is unique
// in the catalog because its core types (`geometry`, `geography`)
// pre-date ADR-0032 and already have first-class IR representations:
//
//   - `geometry` maps to [ir.Geometry] (with `Subtype` and `SRID`
//     fields) per ADR-0035 / v0.28.0. Same-engine PG → PG round-trip
//     uses the existing PostGIS-aware emit path
//     (postgis.go::postgisSubtypeName + ddl_emit.go::Geometry case),
//     gated on the writer's `HasPostGIS` flag.
//   - `geography` has no IR representation today; surfaces via the
//     hint path only (operators get a "--enable-pg-extension postgis"
//     pointer if they hit it).
//
// As a result the catalog entry's [typesByName] is empty (the
// catalog's build/emit machinery is bypassed for geometry / geography
// — they ride on [ir.Geometry] and don't dispatch through
// [ir.ExtensionType]). [hintTypeNames] holds both type names so
// [extensionOwningType] surfaces the actionable refusal on the
// schema reader's "USER-DEFINED type I don't recognise" fallthrough.
//
// The load-bearing v1 surface that this entry DOES contribute is the
// operator-class declaration set. PostGIS introduces operator classes
// over core PG access methods (gist, spgist, brin) for both geometry
// and geography. With `--enable-pg-extension postgis`, the schema
// reader's index-population path preserves these via
// [ir.IndexColumn.OperatorClass] so the same-engine writer emits the
// opclass and PG selects the matching index strategy on the target.
// Without the flag, the opclass is dropped from the IR (and a WARN
// fires per the loud-failure default, same shape as pg_trgm — the
// CREATE INDEX on the target falls back to the AM's default
// opclass, which for gist/geometry is `gist_geometry_ops_2d` so
// the index still works for the 2D case but loses fidelity for nD /
// brin / spgist variants).
//
// The operator-class set covers the catalog PostGIS ships in 3.x:
//
//   - GiST default 2D + nD opclasses: `gist_geometry_ops_2d`,
//     `gist_geometry_ops_nd`, `gist_geography_ops`.
//   - SP-GiST opclasses: `spgist_geometry_ops_2d`,
//     `spgist_geometry_ops_3d`, `spgist_geometry_ops_nd`.
//   - BRIN inclusion opclasses: `brin_geometry_inclusion_ops_2d`,
//     `brin_geometry_inclusion_ops_4d`,
//     `brin_geometry_inclusion_ops_nd`.
//
// PostGIS introduces no new index access methods — gist / spgist /
// brin are core PG. [indexAccessMethods] stays empty; the
// per-opclass passthrough rides on the `idx.Method == ""` path that
// pg_trgm already exercises (extensionOperatorClassEnabled).
//
// Cross-engine PG → MySQL: NOT a default translator. PostGIS's
// cross-engine path landed in v0.28.0 (ADR-0035) via the IR's
// `ir.Geometry` direct mapping to MySQL's spatial column — the
// `--enable-pg-extension postgis` flag isn't required for that path
// (operators with PG `geometry` columns get the cross-engine
// translation regardless). The flag's role is purely PG → PG
// passthrough: opt into the per-opclass round-trip surface that the
// catalog-driven IR machinery enables. Operators passing
// `--enable-pg-extension postgis` against a non-PG target will be
// refused at [validateEnabledPGExtensions] per the loud-failure
// default for non-translator extensions.
var pgPostGISDef = extensionDef{
	typesByName: map[string]struct{}{},
	build: func(udtName string, _ int32) (ir.ExtensionType, error) {
		return ir.ExtensionType{}, fmt.Errorf(
			"postgres: postgis catalog: build called with udt_name %q, "+
				"but postgis types route through ir.Geometry (not ir.ExtensionType); "+
				"this is a framework misuse",
			udtName)
	},
	emitColumn: func(t ir.ExtensionType) (string, error) {
		return "", fmt.Errorf(
			"postgres: postgis catalog: emitColumn called for "+
				"(extension=%q name=%q), but postgis types route through "+
				"ir.Geometry's emit path (ddl_emit.go::Geometry case); "+
				"this is a framework misuse",
			t.Extension, t.Name)
	},
	indexAccessMethods: map[string]struct{}{},
	indexOperatorClasses: map[string]struct{}{
		// GiST — the canonical PostGIS index path.
		"gist_geometry_ops_2d": {},
		"gist_geometry_ops_nd": {},
		"gist_geography_ops":   {},
		// SP-GiST — alternative spatial index strategy.
		"spgist_geometry_ops_2d": {},
		"spgist_geometry_ops_3d": {},
		"spgist_geometry_ops_nd": {},
		// BRIN — block-range coarse spatial index.
		"brin_geometry_inclusion_ops_2d": {},
		"brin_geometry_inclusion_ops_4d": {},
		"brin_geometry_inclusion_ops_nd": {},
	},
	hintTypeNames: map[string]struct{}{
		"geometry":  {},
		"geography": {},
	},
}

// pgCryptoDef is the catalog entry for the `pgcrypto` extension —
// PG's standard cryptographic-functions contrib extension. Like
// PostGIS, pgcrypto has no types sluice needs to passthrough (the
// catalog's typesByName / hintTypeNames are empty); the catalog
// entry exists purely as a **presence gate** for the v0.38.0
// SHA1/SHA2 expression-translator rewrites in expr_translate.go.
//
// When the operator passes `--enable-pg-extension pgcrypto`,
// sluice's `validateAndPreflightExtensions` machinery (see below)
// runs the standard pg_extension preflight check on the target
// before any data moves; the SHA rewrites then fire confidently
// because the extension is known to be installed. Without the flag,
// SHA1/SHA2 calls fall through verbatim and PG's parse-time error
// surfaces the missing extension to the operator.
//
// build/emitColumn refuse loudly — pgcrypto types should never
// dispatch through here. indexAccessMethods and
// indexOperatorClasses are empty (pgcrypto introduces neither).
// hintTypeNames is empty (no operator-visible "you have a pgcrypto
// type" hint surface needed).
//
// ADR-0044 adds defaultExprFunctions: the crypto/digest barewords
// pgcrypto owns that legitimately appear in column DEFAULT /
// GENERATED expressions. The v0.38.0 SHA1/SHA2 expr-translator
// presence-gate role is UNCHANGED — defaultExprFunctions is an
// additive surface for the Tier-3 schema-read gate; it does not
// touch expr_translate.go's rewrite path.
//
// `gen_random_uuid` is deliberately ABSENT from the set — it is
// core PostgreSQL 13+, not pgcrypto-owned on any supported modern
// PG. Listing it would make the Tier-3 gate refuse valid core-PG
// schemas (ADR-0044's load-bearing core-vs-extension correctness
// guard; pinned by TestScanExtensionFunction_GenRandomUUIDNotGated).
var pgCryptoDef = extensionDef{
	typesByName: map[string]struct{}{},
	build: func(udtName string, _ int32) (ir.ExtensionType, error) {
		return ir.ExtensionType{}, fmt.Errorf(
			"postgres: pgcrypto catalog: build called with udt_name %q, "+
				"but pgcrypto has no sluice-passthrough types — the "+
				"catalog entry is a presence-gate for the SHA1/SHA2 "+
				"expression translator only; this is a framework misuse",
			udtName)
	},
	emitColumn: func(t ir.ExtensionType) (string, error) {
		return "", fmt.Errorf(
			"postgres: pgcrypto catalog: emitColumn called for "+
				"(extension=%q name=%q), but pgcrypto has no "+
				"sluice-passthrough types; this is a framework misuse",
			t.Extension, t.Name)
	},
	indexAccessMethods:   map[string]struct{}{},
	indexOperatorClasses: map[string]struct{}{},
	hintTypeNames:        map[string]struct{}{},
	defaultExprFunctions: map[string]struct{}{
		"digest":           {},
		"hmac":             {},
		"crypt":            {},
		"gen_salt":         {},
		"gen_random_bytes": {},
		"encrypt":          {},
		"decrypt":          {},
		"encrypt_iv":       {},
		"decrypt_iv":       {},
		"pgp_sym_encrypt":  {},
		"pgp_sym_decrypt":  {},
		"pgp_pub_encrypt":  {},
		"pgp_pub_decrypt":  {},
		// NOTE: gen_random_uuid is intentionally NOT here — core PG
		// 13+, not pgcrypto. See the doc comment above.
	},
}

// pgUUIDOSSPDef is the catalog entry for the `uuid-ossp` extension
// (ADR-0044 Tier 3). uuid-ossp has no types or indexes sluice needs
// to passthrough — its entire sluice-relevant surface is the set of
// UUID-generator functions that appear in column DEFAULT clauses
// (`DEFAULT uuid_generate_v4()` is the canonical case) and, less
// commonly, in GENERATED expressions.
//
// The entry mirrors pgCryptoDef's shape exactly: typesByName /
// indexAccessMethods / indexOperatorClasses / hintTypeNames are all
// empty; build/emitColumn refuse loudly because a uuid-ossp type
// should never dispatch through the catalog's type machinery (uuid
// columns are `ir.UUID`, not `ir.ExtensionType`). The only populated
// surface is defaultExprFunctions.
//
// The catalog NAME has a hyphen (`uuid-ossp`) — that is the value of
// both `pg_extension.extname` and the `--enable-pg-extension`
// argument; the flag-splitting path must not choke on it (it does
// not — the flag is repeatable, one extension per occurrence, no
// comma-splitting).
//
// Same-engine PG → PG: with `--enable-pg-extension uuid-ossp` the
// expression passes through verbatim (today's accidental behaviour,
// now explicit) and the existing validateAndPreflightExtensions
// machinery preflights uuid-ossp's presence on both source and
// target. Without the flag, the Tier-3 schema-read gate refuses
// loudly and early (ADR-0044 §2).
//
// Cross-engine PG → MySQL: uuid_generate_v1/v1mc/v4() translate to
// MySQL `(UUID())` via pgToMySQLDefaultExpr (ADR-0044 §3); the
// uuid-ossp version distinction does not survive — a DEFAULT means
// "generate a UUID", version-agnostic in practice.
var pgUUIDOSSPDef = extensionDef{
	typesByName: map[string]struct{}{},
	build: func(udtName string, _ int32) (ir.ExtensionType, error) {
		return ir.ExtensionType{}, fmt.Errorf(
			"postgres: uuid-ossp catalog: build called with udt_name %q, "+
				"but uuid-ossp has no sluice-passthrough types — the "+
				"catalog entry exists for the ADR-0044 Tier-3 "+
				"function-default gate only; this is a framework misuse",
			udtName)
	},
	emitColumn: func(t ir.ExtensionType) (string, error) {
		return "", fmt.Errorf(
			"postgres: uuid-ossp catalog: emitColumn called for "+
				"(extension=%q name=%q), but uuid-ossp has no "+
				"sluice-passthrough types; this is a framework misuse",
			t.Extension, t.Name)
	},
	indexAccessMethods:   map[string]struct{}{},
	indexOperatorClasses: map[string]struct{}{},
	hintTypeNames:        map[string]struct{}{},
	defaultExprFunctions: map[string]struct{}{
		"uuid_generate_v1":   {},
		"uuid_generate_v1mc": {},
		"uuid_generate_v4":   {},
		"uuid_generate_v5":   {},
		"uuid_nil":           {},
		"uuid_ns_dns":        {},
		"uuid_ns_url":        {},
		"uuid_ns_oid":        {},
		"uuid_ns_x500":       {},
	},
}

// scanExtensionFunctionInExpr is the ADR-0044 conservative
// function-call token scanner. Given a column DEFAULT / GENERATED
// expression string and the operator's enabled-extension set, it
// reports the first catalog-declared extension-owned function the
// expression references, plus the owning extension.
//
// It is deliberately NOT a SQL parser. It walks the expression
// byte-by-byte, skipping single-quoted string literals (so
// `DEFAULT 'uuid_generate_v4()'` — a literal text default — is NOT
// matched), and at each identifier-start it reads the bareword and
// checks whether (a) it is immediately followed by `(` (modulo
// whitespace) and (b) it is one of the catalog's
// defaultExprFunctions barewords.
//
// Conservatism rules (ADR-0044 §Gotchas — false negatives degrade to
// today's late-failure which is acceptable; false positives that gate
// a core/user function are the real hazard, so the matcher is tight):
//
//   - Word boundary on BOTH sides: a name is matched only when the
//     byte before it is not an identifier byte and the bareword that
//     the scan reads equals the catalog name exactly. So
//     `my_uuid_generate_v4(` (the scanned bareword is
//     `my_uuid_generate_v4`, not in any set) and
//     `uuid_generate_v4_ext(` do NOT match.
//   - Qualified names are NOT matched: if the matched name is
//     immediately preceded by `.` (e.g. `public.uuid_generate_v4()`
//     or `x.uuid_generate_v4()`), the scanner skips it. Rationale:
//     a schema-qualified call is rare in a DEFAULT clause, and
//     refusing to gate it is the conservative choice — a missed gate
//     degrades to the pre-ADR-0044 late parse-error (no worse than
//     status quo), whereas gating a same-named user function in some
//     other schema would be a false-positive refusal of a valid
//     schema. Documented per ADR-0044 §Gotchas.
//   - Case-insensitive on the function name (PG lower-cases unquoted
//     identifiers; pg_get_expr output casing is not version-stable).
//
// enabledExtensions is the operator's `--enable-pg-extension` set; it
// is NOT consulted here — the scanner reports what the expression
// references against the FULL catalog so the caller (the schema-read
// gate) can produce the "you referenced X owned by ext Y; enable it"
// message even when the operator did not enable Y. The gate decides
// pass-vs-refuse from the enabled set; the scanner only classifies.
//
// Returns (fnName, owningExtension, true) on the first hit (scan
// order is left-to-right; the first extension function wins), or
// ("", "", false) when the expression references no catalog
// extension function (core functions like gen_random_uuid(), now(),
// nextval() are absent from every defaultExprFunctions set and so
// always return false — the load-bearing core-vs-extension guard).
func scanExtensionFunctionInExpr(expr string) (fnName, owningExtension string, found bool) {
	if expr == "" {
		return "", "", false
	}
	for i := 0; i < len(expr); {
		c := expr[i]
		if c == '\'' {
			// Skip the whole single-quoted string literal — a
			// function name inside a literal is data, not a call.
			i = scanStringLiteral(expr, i)
			continue
		}
		if !isIdentifierStartByte(c) {
			i++
			continue
		}
		// Read the bareword token [start, j).
		start := i
		j := i + 1
		for j < len(expr) && isIdentifierByte(expr[j]) {
			j++
		}
		word := expr[start:j]

		// Must be a function call: next non-space byte is '('.
		k := j
		for k < len(expr) && (expr[k] == ' ' || expr[k] == '\t' || expr[k] == '\n' || expr[k] == '\r') {
			k++
		}
		isCall := k < len(expr) && expr[k] == '('
		if isCall {
			// Skip schema/table-qualified references conservatively:
			// a leading `.` immediately before the token (modulo no
			// whitespace) means this is `qualifier.word(` — not a
			// bare extension-function call.
			qualified := start > 0 && expr[start-1] == '.'
			if !qualified {
				if ext, ok := lookupExtensionOwningFunction(word); ok {
					return word, ext, true
				}
			}
		}
		i = j
	}
	return "", "", false
}

// isIdentifierStartByte reports whether b can begin an SQL bareword
// identifier. ASCII letters + underscore (digits cannot start an
// identifier; the extension function barewords never do).
func isIdentifierStartByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z':
		return true
	case b >= 'A' && b <= 'Z':
		return true
	case b == '_':
		return true
	}
	return false
}

// lookupExtensionOwningFunction returns the extension that declares
// fnName in its defaultExprFunctions set, matching case-insensitively
// (PG lower-cases unquoted identifiers). Consults the FULL catalog —
// not the operator's enabled subset — so the schema-read gate can
// name the owning extension in its refusal even when the operator
// did not enable it. Core functions (gen_random_uuid, now, …) are in
// no set and return ok=false — the ADR-0044 core-vs-extension guard.
func lookupExtensionOwningFunction(fnName string) (extension string, ok bool) {
	if fnName == "" {
		return "", false
	}
	lower := strings.ToLower(fnName)
	for ext, def := range pgExtensionCatalog {
		if len(def.defaultExprFunctions) == 0 {
			continue
		}
		if _, hit := def.defaultExprFunctions[lower]; hit {
			return ext, true
		}
	}
	return "", false
}

// extensionFunctionDefaultGate is the ADR-0044 §2 schema-read gate.
// For a column whose DEFAULT is an [ir.DefaultExpression] or whose
// GENERATED expression is non-empty, it scans the expression text for
// a catalog-declared extension-owned function. When one is found and
// the owning extension is NOT in enabledExtensions, it returns a
// loud, operator-actionable refusal naming the column, the function,
// the owning extension, the `--enable-pg-extension <ext>` fix, and
// `--exclude-table` as the skip escape (ADR-0044 §2 message shape).
//
// The message deliberately does NOT name `--expr-override`: that
// override (ApplyExpressionOverrides) rewrites only generated-column
// expressions, never DEFAULTs, and runs *after* ReadSchema — this
// gate fires inside ReadSchema, so the run aborts before any
// override could apply. Naming it would be inaccurate operator
// guidance (verified against the pipeline ordering; ADR-0044 §2
// corrected post-implementation).
//
// When the extension IS enabled the expression passes through
// unchanged (today's verbatim behaviour) — return nil; the
// validateAndPreflightExtensions machinery (which already runs for
// every catalog extension, uuid-ossp / pgcrypto included once
// registered) handles the target-presence preflight cleanly and
// early.
//
// Core functions (gen_random_uuid(), now(), nextval(), …) are never
// gated — they are in no defaultExprFunctions set so the scanner
// returns found=false and this returns nil unconditionally.
//
// exprKind is "DEFAULT" or "GENERATED" — woven into the message so
// the operator can tell which clause tripped the gate.
func extensionFunctionDefaultGate(
	tableName, colName, exprKind, exprText string,
	enabledExtensions map[string]bool,
) error {
	fn, owningExt, found := scanExtensionFunctionInExpr(exprText)
	if !found {
		return nil
	}
	if enabledExtensions[owningExt] {
		return nil
	}
	return fmt.Errorf(
		"postgres: column %q.%q %s expression references %s(), which is "+
			"owned by the %q extension. Re-run with "+
			"--enable-pg-extension %s so sluice preflights the extension "+
			"on the target (ADR-0032/ADR-0044 opt-in passthrough), or "+
			"--exclude-table to skip this table (note: --expr-override "+
			"does not apply — it rewrites only generated-column "+
			"expressions and runs after schema-read, while this gate "+
			"fires during schema-read)",
		tableName, colName, exprKind, fn, owningExt, owningExt)
}

// emitExtensionColumn dispatches an [ir.ExtensionType] column to the
// catalog entry's column-DDL renderer. Returns a clear error when
// the extension isn't in the catalog (operator forgot the
// `--enable-pg-extension` flag, or the IR was hand-constructed with
// an unknown extension name).
func emitExtensionColumn(t ir.ExtensionType) (string, error) {
	def, ok := pgExtensionCatalog[t.Extension]
	if !ok {
		return "", fmt.Errorf(
			"postgres: extension %q is not in the catalog "+
				"(recognised: %s); run with --enable-pg-extension or "+
				"add a catalog entry",
			t.Extension, strings.Join(recognisedPGExtensionNames(), ", "))
	}
	return def.emitColumn(t)
}

// lookupExtensionForType returns the (extension, type-name) pair
// that owns udtName when udtName is registered with one of the
// catalog entries the operator enabled. Returns ("", "", false)
// otherwise — the column falls through to the existing user-defined
// dispatch (enum / loud failure).
//
// This is the schema reader's filter into the catalog. It runs on
// every column whose information_schema.data_type is "USER-DEFINED",
// so the lookup must be cheap; the catalog's typesByName maps are
// O(1) lookups.
func lookupExtensionForType(udtName string, enabledExtensions map[string]bool) (extension, typeName string, ok bool) {
	if udtName == "" || len(enabledExtensions) == 0 {
		return "", "", false
	}
	for ext, def := range pgExtensionCatalog {
		if !enabledExtensions[ext] {
			continue
		}
		if _, ok := def.typesByName[udtName]; ok {
			return ext, udtName, true
		}
	}
	return "", "", false
}

// validateAndPreflightExtensions is the shared
// validate-and-source-presence-check helper used by both PG's
// SchemaReader and SchemaWriter when EnableExtensions is invoked.
//
// Two checks fire in order:
//
//  1. Each requested name must be in pgExtensionCatalog. Unknown
//     names refuse loudly with the recognised set listed (operator
//     typo).
//  2. Each requested name must currently be present in pg_extension
//     on the connected database. Missing extensions refuse loudly
//     with an operator-actionable hint (`CREATE EXTENSION X;`).
//
// Returns the validated map[name]bool the caller stores on the
// reader/writer for downstream lookups. Empty / nil input is a
// no-op (returns nil, nil).
//
// The "side" string ("source" or "target") is woven into the error
// message so the operator can tell which DSN is missing the
// extension without having to inspect both.
func validateAndPreflightExtensions(ctx context.Context, db *sql.DB, extensions []string) (map[string]bool, error) {
	return validateAndPreflightExtensionsAt(ctx, db, extensions, "")
}

// validateAndPreflightExtensionsAt is the variant that names which
// side ("source" / "target") the database represents in the
// operator-facing error messages. Empty side defaults to a generic
// "this database" wording.
func validateAndPreflightExtensionsAt(ctx context.Context, db *sql.DB, extensions []string, side string) (map[string]bool, error) {
	if len(extensions) == 0 {
		return nil, nil
	}

	enabled := make(map[string]bool, len(extensions))
	for _, name := range extensions {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok := pgExtensionCatalog[name]; !ok {
			return nil, fmt.Errorf(
				"postgres: --enable-pg-extension %q is not a recognised "+
					"extension (recognised: %s); see "+
					"docs/research/pg-extensions-deployment-frequency.md "+
					"for the v1 shortlist (vector, pg_trgm, hstore, citext, "+
					"postgis — the full v1 set is shipped). pgcrypto added "+
					"in v0.38.0 as a presence-gate for the SHA1/SHA2 "+
					"expression-translator rewrites",
				name, strings.Join(recognisedPGExtensionNames(), ", "))
		}
		enabled[name] = true
	}
	if len(enabled) == 0 {
		return nil, nil
	}

	if err := preflightExtensionsInstalled(ctx, db, enabled, side); err != nil {
		return nil, err
	}
	return enabled, nil
}

// preflightExtensionsInstalled queries pg_extension and returns a
// wrapped error naming the first missing extension. The caller has
// already validated that every name in extensions is in the catalog.
func preflightExtensionsInstalled(ctx context.Context, db *sql.DB, extensions map[string]bool, side string) error {
	if len(extensions) == 0 {
		return nil
	}

	names := make([]string, 0, len(extensions))
	for name := range extensions {
		names = append(names, name)
	}

	// Build a parameterised SELECT EXISTS check per extension. PG's
	// pg_extension is small (~10s of rows on a typical install), so
	// we trade one round-trip for one query per name rather than
	// constructing a single ANY($1) call. The clarity of the
	// error message ("which extension is missing?") wins over the
	// micro-optimisation.
	for _, name := range names {
		var present bool
		err := db.QueryRowContext(ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = $1)",
			name).Scan(&present)
		if err != nil {
			return fmt.Errorf(
				"postgres: --enable-pg-extension preflight: query "+
					"pg_extension for %q: %w",
				name, err)
		}
		if !present {
			return missingExtensionError(name, side)
		}
	}
	return nil
}

// missingExtensionError builds the operator-actionable error a
// missing-extension preflight surfaces. side ("source" / "target")
// names which DSN is missing the extension; empty side falls back
// to a generic wording.
func missingExtensionError(name, side string) error {
	where := "the connected database"
	if side != "" {
		where = "the " + side + " database"
	}
	return fmt.Errorf(
		"postgres: --enable-pg-extension %q: extension is not installed on %s — "+
			"run `CREATE EXTENSION %s;` (or the extension's documented install "+
			"command) and re-run sluice; sluice does not auto-install extensions "+
			"per the contain-Postgres-complexity tenet",
		name, where, name)
}

// errExtensionPreflight is the sentinel that wraps every
// extension-preflight refusal so callers (tests, the orchestrator's
// validate step) can branch on the failure mode without string
// matching. Used via [errors.Is] under the wrapped fmt.Errorf chain.
//
//nolint:unused // exported sentinel; tests assert against it
var errExtensionPreflight = errors.New("postgres: extension preflight refused")

// extensionAccessMethodEnabled reports whether `method` is the
// access-method name of an extension that's in the catalog (and
// therefore safe to emit verbatim in `CREATE INDEX ... USING
// <method>`). Used by the schema writer's index emitter to
// disambiguate "operator typo'd a method" from "operator passed
// through a known extension's index AM."
//
// enabledExtensions is the subset of the catalog the operator opted
// into via `--enable-pg-extension`. A method that's in some
// extension's catalog entry but the operator didn't enable that
// extension surfaces a clear refusal — same shape as the type-
// passthrough preflight.
func extensionAccessMethodEnabled(method string, enabledExtensions map[string]bool) bool {
	for ext, def := range pgExtensionCatalog {
		if !enabledExtensions[ext] {
			continue
		}
		if _, ok := def.indexAccessMethods[method]; ok {
			return true
		}
	}
	return false
}

// extensionOperatorClassEnabled reports whether `opclass` is the
// operator-class name of an extension that's in the catalog AND
// that the operator has opted into via `--enable-pg-extension`.
// Used by the schema reader's index-population path to recognise
// extension-owned opclasses on indexes whose access method itself is
// core PG (the load-bearing pg_trgm case: `gin (col gin_trgm_ops)`
// uses core `gin` as the AM but the extension's `gin_trgm_ops` as the
// opclass).
//
// An opclass that's in some extension's catalog entry but the
// operator didn't enable that extension returns false — same shape
// as the extensionAccessMethodEnabled gate. This preserves the
// loud-failure default: without the flag, the opclass is dropped
// from the IR and the writer emits the index without it (PG falls
// back to the AM's default opclass, which for `gin` over `text`
// will fail at CREATE INDEX with no default operator class — a
// loud failure surfaced at write-time rather than silently emitted).
func extensionOperatorClassEnabled(opclass string, enabledExtensions map[string]bool) bool {
	if opclass == "" {
		return false
	}
	for ext, def := range pgExtensionCatalog {
		if !enabledExtensions[ext] {
			continue
		}
		if _, ok := def.indexOperatorClasses[opclass]; ok {
			return true
		}
	}
	return false
}

// extensionOperatorClassRegistered reports whether `opclass` is the
// operator-class name of any extension in the catalog (regardless of
// whether the operator enabled it). Used by the cross-engine
// refusal path in `internal/pipeline/cross_engine_supportable.go`
// indirectly via the [ir.IndexColumn.OperatorClass] non-empty
// signal — sluice only populates that field for extension-owned
// opclasses, so any non-empty value passing through the IR is by
// construction extension-introduced. This helper exists for tests
// and for symmetry with extensionOperatorClassEnabled.
//
//nolint:unused // exposed for tests; mirrors extensionAccessMethodEnabled
func extensionOperatorClassRegistered(opclass string) bool {
	if opclass == "" {
		return false
	}
	for _, def := range pgExtensionCatalog {
		if _, ok := def.indexOperatorClasses[opclass]; ok {
			return true
		}
	}
	return false
}

// extensionOwningType returns the name of the extension that owns
// the named PG type (information_schema.columns.udt_name), or "" if
// no catalog entry claims it. Used by the schema reader's
// user-defined-fallthrough path to surface a clean operator-
// actionable hint when an unenabled extension type appears (ADR-0032).
//
// Consults both [extensionDef.typesByName] (types dispatched through
// the catalog's build/emit as [ir.ExtensionType]) and
// [extensionDef.hintTypeNames] (types with a first-class IR shape —
// today's only entry is PostGIS `geometry`, whose IR type is
// [ir.Geometry] not [ir.ExtensionType]). The hint surface is the same
// either way: "pass --enable-pg-extension X to opt in".
func extensionOwningType(udtName string) string {
	if udtName == "" {
		return ""
	}
	for ext, def := range pgExtensionCatalog {
		if _, ok := def.typesByName[udtName]; ok {
			return ext
		}
		if _, ok := def.hintTypeNames[udtName]; ok {
			return ext
		}
	}
	return ""
}

// extensionOwningOperatorClass returns the extension name that owns
// `opclass` per the catalog, or "" if no extension claims it. Used by
// the writer's error-hint path to suggest the right
// `--enable-pg-extension <name>` flag when a CREATE INDEX fails on an
// extension-owned opclass that the operator forgot to enable.
func extensionOwningOperatorClass(opclass string) string {
	if opclass == "" {
		return ""
	}
	for ext, def := range pgExtensionCatalog {
		if _, ok := def.indexOperatorClasses[opclass]; ok {
			return ext
		}
	}
	return ""
}
