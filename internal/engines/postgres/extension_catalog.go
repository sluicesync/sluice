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
}

// pgExtensionCatalog is the registry of recognised PG extensions
// available for `--enable-pg-extension` passthrough. Adding an
// extension is an entry here — no interface changes, no per-call-site
// switch updates. Keys are the canonical pg_extension.extname value.
var pgExtensionCatalog = map[string]extensionDef{
	"vector":  pgVectorDef,
	"pg_trgm": pgTrgmDef,
	// hstore / citext / postgis follow in subsequent point
	// releases per the v1 shortlist (docs/research/pg-extensions-
	// deployment-frequency.md).
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
					"for the v1 shortlist (vector, pg_trgm shipped; "+
					"hstore / citext / postgis follow as catalog entries)",
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
