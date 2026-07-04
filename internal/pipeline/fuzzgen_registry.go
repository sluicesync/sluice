// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Type-family registry for the generative round-trip fuzz harness
// (Track 2, Phase 1). This is the *heart* of the harness, per design
// decision #2 of docs/dev/notes/prep-generative-roundtrip-fuzz-harness.md.
//
// Each registered family knows four things:
//
//   (a) how to emit its column DDL in each source dialect (PG / MySQL),
//       at a given shape (scalar, 1-D array, multi-dim ≥2-D);
//   (b) how to generate N random values including the catalogued edge
//       cases (NULL whole-value, NULL element, multi-byte/emoji, min/max,
//       boundary precision, empty, ≥2-D nesting);
//   (c) how to render a value to a per-engine canonical text form for the
//       oracle — this mirrors the battle-test `::text` approach exactly
//       (see migrate_bug7374/75/69_integration_test.go);
//   (d) per migration direction, the *expected* behaviour: faithful
//       round-trip, a documented loud-refuse, or a documented lossy
//       degradation (zone-flatten / array→JSON / wide-varchar down-map).
//
// Decisions (d) are sourced verbatim from docs/type-mapping.md +
// docs/dev/notes catalogued cross-engine limitations — NOT invented here.
// This file carries no build tag: the registry is pure logic and is
// unit-tested by fuzzgen_registry_test.go without Docker.

package pipeline

import (
	"fmt"
	"math/rand"
	"strings"
)

// engineKind is the engine flavour axis. Phase 1 is vanilla mysql:8.0 /
// postgres:16; Phase 2 (Track 1) adds Vitess/PlanetScale flavours by
// extending this enum + the per-kind DDL/oracle branches — an extension,
// not a rewrite (design decision #5). SQLite joined that way: a file-based
// engine (no container), source AND target (ADR-0128/0129/0134), always
// cross-engine here (no sqlite→sqlite direction — see allDirections).
type engineKind int

const (
	enginePG engineKind = iota
	engineMySQL
	engineSQLite
)

func (e engineKind) String() string {
	switch e {
	case enginePG:
		return "postgres"
	case engineMySQL:
		return "mysql"
	case engineSQLite:
		return "sqlite"
	default:
		return "unknown"
	}
}

// shape is the per-family value axis: every family is exercised at every
// applicable shape (the "pin the class, not the representative"
// discipline operationalised — design decision #2).
type shape int

const (
	shapeScalar    shape = iota // a plain column
	shape1DArray                // T[]
	shapeMultiDim               // T[][]  (≥2-D)
	shapeShapeLast = shapeMultiDim
)

func (s shape) String() string {
	switch s {
	case shapeScalar:
		return "scalar"
	case shape1DArray:
		return "1d-array"
	case shapeMultiDim:
		return "multidim-array"
	default:
		return "unknown-shape"
	}
}

// outcome is the per-direction expected behaviour. The three-outcome
// oracle (design decision #3) hinges on distinguishing these:
//
//   - outcomeFaithful     — migrate exits 0 AND src==dst canonical text.
//   - outcomeLoudRefuse    — migrate exits ≠0 at preview/preflight with
//     NO partial target. A loud refusal is a PASS
//     (loud-failure tenet); a *missing* refusal or
//     a partial target is a FAIL.
//   - outcomeLossyDocument — migrate exits 0 but the value is
//     transformed by a *documented* cross-engine
//     degradation (PG array → MySQL JSON,
//     timetz → MySQL zone-flatten, wide varchar →
//     MySQL TEXT). Value-equality is intentionally
//     NOT asserted (asserting it would reproduce
//     the v0.69.0 #16 false-positive class); the
//     oracle asserts only that migrate succeeded
//     and the column exists on the target.
type outcome int

const (
	outcomeFaithful outcome = iota
	outcomeLoudRefuse
	outcomeLossyDocument
)

func (o outcome) String() string {
	switch o {
	case outcomeFaithful:
		return "faithful"
	case outcomeLoudRefuse:
		return "loud-refuse"
	case outcomeLossyDocument:
		return "lossy-documented"
	default:
		return "unknown-outcome"
	}
}

// direction is an ordered (source, target) engine-kind pair.
type direction struct {
	src, dst engineKind
}

func (d direction) String() string { return d.src.String() + "->" + d.dst.String() }

// allDirections is the direction matrix: the Phase-1 four (MySQL→PG,
// PG→MySQL, PG→PG, MySQL→MySQL) plus the SQLite directions the engine
// matrix supports — SQLite as a migrate SOURCE (ADR-0128/0129) into both
// server engines, and SQLite as a migrate TARGET (ADR-0134) from both.
// There is deliberately NO sqlite→sqlite direction: the same-engine
// faithful oracle compares canonical text, and the SQLite writer
// re-canonicalises temporal text on write (ADR-0134 §2), so a byte-text
// compare would need writer-canonicalisation alignment — the SQLite→X→SQLite
// value identity is pinned by the hand-authored round-trip fixtures
// (migrate_sqlite_target_cross_integration_test.go) instead.
func allDirections() []direction {
	return []direction{
		{enginePG, enginePG},
		{engineMySQL, engineMySQL},
		{engineMySQL, enginePG},
		{enginePG, engineMySQL},
		{engineSQLite, enginePG},
		{engineSQLite, engineMySQL},
		{enginePG, engineSQLite},
		{engineMySQL, engineSQLite},
	}
}

// family is one registry entry — one source type family. The family is
// engine-neutral; per-engine specifics live in the closures.
type family struct {
	// name is a stable identifier used in generated column names and
	// failure reports (e.g. "int32", "numeric_unconstrained").
	name string

	// pgType / myType / sqType are the scalar column type spellings in
	// each source dialect. Empty means the family has no native spelling
	// in that source engine (it is then skipped as a *source* there —
	// e.g. inet has no MySQL spelling; the MySQL reader would emit
	// varchar, which is a different family already covered). sqType is
	// the SQLite DECLARED type; only families whose declared type resolves
	// back to the same class under the reader's affinity + ADR-0129
	// declared-temporal/bool rules carry one (see the exclusion notes in
	// registry()).
	pgType string
	myType string
	sqType string

	// sqliteTargetRefused marks a family whose IR type the SQLite WRITER
	// refuses loudly at schema emit (ADR-0134 §1: bit/varbit,
	// inet/cidr/macaddr — no faithful SQLite storage, never coerced to
	// silently-wrong text). Consumed by expectedOutcome for the
	// X→sqlite directions; array shapes are refused there wholesale
	// (ir.Array is on the same emit-refusal list) without needing a flag.
	sqliteTargetRefused bool

	// shapes lists which shapes this family supports as a source. Array
	// shapes only apply to PG sources (MySQL has no array type); a
	// family that is array-capable lists shape1DArray/shapeMultiDim and
	// those are only generated when the source engine is PG.
	shapes []shape

	// gen returns a fresh random scalar literal (already escaped for
	// the given source dialect) plus whether it is SQL NULL. Edge cases
	// (min/max, boundary precision, multibyte, empty) are folded in via
	// the rng so a long enough run hits them.
	gen func(r *rand.Rand, src engineKind) (literal string, isNULL bool)

	// expect returns the expected outcome for (direction, shape). This
	// is the load-bearing classification; its truth table is sourced
	// from docs/type-mapping.md (cited inline at each non-faithful case).
	expect func(d direction, s shape) outcome
}

// canSource reports whether this family can be a source column in the
// given engine at the given shape.
func (f *family) canSource(src engineKind, s shape) bool {
	if src != enginePG && s != shapeScalar {
		return false // only PG has an array type — never an array source elsewhere.
	}
	switch src {
	case enginePG:
		if f.pgType == "" {
			return false
		}
	case engineMySQL:
		if f.myType == "" {
			return false
		}
	case engineSQLite:
		if f.sqType == "" {
			return false
		}
	}
	for _, have := range f.shapes {
		if have == s {
			return true
		}
	}
	return false
}

// columnDDL renders the column type spelling for (source engine, shape).
// PG arrays append `[]` per dimension; multi-dim PG arrays are declared
// `T[][]` (PG ignores declared dimensionality but the spelling documents
// intent and matches the battle-test fixtures).
func (f *family) columnDDL(src engineKind, s shape) string {
	base := f.pgType
	switch src {
	case engineMySQL:
		base = f.myType
	case engineSQLite:
		base = f.sqType
	}
	switch s {
	case shape1DArray:
		return base + "[]"
	case shapeMultiDim:
		return base + "[][]"
	default:
		return base
	}
}

// ---------------------------------------------------------------------
// Value emitters. Each returns a source-dialect literal. Arrays are
// assembled by the generator from these scalars (so NULL-element and
// ≥2-D nesting are generator-driven, exercising the Bug 73/74 class).
// ---------------------------------------------------------------------

// quotePG is standard SQL string quoting (doubled single quotes, no
// backslash escapes) — PG and SQLite share it; MySQL needs quoteMy.
func quotePG(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func quoteMy(s string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "'", "''") + "'"
}

func castPG(lit, typ string) string {
	return lit + "::" + typ
}

// pickStr returns one of the catalogued string edge cases, biased so a
// run of reasonable length hits the multibyte/emoji/empty/wide cases.
func pickStr(r *rand.Rand, maxLen int) string {
	switch r.Intn(8) {
	case 0:
		return ""
	case 1:
		return "café-ñ-Ω-😀-中文" // multibyte / emoji (Bug-class: encoding)
	case 2:
		return strings.Repeat("x", maxLen) // boundary length
	case 3:
		return "a'b\"c\\d" // quote / backslash escaping
	default:
		n := 1 + r.Intn(maxLen)
		var b strings.Builder
		const alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
		for i := 0; i < n; i++ {
			b.WriteByte(alpha[r.Intn(len(alpha))])
		}
		return b.String()
	}
}

// Phase-1 canonical-text comparability scope (a deliberate, documented
// design call — review focus #1, the #16 false-positive hazard):
//
// The oracle ground-truths faithfulness by comparing the per-engine
// CANONICAL TEXT of src vs dst. That text is *engine-specific*: a
// faithful round-tripped value renders differently across engines even
// when the stored value is identical — MySQL `tinyint(1)` → "1" but PG
// `boolean` → "true"; MySQL raw bytes vs PG `\x..` bytea hex; PG array
// `{1,2}` vs MySQL JSON `[1, 2]` (the documented array→JSON policy);
// numeric/char padding differs; etc. Asserting text-equality across
// engines would therefore FAIL faithful migrations — precisely the
// v0.69.0 #16 false-positive-refusal class the contract forbids.
//
// So Phase 1 asserts value-faithfulness via canonical text ONLY for
// SAME-ENGINE directions (PG→PG, MySQL→MySQL) — which is exactly where
// the silent-corruption bugs this harness targets lived (Bug 74 flatten
// PG→PG, Bug 75 bit PG→PG/PG→MySQL). For CROSS-ENGINE directions every
// family is `lossy-documented`: the oracle still enforces the other
// half of the contract (migrate must not silently refuse, must not
// leave a partial target, must create the column and rows — and any
// documented loud-refuse must still fire), but does NOT text-compare a
// representation that legitimately differs by engine. Cross-engine
// value fidelity is pinned exhaustively by the hand-authored battle
// fixtures (migrate_bug75/69/72_integration_test.go); generalising a
// cross-engine value oracle is explicit Phase-2 scope.

// sameEngineFaithful is the default expect: faithful for same-engine,
// lossy-documented for cross-engine (see the scope note above).
func sameEngineFaithful(d direction, _ shape) outcome {
	if d.src == d.dst {
		return outcomeFaithful
	}
	return outcomeLossyDocument
}

// alwaysFaithful is retained as the explicit name for families whose
// canonical text is provably identical on both engines at scalar shape
// AND every direction (currently none claim it — kept so a future
// family can opt in deliberately rather than by oversight).
func alwaysFaithful(d direction, s shape) outcome { return sameEngineFaithful(d, s) }

// expectedOutcome is THE single expectation lookup — every consumer
// (generator, oracle) resolves a (family, direction, shape) through it,
// never through f.expect directly. It centralises the SQLite-TARGET
// refusal classes (ADR-0134 §1: the writer refuses ir.Array — every
// array shape — and the sqliteTargetRefused families loudly at schema
// emit) so the per-family closures, written for the Phase-1 PG/MySQL
// matrix, don't each need a d.dst==sqlite branch (several fall through
// to outcomeFaithful for "any other direction", which would silently
// misclassify a sqlite target). Every non-refused X→sqlite case is
// lossy-documented per the Phase-1 cross-engine scope note above.
func expectedOutcome(f *family, d direction, s shape) outcome {
	if d.dst == engineSQLite {
		if s != shapeScalar || f.sqliteTargetRefused {
			return outcomeLoudRefuse // ADR-0134 emit refusal (array / bit / net)
		}
		return outcomeLossyDocument
	}
	return f.expect(d, s)
}

// withSQLite gives a family its SQLite source spelling (a DECLARED type
// the reader resolves back to the same class via affinity + the ADR-0129
// declared-temporal/bool rules). SQLite-source coverage is deliberately
// the storage-class core — the excluded families are annotated inline in
// registry() with the ADR that documents each asymmetry.
func withSQLite(f *family, declared string) *family {
	f.sqType = declared
	return f
}

// registry is the curated set of families. Every family that produced a
// v0.69.x bug is present and annotated with its Bug number. The axes
// (family × shape) ARE the "pin the class" matrix.
//
// SQLite-SOURCE scope (which families carry an sqType): SQLite has five
// storage classes plus the ADR-0129 declared-type overrides, so the
// sqlite-source families are exactly int64/float8/bool/text/blob/date/
// time/timestamp. The exclusions are documented asymmetries, NOT
// oversights:
//   - numeric_15_4 / numeric_unconstrained: SQLite NUMERIC/DECIMAL
//     affinity silently coerces a decimal literal to REAL/INTEGER at
//     INSERT (the Bug 162 / ADR-0134 §2 crux) — there is no declared
//     type that stores exact decimal text AND reads back as ir.Decimal;
//     a real-world decimal-as-TEXT source is the text family's domain.
//   - json: a JSON-declared column resolves to NUMERIC affinity (the
//     reader has no JSON resolution — ADR-0134 alternatives) and would
//     loud-refuse the object text; JSON-as-TEXT is the text family.
//   - timestamptz / timetz: SQLite is tz-naive (ADR-0134 tz wart).
//   - int8/16/24/32 + all unsigned: SQLite integers are 64-bit signed
//     with no width/sign spellings — int64 IS the class.
//   - char/varchar (+wide): SQLite does not enforce declared length;
//     they widen to TEXT (ADR-0134 §1) — the text family covers it.
//   - varbinary: a VARBINARY-declared column has NUMERIC affinity under
//     SQLite's rules (no BLOB substring) — blob (declared BLOB) is the
//     faithful binary class.
//   - bit/varbit/uuid/inet/cidr/macaddr/enum: no SQLite spelling.
func registry() []*family {
	return []*family{
		// --- Integers: signed + unsigned, all widths (Bug-class:
		//     unsigned-bigint range narrowing, type-mapping.md §unsigned).
		intFamily("int8", "smallint", "tinyint", false, -128, 127),
		intFamily("int16", "smallint", "smallint", false, -32768, 32767),
		intFamily("int24", "integer", "mediumint", false, -8388608, 8388607),
		intFamily("int32", "integer", "int", false, -2147483648, 2147483647),
		// int64 is also the SQLite integer class — its generated range
		// crosses 2^53 both ways, so the sqlite→X directions carry the
		// exact-int discipline (ADR-0128/0132).
		withSQLite(intFamily("int64", "bigint", "bigint", false, -9.0e18, 9.0e18), "INTEGER"),
		// Unsigned MySQL ints. PG has no unsigned; cross-engine is a
		// documented widen (faithful values, type-mapping.md §unsigned).
		uintFamily("uint8", "tinyint unsigned", 0, 255),
		uintFamily("uint16", "smallint unsigned", 0, 65535),
		uintFamily("uint32", "int unsigned", 0, 4294967295),
		// bigint unsigned: deliberate range narrowing on PG target, but
		// values < 2^63 round-trip faithfully and the loud notice is an
		// advisory (migration proceeds) — so outcomeFaithful for the
		// values we generate (all < 2^63). type-mapping.md §unsigned.
		uintBigFamily(),

		// --- Decimal: constrained + UNCONSTRAINED (Bug 69).
		decimalConstrained(),
		decimalUnconstrained(),

		// --- Float. float8 doubles as the SQLite REAL class (SQLite
		//     reals are 8-byte).
		floatFamily("float4", "real", "float", true),
		withSQLite(floatFamily("float8", "double precision", "double", false), "REAL"),

		// --- Boolean (SQLite: ADR-0129 declared-BOOLEAN + 0/1 values).
		withSQLite(boolFamily(), "BOOLEAN"),

		// --- Char / Varchar / Text incl. WIDE varchar >16383 (Bug 72).
		strFamily("char", "char(64)", "char(64)", 64),
		strFamily("varchar", "varchar(255)", "varchar(255)", 255),
		wideVarcharFamily(), // varchar(20000) — Bug 72 down-map class
		withSQLite(textFamily(), "TEXT"),

		// --- Binary / Varbinary / Blob.
		binFamily("varbinary", "bytea", "varbinary(64)"),
		withSQLite(blobFamily(), "BLOB"),

		// --- Bit / Varbit (Bug 75 silent corruption class).
		bitFamily("bit8", "bit(8)", "bit(8)", 8),
		varbitFamily(),

		// --- Temporal: date/time/timestamp/timestamptz/timetz (Bug 71).
		//     The SQLite spellings ride ADR-0129's declared-type override
		//     with the default `iso` TEXT encoding (the generators emit
		//     quoted ISO literals).
		withSQLite(dateFamily(), "DATE"),
		withSQLite(timeFamily(), "TIME"),
		timetzFamily(), // Bug 71 — PG faithful, MySQL zone-flatten/array-refuse
		withSQLite(timestampFamily(), "DATETIME"),
		timestamptzFamily(),

		// --- JSON.
		jsonFamily(),

		// --- UUID (Bug 73/74 array-element class).
		uuidFamily(),

		// --- Network: inet / cidr / macaddr (Bug 73/74 class).
		netFamily("inet", "inet"),
		netFamily("cidr", "cidr"),
		netFamily("macaddr", "macaddr"),

		// --- Enum.
		enumFamily(),
	}
}

// ---- family constructors ----

func arrayShapes() []shape { return []shape{shapeScalar, shape1DArray, shapeMultiDim} }
func scalarOnly() []shape  { return []shape{shapeScalar} }

func intFamily(name, pg, my string, _ bool, lo, hi float64) *family {
	return &family{
		name: name, pgType: pg, myType: my, shapes: arrayShapes(),
		gen: func(r *rand.Rand, _ engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true // NULL
			}
			switch r.Intn(4) {
			case 0:
				return fmt.Sprintf("%d", int64(lo)), false // min boundary
			case 1:
				return fmt.Sprintf("%d", int64(hi)), false // max boundary
			default:
				return fmt.Sprintf("%d", int64(lo)+int64(r.Float64()*(hi-lo))), false
			}
		},
		expect: alwaysFaithful,
	}
}

func uintFamily(name, my string, lo, hi uint64) *family {
	return &family{
		name: name, pgType: "", myType: my, shapes: scalarOnly(),
		gen: func(r *rand.Rand, _ engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			switch r.Intn(3) {
			case 0:
				return fmt.Sprintf("%d", lo), false
			case 1:
				return fmt.Sprintf("%d", hi), false
			default:
				return fmt.Sprintf("%d", lo+uint64(r.Float64()*float64(hi-lo))), false
			}
		},
		// MySQL→PG widens losslessly (values fit signed next rank);
		// MySQL→MySQL faithful. type-mapping.md §unsigned.
		expect: alwaysFaithful,
	}
}

func uintBigFamily() *family {
	return &family{
		name: "uint64", pgType: "", myType: "bigint unsigned", shapes: scalarOnly(),
		gen: func(r *rand.Rand, _ engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			// Stay strictly below 2^63 so the values round-trip
			// faithfully on PG (the >2^63 half is the documented
			// narrowing, surfaced as a loud advisory not a refusal —
			// we don't generate it: it would be lossy-documented, and
			// PG can't even represent it to compare).
			return fmt.Sprintf("%d", uint64(r.Int63())), false
		},
		expect: alwaysFaithful,
	}
}

func decimalConstrained() *family {
	return &family{
		name: "numeric_15_4", pgType: "numeric(15,4)", myType: "decimal(15,4)", shapes: arrayShapes(),
		gen: func(r *rand.Rand, _ engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			whole := r.Int63n(99999999999)
			frac := r.Intn(10000)
			return fmt.Sprintf("%d.%04d", whole, frac), false
		},
		expect: alwaysFaithful,
	}
}

func decimalUnconstrained() *family {
	// Bug 69: unconstrained PG numeric. PG→PG faithful (bare NUMERIC);
	// PG→MySQL is a documented widen to DECIMAL(65,30) — values are
	// preserved but right-padded, so canonical text differs → classify
	// lossy-documented for the scalar cross case (we assert migrate
	// succeeds + column exists, not text-equality). The numeric[] array
	// → MySQL JSON is also lossy-documented. type-mapping.md §69.
	return &family{
		name: "numeric_unconstrained", pgType: "numeric", myType: "", shapes: arrayShapes(),
		gen: func(r *rand.Rand, _ engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			switch r.Intn(3) {
			case 0:
				return "3.14159", false
			case 1:
				return "12345678901234567890.1234567890", false // hi precision
			default:
				return fmt.Sprintf("%d.%d", r.Int63n(1e9), r.Intn(1e6)), false
			}
		},
		// Bug 69 unconstrained numeric: PG→PG faithful (bare NUMERIC);
		// PG→MySQL DECIMAL(65,30) right-pad widen / numeric[]→JSON —
		// cross-engine lossy-documented per the scope note.
		expect: sameEngineFaithful,
	}
}

func floatFamily(name, pg, my string, _ bool) *family {
	return &family{
		name: name, pgType: pg, myType: my, shapes: arrayShapes(),
		gen: func(r *rand.Rand, _ engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			// Use values exactly representable in binary float so the
			// canonical text compare is deterministic across engines.
			vals := []string{"0", "1", "-1", "0.5", "-0.25", "1024", "3.5"}
			return vals[r.Intn(len(vals))], false
		},
		expect: alwaysFaithful,
	}
}

func boolFamily() *family {
	return &family{
		name: "bool", pgType: "boolean", myType: "tinyint(1)", shapes: arrayShapes(),
		gen: func(r *rand.Rand, src engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			// MySQL tinyint(1) and SQLite BOOLEAN both take 0/1 (the
			// ADR-0129 canonical INTEGER bool storage); PG takes the
			// keywords.
			if src != enginePG {
				if r.Intn(2) == 0 {
					return "1", false
				}
				return "0", false
			}
			if r.Intn(2) == 0 {
				return "true", false
			}
			return "false", false
		},
		expect: alwaysFaithful,
	}
}

// strFamily covers bounded char(N)/varchar(N). SCALAR ONLY: the PG
// array-element DDL emitter does not yet carry the element length, so
// `char(64)[]` / `varchar(255)[]` round-trips emit a length-less
// `char[]` (→ invalid `char(0)`, SQLSTATE 22023). This is a documented,
// catalogued pre-existing emit gap — see the doc comment in
// migrate_bug7374_integration_test.go, which omits bounded char/varchar
// arrays for exactly this reason. Phase 1 mirrors that scope; the gap
// is a known loud failure (exit≠0, no corruption), not a fuzz target.
// Unbounded `text[]` (no length) is array-capable — see textFamily.
func strFamily(name, pg, my string, maxLen int) *family {
	return &family{
		name: name, pgType: pg, myType: my, shapes: scalarOnly(),
		gen: func(r *rand.Rand, src engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			s := pickStr(r, maxLen)
			if src == engineMySQL {
				return quoteMy(s), false
			}
			return quotePG(s), false
		},
		expect: alwaysFaithful,
	}
}

func wideVarcharFamily() *family {
	// Bug 72: varchar(20000) > MySQL representable cap. PG→PG faithful;
	// PG→MySQL down-maps to a TEXT family (loud advisory, migration
	// proceeds) — values preserved but the column TYPE differs; the
	// canonical-text oracle still matches (text content is identical),
	// but to avoid coupling to MySQL TEXT trailing-space semantics we
	// classify the cross case lossy-documented. type-mapping.md §72.
	return &family{
		name: "varchar_wide", pgType: "varchar(20000)", myType: "", shapes: scalarOnly(),
		gen: func(r *rand.Rand, _ engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			return quotePG(pickStr(r, 18000)), false
		},
		// Bug 72 wide varchar: PG→PG faithful; PG→MySQL TEXT-family
		// down-map (loud advisory, migration proceeds) — cross-engine
		// lossy-documented per the scope note.
		expect: sameEngineFaithful,
	}
}

func textFamily() *family {
	return &family{
		name: "text", pgType: "text", myType: "text", shapes: arrayShapes(),
		gen: func(r *rand.Rand, src engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			s := pickStr(r, 200)
			if src == engineMySQL {
				return quoteMy(s), false
			}
			return quotePG(s), false
		},
		expect: alwaysFaithful,
	}
}

// randHex emits up to maxBytes random bytes as a lowercase hex string
// (possibly empty — exercises the zero-length binary edge case). A
// 1-in-4 draw pins the byte-boundary values (0x00 / 0xFF / the int8
// sign edge) so every run exercises them rather than leaving them to
// chance.
func randHex(r *rand.Rand, maxBytes int) string {
	if r.Intn(4) == 0 {
		return "00ff7f80"
	}
	n := r.Intn(maxBytes)
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "%02x", r.Intn(256))
	}
	return b.String()
}

// binLiteral renders a hex byte string as a source-dialect binary
// literal (MySQL 0x.. / SQLite x'..' / PG bytea \x.., empty handled).
func binLiteral(h string, src engineKind) string {
	switch src {
	case engineMySQL:
		if h == "" {
			return "''"
		}
		return "0x" + h
	case engineSQLite:
		return "x'" + h + "'" // x'' is the valid empty blob
	default:
		return castPG(quotePG("\\x"+h), "bytea")
	}
}

func binFamily(name, pg, my string) *family {
	return &family{
		name: name, pgType: pg, myType: my, shapes: scalarOnly(),
		gen: func(r *rand.Rand, src engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			return binLiteral(randHex(r, 16), src), false
		},
		// PG bytea ↔ MySQL varbinary are both byte-faithful same-engine;
		// cross-engine byte content is preserved (Blob/Varbinary core).
		expect: alwaysFaithful,
	}
}

func blobFamily() *family {
	return &family{
		name: "blob", pgType: "bytea", myType: "blob", shapes: scalarOnly(),
		gen: func(r *rand.Rand, src engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			return binLiteral(randHex(r, 24), src), false
		},
		expect: alwaysFaithful,
	}
}

func bitFamily(name, pg, my string, n int) *family {
	return &family{
		name: name, pgType: pg, myType: my, shapes: scalarOnly(),
		// ir.Bit has no faithful SQLite storage — the writer refuses it
		// loudly at emit (ADR-0134 §1).
		sqliteTargetRefused: true,
		gen: func(r *rand.Rand, src engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			var b strings.Builder
			for i := 0; i < n; i++ {
				if r.Intn(2) == 0 {
					b.WriteByte('0')
				} else {
					b.WriteByte('1')
				}
			}
			if src == engineMySQL {
				return "b'" + b.String() + "'", false
			}
			return "B'" + b.String() + "'", false
		},
		// Bug 75: bit must be faithful & distinct in every direction
		// under the bit-string IR contract.
		expect: alwaysFaithful,
	}
}

func varbitFamily() *family {
	return &family{
		name: "varbit", pgType: "bit varying(16)", myType: "bit(16)", shapes: scalarOnly(),
		// ir.Varbit/ir.Bit are on the SQLite writer's emit-refusal list
		// (ADR-0134 §1).
		sqliteTargetRefused: true,
		gen: func(r *rand.Rand, src engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			n := 1 + r.Intn(16)
			var b strings.Builder
			for i := 0; i < n; i++ {
				if r.Intn(2) == 0 {
					b.WriteByte('0')
				} else {
					b.WriteByte('1')
				}
			}
			if src == engineMySQL {
				return "b'" + b.String() + "'", false
			}
			return "B'" + b.String() + "'", false
		},
		// PG bit varying has no fixed width; MySQL BIT(16) zero-pads to
		// declared width so cross-engine canonical text differs by
		// leading zeros — documented (Bug 75 fixture pins the exact
		// padding). Classify cross lossy-documented; same-engine
		// faithful.
		// PG bit varying vs MySQL BIT(16) zero-pads differently
		// cross-engine (Bug 75 fixture pins the exact padding);
		// same-engine faithful.
		expect: sameEngineFaithful,
	}
}

func dateFamily() *family {
	return &family{
		name: "date", pgType: "date", myType: "date", shapes: arrayShapes(),
		gen: func(r *rand.Rand, _ engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			y := 1970 + r.Intn(80)
			m := 1 + r.Intn(12)
			d := 1 + r.Intn(28)
			return fmt.Sprintf("'%04d-%02d-%02d'", y, m, d), false
		},
		expect: alwaysFaithful,
	}
}

func timeFamily() *family {
	return &family{
		name: "time", pgType: "time", myType: "time(6)", shapes: arrayShapes(),
		gen: func(r *rand.Rand, _ engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			return fmt.Sprintf("'%02d:%02d:%02d'", r.Intn(24), r.Intn(60), r.Intn(60)), false
		},
		expect: alwaysFaithful,
	}
}

func timetzFamily() *family {
	// Bug 71/73: PG timetz. PG→PG faithful (per-conn binary codec).
	// PG→MySQL drops the zone — documented zone-flatten (migration
	// proceeds, value transformed) → lossy-documented for scalar.
	// timetz[] has no faithful binary array leaf → LOUD REFUSE
	// (asserted by the Bug 73 fixture). This is the load-bearing
	// loud-refuse-is-a-PASS case.
	return &family{
		name: "timetz", pgType: "timetz", myType: "", shapes: arrayShapes(),
		gen: func(r *rand.Rand, _ engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			off := []string{"+00", "+05", "-07", "+05:30"}[r.Intn(4)]
			return fmt.Sprintf("'%02d:%02d:%02d%s'", r.Intn(24), r.Intn(60), r.Intn(60), off), false
		},
		expect: func(d direction, s shape) outcome {
			if d.src == enginePG && d.dst == enginePG {
				if s == shapeScalar {
					return outcomeFaithful
				}
				// timetz[] (1-D or multi-dim): documented loud-refuse
				// (no faithful binary array leaf) — see
				// migrate_bug7374_integration_test.go.
				return outcomeLoudRefuse
			}
			if d.src == enginePG && d.dst == engineMySQL {
				return outcomeLossyDocument // zone-flatten / array→JSON
			}
			return outcomeFaithful
		},
	}
}

func timestampFamily() *family {
	return &family{
		name: "timestamp", pgType: "timestamp", myType: "datetime(6)", shapes: arrayShapes(),
		gen: func(r *rand.Rand, src engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			// PG timestamp / MySQL datetime take the full 1970..2049 spread.
			year := 1970 + r.Intn(80)
			if src == engineSQLite {
				// A SQLite DATETIME reads back as ir.Timestamp (ADR-0129),
				// which the MySQL writer emits as TIMESTAMP(6) — supported
				// instants end 2038-01-19 (and exclude the 1970 epoch
				// second). Stay inside 1971..2030 so sqlite→mysql values
				// are representable — the same window timestamptzFamily
				// uses for the same wall. An out-of-window value is a
				// documented LOUD refusal (Error 1292), not a fuzz target.
				year = 1971 + r.Intn(60)
			}
			return fmt.Sprintf("'%04d-%02d-%02d %02d:%02d:%02d'",
				year, 1+r.Intn(12), 1+r.Intn(28),
				r.Intn(24), r.Intn(60), r.Intn(60)), false
		},
		expect: alwaysFaithful,
	}
}

func timestamptzFamily() *family {
	return &family{
		name: "timestamptz", pgType: "timestamptz", myType: "timestamp(6)", shapes: arrayShapes(),
		gen: func(r *rand.Rand, src engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			ts := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
				1971+r.Intn(60), 1+r.Intn(12), 1+r.Intn(28),
				r.Intn(24), r.Intn(60), r.Intn(60))
			if src == enginePG {
				// PG timestamptz accepts (and needs, to be unambiguous)
				// an explicit zone; MySQL TIMESTAMP rejects the `+00`
				// suffix (Error 1292) and stores UTC implicitly.
				return "'" + ts + "+00'", false
			}
			return "'" + ts + "'", false
		},
		// timestamptz scalar round-trips same-engine (both store UTC);
		// cross-engine rendering differs (and PG→MySQL array→JSON) —
		// lossy-documented per the scope note.
		expect: sameEngineFaithful,
	}
}

func jsonFamily() *family {
	return &family{
		name: "json", pgType: "jsonb", myType: "json", shapes: scalarOnly(),
		gen: func(r *rand.Rand, _ engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			docs := []string{
				`{"a": 1, "b": [true, null, "x"]}`,
				`[1, 2, 3]`,
				`{"nested": {"k": "café 😀"}}`,
				`"scalar"`,
				`123`,
			}
			return quotePG(docs[r.Intn(len(docs))]), false
		},
		// JSON value-equality across PG jsonb vs MySQL JSON is
		// normalisation-sensitive (key order/whitespace); same-engine
		// faithful, cross-engine lossy-documented (value preserved,
		// canonical text representation differs).
		// PG jsonb vs MySQL JSON normalise key order/whitespace
		// differently; same-engine faithful, cross-engine
		// lossy-documented.
		expect: sameEngineFaithful,
	}
}

func uuidFamily() *family {
	return &family{
		name: "uuid", pgType: "uuid", myType: "", shapes: arrayShapes(),
		gen: func(r *rand.Rand, _ engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			var b [16]byte
			for i := range b {
				b[i] = byte(r.Intn(256))
			}
			u := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
			return castPG(quotePG(u), "uuid"), false
		},
		// PG→MySQL uuid → char(36) / array→JSON: value preserved,
		// representation differs → cross-engine lossy-documented.
		expect: sameEngineFaithful,
	}
}

func netFamily(name, pgType string) *family {
	return &family{
		name: name, pgType: pgType, myType: "", shapes: arrayShapes(),
		// ir.Inet/Cidr/Macaddr have no faithful SQLite storage — refused
		// loudly at emit (ADR-0134 §1).
		sqliteTargetRefused: true,
		gen: func(r *rand.Rand, _ engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			var lit string
			switch name {
			case "inet":
				lit = fmt.Sprintf("%d.%d.%d.%d", r.Intn(256), r.Intn(256), r.Intn(256), r.Intn(256))
			case "cidr":
				lit = fmt.Sprintf("%d.%d.%d.0/24", r.Intn(256), r.Intn(256), r.Intn(256))
			default: // macaddr
				lit = fmt.Sprintf("08:00:2b:%02x:%02x:%02x", r.Intn(256), r.Intn(256), r.Intn(256))
			}
			return castPG(quotePG(lit), pgType), false
		},
		// inet/cidr/macaddr → MySQL VARCHAR / array→JSON: cross-engine
		// lossy-documented; same-engine faithful.
		expect: sameEngineFaithful,
	}
}

func enumFamily() *family {
	// PG enum requires a CREATE TYPE; the generator special-cases the
	// enum DDL. MySQL ENUM is column-level. Same-engine faithful;
	// PG→MySQL faithful (column-level ENUM), MySQL→PG faithful (PG enum
	// type). Scalar only (no enum arrays in Phase 1).
	return &family{
		name: "enum", pgType: "__enum__", myType: "enum('red','green','blue')", shapes: scalarOnly(),
		gen: func(r *rand.Rand, _ engineKind) (string, bool) {
			if r.Intn(6) == 0 {
				return "", true
			}
			return quotePG([]string{"red", "green", "blue"}[r.Intn(3)]), false
		},
		expect: alwaysFaithful,
	}
}
