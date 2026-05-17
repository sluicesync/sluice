// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

// The allowlists that make the Bug 14 post-translation PG-validity
// gate safe. THIS IS THE LOAD-BEARING ARTIFACT for false-positive
// safety — every entry here is a function that is provably acceptable
// in PostgreSQL DDL (DEFAULT / GENERATED / CHECK position), either
// because it is a PG built-in or because the MySQL→PG translator
// guarantees it is rewritten before it reaches PG's parser.
//
// CURATION RATIONALE
//
//   - translatedMySQLFunctions: the EXACT set of MySQL function names
//     the PG writer's translator (internal/engines/postgres/
//     expr_translate.go, translateExprForPG) provably rewrites into a
//     PG-valid form. Because the translator removes these before the
//     expression reaches PG, the MySQL spelling never causes a parse
//     error — so allowlisting the MySQL name is correct. Sourced by
//     enumerating every rewriteXxx rule in expr_translate.go. If a
//     translator rule is added/removed, this set must move in lockstep
//     (a TestTranslatedFunctionsCoverTranslatorRules pin enforces the
//     direction that matters: every name here must be a real MySQL
//     function the translator handles).
//
//   - pgValidFunctions: PG core / built-in functions PLUS the exact
//     output function-forms the translator emits (to_hex,
//     array_position, json_build_object, encode, digest, …). This is
//     conservative-INCLUSIVE: it errs toward listing a function rather
//     than risking a false-positive refusal of a valid schema. It is
//     NOT exhaustive of every PG function in existence — it does not
//     need to be: a PG function genuinely missing here degrades to
//     today's late-migrate-failure (a missed detection, no worse than
//     status quo), which the loud-failure tenet explicitly accepts as
//     the safe failure direction. The unsafe direction — refusing a
//     valid schema — is guarded by keeping this list broad.
//
// Both sets are matched case-insensitively (callers lower-case before
// lookup; PG lower-cases unquoted identifiers).

// translatedMySQLFunctions is every MySQL function name the PG writer's
// translateExprForPG provably rewrites to a PG-valid form. Keep in
// lockstep with internal/engines/postgres/expr_translate.go.
var translatedMySQLFunctions = stringSet(
	// v1 scope
	"concat", "json_unquote", "json_extract", "ifnull", "if", "cast",
	"json_valid",
	// v0.11.0 catalog batch
	"now", "current_timestamp", "localtimestamp", "localtime",
	"curdate", "current_date", "curtime", "current_time",
	"unix_timestamp", "from_unixtime",
	"char_length", "character_length", "lcase", "ucase",
	"substr", "mid",
	// v0.11.1 catalog batch
	"rand", "uuid", "isnull", "regexp_replace", "instr", "locate",
	"date_add", "date_sub", "date_format",
	// v0.35.0 catalog batch
	"hex", "field", "dayname", "monthname", "weekofyear", "quarter",
	"datediff",
	// v0.37.0 catalog batch
	"timestampdiff", "json_object", "json_array", "last_day",
	// v0.38.0 hash family (md5 is core; sha1/sha2 gated on pgcrypto —
	// the translator emits encode(digest(...)) only with the flag, and
	// without the flag sha1/sha2 would otherwise fall through verbatim.
	// They are intentionally NOT in this set so that, absent
	// `--enable-pg-extension pgcrypto`, the general gate refuses them
	// LOUDLY at preview/preflight instead of letting PG's parse error
	// surface mid-migrate. With the flag, pgcrypto's digest/encode are
	// covered via the extension path. md5 → core md5(), always safe.)
	"md5",
)

// pgValidFunctions is the conservative-inclusive set of PG core /
// built-in functions plus the exact function-forms translateExprForPG
// emits. A name here is treated as PG-valid in DDL position.
var pgValidFunctions = stringSet(
	// ---- Exact output forms the translator emits ----
	// NOTE: `digest` is DELIBERATELY ABSENT — it is pgcrypto-owned, not
	// PG core. The translator emits `encode(digest(...))` only when the
	// operator passed `--enable-pg-extension pgcrypto` (the v0.38.0
	// SHA1/SHA2 gating). So `digest` is covered by the extension path
	// when enabled; absent the flag, a bare `digest(...)` correctly
	// loud-refuses here (consistent with gaps.go's SHA1/SHA2 ext-gate
	// intent). `encode` IS core PG (`encode(bytea,text)`), so it stays.
	"coalesce", "extract", "to_timestamp", "to_char", "length",
	"lower", "upper", "substring", "random", "gen_random_uuid",
	"regexp_replace", "strpos", "to_hex", "array_position", "age",
	"json_build_object", "json_build_array", "date_trunc", "encode",
	"md5", "cast",

	// ---- PG core string functions ----
	"char_length", "character_length", "bit_length", "octet_length",
	"trim", "btrim", "ltrim", "rtrim", "lpad", "rpad", "left", "right",
	"reverse", "repeat", "replace", "translate", "split_part",
	"initcap", "ascii", "chr", "concat", "concat_ws", "format",
	"position", "overlay", "quote_ident", "quote_literal",
	"quote_nullable", "starts_with", "to_ascii", "convert_from",
	"convert_to", "regexp_match", "regexp_matches", "regexp_replace",
	"regexp_split_to_array", "regexp_split_to_table", "substr",
	"string_to_array", "array_to_string", "unistr", "normalize",

	// ---- PG core numeric / math functions ----
	"abs", "cbrt", "ceil", "ceiling", "degrees", "div", "exp", "factorial",
	"floor", "gcd", "lcm", "ln", "log", "log10", "mod", "pi", "power",
	"radians", "round", "scale", "sign", "sqrt", "trunc", "width_bucket",
	"min_scale", "trim_scale", "random", "setseed",
	"sin", "cos", "tan", "cot", "asin", "acos", "atan", "atan2",
	"sinh", "cosh", "tanh", "asinh", "acosh", "atanh",
	"sind", "cosd", "tand", "cotd", "asind", "acosd", "atand", "atan2d",

	// ---- PG core date/time functions ----
	"now", "current_date", "current_time", "current_timestamp",
	"localtime", "localtimestamp", "clock_timestamp",
	"statement_timestamp", "transaction_timestamp", "timeofday",
	"date_part", "date_bin", "isfinite", "justify_days",
	"justify_hours", "justify_interval", "make_date", "make_interval",
	"make_time", "make_timestamp", "make_timestamptz", "to_date",
	"to_number", "extract", "age",

	// ---- PG core conditional / comparison ----
	"coalesce", "nullif", "greatest", "least", "num_nonnulls",
	"num_nulls",

	// ---- PG core array functions ----
	"array_append", "array_cat", "array_dims", "array_fill",
	"array_length", "array_lower", "array_ndims", "array_position",
	"array_positions", "array_prepend", "array_remove", "array_replace",
	"array_to_string", "array_upper", "cardinality", "trim_array",
	"unnest", "array_agg",

	// ---- PG core JSON / JSONB ----
	"to_json", "to_jsonb", "array_to_json", "row_to_json",
	"json_build_array", "json_build_object", "jsonb_build_array",
	"jsonb_build_object", "json_object", "jsonb_object",
	"json_array_elements", "json_array_elements_text",
	"jsonb_array_elements", "jsonb_array_elements_text",
	"json_array_length", "jsonb_array_length", "json_each", "jsonb_each",
	"json_each_text", "jsonb_each_text", "json_extract_path",
	"jsonb_extract_path", "json_extract_path_text",
	"jsonb_extract_path_text", "json_object_keys", "jsonb_object_keys",
	"json_populate_record", "jsonb_populate_record", "json_typeof",
	"jsonb_typeof", "json_strip_nulls", "jsonb_strip_nulls",
	"jsonb_set", "jsonb_insert", "jsonb_pretty", "jsonb_path_exists",
	"jsonb_path_match", "jsonb_path_query", "jsonb_path_query_array",
	"jsonb_path_query_first", "json_query", "json_value", "json_exists",
	"json_scalar", "json_serialize",

	// ---- PG core hashing / encoding / binary ----
	"md5", "encode", "decode", "to_hex", "sha224", "sha256", "sha384",
	"sha512", "get_byte", "set_byte", "get_bit", "set_bit",

	// ---- PG core network / uuid ----
	"gen_random_uuid", "uuid", "host", "hostmask", "masklen", "netmask",
	"network", "set_masklen", "abbrev", "broadcast", "family", "inet_merge",
	"inet_same_family", "macaddr8_set7bit",

	// ---- PG core range / misc ----
	"lower", "upper", "isempty", "lower_inc", "upper_inc", "lower_inf",
	"upper_inf", "range_merge", "tstzrange", "tsrange", "daterange",
	"int4range", "int8range", "numrange",

	// ---- PG core type-conversion-ish / system (rare in DDL but valid)
	"to_timestamp", "to_date", "to_number", "to_char", "cast",
	"pg_typeof", "format_type", "text", "varchar", "bool", "int",
	"int2", "int4", "int8", "float4", "float8", "numeric",
	"timestamp", "timestamptz", "date", "time", "interval", "bytea",

	// NOTE: the PG geometry constructors point()/box()/circle()/line()/
	// lseg()/path()/polygon() are DELIBERATELY ABSENT. In a MySQL→PG
	// migration a `POINT(x, y)` (etc.) call in a generated column /
	// CHECK is overwhelmingly MySQL's spatial-GEOMETRY constructor
	// (Bug 13): MySQL returns a GEOMETRY, but the column's mapped PG
	// type is not PG `point`, so PG rejects the DDL (SQLSTATE 42804 /
	// 42883 depending on shape) — a structural false-green if not
	// refused. Listing them would make Bug 13 silently pass. Keeping
	// them out makes the general gate refuse `POINT(x,y)` loudly; the
	// operator's `--expr-override` is the escape hatch for the rare
	// genuine PG-`point()`-in-cross-engine-DDL case. Per the loud-
	// failure tenet this is the correct conservative direction here:
	// the spatial-constructor false-green is the documented Bug 13
	// hazard; a refused-but-actually-valid PG point() is recoverable
	// via one --expr-override flag.
)

// extensionOwnedFunctions mirrors the postgres package's per-extension
// `defaultExprFunctions` catalog (ADR-0044) for the engine-neutral
// allowlist gate. The keys are the canonical `pg_extension.extname`
// values that an operator passes to `--enable-pg-extension`. When the
// operator has enabled an extension, the functions it owns are
// PG-valid in DDL position and must NOT trip the general gate.
//
// This duplicates a small slice of postgres-package knowledge into
// internal/translate on purpose — exactly the pattern v0.68.1's
// curated `gapPatterns` already established (the engine-neutral gate
// cannot import internal/engines/postgres without breaking the
// IR-first / registry tenet). The duplication is bounded (catalog
// extensions only) and pinned by a coverage test so it cannot silently
// drift from the postgres-package source of truth.
var extensionOwnedFunctions = map[string]map[string]bool{
	"pgcrypto": stringSet(
		"digest", "hmac", "crypt", "gen_salt", "gen_random_bytes",
		"encrypt", "decrypt", "encrypt_iv", "decrypt_iv",
		"pgp_sym_encrypt", "pgp_sym_decrypt",
		"pgp_pub_encrypt", "pgp_pub_decrypt",
	),
	"uuid-ossp": stringSet(
		"uuid_generate_v1", "uuid_generate_v1mc", "uuid_generate_v4",
		"uuid_generate_v5", "uuid_nil", "uuid_ns_dns", "uuid_ns_url",
		"uuid_ns_oid", "uuid_ns_x500",
	),
	"vector": stringSet(
		"l2_distance", "cosine_distance", "inner_product",
		"l1_distance", "vector_dims", "vector_norm",
	),
}

// extensionFunctionEnabled reports whether fnName (already lower-cased)
// is owned by some extension the operator has enabled. enabled is the
// operator's `--enable-pg-extension` set; an entry's presence (value
// true) means the extension is on.
func extensionFunctionEnabled(fnName string, enabled map[string]bool) bool {
	for ext, on := range enabled {
		if !on {
			continue
		}
		if funcs, ok := extensionOwnedFunctions[ext]; ok && funcs[fnName] {
			return true
		}
	}
	return false
}

// sqlGrammarKeywords are SQL keyword / operator forms that can legally
// be immediately followed by `(` without being a function call:
// `x IN (...)`, `... AND (...)`, `NOT (...)`, `EXISTS (...)`,
// `x BETWEEN (a) AND (b)`, `ALL (...)`, `ANY (...)`, etc. The scanner
// excludes these so a bare `IN (` is never mistaken for an `in()`
// function — a false-positive-safety requirement (pinned by the
// `status IN ('a','b','c')` valid-expr pin). Keeping this list focused
// on genuine grammar keywords (not function-like keywords such as
// EXTRACT/SUBSTRING/TRIM/OVERLAY/CAST which ARE call-shaped and are
// covered by the allowlist instead) keeps the gate tight.
var sqlGrammarKeywords = stringSet(
	"in", "not", "and", "or", "between", "like", "ilike", "similar",
	"exists", "all", "any", "some", "is", "case", "when", "then",
	"else", "end", "distinct", "as", "asc", "desc", "collate",
	"escape", "array", "row", "values", "using", "from", "for",
)

// sqlCastTargetTypeNames are SQL type-name spellings that legitimately
// appear *as a parameterized CAST/`::` target type* — `CAST(x AS
// DECIMAL(10,2))`, `CAST(x AS CHAR(20))`, `x::numeric(12,4)`. A
// type-specifier-with-length is grammar, NOT a function call, so the
// scanner must not flag it (Bug #16: v0.68.3 misread `DECIMAL(`/
// `CHAR(`/`BINARY(`/`NCHAR(` as unknown calls and spuriously refused
// schemas v0.68.2 migrated clean — the translator rewrites CHAR→VARCHAR
// and PG accepts decimal/numeric natively).
//
// This is consulted ONLY when the token is in cast-target position
// (immediately after `AS` inside a cast, or after `::`). Outside that
// position these same words used call-shaped are still flagged — that
// is the load-bearing distinction: MySQL's `CHAR(65)` *scalar* function
// (no PG form; the translator does NOT rewrite it) must still
// loud-refuse, so a blanket type-name allowlist would be wrong (it
// would re-open the v0.68.1-class false-green). The set deliberately
// EXCLUDES `signed`/`unsigned` — those MySQL-only integer-cast targets
// have no parens and are owned by mysqlOnlyCastTarget, which must keep
// refusing them.
var sqlCastTargetTypeNames = stringSet(
	"decimal", "dec", "numeric", "char", "character", "varchar",
	"nchar", "nvarchar", "bpchar", "binary", "varbinary", "bit",
	"varbit", "float", "real", "double", "money", "smallint", "int",
	"integer", "bigint", "boolean", "bool", "date", "time", "timestamp",
	"timestamptz", "datetime", "interval", "uuid", "json", "jsonb",
	"xml", "text", "bytea", "year",
)

// stringSet builds a lookup set from the given names. All names are
// stored already-lower-cased (the source literals above are written
// lower-case); callers lower-case the scanned identifier before lookup.
func stringSet(names ...string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}
