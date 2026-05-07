# ADR-0016: Layered cross-dialect expression translation

## Status

Accepted. Implemented in v0.3.3 (`internal/engines/mysql/expr_translate.go`,
`internal/engines/postgres/expr_translate.go`).

## Context

v0.3.1 added GENERATED-column support and v0.3.2 added CHECK constraints,
both with verbatim expression passthrough: the source's `GENERATION_EXPRESSION` /
`pg_get_expr` text was carried through the IR and emitted on the target
unchanged, modulo identifier-quoting normalization at the read boundary
(stripping MySQL backticks, MySQL's `_charset'…'` introducers, and the
C-style apostrophe-escape form).

The verbatim policy's claim — "non-portable expressions fail loudly on
the target" — held, but cross-engine validation (Bug 10 in the
PlanetScale-validation catalog) found that "non-portable" included
several very common idioms:

- MySQL `CONCAT(a, b)` in a generated column targeting Postgres → PG
  rejects with `generation expression is not immutable` because PG's
  `concat()` is `STABLE`, not `IMMUTABLE`.
- MySQL `JSON_UNQUOTE(JSON_EXTRACT(j, '$.k'))` → PG has the dedicated
  `->>'k'` operator instead and doesn't recognize the function names.
- PG `(expr)::type` casts → MySQL needs `CAST(expr AS …)` with the
  MySQL spelling of the target type.
- PG `a || b` string concatenation → MySQL needs `CONCAT(a, b)` unless
  the session has `PIPES_AS_CONCAT` set, which sluice doesn't assume.
- PG `~~` / `~~*` LIKE operators → MySQL's `LIKE` and a
  case-insensitive equivalent.
- PG `col = ANY(ARRAY[a, b, c])` → MySQL `col IN (a, b, c)`.

Operators hit these on real tables. The fail-loud message named the
target-side parser error but didn't point at a fix path; the only
workaround was to drop the generated column / CHECK on the source
before migrating and recreate it manually.

Three options were on the table:

1. **Stay verbatim, document harder.** Lean on the failure mode and
   improve the operator-facing diagnostic. Operators still own the
   rewrite. Lowest engineering cost, but the same idioms recur on every
   cross-engine migration; the cost just lands on the operator's lap
   each time.
2. **Build a full SQL parser and translate aggressively.** Maximum
   coverage, but a real parser is a large surface to maintain, and the
   translation table becomes a moving target as new function names are
   added on either side. The "loud failure beats silent corruption"
   tenet would be at risk: a broken translation that produced
   syntactically valid but semantically wrong output is much worse than
   a parse error.
3. **A small, mechanical translation table run at the writer
   boundary, layered on top of the existing verbatim passthrough.**
   Cover the high-frequency cases identified in cross-engine testing;
   leave anything else to fall through verbatim and rely on the
   loud-failure fallback. Translation is additive — a recognized
   construct gets rewritten, an unrecognized one behaves exactly as
   before this ADR.

## Decision

Option 3. Two new files, one per engine:

- `internal/engines/postgres/expr_translate.go` →
  `translateExprForPG(expr string) string` translates MySQL idioms to
  PG when emitting a MySQL-source expression on a PG target.
- `internal/engines/mysql/expr_translate.go` →
  `translateExprForMySQL(expr string) string` translates PG idioms to
  MySQL when emitting a PG-source expression on a MySQL target.

The IR's `Column` and `CheckConstraint` carry a `GeneratedExprDialect`
/ `ExprDialect` field set by each schema reader to the source engine's
canonical dialect name (`"mysql"` or `"postgres"`; both MySQL flavors
share the `"mysql"` tag because the wire dialect is identical). The
DDL emitters compare the dialect tag against their own and run the
translator only when they differ. An empty tag (older IR, hand-built
test fixtures) emits verbatim — same behaviour as before this ADR.

The v1 translation set is intentionally tiny — five rewrites per
direction, all listed in the file-level doc comments on the two
translator files. The translator uses a string-aware walker (respects
single-quoted literals and balanced parens) to find each construct;
no full SQL parser is involved.

## Consequences

**Three-leg translation policy.** Cross-engine expressions now flow
through three passes in this order:

1. Identifier-quote / charset-introducer normalization (read boundary,
   in the source engine's reader).
2. Operator/function translation (writer boundary, in the target
   engine's DDL emitter, gated by dialect-tag mismatch).
3. Verbatim passthrough (everything else; the existing fallback).

**Coverage stays narrow on purpose.** v1 covers the constructs Bug 10
named explicitly. Anything else still produces a target-side parse
error; the operator's recovery path is the same as before (drop the
expression on the source and recreate manually, or add a per-column
override when that lands as a future enhancement). The cost of
expanding the table is real — every entry needs an integration test
and a doc note — so growth should be driven by failures observed in
the field, not by chasing completeness.

**Translation isn't a replacement for the fail-loud tenet.** The "loud
failure beats silent corruption" rule still applies. Translation only
runs when the output is mechanically derivable from the input; if a
cast type isn't in the small lookup table, the cast falls through
verbatim and the target's parser produces the loud error. We never
guess.

**Engine-name coupling is loose.** Translators key off the IR's
dialect tag (`"mysql"` / `"postgres"`), not the engine-registry name.
Adding a new MySQL flavor (e.g. another Vitess-derived service) keeps
the same dialect tag and gets the translation layer for free. A future
engine with its own dialect would need its own translator helpers and
a new tag.

**No per-column overrides yet.** Operators with truly novel
expressions still rely on the source-side rewrite path. A future
enhancement could add `--expr-override` (analogous to the existing
type-mapping override) for the cases where neither the verbatim
fallback nor the v1 translation table cover the construct. Out of
scope for this ADR.

## Added in v0.8.0

The v1 translation set was context-free: each rewrite was determined
solely by the function name and arity in the expression text.
Real-world testing surfaced a class of MySQL idioms the rewrite
couldn't handle without column-type context — integer-literal
comparisons against a tinyint(1) column that gets mapped to PG
`BOOLEAN`. PG's strict typing rejects these:

- `0 <> is_active` (in a CHECK constraint)
- `0 = is_active` and `1 = deleted` (in CHECK or generated-column bodies)
- `coalesce(is_active, 0)` (in a generated-column default fallback)

MySQL accepts them all via implicit coercion, so they show up in
production schemas; PG rejects them at CREATE TABLE time. Without
column context the translator can't distinguish `0 = is_active` (a
bool comparison to rewrite) from `0 = qty` (an integer comparison to
leave alone), so v1's design — context-free rewrites — couldn't reach
this case.

The v0.8.0 addition extends the writer-side translator's contract:
`translateExprForPG(expr, ctx)` now takes an `ExprContext` carrying
the set of bool-mapped columns in the table being emitted. Callers
(`translateGeneratedExpr` and `translateCheckExpr` in the PG DDL
writer) build the context from the IR table they already have. The
existing context-free rewrites ignore the new argument; the new
`rewriteBoolIdioms` pass uses it to gate two rewrites:

- `<int_lit> <op> <bool_ident>` and `<bool_ident> <op> <int_lit>`
  where `op ∈ {=, !=, <>}` and `int_lit ∈ {0, 1}` → the int literal
  is replaced with `false` (for `0`) or `true` (for `1`). Other
  comparison operators (`<`, `>`, `<=`, `>=`) and other literals
  (`2`, `NULL`, etc.) fall through verbatim.
- `COALESCE(<bool_ident>, <int_lit>)` and the symmetric
  `COALESCE(<int_lit>, <bool_ident>)` (two-arg shape only) → the
  int literal is rewritten to the matching bool literal. `IFNULL` is
  already renamed to `COALESCE` by an earlier pass, so the rewrite
  catches `IFNULL(is_active, 0)` too.

The pass runs LAST in `translateExprForPG` so the prior rewrites
(`IFNULL → COALESCE`, etc.) have canonicalised the syntax it needs to
match. It uses the same string-aware walker as the v1 rewrites
(`rewriteFunctionCalls`, `scanStringLiteral`, etc.) — no full SQL
parser is involved.

**Scope discipline.** The bool-idiom rewrite ONLY fires when the
caller supplies a non-empty `BoolColumns` set. Every existing test
that calls `translateExprForPG` directly with `ExprContext{}` keeps
its prior behaviour; the new behaviour is opt-in via the table-
context build at the call site. Same loud-failure tenet applies —
patterns outside the rewrite set hit the target's parser.

**Why writer-side, not reader-side.** The MySQL reader already maps
`tinyint(1)` to `ir.Boolean`, so the bool-mapping is a known fact at
read time. But `0` → `false` is PG-specific: the same expression
emitted on a MySQL target is fine as-is. The rewrite has to live at
the target writer's boundary so the IR stays target-agnostic. This
is the same rule that put the v1 set there.

**Direction asymmetry.** Only the MySQL → PG direction is in scope:
the symmetric PG → MySQL case (PG `BOOLEAN` writer comparing against
int) is rare in real schemas and hasn't surfaced in cross-engine
testing. The PG → MySQL translator (`internal/engines/mysql/expr_
translate.go`) is unchanged.

## Added in v0.9.1

Three additional rules surfaced from real-world stretch testing of the v0.9.0 release on production-shaped schemas. Each was the residual of a bug an earlier release partially closed; together they finish the cross-engine rewrite story for the patterns operators actually have in their schemas.

### CAST CHAR with CHARSET / COLLATE

`CAST(x AS CHAR(N) CHARSET utf8mb4 [COLLATE utf8mb4_bin])` is a common MySQL idiom in generated-column bodies and CHECK constraints. PG's grammar rejects both the CHARSET and COLLATE decorations, and the CHAR(N) target itself has different semantics: PG's `CHAR(N)` is fixed-length blank-padded, while MySQL's `CAST(... AS CHAR(N))` truncates without padding.

The new `rewriteCASTCharCharset` rule strips the charset/collate clauses and switches the type to `VARCHAR(N)` — which matches MySQL's no-padding semantics. The bare form `CAST(x AS CHAR)` (no length) becomes `CAST(x AS TEXT)`. Other CAST targets (DECIMAL, DATE, BINARY, etc.) pass through verbatim.

### Outer-column-type-aware COALESCE direction

v0.8.0 / v0.9.0's bool-idiom rewrites always converted the int literal to a bool literal in `coalesce(<bool>, <int_lit>)` patterns. That's the right answer when the outer column is BOOLEAN, but the wrong answer when the outer column is integer-typed — for example, a MySQL `tinyint(1)` source column widened to `smallint` via `--type-override`. In that case the int literal is the right answer and the bool side needs to cast to int instead.

`ExprContext.OuterColumnIsInteger` flips the rewrite direction. `translateGeneratedExpr` sets the flag based on the emitted column's IR type. The comparison rewrite (the other half of the bool-idiom pass) stays bool-context-only — int-context comparisons (`<int_lit> = <bool_ident>`) work via PG's implicit-cast handling.

### Enum-typed generated column body cast

A STORED GENERATED column whose body returns text — typically a `CASE` expression with enum-valued string literals, or a `COALESCE` over text columns — needs an explicit cast to the enum type when the column itself is enum-typed. The error pattern is the same as Bug 23's DEFAULT case: PG rejects with "column X is of type Y_enum but expression is of type text".

The fix lives in `emitColumnDef` rather than the translator: the generated-expression body is wrapped with `::"<enum_type>"` after the body emit. Works for any text-returning shape; no per-arm CASE recognition required. Mirrors the `DEFAULT 'value'::"<enum_type>"` pattern already emitted for non-generated columns. The cast wraps the whole expression body, so:

```
GENERATED ALWAYS AS (CASE WHEN x IS NULL THEN 'a' ELSE 'b' END)::"foo_status_enum" STORED
```

instead of per-arm casting.

## Added in v0.10.0: `--expr-override` (the operator escape hatch)

The pattern-matching translator's coverage has been growing one rule at a time as real-world testing surfaces new idioms. Each rule lands with a test, an ADR-0016 entry, and an integration repro — but the v0.x release cycle has made it clear that operators sometimes hit dialect quirks the table doesn't have a rule for, and the only path forward today is "drop the column on the source, recreate manually." That's an unhelpful failure mode for what's almost always a single-line gap.

`--expr-override` (CLI) and `expression_mappings:` (YAML) are the always-works escape hatch. The operator names a column and supplies the target-dialect expression text directly; sluice emits it verbatim, and the translator skips the column entirely. This separates two questions that were previously bundled:

- **What does sluice know how to translate?** Answer: the rules in this ADR's "Cumulative scope" table. Coverage grows over time.
- **What does the operator do when sluice doesn't know?** Answer: `--expr-override`. Always works; one config line.

### Mechanism

The override applies before the writer-side translator runs. `internal/translate/expr_override.go::ApplyExpressionOverrides` walks the schema, replaces `Column.GeneratedExpr` with the operator's text, and clears `Column.GeneratedExprDialect`. The cleared dialect tag is the signal to `translateGeneratedExpr` (PG side) to short-circuit verbatim — same code path that runs for same-dialect expressions where no translation is needed.

This means the rest of the pipeline (DDL emit, schema preview, schema diff, migrate, sync start) sees the override transparently. No special override-aware path; the column just looks like a same-dialect column from the writer's perspective.

### Strict validation

The override rejects three operator-typo cases at config-load time, before any DSN is dialed:

- Override references a table the source schema doesn't contain.
- Override references a column the table doesn't have.
- Override references a column that exists but isn't a generated column.

The third check exists because "I overrode the wrong column name" is the most common operator mistake and silent passthrough would leave the operator wondering why their override didn't fire. Strict validation surfaces it as `expression_mappings target X.Y but the column is not a generated column`.

### Scope

v0.10.0 covers generated-column bodies only. CHECK constraints, index expressions, and DEFAULT expressions don't have an override surface yet — when (if) real-world testing surfaces the need, each gets its own override type with the same shape. Generated columns are the most-bitten case (Bugs 17 and 23 across the v0.8.x / v0.9.x cycles), so the v1 cut focuses there.

### Cumulative scope

After v0.11.1 the writer-side translator covers:

| Direction | Idiom | Rewrite |
| --- | --- | --- |
| MySQL → PG | `CONCAT(a, b)` | `(a \|\| b)` |
| MySQL → PG | `JSON_UNQUOTE(JSON_EXTRACT(j, '$.k'))` | `(j->>'k')` |
| MySQL → PG | `JSON_EXTRACT(j, '$.k')` | `(j->'k')` |
| MySQL → PG | `IFNULL(a, b)` | `COALESCE(a, b)` |
| MySQL → PG | `IF(c, a, b)` | `CASE WHEN c THEN a ELSE b END` |
| MySQL → PG | `CAST(x AS CHAR(N) [CHARSET y])` | `CAST(x AS VARCHAR(N))` |
| MySQL → PG | `CAST(x AS CHAR)` | `CAST(x AS TEXT)` |
| MySQL → PG | bool-context: `<int_lit> [op] <bool>` / `<bool> [op] <int_lit>` | `<bool_lit> [op] <bool>` etc. |
| MySQL → PG | bool-context: `COALESCE(<bool>, <int_lit>)` | `COALESCE(<bool>, <bool_lit>)` |
| MySQL → PG | int-context: `COALESCE(<expr>, <int_lit>)` | `COALESCE(<expr>::int, <int_lit>)` (v0.10.1: applied unconditionally on non-literal sides) |
| MySQL → PG | `NOW()` / `CURRENT_TIMESTAMP()` (argless) | `CURRENT_TIMESTAMP` (bare keyword) |
| MySQL → PG | `LOCALTIMESTAMP()` / `LOCALTIME()` (argless) | `LOCALTIMESTAMP` (bare keyword) |
| MySQL → PG | `UNIX_TIMESTAMP(x)` | `EXTRACT(EPOCH FROM x)::bigint` |
| MySQL → PG | `UNIX_TIMESTAMP()` (argless) | `EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::bigint` |
| MySQL → PG | `FROM_UNIXTIME(x)` (single-arg) | `TO_TIMESTAMP(x)` |
| MySQL → PG | `CHAR_LENGTH(x)` / `CHARACTER_LENGTH(x)` | `LENGTH(x)` |
| MySQL → PG | `LCASE(x)` | `LOWER(x)` |
| MySQL → PG | `UCASE(x)` | `UPPER(x)` |
| MySQL → PG | `SUBSTR(x, …)` / `MID(x, …)` (2-arg or 3-arg) | `SUBSTRING(x, …)` |
| MySQL → PG | `RAND()` (argless) | `RANDOM()` |
| MySQL → PG | `UUID()` (argless) | `gen_random_uuid()` |
| MySQL → PG | `ISNULL(x)` (function form) | `(x IS NULL)` |
| MySQL → PG | `REGEXP_REPLACE(x, p, r)` (3-arg) | `REGEXP_REPLACE(x, p, r, 'g')` |
| MySQL → PG | `INSTR(s, sub)` | `STRPOS(s, sub)` |
| MySQL → PG | `LOCATE(sub, s)` (2-arg) | `STRPOS(s, sub)` (arg-swap) |
| MySQL → PG | `DATE_ADD(d, INTERVAL n unit)` | `(d + INTERVAL 'n unit')` (singular units only) |
| MySQL → PG | `DATE_SUB(d, INTERVAL n unit)` | `(d - INTERVAL 'n unit')` (singular units only) |
| MySQL → PG | `DATE_FORMAT(x, '<fmt>')` | `TO_CHAR(x, '<pg_fmt>')` (token-mapped; loud-fail on unknown token) |
| PG → MySQL | unchanged | unchanged |

The verbatim-passthrough policy still owns everything outside this set. See `docs/dev/translator-coverage.md` for the next batch of candidate rules sourced from sqlglot, pgloader, and dolt's function registry.

### v0.11.0 batch caveats

- **`UNIX_TIMESTAMP` and STORED generated columns.** PG treats `extract(epoch from timestamp)` as `STABLE`, not `IMMUTABLE`, which blocks STORED generated columns. The rewrite still helps for CHECK constraints, DEFAULTs, and VIRTUAL bodies; STORED bodies fall back to the loud-failure tenet — operator escape via `--expr-override` (v0.10.0).
- **`NOW(precision)` form falls through verbatim.** PG's `CURRENT_TIMESTAMP` keyword does accept `(precision)` (e.g. `CURRENT_TIMESTAMP(6)`), but it has to be present at parse time — the bare keyword form doesn't. The argless form is the overwhelming majority of real-world use; the precision form falls through unchanged today and the loud-failure tenet kicks in if a target rejects it.
- **`FROM_UNIXTIME(epoch, fmt)` two-arg form falls through.** The format-string side has no clean PG equivalent; documented in the catalog as out-of-scope.
- **`CHAR_LENGTH` semantic match.** PG's `LENGTH(text)` returns characters (matching MySQL's `CHAR_LENGTH`); on `bytea` it returns bytes. The rewrite only fires when the source called `CHAR_LENGTH` / `CHARACTER_LENGTH`, so the column-context is implicit. The reverse direction (MySQL `LENGTH(x)` byte length → PG `OCTET_LENGTH(x)`) is not part of this batch — it requires column-type context to fire safely.

### v0.11.1 batch caveats

- **`UUID()` PG version baseline.** `gen_random_uuid()` is built into PG 13+. Pre-13 needs the `uuid-ossp` extension and a different name (`uuid_generate_v4()`). sluice's PG baseline is modern enough that the rewrite assumes 13+ unconditionally; if older PG support becomes a concern, gate on the same version check sluice already runs for capability declaration. Note: the MySQL schema reader may already canonicalize `CHAR(36)` columns with `DEFAULT (UUID())` into the IR's `UUID` type, in which case the type-mapping path emits `DEFAULT gen_random_uuid()` and this expression rewrite never sees the call.
- **`ISNULL(x)` standalone in integer-typed generated columns.** The rewrite emits `(x IS NULL)` which returns boolean; PG's strict typing will reject it as a body for an integer-typed column without a cast. The aggressive `::int` cast (v0.10.1) only fires inside `COALESCE(..., <int_lit>)`, not on standalone bool-returning bodies. Real-world fix: `--expr-override` (v0.10.0). Could be extended to a column-context-aware standalone wrapper if it surfaces.
- **`LOCATE` 3-arg form falls through.** PG's `STRPOS` doesn't accept a starting-position argument; the equivalent expression is a `SUBSTRING(s FROM start) + STRPOS(...)` composition with offset bookkeeping. Single-call rewrite would silently differ; falls through under the loud-failure tenet.
- **`REGEXP_REPLACE` regex-dialect divergence.** MySQL uses ICU regex, PG uses POSIX. The rewrite handles the global-flag arity difference; pattern-syntax divergence (lookaheads, named captures, etc.) is the operator's responsibility — PG raises a clean error on unsupported syntax at apply time, or `--expr-override` for known-divergent patterns.
- **`DATE_ADD` / `DATE_SUB` non-literal counts and compound units fall through.** The PG quoted-interval form needs a literal-typed expression; column-reference counts (`INTERVAL n_days DAY`) and MySQL's compound units (`HOUR_MINUTE`, `DAY_HOUR`, etc.) aren't covered. Could be extended to emit `(d + n_days * INTERVAL '1 day')` for the column-reference case if it surfaces. `QUARTER` also falls through (PG doesn't accept `INTERVAL '1 quarter'`).
- **`DATE_FORMAT` immutability.** PG's `TO_CHAR` is `STABLE`, not `IMMUTABLE` (it depends on `lc_time` and other session GUCs), which blocks the rewrite output from STORED generated columns. The rewrite makes the cross-engine syntax valid; the immutability constraint is the operator's call (use VIRTUAL on PG ≥18, an immutable wrapper function, or `--expr-override`).
- **`DATE_FORMAT` strict mode on unknown tokens.** Any `%X` token outside the supported set causes the entire DATE_FORMAT call to fall through verbatim — silent partial translation would produce a format string PG would render incorrectly without raising an error. The unsupported tokens (`%D` ordinal day suffix, `%U`/`%u`/`%V`/`%v`/`%w`/`%X`/`%x` for various week-numbering modes, `%f` microseconds with different formatting) get the loud-failure path.

## Updated in v0.10.1: aggressive int-context COALESCE cast

v0.9.1 / v0.9.2's int-context COALESCE rewrite gated the `::int` cast on a hand-coded `isBoolReturning` detector that recognised bare bool idents, comparisons (`=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`), `IS NULL`/`IS NOT NULL`, keyword forms (`LIKE`, `BETWEEN`, `IN`), and parenthesised wrappers. v0.10.0 real-world testing kept surfacing expression shapes the detector missed — function calls returning bool, AND/OR chains, NOT prefixes, EXISTS subqueries — each producing a fresh "still not handled" bug report.

v0.10.1 drops the detector and casts the non-literal side unconditionally when the outer column is integer-typed. The reasoning chain:

- The column is integer-typed (the operator's intent for that column's stored value).
- A `COALESCE` whose second arg is a 0/1 literal must produce an int (so it's storable in the int column).
- Therefore the FIRST arg must also be coercible to int.
- A `::int` cast on the first arg either:
  - Helps (bool → int succeeds: true→1, false→0).
  - Is a syntactic no-op (already int).
  - Fails at apply time on a non-numeric expression — but the column's type would have rejected the body anyway. **Loud-failure tenet preserved.**

The cost is one extra `::int` token in the emitted DDL for COALESCE expressions on already-int columns. The benefit is that every bool-returning shape — recognised or not — now translates correctly without operator intervention.

### When this still doesn't fire

`OuterColumnIsInteger` is set only when the column being emitted has IR type `ir.Integer`. For BOOLEAN-typed columns (the v0.8.0 default for `tinyint(1)` columns without `--type-override`), the bool-context path takes over instead — the int literal becomes a bool literal. The two paths are mutually exclusive based on the outer column's IR type.

For columns that aren't generated columns (CHECK constraint references, index expressions), the OuterColumnIsInteger flag isn't set today; CHECK/index expressions return their own types regardless of the column they reference. Operator escape hatch: `--expr-override` (v0.10.0) is the always-works fallback for any case the translator misses.
