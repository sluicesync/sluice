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
