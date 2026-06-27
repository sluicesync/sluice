// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-dialect expression translation for the MySQL writer.
//
// Translation in sluice is a layered policy:
//
//  1. Identifier-quote / charset-introducer normalization runs at the
//     read boundary in the source engine. The Postgres reader's
//     pg_get_expr already emits canonical, non-quoted expression text;
//     the MySQL reader strips backticks, MySQL's `_charset'…'`
//     introducers, and the C-style apostrophe-escape form. By the time
//     IR gets here the body is dialect-quoted only by its operators
//     and function names.
//
//  2. A small set of high-frequency operator/function rewrites runs
//     here, at the writer boundary, when an IR expression's dialect
//     tag (Column.GeneratedExprDialect / CheckConstraint.ExprDialect)
//     differs from this writer's dialect. The translation table is
//     intentionally tiny — see the v1 scope below — and only handles
//     idioms that broke real cross-engine migrations during testing.
//
//  3. Anything not matched by either pass falls through verbatim. The
//     "loud failure on the target beats silent corruption" tenet still
//     holds: an unrecognized non-portable construct surfaces as a
//     CREATE TABLE rejection on the target, not a guess at translation.
//
// v1 scope (Postgres → MySQL):
//
//   - (expr)::type → CAST(expr AS <mysql-type>)
//     PG cast operator. The type-name table is small: numeric →
//     DECIMAL, text/varchar → CHAR, boolean → UNSIGNED (TINYINT(1)
//     can't appear inside CAST), int → SIGNED, bigint → SIGNED.
//
//   - a || b → CONCAT(a, b)
//     PG string concatenation. Multi-arg || chains collapse into a
//     single CONCAT call. We do not assume MySQL's PIPES_AS_CONCAT
//     SQL mode.
//
//   - expr ~~ pat   → expr LIKE pat
//   - expr ~~* pat  → LOWER(expr) LIKE LOWER(pat)
//     PG operator forms of LIKE / ILIKE.
//
//   - col = ANY(ARRAY[a, b, c]) → col IN (a, b, c)
//
// See ADR-0016 for the design rationale.

package mysql

// dialectName is the canonical name this engine uses for the
// ExprDialect / GeneratedExprDialect tags on IR expressions. Held as
// a constant so readers and the translator stay in sync if the
// canonical name ever changes. Both MySQL flavors (vanilla,
// PlanetScale) share the same dialect tag — the wire dialect is
// MySQL even when the registry name is "planetscale".
const dialectName = "mysql"

// translatableSourceDialect is the ONE other engine's dialect this writer's
// cross-dialect translator ([translateExprForMySQL]) is built to accept as
// input — Postgres. The DDL-emit dispatch translates an IR expression ONLY when
// its dialect tag equals this; every other value — "mysql" (self), "" (untagged
// same-engine / hand-built IR), "sqlite", or any future engine — emits VERBATIM
// (ADR-0133 §2). This closes the latent bug where an unknown dialect was fed
// through the PG→MySQL translator (a silent mistranslation): a SQLite-tagged
// body now passes through and fails loudly at target DDL time if non-portable,
// rather than being silently rewritten. Byte-identical for every existing
// producer — the only non-self, non-empty dialect produced today is "postgres".
const translatableSourceDialect = "postgres"

// translateExprForMySQL translates a Postgres-dialect expression into
// MySQL-dialect form for the v1 set of cross-engine constructs.
// Unrecognized constructs pass through verbatim — translation is
// additive on top of the existing verbatim-passthrough policy.
//
// The input is the IR's expression text, already stripped of
// schema-qualified casts pg_get_expr sometimes emits (we don't
// assume PG removes those — the cast rewriter handles them). What
// remains is the substantive expression body in PG dialect.
func translateExprForMySQL(expr string) string {
	if expr == "" {
		return expr
	}
	// Source-quote normalization (the first leg of ADR-0016's three-leg
	// policy, performed writer-side for the PG→MySQL direction). The PG
	// reader's pg_get_expr quotes identifiers that require it — a
	// reserved-word column ref like `order` comes back as the double-
	// quoted "order". Unlike the MySQL reader (which strips its
	// backticks at the read boundary), the PG reader cannot strip these
	// — it needs them for same-dialect PG→PG correctness — so the
	// MySQL writer must convert PG's double-quote identifier form to
	// MySQL's backtick form here, before the requote leg runs. Without
	// this, a PG-source generated/CHECK/index/DEFAULT body referencing
	// a reserved-word column emits the broken `"`order`"` shape and the
	// MySQL parser mis-evaluates it (catalog: the ADR-0045 §4 sweep's
	// PG→MySQL leg). This runs first because the later operator/function
	// rewrites and the requote pass must see MySQL-form identifiers.
	expr = rewritePGIdentQuotes(expr)
	// Order matters. Casts must run before the || rewriter so a cast
	// on a string-concat operand doesn't confuse the concat-chain
	// detector. ANY(ARRAY[...]) must run before the cast pass too —
	// a status::text = ANY (ARRAY['a'::text, 'b'::text]) reads more
	// cleanly when the outer = ANY is normalized first.
	expr = rewriteEqAnyArray(expr)
	expr = rewritePGCasts(expr)
	expr = rewriteLikeOperators(expr)
	expr = rewriteConcatOperator(expr)
	return expr
}
