// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

// General MySQL → Postgres untranslatable-expression backstop
// (Bug 14). This is the *allowlist* counterpart to the curated
// denylist in gaps.go (ScanMySQLToPGGaps / RefuseOnLoudGaps).
//
// WHY THIS EXISTS (the v0.68.1 correction)
//
// v0.68.1 shipped a "structural backstop" that refused MySQL→PG
// migrations carrying a *known* MySQL-only construct. That gate keys
// on a curated denylist (gapPatterns: GREATEST, LEAST, REGEXP_LIKE,
// FIND_IN_SET, CONVERT_TZ, INET_ATON/NTOA, SHA1/SHA2). A curated
// denylist is fundamentally insufficient: any MySQL-only construct
// *outside* the curated set (SOUNDEX() in a CHECK; ELT(); MAKE_SET();
// CAST(... AS UNSIGNED); regexp_like(); POINT(x,y) in a generated
// column; …) still fell through the PG writer's translator verbatim.
// `schema preview` then exited 0 emitting invalid PostgreSQL (a
// structural false-green) and `migrate` hard-aborted at create-tables
// with a raw PG SQLSTATE (42883 / 42704 / 42804), leaving a partial
// target. The v0.68.1 notes overclaimed that the backstop "generalizes
// to any untranslated MySQL-ism" — it did not. Bug 14 is the general
// fix.
//
// THE DESIGN: post-translation-equivalent allowlist gate
//
// Instead of denylisting *known-bad* MySQL functions, we allowlist
// *provably-PG-valid* ones. The scanner walks every translator-
// applicable expression (DEFAULT / GENERATED / CHECK) and extracts
// every bare function-call identifier. A call is refused unless its
// name is one of:
//
//   - a MySQL function the PG writer's translator provably rewrites
//     into a PG-valid form (translatedMySQLFunctions) — these never
//     reach PG verbatim, so they are safe;
//   - a PG core / builtin function or the exact output forms the
//     translator emits (pgValidFunctions) — these ARE PG-valid;
//   - a function owned by an extension the operator enabled via
//     `--enable-pg-extension` (ADR-0032 / ADR-0044 catalog).
//
// Anything else — a bare unrecognized function-call identifier in a
// position that would reach PG's parser — is a LOUD, operator-
// actionable refusal at BOTH `schema preview` and `migrate` preflight,
// BEFORE any DDL is applied.
//
// FALSE-POSITIVE SAFETY IS THE LOAD-BEARING RISK
//
// Per the loud-failure tenet's conservatism: a *missed* detection
// degrades to today's late-migrate-failure (no worse than status quo).
// A *false-positive* that refuses a genuinely-valid schema is the real
// hazard — it blocks a migration that would have worked. So the
// scanner is deliberately tight:
//
//   - Only a *bare unrecognized function-call identifier* trips it.
//     String literals, column references, operators sluice already
//     handles (||, ::, <=>→IS NOT DISTINCT FROM, …), the catalogued
//     translations, arithmetic, and SQL keyword-forms (CASE, EXTRACT,
//     ARRAY[...], INTERVAL) do NOT.
//   - Schema/table-qualified calls (`public.f(...)`) are NOT refused
//     (conservative — a missed gate degrades to late-failure; a
//     false-positive on a valid qualified call does not).
//   - A column whose name coincides with a function name is NOT a
//     call (the `(`-follows check distinguishes `total(` from
//     `total > 0`).
//   - The allowlist is conservative-inclusive of the full PG core
//     builtin surface plus every form the translator emits.
//   - `--expr-override` remains the per-expression escape hatch: the
//     override retags the expression dialect off "mysql", and the
//     scanner (like ScanMySQLToPGGaps) only inspects mysql-dialect
//     expressions, so an overridden expression is never gated.
//
// The curated ScanMySQLToPGGaps stays as the *specific actionable-hint*
// layer (better, construct-specific messages for the known cases) on
// TOP of this general gate — it is not replaced.

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate/exprident"
)

// UntranslatableExpr is one detected untranslatable function-call site.
// The fields locate the source precisely enough that the operator can
// target it with `--expr-override`.
type UntranslatableExpr struct {
	// Table is the source-side table name.
	Table string
	// Column is the column the expression belongs to (DEFAULT /
	// GENERATED). Empty for CHECK constraints.
	Column string
	// Constraint is the named CHECK constraint. Empty for column-level.
	Constraint string
	// Field is "DEFAULT" / "GENERATED" / "CHECK".
	Field string
	// Expression is the raw source-dialect expression text.
	Expression string
	// Function is the offending bare function-call identifier
	// (lower-cased; the form the operator will see in the PG error).
	Function string
}

// ScanUntranslatableMySQLToPGExprs walks every translator-applicable
// expression (DEFAULT / GENERATED / CHECK) in schema and returns the
// list of function-call identifiers that are NOT provably PG-valid
// after the MySQL→PG translator runs. Cross-engine MySQL→Postgres
// only — returns nil for any other engine pair or a nil schema
// (mirrors ScanMySQLToPGGaps's scoping exactly).
//
// enabledPGExtensions is the operator's `--enable-pg-extension` set;
// a function owned by an enabled extension is treated as PG-valid.
//
// Results are sorted by (table, column, constraint, function) so the
// refusal message is stable across runs.
func ScanUntranslatableMySQLToPGExprs(
	schema *ir.Schema,
	sourceEngine, targetEngine string,
	enabledPGExtensions map[string]bool,
) []UntranslatableExpr {
	if schema == nil {
		return nil
	}
	if !strings.EqualFold(sourceEngine, "mysql") || !strings.EqualFold(targetEngine, "postgres") {
		return nil
	}

	var out []UntranslatableExpr
	for _, tbl := range schema.Tables {
		if tbl == nil {
			continue
		}
		for _, col := range tbl.Columns {
			if def, ok := col.Default.(ir.DefaultExpression); ok && strings.EqualFold(def.Dialect, "mysql") {
				for _, fn := range untranslatableFunctions(def.Expr, enabledPGExtensions) {
					out = append(out, UntranslatableExpr{
						Table: tbl.Name, Column: col.Name, Field: "DEFAULT",
						Expression: def.Expr, Function: fn,
					})
				}
			}
			if col.GeneratedExpr != "" && strings.EqualFold(col.GeneratedExprDialect, "mysql") {
				for _, fn := range untranslatableFunctions(col.GeneratedExpr, enabledPGExtensions) {
					out = append(out, UntranslatableExpr{
						Table: tbl.Name, Column: col.Name, Field: "GENERATED",
						Expression: col.GeneratedExpr, Function: fn,
					})
				}
			}
		}
		for _, ck := range tbl.CheckConstraints {
			if ck == nil || !strings.EqualFold(ck.ExprDialect, "mysql") {
				continue
			}
			for _, fn := range untranslatableFunctions(ck.Expr, enabledPGExtensions) {
				out = append(out, UntranslatableExpr{
					Table: tbl.Name, Constraint: ck.Name, Field: "CHECK",
					Expression: ck.Expr, Function: fn,
				})
			}
		}
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Table != out[j].Table {
			return out[i].Table < out[j].Table
		}
		if out[i].Column != out[j].Column {
			return out[i].Column < out[j].Column
		}
		if out[i].Constraint != out[j].Constraint {
			return out[i].Constraint < out[j].Constraint
		}
		return out[i].Function < out[j].Function
	})
	return out
}

// untranslatableFunctions returns the distinct (de-duplicated, source-
// order-stable) lower-cased function-call identifiers in expr that are
// NOT provably PG-valid. Empty result means every function call in the
// expression is either a translator-rewritten MySQL function, a
// PG-valid function, or an enabled-extension function.
//
// The walk reuses the shared, battle-tested exprident scan primitives
// (string-literal-aware, paren-balanced) — the SAME discipline the
// ADR-0044 scanExtensionFunctionInExpr scanner uses — so a function
// name appearing inside a quoted string literal is data, not a call,
// and a qualified `schema.fn(` is conservatively skipped.
func untranslatableFunctions(expr string, enabledExt map[string]bool) []string {
	if strings.TrimSpace(expr) == "" {
		return nil
	}
	var out []string
	seen := map[string]bool{}
	for _, name := range scanFunctionCallIdents(expr) {
		lower := strings.ToLower(name)
		if seen[lower] {
			continue
		}
		if pgValidFunctions[lower] || translatedMySQLFunctions[lower] {
			continue
		}
		if enabledExt != nil && extensionFunctionEnabled(lower, enabledExt) {
			continue
		}
		seen[lower] = true
		out = append(out, lower)
	}
	// Bug 12 sub-case: `CAST(x AS UNSIGNED|SIGNED|...)`. The `cast`
	// function itself is legitimately PG-valid (the translator rewrites
	// the CHAR forms), so the function-name scan above correctly does
	// NOT flag it. But MySQL's `UNSIGNED` / `SIGNED` / `UNSIGNED
	// INTEGER` / `SIGNED INTEGER` CAST *target types* have no PG
	// equivalent and the translator passes them through verbatim → PG
	// rejects them (SQLSTATE 42704) mid-pipeline, a structural
	// false-green. Surface them here as a synthetic "cast-as-unsigned"
	// offending token so the gate refuses loudly and the operator gets
	// the `--expr-override` remedy. Conservative: only the exact
	// MySQL-only CAST target keywords trip this; a PG-valid
	// `CAST(x AS numeric)` / `CAST(x AS bigint)` does not.
	if tok := mysqlOnlyCastTarget(expr); tok != "" && !seen[tok] {
		out = append(out, tok)
	}
	return out
}

// mysqlOnlyCastTarget reports the MySQL-only CAST/CONVERT target-type
// keyword (lower-cased, e.g. "cast-as-unsigned") when expr contains a
// top-level `... AS UNSIGNED|SIGNED[ INTEGER]` cast target, or "" when
// it does not. String-literal-aware (a literal `'AS UNSIGNED'` is data,
// not a cast). Deliberately tight — it matches only the exact MySQL
// integer-cast keywords PG has no spelling for; PG-valid cast targets
// (numeric, bigint, text, …) and any other token do not match, so a
// valid `CAST(x AS numeric(20,0))` is never refused.
func mysqlOnlyCastTarget(expr string) string {
	const kw = " AS "
	for i := 0; i < len(expr); {
		if expr[i] == '\'' {
			i = exprident.ScanStringLiteral(expr, i)
			continue
		}
		if i+len(kw) <= len(expr) && strings.EqualFold(expr[i:i+len(kw)], kw) {
			rest := strings.TrimLeft(expr[i+len(kw):], " \t\n\r")
			lr := strings.ToLower(rest)
			switch {
			case strings.HasPrefix(lr, "unsigned integer"):
				return "cast-as-unsigned"
			case strings.HasPrefix(lr, "signed integer"):
				return "cast-as-signed"
			case castWordIs(lr, "unsigned"):
				return "cast-as-unsigned"
			case castWordIs(lr, "signed"):
				return "cast-as-signed"
			}
			i += len(kw)
			continue
		}
		i++
	}
	return ""
}

// castWordIs reports whether lr starts with word as a complete token
// (the next byte after word is not an identifier byte) — so "unsigned"
// matches `UNSIGNED)` / `UNSIGNED ` but not a hypothetical
// `unsignedfoo`.
func castWordIs(lr, word string) bool {
	if !strings.HasPrefix(lr, word) {
		return false
	}
	if len(lr) == len(word) {
		return true
	}
	return !exprident.IsIdentifierByte(lr[len(word)])
}

// scanFunctionCallIdents walks expr and returns every bare
// function-call identifier (a bareword immediately followed, modulo
// whitespace, by `(`). It is intentionally NOT a SQL parser. The walk
// is byte-by-byte, skipping single-quoted string literals so a
// function name inside a literal is not matched, and skipping
// schema/table-qualified names (a leading `.` immediately before the
// token) conservatively.
//
// This mirrors scanExtensionFunctionInExpr's discipline (ADR-0044)
// but returns *all* call idents rather than the first catalog hit,
// because the allowlist gate must classify every call in the
// expression, not just stop at the first extension function.
func scanFunctionCallIdents(expr string) []string {
	var out []string
	// prevSig: previous significant identifier token (upper-cased),
	// reset by any non-space/non-ident byte. Only used to recognise a
	// CAST target — `... AS DECIMAL(10,2)` — so the type specifier is
	// not misread as a `decimal()` call (Bug #16). Mirrors requote.go's
	// prevTok discipline.
	prevSig := ""
	for i := 0; i < len(expr); {
		c := expr[i]
		if c == '\'' {
			// A string literal is a token boundary, never an AfterToken
			// trigger.
			i = exprident.ScanStringLiteral(expr, i)
			prevSig = ""
			continue
		}
		if !isIdentStartByte(c) {
			// Whitespace does not break the prev-significant-token chain
			// (`AS   DECIMAL`); any other punctuation does. `::` and `.`
			// adjacency is recovered by direct lookback below, so a reset
			// here is safe.
			if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
				prevSig = ""
			}
			i++
			continue
		}
		start := i
		j := i + 1
		for j < len(expr) && exprident.IsIdentifierByte(expr[j]) {
			j++
		}
		word := expr[start:j]
		// Must be a call: next non-space byte is '('.
		k := j
		for k < len(expr) && (expr[k] == ' ' || expr[k] == '\t' || expr[k] == '\n' || expr[k] == '\r') {
			k++
		}
		if k < len(expr) && expr[k] == '(' {
			// Skip qualified `qualifier.word(` conservatively.
			qualified := start > 0 && expr[start-1] == '.'
			// A parameterized CAST/`::` *target type* (`CAST(x AS
			// DECIMAL(10,2))`, `x::numeric(12,4)`) is grammar, not a
			// call — Bug #16. This is deliberately context-bound: the
			// token must be a recognised SQL type name AND sit in
			// cast-target position (right after `AS`, or right after
			// `::`). The same word used call-shaped elsewhere (MySQL's
			// `CHAR(65)` scalar — no PG form, not translator-rewritten)
			// is still flagged; a blanket type-name allowlist would
			// re-open the v0.68.1-class false-green.
			castTarget := (prevSig == "AS" || precededByColonColon(expr, start)) &&
				sqlCastTargetTypeNames[strings.ToLower(word)]
			// SQL keyword/operator-forms can legally precede `(` without
			// being a function call: `x IN (...)`, `... AND (...)`,
			// `NOT (...)`, `EXISTS (...)`, `ARRAY[...]`, etc. These are
			// grammar, not callable identifiers — excluding them is a
			// false-positive-safety requirement (a bare `IN (` is NOT an
			// "in()" function).
			if !qualified && !castTarget && !sqlGrammarKeywords[strings.ToLower(word)] {
				out = append(out, word)
			}
		}
		prevSig = strings.ToUpper(word)
		i = j
	}
	return out
}

// precededByColonColon reports whether the byte position start is
// immediately preceded (skipping only spaces/tabs) by the PG cast
// operator `::`. Used to recognise `expr :: type(args)` so the type
// specifier is not misread as a function call (Bug #16).
func precededByColonColon(expr string, start int) bool {
	p := start - 1
	for p >= 0 && (expr[p] == ' ' || expr[p] == '\t') {
		p--
	}
	return p >= 1 && expr[p] == ':' && expr[p-1] == ':'
}

// isIdentStartByte reports whether b can begin an SQL bareword
// identifier (ASCII letter or underscore; a digit cannot start one,
// and a leading digit is part of a numeric literal, never a call).
func isIdentStartByte(b byte) bool {
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

// RefuseOnUntranslatableExprs returns a non-nil, operator-actionable
// error when scanning schema for the given MySQL→PG pair surfaces one
// or more function-call identifiers that are not provably PG-valid;
// nil otherwise. The error names every offending site (table +
// column/constraint + the offending function) and the `--expr-override`
// remedy so the operator can act before any data or DDL moves.
//
// contextID is the caller's phase label ("schema preview" / "migrate")
// so the same diagnostic reads correctly at either surface — the
// message naming is identical at both sites (the v0.68.1-consistency
// requirement). enabledPGExtensions is the operator's
// `--enable-pg-extension` set (an enabled extension's functions are
// PG-valid and don't trip the gate).
//
// Returns nil immediately for non-MySQL→PG pairs
// (ScanUntranslatableMySQLToPGExprs already short-circuits those).
//
// This is the GENERAL backstop. It SUBSUMES the curated
// RefuseOnLoudGaps for any case the curated denylist misses
// (SOUNDEX/ELT/… and Bug 12 / Bug 13) — those become loud refusals
// here rather than silent false-green. The two gates are complementary:
// callers run RefuseOnLoudGaps FIRST (its construct-specific messages
// are better when the curated case applies), then this general gate.
func RefuseOnUntranslatableExprs(
	schema *ir.Schema,
	sourceEngine, targetEngine, contextID string,
	enabledPGExtensions map[string]bool,
) error {
	bad := ScanUntranslatableMySQLToPGExprs(schema, sourceEngine, targetEngine, enabledPGExtensions)
	if len(bad) == 0 {
		return nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d expression(s) reference a function with no provable "+
		"PostgreSQL-valid form and would emit invalid PostgreSQL DDL", contextID, len(bad))
	if contextID == "migrate" {
		b.WriteString(" and abort the migration after partially creating the target")
	}
	b.WriteString(". sluice refuses before any DDL is applied (loud-failure tenet) " +
		"rather than emitting DDL PostgreSQL rejects mid-pipeline:")
	for _, u := range bad {
		b.WriteString("\n  - ")
		if u.Constraint != "" {
			fmt.Fprintf(&b, "table %q CHECK constraint %q", u.Table, u.Constraint)
		} else {
			fmt.Fprintf(&b, "table %q column %q %s", u.Table, u.Column, u.Field)
		}
		var what string
		switch u.Function {
		case "cast-as-unsigned":
			what = "a `CAST(... AS UNSIGNED)` whose MySQL-only target type has no PostgreSQL spelling and"
		case "cast-as-signed":
			what = "a `CAST(... AS SIGNED)` whose MySQL-only target type has no PostgreSQL spelling and"
		default:
			what = u.Function + "(...) is not a PostgreSQL built-in and"
		}
		fmt.Fprintf(&b, ": %s sluice's "+
			"MySQL→PostgreSQL translator does not rewrite it. Supply a "+
			"PostgreSQL-valid form with `--expr-override` (or "+
			"`--exclude-table %s` to skip the table), or enable the owning "+
			"extension with `--enable-pg-extension` if it is extension-provided. "+
			"Source expression: %s",
			what, u.Table, u.Expression)
	}
	return errors.New(b.String())
}
