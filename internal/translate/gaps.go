// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

// Translator-gap scanner — surfaces MySQL → Postgres expression
// patterns that sluice's translator catalog deliberately doesn't
// rewrite, so the operator gets an advisory warning at preview /
// migrate-dry-run time rather than discovering the failure mid-
// migration (or worse: a silent runtime divergence on PG-15+ for
// regex / GREATEST / LEAST patterns whose function name PG accepts
// but interprets differently).
//
// Each [Gap] names the table + column or constraint, the matched
// pattern, the catalog rule number, severity (loud → PG parse-fail at
// apply; silent → PG accepts but diverges in semantics), and an
// operator-actionable `--expr-override` hint. The scanner runs only
// on cross-engine MySQL → Postgres pairs; same-engine pairs and
// PostgreSQL → MySQL migrations don't trip these patterns.

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// Severity classifies how a detected gap will surface if the operator
// proceeds without addressing it.
type Severity int

const (
	// SeverityLoud means PG's parse-time error will catch the gap at
	// apply time. The migration fails before any data corruption can
	// happen; the operator sees a clear `function X does not exist`
	// or similar message.
	SeverityLoud Severity = iota

	// SeveritySilent means PG parses + runs the expression without
	// error but produces different output than MySQL would. Tables
	// migrate cleanly; rows round-trip; the divergence only shows
	// up later when an application or downstream consumer sees the
	// different values.
	SeveritySilent
)

// String returns "loud" / "silent". Used in JSON output and human-
// readable rendering.
func (s Severity) String() string {
	switch s {
	case SeverityLoud:
		return "loud"
	case SeveritySilent:
		return "silent"
	default:
		return "unknown"
	}
}

// Gap is one detected translator-gap site. The fields are populated
// to identify the source location precisely enough that the operator
// can target it with `--expr-override`.
type Gap struct {
	// Table is the source-side table name the gap was detected in.
	Table string

	// Column is the column the gap appears in, when the gap is in a
	// column-level field (DEFAULT or GENERATED). Empty for CHECK
	// constraint gaps.
	Column string

	// Constraint is the named CHECK constraint the gap appears in.
	// Empty for column-level gaps; populated for table-level CHECKs.
	Constraint string

	// Field names which kind of expression carried the gap: "DEFAULT"
	// (DefaultExpression), "GENERATED" (Column.GeneratedExpr), or
	// "CHECK" (CheckConstraint.Expr).
	Field string

	// Expression is the raw source-dialect expression text the
	// scanner matched against. Operators paste this into
	// `--expr-override` lookups; renderers include it verbatim.
	Expression string

	// Pattern is the matched function name (e.g. "GREATEST",
	// "REGEXP_LIKE"). Case is canonicalized to upper for stable
	// JSON keys.
	Pattern string

	// RuleNum is the [`translator-coverage.md`] catalog rule number
	// (#11, #13, etc.) so operators can cross-reference the doc.
	RuleNum int

	// Severity is the surfaced-vs-silent classification.
	Severity Severity

	// Note is the human-readable advisory: what's wrong + what the
	// `--expr-override` snippet to fix it looks like. Stable wording
	// for renderers; ends without trailing newline.
	Note string
}

// ScanMySQLToPGGaps walks every translator-applicable expression in
// schema and returns the list of detected gaps. Cross-engine MySQL →
// Postgres only — the scanner returns nil for any other engine pair
// or when schema is nil.
//
// enabledPGExtensions is the set the operator passed via
// `--enable-pg-extension`; when "pgcrypto" is present the SHA1/SHA2
// detector skips its emission (the rewrite ships per v0.38.0).
//
// Returns gaps sorted by (table, column, constraint, pattern) so
// rendering is stable across runs.
func ScanMySQLToPGGaps(schema *ir.Schema, sourceEngine, targetEngine string, enabledPGExtensions map[string]bool) []Gap {
	if schema == nil {
		return nil
	}
	if !strings.EqualFold(sourceEngine, "mysql") || !strings.EqualFold(targetEngine, "postgres") {
		return nil
	}

	var gaps []Gap
	for _, tbl := range schema.Tables {
		gaps = append(gaps, scanColumnGaps(tbl, enabledPGExtensions)...)
		gaps = append(gaps, scanCheckGaps(tbl, enabledPGExtensions)...)
	}

	sort.SliceStable(gaps, func(i, j int) bool {
		if gaps[i].Table != gaps[j].Table {
			return gaps[i].Table < gaps[j].Table
		}
		if gaps[i].Column != gaps[j].Column {
			return gaps[i].Column < gaps[j].Column
		}
		if gaps[i].Constraint != gaps[j].Constraint {
			return gaps[i].Constraint < gaps[j].Constraint
		}
		return gaps[i].Pattern < gaps[j].Pattern
	})
	return gaps
}

// scanColumnGaps detects gaps in column-level expressions
// (DefaultExpression body, Column.GeneratedExpr). Returns one Gap
// per matched pattern per column-field; a column with both a
// DEFAULT gap and a GENERATED gap returns two entries.
func scanColumnGaps(tbl *ir.Table, enabledExt map[string]bool) []Gap {
	if tbl == nil {
		return nil
	}
	var out []Gap
	for _, col := range tbl.Columns {
		// DEFAULT (DefaultExpression body)
		if def, ok := col.Default.(ir.DefaultExpression); ok && strings.EqualFold(def.Dialect, "mysql") {
			for _, g := range detectGaps(def.Expr, enabledExt) {
				g.Table = tbl.Name
				g.Column = col.Name
				g.Field = "DEFAULT"
				out = append(out, g)
			}
		}
		// GENERATED (Column.GeneratedExpr)
		if col.GeneratedExpr != "" && strings.EqualFold(col.GeneratedExprDialect, "mysql") {
			for _, g := range detectGaps(col.GeneratedExpr, enabledExt) {
				g.Table = tbl.Name
				g.Column = col.Name
				g.Field = "GENERATED"
				out = append(out, g)
			}
		}
	}
	return out
}

// scanCheckGaps detects gaps in CHECK constraint expressions.
// One Gap per matched pattern per constraint.
func scanCheckGaps(tbl *ir.Table, enabledExt map[string]bool) []Gap {
	if tbl == nil {
		return nil
	}
	var out []Gap
	for _, ck := range tbl.CheckConstraints {
		if !strings.EqualFold(ck.ExprDialect, "mysql") {
			continue
		}
		for _, g := range detectGaps(ck.Expr, enabledExt) {
			g.Table = tbl.Name
			g.Constraint = ck.Name
			g.Field = "CHECK"
			out = append(out, g)
		}
	}
	return out
}

// gapPattern is one entry in the detector table. Each names a
// MySQL function (or operator-form) sluice does NOT translate, and
// carries the catalog rule + severity + advisory note.
type gapPattern struct {
	name        string
	rule        int
	severity    Severity
	note        string
	requiresExt string // when non-empty, pattern is skipped if extension is enabled
}

// gapPatterns is the registry of MySQL → PG translator gaps the
// scanner detects. Order doesn't matter — detectGaps sorts results
// by pattern name at the call site.
var gapPatterns = []gapPattern{
	{
		name:     "GREATEST",
		rule:     11,
		severity: SeveritySilent,
		note:     "PG accepts GREATEST but ignores NULL args; MySQL returns NULL if any arg is NULL. Wrap with COALESCE on each side, or use --expr-override.",
	},
	{
		name:     "LEAST",
		rule:     11,
		severity: SeveritySilent,
		note:     "PG accepts LEAST but ignores NULL args; MySQL returns NULL if any arg is NULL. Wrap with COALESCE on each side, or use --expr-override.",
	},
	{
		name:     "REGEXP_LIKE",
		rule:     13,
		severity: SeveritySilent,
		note:     "PG 15+ accepts regexp_like() but uses POSIX regex flavour; MySQL uses ICU. Lookaheads, named groups, and Unicode-property classes don't translate. Use --expr-override with `x ~ 'pattern'` for compatible patterns.",
	},
	{
		name:     "FIND_IN_SET",
		rule:     21,
		severity: SeverityLoud,
		note:     "No portable PG equivalent in CHECK/GENERATED contexts. PG's `(needle = ANY(string_to_array(csv, ',')))` covers membership-only; the full position semantic needs a LATERAL subquery which is invalid in CHECK/GENERATED. Refactor to IN() or use --expr-override.",
	},
	{
		name:     "CONVERT_TZ",
		rule:     23,
		severity: SeverityLoud,
		note:     "PG has no CONVERT_TZ function. Equivalent: `(ts AT TIME ZONE 'from' AT TIME ZONE 'to')`. Semantics depend on the column's timestamp-vs-timestamptz type; verify before using --expr-override.",
	},
	{
		name:     "INET_ATON",
		rule:     29,
		severity: SeverityLoud,
		note:     "No portable PG equivalent in core. Best path: change the column type to PG's native `inet` type via --type-override. For integer-typed legacy columns, a custom IMMUTABLE function is needed; out of scope for sluice's translator.",
	},
	{
		name:     "INET_NTOA",
		rule:     29,
		severity: SeverityLoud,
		note:     "No portable PG equivalent in core. Best path: change the column type to PG's native `inet` type via --type-override.",
	},
	{
		name:        "SHA1",
		rule:        10,
		severity:    SeverityLoud,
		note:        "PG core has no sha1(). v0.38.0 ships SHA1 → encode(digest(x, 'sha1'), 'hex') GATED on `--enable-pg-extension pgcrypto`. Pass the flag (and ensure pgcrypto is installed on the target) for the auto-rewrite, or use --expr-override.",
		requiresExt: "pgcrypto",
	},
	{
		name:        "SHA2",
		rule:        10,
		severity:    SeverityLoud,
		note:        "PG core has no sha2(). v0.38.0 ships SHA2 → encode(digest(x, '<algo>'), 'hex') GATED on `--enable-pg-extension pgcrypto`. Pass the flag (and ensure pgcrypto is installed on the target) for the auto-rewrite, or use --expr-override.",
		requiresExt: "pgcrypto",
	},
}

// LoudGaps returns only the SeverityLoud subset of gaps. These are
// the MySQL-only constructs sluice's translator catalog deliberately
// does NOT rewrite and that PG will reject at parse time — i.e. the
// untranslated tail that, left unrefused, aborts `migrate` after
// partial table creation with no preview warning (Bug 8 structural
// false-green). SeveritySilent gaps stay advisory (they don't abort
// the pipeline; refusing on them would over-block).
//
// This is the keystone of the v0.68.1 structural backstop: a loud
// gap is a hard refusal at BOTH `schema preview` and `migrate`
// pre-flight (before any DDL is applied), so preview is never a
// false-green and migrate never leaves a partially-migrated target.
// The detector keys strictly on the curated known-MySQL-only
// gapPatterns set (word-boundary, string-literal-safe) — a construct
// not in that set degrades to today's late-migrate-failure (no
// worse than status quo), never a false-positive refusal of a valid
// schema.
func LoudGaps(gaps []Gap) []Gap {
	var out []Gap
	for _, g := range gaps {
		if g.Severity == SeverityLoud {
			out = append(out, g)
		}
	}
	return out
}

// RefuseOnLoudGaps returns a non-nil, operator-actionable error when
// scanning schema for the given engine pair surfaces one or more
// SeverityLoud translator gaps; nil otherwise. The error names every
// offending site (table + column/constraint + matched construct) and
// the per-construct workaround so the operator can act before any
// data or DDL moves.
//
// contextID is the caller's phase label ("schema preview" /
// "migrate") so the same diagnostic reads correctly at either
// surface. enabledPGExtensions is the operator's
// `--enable-pg-extension` set (an enabled extension suppresses the
// extension-gated patterns, mirroring ScanMySQLToPGGaps).
//
// Returns nil immediately for non-cross-engine / non-MySQL→PG pairs
// (ScanMySQLToPGGaps already short-circuits those) — the refusal is
// scoped exactly to the direction the gap catalog covers.
func RefuseOnLoudGaps(
	schema *ir.Schema,
	sourceEngine, targetEngine, contextID string,
	enabledPGExtensions map[string]bool,
) error {
	loud := LoudGaps(ScanMySQLToPGGaps(schema, sourceEngine, targetEngine, enabledPGExtensions))
	if len(loud) == 0 {
		return nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d untranslatable MySQL-only construct(s) would emit invalid "+
		"PostgreSQL DDL and abort the migration", contextID, len(loud))
	if contextID == "migrate" {
		b.WriteString(" after partially creating the target")
	}
	b.WriteString(". sluice refuses before any DDL is applied (loud-failure tenet) " +
		"rather than emitting DDL PostgreSQL rejects mid-pipeline:")
	for _, g := range loud {
		b.WriteString("\n  - ")
		if g.Constraint != "" {
			fmt.Fprintf(&b, "table %q CHECK constraint %q", g.Table, g.Constraint)
		} else {
			fmt.Fprintf(&b, "table %q column %q %s", g.Table, g.Column, g.Field)
		}
		fmt.Fprintf(&b, ": %s() has no portable PostgreSQL equivalent. %s", g.Pattern, g.Note)
	}
	return errors.New(b.String())
}

// detectGaps scans expr for every gapPattern that's currently
// in-scope (extension-gated patterns are skipped when the operator
// has opted into the extension). Returns one Gap per match (no
// dedupe for repeated calls in the same expression — operators
// generally want the loud count anyway).
//
// The match is a word-boundary regex on the function-call shape
// `\bNAME\s*\(`. Operator-form REGEXP (`x REGEXP 'pat'`) and RLIKE
// are not detected — covering them would need a richer parser and
// the function-form (REGEXP_LIKE) already surfaces 90% of the
// regex divergence.
func detectGaps(expr string, enabledExt map[string]bool) []Gap {
	if expr == "" {
		return nil
	}
	var out []Gap
	for _, pat := range gapPatterns {
		if pat.requiresExt != "" && enabledExt[pat.requiresExt] {
			// Extension enabled → rewrite ships, skip the warning.
			continue
		}
		if !matchesFunctionCall(expr, pat.name) {
			continue
		}
		out = append(out, Gap{
			Expression: expr,
			Pattern:    pat.name,
			RuleNum:    pat.rule,
			Severity:   pat.severity,
			Note:       pat.note,
		})
	}
	return out
}

// matchesFunctionCall returns true when expr contains a
// case-insensitive word-bounded occurrence of `name(` (with optional
// whitespace between the name and the open-paren). Word-boundary
// matching avoids false positives like `IS_GREATEST_HIT(` matching
// `GREATEST(`.
//
// One regex per pattern is compiled once at first use via the
// gapMatcher cache below — the scanner is not hot-path but no need
// to recompile per-row either.
func matchesFunctionCall(expr, name string) bool {
	re := gapMatcher(name)
	return re.MatchString(expr)
}

// gapMatcherCache caches compiled regexes per function name. The
// pattern is `(?i)\bNAME\s*\(`. Sized to the number of gapPatterns
// (~10 entries); never grows during normal sluice runtime.
var gapMatcherCache = map[string]*regexp.Regexp{}

// gapMatcher returns the compiled `\bNAME\s*\(` regex for name.
// First-use caches; subsequent calls reuse the cached *Regexp.
// Concurrent calls are racy but the worst case is duplicate
// compilation, not a wrong match — acceptable for the scanner's
// once-per-preview-run usage.
func gapMatcher(name string) *regexp.Regexp {
	if re, ok := gapMatcherCache[name]; ok {
		return re
	}
	// `(?i)` makes the match case-insensitive; `\b` is a word boundary;
	// `\s*` permits whitespace between the function name and `(`.
	re := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(name) + `\s*\(`)
	gapMatcherCache[name] = re
	return re
}
