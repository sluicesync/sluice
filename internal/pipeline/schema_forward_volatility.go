// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// ADR-0058 §2a — text-based volatility detection on ADD COLUMN
// DEFAULT expressions.
//
// Bug 90 closure (v0.79.1): v0.79.0 shipped --forward-schema-add-column
// with a refuse-loudly check that only fired when [ir.Column.Default]
// was [ir.DefaultExpression]. The CDC reader's RelationMessage / binlog
// projection drops the DEFAULT clause entirely (pgoutput's
// RelationMessage doesn't carry attdefault; MySQL's TableMapEvent
// doesn't carry it either), so the post-DDL [ir.SchemaSnapshot] always
// arrives with Default == nil. The existing check is dead-code in the
// production path; computed/volatile DEFAULTs forwarded silently,
// causing source/target divergence on every pre-existing row (the
// source materialized per-row values via the table rewrite; the target
// session's ALTER materialized a single target-session-evaluated value
// for its own pre-existing rows — or NULL if the column is nullable
// and the engine's ADD COLUMN DEFAULT semantics don't backfill).
//
// The fix runs a text-based volatility scan on the source's DEFAULT
// expression text (probed at intercept time via a source SchemaReader
// because the CDC stream cannot carry it). The scan is deliberately
// conservative — refuse-on-uncertainty: any function call that isn't
// on the known-deterministic allowlist triggers refusal. Better to
// over-refuse and force the operator to use the drained-model recovery
// than to silently corrupt.

import (
	"fmt"
	"regexp"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// volatileDefaultFunctions is the explicit deny-list of source DEFAULT
// function names that diverge between source per-row materialization
// and target-session ALTER evaluation. ADR-0058 §2a names NOW(),
// nextval(...), random() as the documented examples; the list below
// extends to the obvious cousins on PG and MySQL.
//
// Names are lowercase; the scanner lowercases the input before
// matching. Both engines accept these identifiers case-insensitively
// in SQL, so the match must be case-insensitive too.
var volatileDefaultFunctions = map[string]struct{}{
	// ----- Time-volatile (PG + MySQL) -----
	"now":                   {},
	"current_timestamp":     {},
	"current_time":          {},
	"current_date":          {},
	"localtime":             {},
	"localtimestamp":        {},
	"transaction_timestamp": {}, // PG
	"statement_timestamp":   {}, // PG
	"clock_timestamp":       {}, // PG
	"timeofday":             {}, // PG (returns text)
	"utc_timestamp":         {}, // MySQL
	"utc_date":              {}, // MySQL
	"utc_time":              {}, // MySQL
	"unix_timestamp":        {}, // MySQL
	"sysdate":               {}, // MySQL + Oracle-ish PG via extension
	"curdate":               {}, // MySQL
	"curtime":               {}, // MySQL

	// ----- Sequence-stateful (PG + MySQL) -----
	"nextval":        {}, // PG
	"currval":        {}, // PG
	"setval":         {}, // PG
	"last_insert_id": {}, // MySQL
	"lastval":        {}, // PG

	// ----- Random / non-deterministic -----
	"random":             {}, // PG
	"gen_random_uuid":    {}, // PG (pgcrypto + 13+ built-in)
	"uuid_generate_v1":   {}, // PG (uuid-ossp)
	"uuid_generate_v1mc": {}, // PG (uuid-ossp)
	"uuid_generate_v3":   {}, // PG (uuid-ossp) — deterministic per (ns,name) but the input usually varies
	"uuid_generate_v4":   {}, // PG (uuid-ossp)
	"uuid_generate_v5":   {}, // PG (uuid-ossp) — same caveat as v3
	"rand":               {}, // MySQL
	"uuid":               {}, // MySQL
	"uuid_short":         {}, // MySQL

	// ----- Session-state (PG) -----
	"current_user":     {},
	"session_user":     {},
	"user":             {}, // alias for current_user in PG / connection user in MySQL
	"current_role":     {},
	"current_schema":   {},
	"current_database": {},
	"current_catalog":  {},
	"inet_client_addr": {},
	"inet_client_port": {},
	"inet_server_addr": {},
	"inet_server_port": {},
	"pg_backend_pid":   {},

	// ----- Crypto-random / session-state (MySQL) -----
	"connection_id": {},
}

// deterministicDefaultFunctions is the explicit allowlist of source
// DEFAULT function names sluice recognizes as definitely-deterministic
// at evaluation time. Refuse-on-uncertainty means we refuse on any
// function name not on this list — but a small allowlist keeps the
// common safe cases (string formatting, simple math, casts) from
// triggering false refusals.
//
// Names are lowercase. Conservative: when in doubt, leave OFF this
// list so the refuse-on-uncertainty default kicks in.
var deterministicDefaultFunctions = map[string]struct{}{
	"abs":         {},
	"ceil":        {},
	"ceiling":     {},
	"floor":       {},
	"round":       {},
	"trunc":       {},
	"length":      {},
	"char_length": {},
	"upper":       {},
	"lower":       {},
	"concat":      {},
	"coalesce":    {},
	"greatest":    {},
	"least":       {},
	"nullif":      {},
	"cast":        {},
	"convert":     {},
}

// funcNameRE matches identifier-followed-by-open-paren patterns —
// the shape of a SQL function call. Captures the identifier (group 1).
// Matches both schema-qualified ("public.nextval") and bare names;
// the scanner strips the schema-qualifier before lookup.
var funcNameRE = regexp.MustCompile(`(?i)\b([a-z_][a-z0-9_]*)\s*\(`)

// bareNameRE matches a bare identifier (no following parens) — for
// keyword forms like CURRENT_TIMESTAMP, CURRENT_DATE, LOCALTIME that
// PG accepts without parens. Captures the identifier (group 1).
var bareNameRE = regexp.MustCompile(`(?i)\b([a-z_][a-z0-9_]*)\b`)

// classifyDefaultVolatility examines a DEFAULT expression text and
// returns:
//
//   - (true, "") when the expression is safe to forward — a literal
//     constant, NULL, a quoted string, a numeric, or a function call
//     limited to the deterministic allowlist.
//   - (false, reason) when the expression is unsafe — names a
//     known-volatile function or contains an unknown function call
//     (refuse-on-uncertainty).
//
// The detection is purely syntactic — no parser, no execution. ADR-0058
// §2a explicitly takes the conservative path: better to over-refuse
// than to silently forward a DEFAULT whose target-session evaluation
// would diverge from the source's per-row insert values.
func classifyDefaultVolatility(expr string) (safe bool, reason string) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return true, ""
	}
	// Strip a single trailing PG type cast (`::type`) — the schema
	// reader's translateDefault already does this on Default,
	// but the probe might surface raw column_default text. Cheap
	// belt-and-suspenders.
	trimmed = stripTrailingTypeCast(trimmed)
	// Strip outer parens (PG sometimes wraps the expression).
	for len(trimmed) >= 2 && trimmed[0] == '(' && trimmed[len(trimmed)-1] == ')' {
		// Only strip if the parens balance to the outer pair.
		if !parensBalanceAtOuter(trimmed) {
			break
		}
		trimmed = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	}
	lower := strings.ToLower(trimmed)

	// Quick-pass for clearly literal shapes: a quoted string, a
	// numeric, a boolean, NULL, TRUE/FALSE.
	if isLiteralShape(lower) {
		return true, ""
	}

	// Bare-name volatile keywords (CURRENT_TIMESTAMP, CURRENT_DATE,
	// LOCALTIME, LOCALTIMESTAMP without parens — PG syntax).
	for _, m := range bareNameRE.FindAllStringSubmatch(lower, -1) {
		name := m[1]
		if _, ok := volatileDefaultFunctions[name]; ok {
			return false, fmt.Sprintf("references volatile/stateful identifier %q", name)
		}
	}

	// Function calls — any "<name>(" pattern. Strip an optional
	// schema qualifier ("pg_catalog.now" → "now") before lookup.
	for _, m := range funcNameRE.FindAllStringSubmatch(lower, -1) {
		full := m[1]
		// PG often qualifies built-ins with "pg_catalog."; the
		// regex captures only the rightmost identifier (it doesn't
		// include the dot), so the schema-qualifier appears in the
		// preceding text — which doesn't change the match. The
		// scanner's lookup is on the bare name.
		bare := full
		if idx := strings.LastIndex(bare, "."); idx >= 0 {
			bare = bare[idx+1:]
		}
		if _, ok := volatileDefaultFunctions[bare]; ok {
			return false, fmt.Sprintf("references volatile/stateful function %q", bare)
		}
		if _, ok := deterministicDefaultFunctions[bare]; ok {
			continue
		}
		// Refuse-on-uncertainty: an unknown function name is
		// treated as unsafe. ADR-0058 §2a takes this conservative
		// stance — operators with a deterministic custom function
		// must use the drained-model recovery rather than rely on
		// sluice's classifier.
		return false, fmt.Sprintf("references unknown function %q "+
			"(sluice cannot prove determinism; refusing on uncertainty)", bare)
	}
	return true, ""
}

// classifyDefaultValueVolatility wraps [classifyDefaultVolatility] on
// an [ir.DefaultValue]. Returns (true, "") for [ir.DefaultNone] and
// [ir.DefaultLiteral] (both unambiguously safe). For
// [ir.DefaultExpression], delegates to the text-scan. For nil (the
// CDC-projection case where Default isn't carried), treats as no-info
// and returns (true, "") — the intercept's probe surfaces the
// source's canonical Default and re-classifies via this function.
func classifyDefaultValueVolatility(d ir.DefaultValue) (safe bool, reason string) {
	switch v := d.(type) {
	case nil:
		// No info — caller must probe the source. Treat as safe at
		// this layer; the higher-level caller (probeAndClassify) is
		// responsible for surfacing the canonical text.
		return true, ""
	case ir.DefaultNone:
		return true, ""
	case ir.DefaultLiteral:
		return true, ""
	case ir.DefaultExpression:
		return classifyDefaultVolatility(v.Expr)
	default:
		return false, fmt.Sprintf("unrecognized DefaultValue type %T", d)
	}
}

// isLiteralShape returns true if s is unambiguously a literal: a
// quoted string ('...'), a numeric, a boolean keyword, NULL, TRUE,
// FALSE, or a parameterised array-of-literals shape. Case-insensitive
// callers should lowercase first.
func isLiteralShape(s string) bool {
	if s == "" {
		return true
	}
	if s == "null" || s == "true" || s == "false" {
		return true
	}
	// Quoted string literal: 'value' (PG/MySQL); doubled inner
	// quotes are allowed but don't affect the shape check.
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return true
	}
	// Numeric literal: leading sign + digits + optional decimal +
	// optional exponent. Cheap regex.
	if numericRE.MatchString(s) {
		return true
	}
	return false
}

var numericRE = regexp.MustCompile(`^[+-]?\d+(\.\d+)?([eE][+-]?\d+)?$`)

// stripTrailingTypeCast removes a single `::type` suffix from a PG
// expression. Mirrors postgres.stripTypeCast but stays in-package to
// avoid a cross-engine dependency from the pipeline layer.
func stripTrailingTypeCast(s string) string {
	// Find the last "::" not inside parens.
	depth := 0
	for i := 0; i < len(s)-1; i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ':':
			if depth == 0 && s[i+1] == ':' {
				// Found a top-level ::; only strip if it's the
				// LAST one and the suffix looks like a type name.
				suffix := strings.TrimSpace(s[i+2:])
				// Type name is [a-zA-Z_][a-zA-Z0-9_ (),"]+ — loose
				// check that there's no further ::.
				if !strings.Contains(suffix, "::") && isTypeNameShape(suffix) {
					return strings.TrimSpace(s[:i])
				}
			}
		}
	}
	return s
}

func isTypeNameShape(s string) bool {
	if s == "" {
		return false
	}
	// Allow identifiers, dots, spaces, parens, digits, commas,
	// double quotes — the universe of PG type names.
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_' || r == '.' || r == ' ' || r == '(' || r == ')' ||
			r == ',' || r == '"' || r == '[' || r == ']':
		default:
			return false
		}
	}
	return true
}

// parensBalanceAtOuter returns true if s starts with '(' and ends
// with ')' AND the opening paren matches the closing paren (not two
// independent sub-expressions like "(a)+(b)").
func parensBalanceAtOuter(s string) bool {
	if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
		return false
	}
	depth := 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i < len(s)-1 {
				return false
			}
		}
	}
	return depth == 0
}
