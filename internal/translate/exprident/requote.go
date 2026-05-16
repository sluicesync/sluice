// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package exprident

import "strings"

// Config parameterizes [RequoteIdentifiers] for a target dialect.
//
// The mechanism (literal-aware byte walk, already-quoted passthrough,
// function-call / numeric / keyword exclusion) is dialect-neutral and
// lives here. The dialect-specific inputs are:
//
//   - QuoteByte: the identifier quote character — '`' for MySQL,
//     '"' for PostgreSQL.
//   - Reserved: the target's reserved-word set (upper-cased keys).
//     These are dialect definitions and stay engine-owned.
//   - GrammarExclusions: the subset of reserved words that legitimately
//     appear unquoted in an expression body in a grammatical role
//     (operators, NULL/boolean literals, CAST target type names,
//     CASE/control keywords, …). A token in this set is never
//     re-quoted even though it is reserved (upper-cased keys).
//   - SkipWSBeforeParen: when true, whitespace (space/tab) between an
//     identifier and a following '(' is skipped before deciding the
//     token is in call/type-name position. Postgres needs this (its
//     emitted text can read `coalesce (...)`); MySQL does not (its
//     historical behaviour requires the '(' to immediately follow).
//     This single flag is the only behavioural divergence between the
//     two engines' historical helpers and is preserved exactly.
type Config struct {
	QuoteByte         byte
	Reserved          map[string]struct{}
	GrammarExclusions map[string]struct{}
	SkipWSBeforeParen bool
}

// IsIdentStartByte reports whether b can begin an unquoted SQL
// identifier (letter or underscore — not a digit, so numeric literals
// aren't mistaken for identifiers).
func IsIdentStartByte(b byte) bool {
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

// RequoteIdentifiers re-applies the target dialect's identifier
// quoting to bare tokens in an expression body that are reserved words
// used as column references.
//
// Background (ADR-0016 three-leg policy / ADR-0045 consolidation): a
// source reader strips its own engine's identifier quotes at the read
// boundary so the IR text is portable to either writer (Postgres can't
// accept backticks; the MySQL reader strips backticks). That strip is
// lossy for the narrow case of an identifier that is also a reserved
// word in the target — e.g. a column named `order` or `key`. Once the
// quotes are gone, the target parser rejects the bare reserved word.
// This pass re-quotes exactly those tokens.
//
// It is deliberately a small, mechanical pass:
//
//   - String literals are copied verbatim (literal-aware walk).
//   - Already-quoted identifiers (delimited by cfg.QuoteByte, with the
//     doubled-quote escape) are copied verbatim.
//   - A bare token is re-quoted only when it is in cfg.Reserved, is NOT
//     in cfg.GrammarExclusions, and is NOT in call/type-name position
//     (immediately — or, when cfg.SkipWSBeforeParen, across whitespace
//     — followed by '('; several reserved words double as built-in
//     function or type names).
//   - Numeric literals are never treated as identifiers (the token
//     scan starts only on [IsIdentStartByte]).
//
// Everything else is verbatim passthrough, consistent with the
// project's translation policy.
func RequoteIdentifiers(expr string, cfg Config) string {
	if expr == "" {
		return expr
	}
	q := cfg.QuoteByte
	var sb strings.Builder
	sb.Grow(len(expr) + 8)
	for i := 0; i < len(expr); {
		c := expr[i]
		switch {
		case c == '\'':
			// String literal — copy verbatim.
			end := ScanStringLiteral(expr, i)
			sb.WriteString(expr[i:end])
			i = end
		case c == q:
			// Already-quoted identifier — copy verbatim (including the
			// closing quote, honouring doubled-quote escapes).
			j := i + 1
			for j < len(expr) {
				if expr[j] == q {
					if j+1 < len(expr) && expr[j+1] == q {
						j += 2
						continue
					}
					j++
					break
				}
				j++
			}
			sb.WriteString(expr[i:j])
			i = j
		case IsIdentStartByte(c):
			j := i
			for j < len(expr) && IsIdentifierByte(expr[j]) {
				j++
			}
			tok := expr[i:j]
			// Decide whether the token is in call / type-name position.
			k := j
			if cfg.SkipWSBeforeParen {
				for k < len(expr) && (expr[k] == ' ' || expr[k] == '\t') {
					k++
				}
			}
			callPos := k < len(expr) && expr[k] == '('
			if !callPos && shouldRequote(tok, cfg) {
				sb.WriteByte(q)
				sb.WriteString(tok)
				sb.WriteByte(q)
			} else {
				sb.WriteString(tok)
			}
			i = j
		default:
			sb.WriteByte(c)
			i++
		}
	}
	return sb.String()
}

// shouldRequote reports whether tok (case-insensitive) is a reserved
// word that must be quoted when it appears as a column reference
// inside an expression — i.e. it is in cfg.Reserved AND it is not one
// of the expression-grammar keywords that legitimately appear unquoted
// in an expression body.
func shouldRequote(tok string, cfg Config) bool {
	u := strings.ToUpper(tok)
	if _, excluded := cfg.GrammarExclusions[u]; excluded {
		return false
	}
	_, reserved := cfg.Reserved[u]
	return reserved
}
