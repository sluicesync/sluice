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
//   - GrammarContextual: reserved words that are grammar glue ONLY in a
//     specific syntactic position and are an ordinary column identifier
//     everywhere else. `FROM` is the motivating case: it is grammar in
//     `IS [NOT] DISTINCT FROM` and inside `EXTRACT(…FROM…)` /
//     `SUBSTRING(…FROM…)` / `TRIM(…FROM…)` / `OVERLAY(…FROM…)`, but a
//     de-quoted user column literally named `from` must still be
//     re-quoted. A blanket GrammarExclusions entry would suppress the
//     column case and emit invalid DDL (SQLSTATE 42601); the contextual
//     rule discriminates. Keys are upper-cased; see [ContextRule].
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
	GrammarContextual map[string]ContextRule
	SkipWSBeforeParen bool
}

// ContextRule describes when a [Config.GrammarContextual] token is
// grammar glue (left bare) versus a column identifier (re-quoted). The
// token is treated as grammar — and therefore NOT re-quoted — when
// either:
//
//   - the previous significant token (the last identifier-like token
//     scanned, ignoring whitespace and punctuation) is in AfterToken, or
//   - the innermost enclosing function-call name is in InFunction.
//
// Both sets are upper-cased keys; the probe is upper-cased. When
// neither matches, the token falls through to ordinary reserved-word
// re-quoting. This is deliberately the tightest discrimination that
// keeps grammar-`FROM` bare while re-quoting a column named `from`:
// AfterToken={DISTINCT} covers `IS [NOT] DISTINCT FROM`;
// InFunction={EXTRACT,SUBSTRING,TRIM,OVERLAY} covers the special
// function syntaxes that take a bare `FROM`. A bare `FROM` in any other
// expression position is, by PG/MySQL grammar, only ever a de-quoted
// column reference.
type ContextRule struct {
	AfterToken map[string]struct{}
	InFunction map[string]struct{}
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
//     in cfg.GrammarExclusions, is NOT exempted by a matching
//     cfg.GrammarContextual rule, and is NOT in call/type-name position
//     (immediately — or, when cfg.SkipWSBeforeParen, across whitespace
//     — followed by '('; several reserved words double as built-in
//     function or type names).
//   - Numeric literals are never treated as identifiers (the token
//     scan starts only on [IsIdentStartByte]).
//
// To evaluate cfg.GrammarContextual the walk maintains two cheap pieces
// of state: prevTok (the last identifier-like token seen, upper-cased)
// and fnStack (the enclosing function-call names by paren depth). Both
// are pure bookkeeping — they do not change the output for any token
// that has no contextual rule.
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
	// prevTok: last identifier-like token seen (upper-cased), for
	// ContextRule.AfterToken. fnStack: enclosing function-call names by
	// paren depth (upper-cased; "" for a non-call paren group), for
	// ContextRule.InFunction.
	prevTok := ""
	var fnStack []string
	for i := 0; i < len(expr); {
		c := expr[i]
		switch {
		case c == '\'':
			// String literal — copy verbatim. A literal is a
			// significant token boundary: clear prevTok so a `FROM`
			// after e.g. `TRIM(BOTH ' ' FROM s)` is not mistaken for
			// AfterToken=' '-was-an-ident (it never was).
			end := ScanStringLiteral(expr, i)
			sb.WriteString(expr[i:end])
			prevTok = ""
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
			// A quoted identifier is not a bare keyword; it cannot be
			// the AfterToken trigger for a following contextual word.
			prevTok = ""
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
			if !callPos && shouldRequote(tok, cfg, prevTok, fnStack) {
				sb.WriteByte(q)
				sb.WriteString(tok)
				sb.WriteByte(q)
			} else {
				sb.WriteString(tok)
			}
			// Bookkeeping: remember this token for the next contextual
			// decision. If it heads a call, the matching '(' (handled
			// in the default branch) will pop it off as the function
			// name for that depth.
			prevTok = strings.ToUpper(tok)
			i = j
		case c == '(':
			// Push the enclosing function name for this depth: prevTok
			// is the call head iff this '(' immediately follows an
			// identifier token (the call-position test above used the
			// same adjacency). A non-call '(' pushes "".
			fnStack = append(fnStack, prevTok)
			sb.WriteByte(c)
			prevTok = ""
			i++
		case c == ')':
			if n := len(fnStack); n > 0 {
				fnStack = fnStack[:n-1]
			}
			sb.WriteByte(c)
			prevTok = ""
			i++
		default:
			// Any other punctuation/operator byte ends the current
			// token run but is not itself an AfterToken trigger.
			if c != ' ' && c != '\t' {
				prevTok = ""
			}
			sb.WriteByte(c)
			i++
		}
	}
	return sb.String()
}

// shouldRequote reports whether tok (case-insensitive) is a reserved
// word that must be quoted when it appears as a column reference
// inside an expression — i.e. it is in cfg.Reserved, is NOT an
// unconditional expression-grammar keyword, and is NOT exempted by a
// matching contextual rule for the current position (prevTok is the
// last identifier-like token, upper-cased; fnStack is the enclosing
// function-call-name stack, upper-cased).
func shouldRequote(tok string, cfg Config, prevTok string, fnStack []string) bool {
	u := strings.ToUpper(tok)
	if _, excluded := cfg.GrammarExclusions[u]; excluded {
		return false
	}
	if _, reserved := cfg.Reserved[u]; !reserved {
		return false
	}
	// Reserved — but a contextual rule may say it is grammar glue in
	// this exact position (e.g. `FROM` after `DISTINCT` or inside an
	// EXTRACT/SUBSTRING/TRIM/OVERLAY call). Outside those positions a
	// bare reserved word is a de-quoted column reference and is
	// re-quoted.
	if rule, ok := cfg.GrammarContextual[u]; ok && rule.isGrammar(prevTok, fnStack) {
		return false
	}
	return true
}

// isGrammar reports whether a [Config.GrammarContextual] token is in a
// grammar position: the previous significant token is in AfterToken,
// or the innermost enclosing function-call name is in InFunction.
func (r ContextRule) isGrammar(prevTok string, fnStack []string) bool {
	if prevTok != "" {
		if _, ok := r.AfterToken[prevTok]; ok {
			return true
		}
	}
	if n := len(fnStack); n > 0 {
		if _, ok := r.InFunction[fnStack[n-1]]; ok {
			return true
		}
	}
	return false
}
