// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package exprident holds the low-level, string-literal-aware scan
// primitives and the engine-parameterized identifier re-quoting
// mechanism shared by the MySQL and Postgres expression-translation
// layers (ADR-0045).
//
// Before ADR-0045 these four scan primitives were byte-identical
// duplicates in internal/engines/mysql/expr_walk.go and
// internal/engines/postgres/expr_walk.go. They are dialect-neutral
// (they only understand SQL single-quoted string literals, balanced
// parens/brackets, and ASCII identifier bytes), so they belong in one
// place. The reserved-word / grammar-keyword sets and the quote
// character stay engine-owned (they are dialect definitions); they
// are passed into [RequoteIdentifiers] via [Config].
//
// This package is a leaf: it imports only the standard library, so
// both engine packages can import it without an engine→engine import
// cycle (and the parent internal/translate package is unrelated).
package exprident

import "strings"

// IsIdentifierByte reports whether b is a continuation byte for an
// SQL identifier. ASCII letters, digits, underscore.
func IsIdentifierByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z':
		return true
	case b >= 'A' && b <= 'Z':
		return true
	case b >= '0' && b <= '9':
		return true
	case b == '_':
		return true
	}
	return false
}

// ScanStringLiteral returns the index just past the closing quote of
// the single-quoted string starting at expr[start]. Doubled quotes
// (”) are treated as an escape sequence within the literal. If the
// literal is unterminated (malformed input), returns len(expr) so the
// caller copies the rest verbatim.
func ScanStringLiteral(expr string, start int) int {
	i := start + 1
	for i < len(expr) {
		if expr[i] == '\'' {
			if i+1 < len(expr) && expr[i+1] == '\'' {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return len(expr)
}

// ScanParenGroup takes an index pointing at '(' and returns the index
// of the matching ')' and ok=true. Respects nested parens and string
// literals.
func ScanParenGroup(expr string, open int) (int, bool) {
	if open >= len(expr) || expr[open] != '(' {
		return 0, false
	}
	depth := 1
	for i := open + 1; i < len(expr); {
		switch expr[i] {
		case '\'':
			i = ScanStringLiteral(expr, i)
		case '(':
			depth++
			i++
		case ')':
			depth--
			if depth == 0 {
				return i, true
			}
			i++
		default:
			i++
		}
	}
	return 0, false
}

// SplitTopLevelArgs splits a function-argument string on commas that
// are at depth zero (not inside nested parens, brackets, or string
// literals). Returns nil for an empty / whitespace-only input.
func SplitTopLevelArgs(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); {
		switch s[i] {
		case '\'':
			i = ScanStringLiteral(s, i)
		case '(', '[':
			depth++
			i++
		case ')', ']':
			depth--
			i++
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
				i++
				continue
			}
			i++
		default:
			i++
		}
	}
	parts = append(parts, s[start:])
	return parts
}
