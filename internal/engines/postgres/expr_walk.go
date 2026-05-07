// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// String-aware walker shared by translateExprForPG. Kept in a
// separate file from the rewrite rules so the v1 translation table
// stays the load-bearing artifact in expr_translate.go and this file
// holds only mechanical parsing helpers.

package postgres

import (
	"strings"
	"unicode"
)

// rewriteFunctionCalls walks expr, finds every call to fnName at top
// level (respecting string literals), and replaces it with the result
// of replacer(args). When replacer returns "" the call is left
// untouched (the verbatim-passthrough fallback). The walker recurses
// into argument text first so nested calls of the same function are
// rewritten innermost-first.
//
// Function-name matching is case-insensitive. The opening paren is
// allowed to be preceded by zero or more spaces — MySQL's
// information_schema can format a call with a space before the
// paren on the way back out (e.g. "concat (a, b)"). The check that
// a candidate is actually a call (not a reference to a column whose
// name happens to coincide with the function) is "the next non-space
// char after the identifier is '('".
func rewriteFunctionCalls(expr, fnName string, replacer func(args []string) string) string {
	var sb strings.Builder
	for i := 0; i < len(expr); {
		// Skip and copy through string literals verbatim. PG and
		// MySQL both single-quote strings; both accept '' as the
		// escape for an embedded quote (PG uses standard_conforming_
		// strings, MySQL stores the same escaped form after the read-
		// boundary normalizer runs).
		if expr[i] == '\'' {
			end := scanStringLiteral(expr, i)
			sb.WriteString(expr[i:end])
			i = end
			continue
		}
		// Try to match the function name at this position.
		nameLen, ok := matchIdentifier(expr, i, fnName)
		if !ok {
			sb.WriteByte(expr[i])
			i++
			continue
		}
		// Ensure the previous character isn't part of an identifier
		// — we don't want CONCAT to match the tail of MYCONCAT.
		if i > 0 && isIdentifierByte(expr[i-1]) {
			sb.WriteByte(expr[i])
			i++
			continue
		}
		// Skip whitespace between the name and the opening paren.
		j := i + nameLen
		for j < len(expr) && unicode.IsSpace(rune(expr[j])) {
			j++
		}
		if j >= len(expr) || expr[j] != '(' {
			sb.WriteByte(expr[i])
			i++
			continue
		}
		// Find the matching close paren.
		end, ok := scanParenGroup(expr, j)
		if !ok {
			sb.WriteByte(expr[i])
			i++
			continue
		}
		inside := expr[j+1 : end]
		args := splitTopLevelArgs(inside)
		// Recurse into each arg so nested same-name calls are
		// rewritten before the outer call sees them.
		recursed := make([]string, len(args))
		for k, a := range args {
			recursed[k] = rewriteFunctionCalls(a, fnName, replacer)
		}
		out := replacer(recursed)
		if out == "" {
			// Replacer declined; emit the call (with rewritten args)
			// verbatim so the verbatim-passthrough policy still
			// applies. Whitespace in the original argument text is
			// preserved unless an inner rewrite changed the arg.
			sb.WriteString(expr[i:j])
			sb.WriteByte('(')
			for k := range args {
				if k > 0 {
					sb.WriteByte(',')
				}
				if recursed[k] == args[k] {
					sb.WriteString(args[k])
				} else {
					sb.WriteString(recursed[k])
				}
			}
			sb.WriteByte(')')
		} else {
			sb.WriteString(out)
		}
		i = end + 1
	}
	return sb.String()
}

// matchIdentifier reports whether expr[i:] starts with a case-
// insensitive match for name. Returns the matched length on success.
func matchIdentifier(expr string, i int, name string) (int, bool) {
	if i+len(name) > len(expr) {
		return 0, false
	}
	if !strings.EqualFold(expr[i:i+len(name)], name) {
		return 0, false
	}
	// The next char must not be an identifier byte, or it would
	// extend the name (CONCAT vs CONCATENATE).
	if i+len(name) < len(expr) && isIdentifierByte(expr[i+len(name)]) {
		return 0, false
	}
	return len(name), true
}

// isIdentifierByte reports whether b is a continuation byte for an
// SQL identifier. Conservative: ASCII letters, digits, underscore.
func isIdentifierByte(b byte) bool {
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

// scanStringLiteral returns the index just past the closing quote of
// the single-quoted string starting at expr[start]. Doubled quotes
// (”) are treated as an escape sequence within the literal. If the
// literal is unterminated (malformed input), returns len(expr) so
// the caller copies the rest verbatim.
func scanStringLiteral(expr string, start int) int {
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

// scanParenGroup, given an index pointing at '(', returns the index
// of the matching ')' and ok=true. Respects nested parens and string
// literals.
func scanParenGroup(expr string, open int) (int, bool) {
	if open >= len(expr) || expr[open] != '(' {
		return 0, false
	}
	depth := 1
	for i := open + 1; i < len(expr); {
		switch expr[i] {
		case '\'':
			i = scanStringLiteral(expr, i)
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

// splitTopLevelArgs splits a function-argument string on commas that
// are at depth zero (not inside nested parens or string literals).
// Returns nil for an empty / whitespace-only input.
func splitTopLevelArgs(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); {
		switch s[i] {
		case '\'':
			i = scanStringLiteral(s, i)
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
