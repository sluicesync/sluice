// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// String-aware walker and rewrite rules for translateExprForMySQL.
// Kept separate from the entry point in expr_translate.go so the v1
// translation table stays the load-bearing artifact and this file
// holds only mechanical parsing helpers.

package mysql

import (
	"strings"
	"unicode"
)

// rewritePGCasts finds every PG cast operator (expr)::type and
// rewrites it as CAST(expr AS <mysql-type>). The operand can be a
// parenthesised group, a function call, or a bare identifier; the
// helper handles each. Type names are mapped via pgTypeToMySQL;
// unrecognized type names fall through unchanged.
func rewritePGCasts(expr string) string {
	var sb strings.Builder
	for i := 0; i < len(expr); {
		// String literals copy through verbatim.
		if expr[i] == '\'' {
			end := scanStringLiteral(expr, i)
			sb.WriteString(expr[i:end])
			i = end
			continue
		}
		if i+1 < len(expr) && expr[i] == ':' && expr[i+1] == ':' {
			// Find the operand: the immediately preceding non-space
			// expression chunk that's already been emitted into sb.
			operand, prefixLen, ok := pullCastOperand(sb.String())
			if !ok {
				sb.WriteString("::")
				i += 2
				continue
			}
			// Find the type-name immediately following the ::, plus
			// any (precision[, scale]) modifier.
			j := i + 2
			for j < len(expr) && unicode.IsSpace(rune(expr[j])) {
				j++
			}
			typeStart := j
			for j < len(expr) && isIdentifierByte(expr[j]) {
				j++
			}
			if typeStart == j {
				sb.WriteString("::")
				i += 2
				continue
			}
			// PG type names can be multi-word (e.g. `character
			// varying`, `double precision`, `timestamp with time
			// zone`). Try to extend the type name across spaces if
			// the resulting longer form is recognized.
			if j < len(expr) && expr[j] == ' ' {
				lookahead := j + 1
				for lookahead < len(expr) && (isIdentifierByte(expr[lookahead]) || expr[lookahead] == ' ') {
					lookahead++
				}
				candidate := strings.TrimSpace(expr[typeStart:lookahead])
				if pgTypeToMySQL(candidate, "") != "" {
					j = lookahead
				}
			}
			pgType := expr[typeStart:j]
			modifier := ""
			if j < len(expr) && expr[j] == '(' {
				end, mok := scanParenGroup(expr, j)
				if mok {
					modifier = expr[j+1 : end]
					j = end + 1
				}
			}
			mysqlType := pgTypeToMySQL(pgType, modifier)
			if mysqlType == "" {
				// Unrecognized type — leave verbatim. The verbatim-
				// passthrough policy still applies as the fallback.
				sb.WriteString("::")
				sb.WriteString(pgType)
				if modifier != "" {
					sb.WriteByte('(')
					sb.WriteString(modifier)
					sb.WriteByte(')')
				}
				i = j
				continue
			}
			// Rewind the operand from sb and emit CAST(operand AS type).
			cur := sb.String()
			rewound := cur[:len(cur)-prefixLen]
			sb.Reset()
			sb.WriteString(rewound)
			sb.WriteString("CAST(")
			sb.WriteString(strings.TrimSpace(operand))
			sb.WriteString(" AS ")
			sb.WriteString(mysqlType)
			sb.WriteByte(')')
			i = j
			continue
		}
		sb.WriteByte(expr[i])
		i++
	}
	return sb.String()
}

// pullCastOperand looks at the tail of already-emitted text and
// returns the operand of an upcoming :: cast, plus the byte length
// to remove from the tail. Three operand shapes are recognized, in
// priority order:
//
//   - A function call:                 IDENT(...)       → name + args
//   - A parenthesised group:           (expr)           → expr (parens stripped)
//   - A bare identifier (or qualified IDENT.IDENT chain)
//
// Returns ok=false when no operand can be identified (e.g. the ::
// follows a binary operator); the caller emits the literal :: and
// moves on.
//
// Parens around a single-expression operand are stripped so the
// emitted CAST doesn't carry redundant parens (CAST((qty) AS DECIMAL)
// is valid but uglier than CAST(qty AS DECIMAL)). A function call's
// argument-list parens are kept because they're part of the call.
func pullCastOperand(s string) (operand string, prefixLen int, ok bool) {
	// Strip trailing whitespace so we don't drop spaces from the
	// rewound output.
	end := len(s)
	for end > 0 && unicode.IsSpace(rune(s[end-1])) {
		end--
	}
	if end == 0 {
		return "", 0, false
	}
	// A trailing single quote is the close of a string literal. Walk
	// back to the opening quote (handling doubled-quote escapes the
	// same way the forward scanner does).
	if s[end-1] == '\'' {
		start := scanStringLiteralBackward(s, end-1)
		if start < end-1 {
			return s[start:end], end - start, true
		}
		return "", 0, false
	}
	// A trailing ')' could be the close of a parenthesised group or a
	// function call. Walk back to the matching '('.
	if s[end-1] == ')' {
		depth := 1
		i := end - 2
		for i >= 0 {
			switch s[i] {
			case '\'':
				start := scanStringLiteralBackward(s, i)
				i = start - 1
			case ')':
				depth++
				i--
			case '(':
				depth--
				if depth == 0 {
					// Look back from i to see if there's an
					// identifier preceding the open paren — that
					// makes this a function call rather than a
					// parenthesised expression.
					name := i
					for name > 0 && isIdentifierByte(s[name-1]) {
						name--
					}
					if name < i {
						// Function call: include the name.
						return s[name:end], end - name, true
					}
					// Parenthesised expression: strip the outer parens.
					inner := s[i+1 : end-1]
					return inner, end - i, true
				}
				i--
			default:
				i--
			}
		}
		return "", 0, false
	}
	// Identifier (possibly with qualified .name segments).
	i := end - 1
	for i >= 0 && (isIdentifierByte(s[i]) || s[i] == '.') {
		i--
	}
	if i+1 == end {
		return "", 0, false
	}
	return s[i+1 : end], end - (i + 1), true
}

// scanStringLiteralBackward, given an index pointing at a closing
// quote of a single-quoted literal in s, returns the index of the
// opening quote. Used by pullCastOperand when walking right-to-left
// past a parenthesised group.
func scanStringLiteralBackward(s string, closeIdx int) int {
	// Conservative: scan forward from the start to find string
	// literal boundaries, then return the start of the literal that
	// contains closeIdx.
	for i := 0; i < closeIdx; {
		if s[i] == '\'' {
			end := scanStringLiteral(s, i)
			if end > closeIdx {
				return i
			}
			i = end
			continue
		}
		i++
	}
	return closeIdx
}

// pgTypeToMySQL maps a PG type-name (lowercased on lookup) to the
// MySQL type-name appropriate inside a CAST. Returns "" for types
// outside the v1 scope; the caller falls back to verbatim
// passthrough.
//
// Modifier is the parenthesised "(precision)" or "(precision,scale)"
// the original cast carried, if any. NUMERIC keeps the modifier
// (mapped to DECIMAL); CHAR/VARCHAR keep theirs; integer types
// discard them since SIGNED / UNSIGNED don't take a precision in
// MySQL CAST.
func pgTypeToMySQL(pgType, modifier string) string {
	switch strings.ToLower(strings.TrimSpace(pgType)) {
	case "numeric", "decimal":
		if modifier != "" {
			return "DECIMAL(" + modifier + ")"
		}
		return "DECIMAL"
	case "real", "float4":
		return "DECIMAL"
	case "double", "float8", "double precision":
		return "DECIMAL"
	case "int2", "smallint":
		return "SIGNED"
	case "int", "int4", "integer":
		return "SIGNED"
	case "int8", "bigint":
		return "SIGNED"
	case "boolean", "bool":
		return "UNSIGNED"
	case "text":
		return "CHAR"
	case "varchar", "character varying":
		if modifier != "" {
			return "CHAR(" + modifier + ")"
		}
		return "CHAR"
	case "char", "bpchar", "character":
		if modifier != "" {
			return "CHAR(" + modifier + ")"
		}
		return "CHAR"
	case "date":
		return "DATE"
	case "timestamp":
		return "DATETIME"
	case "timestamptz", "timestamp with time zone":
		return "DATETIME"
	case "time":
		return "TIME"
	case "json", "jsonb":
		return "JSON"
	}
	return ""
}

// rewriteConcatOperator collapses a chain of ||-separated operands
// into a single CONCAT(...) call. The walker treats `||` at depth 0
// (not inside parens or string literals) as a chain separator and
// re-emits the surrounding text.
//
// pg_get_expr loves to wrap || chains in parens — `((a || b) || c)`
// rather than `a || b || c`. We flatten by recursively rewriting
// each operand: if an operand strips down to a single `(...)` whose
// inside is itself a || chain, we splice its operands into the outer
// chain so the final CONCAT call has all the leaf operands.
func rewriteConcatOperator(expr string) string {
	// Strip a single layer of outer redundant parens — pg_get_expr
	// often wraps the whole expression body in `(...)`. Without this
	// the depth-0 walker wouldn't see top-level `||` operators.
	stripped := stripOuterParens(expr)
	args := collectConcatOperands(stripped)
	if args == nil {
		return expr
	}
	if len(args) == 1 {
		return args[0]
	}
	return "CONCAT(" + strings.Join(args, ", ") + ")"
}

// stripOuterParens returns s with at most one layer of redundant
// outer parentheses removed. "Redundant" means the open paren at
// position 0 matches a close paren at the very end of the string;
// the contents in between are returned unchanged. If s isn't a
// fully-parenthesised single group, it's returned as-is.
func stripOuterParens(s string) string {
	t := strings.TrimSpace(s)
	if len(t) < 2 || t[0] != '(' {
		return s
	}
	end, ok := scanParenGroup(t, 0)
	if !ok || end != len(t)-1 {
		return s
	}
	return t[1 : len(t)-1]
}

// collectConcatOperands walks expr at top-level, splitting on `||`
// and returning the list of trimmed operands. Each operand has its
// outer parens stripped if doing so exposes another || chain (the
// flattening that lets pg_get_expr's nested form collapse into one
// CONCAT call). Returns nil if no || was found at top level — the
// caller emits the expression verbatim.
func collectConcatOperands(expr string) []string {
	var raw []string
	depth := 0
	start := 0
	for i := 0; i < len(expr); {
		switch expr[i] {
		case '\'':
			i = scanStringLiteral(expr, i)
		case '(', '[':
			depth++
			i++
		case ')', ']':
			depth--
			i++
		case '|':
			if depth == 0 && i+1 < len(expr) && expr[i+1] == '|' {
				raw = append(raw, expr[start:i])
				start = i + 2
				i += 2
				continue
			}
			i++
		default:
			i++
		}
	}
	if len(raw) == 0 {
		return nil
	}
	raw = append(raw, expr[start:])

	// Trim each operand and try to flatten parenthesised nested
	// chains.
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		t := strings.TrimSpace(r)
		if t == "" {
			continue
		}
		// If t is a parenthesised group, peek inside to see if it's
		// itself a || chain. If so, splice its operands in.
		if len(t) >= 2 && t[0] == '(' {
			endParen, ok := scanParenGroup(t, 0)
			if ok && endParen == len(t)-1 {
				inner := t[1 : len(t)-1]
				nested := collectConcatOperands(inner)
				if nested != nil {
					out = append(out, nested...)
					continue
				}
			}
		}
		out = append(out, t)
	}
	return out
}

// rewriteLikeOperators replaces ~~ with LIKE and ~~* with the
// LOWER(expr) LIKE LOWER(pat) form. MySQL's case sensitivity for
// LIKE depends on collation; using LOWER() on both sides mimics PG's
// ILIKE behaviour without depending on collation declaration.
//
// Both ~~ and ~~* are PG operator forms — never present in user-
// written SQL, but pg_get_expr emits them when the source DDL used
// LIKE / ILIKE.
func rewriteLikeOperators(expr string) string {
	// First handle ~~* (case-insensitive). The order matters because
	// ~~ is a prefix of ~~*.
	expr = rewriteCaseInsensitiveLike(expr)
	// Now plain ~~ → LIKE. The replacement word carries no padding
	// of its own — the original text supplied any spaces around the
	// operator and we preserve them.
	expr = replaceOperatorAnyDepth(expr, "~~", "LIKE")
	return expr
}

// rewriteCaseInsensitiveLike rewrites every top-level `lhs ~~* rhs`
// into `LOWER(lhs) LIKE LOWER(rhs)`. Operand boundaries follow the
// same balanced-paren / string-literal walker the rest of this file
// uses.
func rewriteCaseInsensitiveLike(expr string) string {
	for {
		idx := findOperatorAnyDepth(expr, "~~*")
		if idx < 0 {
			return expr
		}
		lhs, lhsStart, ok := scanOperandBefore(expr, idx)
		if !ok {
			// Can't identify an operand; replace with " LIKE "
			// (prefixed with LOWER on the right) as a best-effort and
			// move on. In practice this branch shouldn't fire on
			// well-formed PG-emitted text.
			expr = expr[:idx] + " LIKE " + expr[idx+3:]
			continue
		}
		rhs, rhsEnd, ok := scanOperandAfter(expr, idx+3)
		if !ok {
			expr = expr[:idx] + " LIKE " + expr[idx+3:]
			continue
		}
		out := expr[:lhsStart] +
			"LOWER(" + strings.TrimSpace(lhs) + ") LIKE LOWER(" + strings.TrimSpace(rhs) + ")" +
			expr[rhsEnd:]
		expr = out
	}
}

// replaceOperatorAnyDepth replaces every occurrence of op outside
// string literals with replacement. Used for the simple ~~ → LIKE
// rewrite where the operator never appears in identifier text and
// the surrounding operands don't need restructuring. Depth doesn't
// matter — ~~ is uniquely an operator.
func replaceOperatorAnyDepth(expr, op, replacement string) string {
	var sb strings.Builder
	for i := 0; i < len(expr); {
		if expr[i] == '\'' {
			end := scanStringLiteral(expr, i)
			sb.WriteString(expr[i:end])
			i = end
			continue
		}
		if i+len(op) <= len(expr) && expr[i:i+len(op)] == op {
			sb.WriteString(replacement)
			i += len(op)
			continue
		}
		sb.WriteByte(expr[i])
		i++
	}
	return sb.String()
}

// lastTopLevelArrayCast scans s for a top-level `::` followed by a
// type name and a trailing `[]` (the array-cast suffix), returning
// the index of the `::`. Returns -1 if no array-cast suffix is
// present. "Top level" means depth zero w.r.t. parens and outside
// string literals.
func lastTopLevelArrayCast(s string) int {
	if !strings.HasSuffix(strings.TrimRight(s, " \t"), "[]") {
		return -1
	}
	depth := 0
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
		default:
			if depth == 0 && i+1 < len(s) && s[i] == ':' && s[i+1] == ':' {
				// Verify the rest of the string (after this ::, type
				// name, optional precision-modifier parens) ends in []
				// at depth zero.
				j := i + 2
				for j < len(s) && unicode.IsSpace(rune(s[j])) {
					j++
				}
				// Type name: identifier, optionally multi-word
				// (`character varying`).
				for j < len(s) && (isIdentifierByte(s[j]) || s[j] == ' ') {
					j++
				}
				// Optional `(N)` modifier.
				if j < len(s) && s[j] == '(' {
					end, ok := scanParenGroup(s, j)
					if ok {
						j = end + 1
					}
				}
				// Now we must reach `[]` then end of string.
				if j+1 < len(s) && s[j] == '[' && s[j+1] == ']' {
					return i
				}
			}
			i++
		}
	}
	return -1
}

// findOperatorAnyDepth returns the byte index of the first
// occurrence of op outside string literals, regardless of paren
// nesting depth. Used for operator rewrites where the operator
// (e.g. `~~*`, `~~`) is never present inside identifiers or
// strings — paren depth has no effect on whether a match is real.
func findOperatorAnyDepth(expr, op string) int {
	for i := 0; i < len(expr); {
		if expr[i] == '\'' {
			i = scanStringLiteral(expr, i)
			continue
		}
		if i+len(op) <= len(expr) && expr[i:i+len(op)] == op {
			return i
		}
		i++
	}
	return -1
}

// scanOperandBefore returns the operand text immediately preceding
// position idx (a top-level operator's start). The operand is
// determined by the trailing-text rule used by the cast rewriter:
// parenthesised group, function call (treated as a paren group), or
// bare identifier.
func scanOperandBefore(expr string, idx int) (operand string, start int, ok bool) {
	// Trim trailing whitespace.
	end := idx
	for end > 0 && unicode.IsSpace(rune(expr[end-1])) {
		end--
	}
	if end == 0 {
		return "", 0, false
	}
	if expr[end-1] == ')' {
		depth := 1
		i := end - 2
		for i >= 0 {
			switch expr[i] {
			case '\'':
				st := scanStringLiteralBackward(expr, i)
				i = st - 1
			case ')':
				depth++
				i--
			case '(':
				depth--
				if depth == 0 {
					// Include any function-name identifier preceding
					// the open paren.
					name := i
					for name > 0 && isIdentifierByte(expr[name-1]) {
						name--
					}
					return expr[name:end], name, true
				}
				i--
			default:
				i--
			}
		}
		return "", 0, false
	}
	// Identifier or chained access.
	i := end - 1
	for i >= 0 && (isIdentifierByte(expr[i]) || expr[i] == '.') {
		i--
	}
	return expr[i+1 : end], i + 1, true
}

// scanOperandAfter returns the operand text starting at position idx
// (just past a top-level operator). Symmetric to scanOperandBefore.
func scanOperandAfter(expr string, idx int) (operand string, end int, ok bool) {
	// Skip leading whitespace.
	i := idx
	for i < len(expr) && unicode.IsSpace(rune(expr[i])) {
		i++
	}
	if i >= len(expr) {
		return "", 0, false
	}
	start := i
	if expr[i] == '\'' {
		stringEnd := scanStringLiteral(expr, i)
		return expr[start:stringEnd], stringEnd, true
	}
	if expr[i] == '(' {
		paren, pok := scanParenGroup(expr, i)
		if !pok {
			return "", 0, false
		}
		return expr[start : paren+1], paren + 1, true
	}
	// Identifier (possibly followed by parens for a function call).
	for i < len(expr) && (isIdentifierByte(expr[i]) || expr[i] == '.') {
		i++
	}
	if i < len(expr) && expr[i] == '(' {
		paren, pok := scanParenGroup(expr, i)
		if pok {
			return expr[start : paren+1], paren + 1, true
		}
	}
	return expr[start:i], i, true
}

// rewriteEqAnyArray rewrites `lhs = ANY(ARRAY[a, b, c])` into
// `lhs IN (a, b, c)`. The match is conservative: the right-hand side
// of `= ANY` must be a single-line ARRAY[...] literal at depth zero;
// any other shape (subselect, function call, casted array literal
// where the cast wraps the whole ANY) falls through verbatim.
//
// pg_get_expr commonly emits the array-literal form for IN-list
// CHECKs that the source DDL declared with `IN (...)`, so the
// reverse rewrite restores the source intent.
func rewriteEqAnyArray(expr string) string {
	// Track the search start so non-matching `= ANY` occurrences
	// (e.g. inside something we can't translate) don't loop forever.
	searchStart := 0
	for {
		rel := strings.Index(strings.ToUpper(expr[searchStart:]), "= ANY")
		if rel < 0 {
			return expr
		}
		idx := searchStart + rel
		// Find the opening paren after `= ANY`.
		j := idx + len("= ANY")
		for j < len(expr) && unicode.IsSpace(rune(expr[j])) {
			j++
		}
		if j >= len(expr) || expr[j] != '(' {
			searchStart = idx + len("= ANY")
			continue
		}
		end, ok := scanParenGroup(expr, j)
		if !ok {
			searchStart = idx + len("= ANY")
			continue
		}
		inner := strings.TrimSpace(expr[j+1 : end])
		// Strip a trailing PG cast on the whole ARRAY (e.g.
		// `ARRAY[...]::text[]` or `(ARRAY[...])::text[]`). Casts on
		// arrays use `[]` after the type name; we strip up to the
		// `[]` so any internal `::` in the inner literals isn't
		// confused with the outer cast.
		if cut := lastTopLevelArrayCast(inner); cut > 0 {
			inner = strings.TrimSpace(inner[:cut])
		}
		// Strip one redundant outer paren layer: PG sometimes emits
		// `(ARRAY[...])::text[]` rather than the flat form.
		if len(inner) >= 2 && inner[0] == '(' {
			endParen, pok := scanParenGroup(inner, 0)
			if pok && endParen == len(inner)-1 {
				inner = strings.TrimSpace(inner[1 : len(inner)-1])
			}
		}
		// Inner must be ARRAY[...].
		upper := strings.ToUpper(inner)
		if !strings.HasPrefix(upper, "ARRAY[") || !strings.HasSuffix(inner, "]") {
			searchStart = end + 1
			continue
		}
		body := inner[len("ARRAY[") : len(inner)-1]
		args := splitTopLevelArgs(body)
		// Strip per-arg PG casts so each item is plain.
		clean := make([]string, 0, len(args))
		for _, a := range args {
			a = strings.TrimSpace(a)
			if cast := strings.Index(a, "::"); cast > 0 {
				// Leave string literals alone, but strip casts on
				// either bare literals or identifiers. Use Index
				// (first `::`) rather than LastIndex so chained casts
				// or multi-word type names don't trip us up.
				head := strings.TrimSpace(a[:cast])
				if head != "" {
					a = head
				}
			}
			clean = append(clean, a)
		}
		// Replace `= ANY(...)` with `IN (...)`. The leading `=` is
		// preserved on the source side as part of the operator we
		// match, so we substitute the whole `= ANY (...)` span.
		replacement := "IN (" + strings.Join(clean, ", ") + ")"
		expr = expr[:idx] + replacement + expr[end+1:]
		searchStart = idx + len(replacement)
	}
}

// isIdentifierByte reports whether b is a continuation byte for an
// SQL identifier. ASCII letters, digits, underscore.
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
// (”) are treated as an escape sequence within the literal.
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
// are at depth zero (not inside nested parens, brackets, or string
// literals). Returns nil for an empty / whitespace-only input.
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
