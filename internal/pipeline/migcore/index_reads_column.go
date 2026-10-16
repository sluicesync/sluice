// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate/exprident"
)

// IndexReadsColumn reports whether an entry of idx reads column name: a
// plain entry naming it, or an expression entry naming it anywhere in its
// text (KEY-COLLATION-FUNCTIONAL-KEY-PART). The key-collation doors ask
// which columns decide that two rows are one, and a functional key part
// `(lower(email))` decides it through email exactly as a plain one does:
// counting only plain entries left a collation change of email invisible to
// every window door while the applier's guard compares through the
// expression.
//
// The expression is scanned for identifier tokens outside string literals,
// compared case-insensitively (the IR holds it with quoting stripped). That
// over-approximates — a function or type name spelled like a column (a
// column named `text` beside `(email)::text`) counts too — and the cost of
// that is a door that refuses or stamps more, never one that sees less.
func IndexReadsColumn(idx *ir.Index, name string) bool {
	if idx == nil {
		return false
	}
	for _, c := range idx.Columns {
		if c.Column == name || (c.Column == "" && expressionNames(c.Expression, name)) {
			return true
		}
	}
	return false
}

// expressionNames reports whether expr holds name as an identifier token
// outside a string literal. A name that is not one token (a space or other
// punctuation in it, which the stripped quoting no longer delimits) counts
// wherever its text appears.
func expressionNames(expr, name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if b := name[i]; !exprident.IsIdentifierByte(b) && b < 0x80 && b != '$' {
			return strings.Contains(strings.ToLower(expr), strings.ToLower(name))
		}
	}
	for i := 0; i < len(expr); {
		b := expr[i]
		switch {
		case b == '\'':
			i = exprident.ScanStringLiteral(expr, i)
		case exprident.IsIdentStartByte(b) || b >= 0x80:
			start := i
			for i < len(expr) && (exprident.IsIdentifierByte(expr[i]) || expr[i] >= 0x80 || expr[i] == '$') {
				i++
			}
			if strings.EqualFold(expr[start:i], name) {
				return true
			}
		default:
			i++
		}
	}
	return false
}
