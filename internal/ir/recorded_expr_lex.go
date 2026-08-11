// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "fmt"

// This file is the structural lex check for RECORDED expression text —
// the Bug 243 detector (sluice-testing catalog, 2026-08-11).
//
// # What it catches, and what it deliberately does not
//
// Backup manifests recorded by readers older than the 2026-08-11
// expression-escaping fix can carry a MANGLED expression: the old blind
// un-escape consumed the wrong pair of an interior apostrophe's escape,
// so `CHECK (name <> 'o''brien')` was recorded as text whose literals no
// longer terminate — `name <> 'o\\'brien'` lexes as a literal, a stray
// identifier, and an UNTERMINATED literal. Emitting that at restore
// produced the target server's raw parse error (MySQL 1064) after the
// preceding tables had already been created, and `backup verify` passed
// the chain it came from.
//
// Every mangle of that family shares one structural signature: under the
// IR's portable literal spelling — apostrophe-delimited, interior
// apostrophes DOUBLED, backslash an ORDINARY character — the expression
// ends inside an open literal. That is what this check detects. It is
// deliberately NOT a SQL parser and NOT a repair: a recorded expression
// that fails here names a recording no current binary can prove the
// intent of, and the tenet answer is a loud refusal with the remedy (a
// fresh full backup from the live source), never a guessed rewrite.
//
// The stated blind spot, because a gate must not read broader than it
// is: a structurally VALID recording can still carry the WRONG value —
// the same era's readers recorded literal backslashes in MySQL's doubled
// spelling (`'a\\d'`), which lexes cleanly here and restores correctly
// to MySQL but names a different predicate on a PostgreSQL target. That
// class is not structurally detectable and is filed separately (the Bug
// 243 backlog residue), not silently covered by this check.

// UnterminatedLiteralAt reports whether expr ends inside an open string
// literal under the portable spelling (” doubling; backslash ordinary),
// and the byte offset at which that literal opened.
func UnterminatedLiteralAt(expr string) (openedAt int, malformed bool) {
	for i := 0; i < len(expr); {
		if expr[i] != '\'' {
			i++
			continue
		}
		opened := i
		i++
		closed := false
		for i < len(expr) {
			if expr[i] != '\'' {
				i++
				continue
			}
			if i+1 < len(expr) && expr[i+1] == '\'' {
				i += 2 // doubled apostrophe: part of the value
				continue
			}
			i++
			closed = true
			break
		}
		if !closed {
			return opened, true
		}
	}
	return 0, false
}

// TableExpressionLexProblems runs [UnterminatedLiteralAt] over every
// expression-bearing field a recorded table carries — CHECK constraints,
// generated-column bodies, expression defaults, functional-index
// expressions, partial-index predicates, and the CHECK bodies of a
// DOMAIN-typed column — and describes each failure with the field that
// carries it, so a refusal can name what an operator would have to look
// at. A nil table contributes nothing.
func TableExpressionLexProblems(t *Table) []string {
	if t == nil {
		return nil
	}
	var out []string
	add := func(what, expr string) {
		if expr == "" {
			return
		}
		if at, bad := UnterminatedLiteralAt(expr); bad {
			out = append(out, fmt.Sprintf(
				"table %q: %s: string literal opened at byte %d never closes: %q", t.Name, what, at, expr,
			))
		}
	}
	for _, chk := range t.CheckConstraints {
		if chk != nil {
			add(fmt.Sprintf("CHECK %q", chk.Name), chk.Expr)
		}
	}
	for _, c := range t.Columns {
		if c == nil {
			continue
		}
		add(fmt.Sprintf("generated column %q", c.Name), c.GeneratedExpr)
		if de, ok := c.Default.(DefaultExpression); ok {
			add(fmt.Sprintf("column %q DEFAULT expression", c.Name), de.Expr)
		}
		if dom, ok := c.Type.(Domain); ok {
			for _, dc := range dom.Checks {
				add(fmt.Sprintf("column %q DOMAIN %q CHECK %q", c.Name, dom.Name, dc.Name), dc.Body)
			}
		}
	}
	for _, idx := range t.Indexes {
		if idx == nil {
			continue
		}
		for _, ic := range idx.Columns {
			add(fmt.Sprintf("index %q expression", idx.Name), ic.Expression)
		}
		add(fmt.Sprintf("index %q predicate", idx.Name), idx.Predicate)
	}
	return out
}

// SchemaExpressionLexProblems is [TableExpressionLexProblems] over every
// table of a recorded schema. A nil schema contributes nothing.
func SchemaExpressionLexProblems(s *Schema) []string {
	if s == nil {
		return nil
	}
	var out []string
	for _, t := range s.Tables {
		out = append(out, TableExpressionLexProblems(t)...)
	}
	return out
}
