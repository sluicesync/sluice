// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Thin delegations to the shared internal/translate/exprident package
// (ADR-0045). The scan primitives and the identifier-requote mechanism
// were byte-identical between the mysql and postgres engine packages;
// they now live in one place. These unexported aliases keep this
// package's existing call sites unchanged, which is what makes the
// relocation a verified behaviour-preserving move.

package postgres

import "github.com/orware/sluice/internal/translate/exprident"

func isIdentifierByte(b byte) bool { return exprident.IsIdentifierByte(b) }

func scanStringLiteral(expr string, start int) int {
	return exprident.ScanStringLiteral(expr, start)
}

func scanParenGroup(expr string, open int) (int, bool) {
	return exprident.ScanParenGroup(expr, open)
}

func splitTopLevelArgs(s string) []string { return exprident.SplitTopLevelArgs(s) }

// pgRequoteConfig is the engine-owned [exprident.Config] for
// PostgreSQL: double-quote quote char, the PG reserved-word set, the
// expression-grammar exclusion set, and the PG-only "skip whitespace
// before '('" tweak (PG's emitted text can read `coalesce (...)` /
// `numeric (...)`). The reserved/grammar sets stay in this package —
// they are dialect definitions (see reserved_idents.go).
var pgRequoteConfig = exprident.Config{
	QuoteByte:         '"',
	Reserved:          pgReservedWords,
	GrammarExclusions: pgExprGrammarKeywords,
	GrammarContextual: pgExprContextualKeywords,
	SkipWSBeforeParen: true,
}

// requotePGReservedIdents re-applies double-quote quoting to bare
// identifiers in a cross-engine expression body that are PostgreSQL
// reserved words used as column references. Thin wrapper over the
// shared mechanism with the PG dialect config.
//
// Background (validation-rig catalog Bug 63): when the source dialect
// is not "postgres", a generated-column / CHECK / index expression
// arrives in the IR spelled in the source engine's dialect with that
// engine's identifier quotes stripped at the read boundary. The PG
// writer's cross-dialect translator (translateExprForPG) rewrites
// function/operator spellings but not bare idents, so a MySQL source
// column named `order` or `key` lands as the bare token `order` / `key`
// and CREATE TABLE fails with SQLSTATE 42601. The fix is target-side,
// where PG's reserved-word set is known.
//
// Same-engine PG→PG never reaches this helper — the PG reader returns
// pg_get_expr output with reserved-word column refs already correctly
// double-quoted, and the writer's same-dialect path emits that text
// verbatim. The helper is invoked only on the cross-dialect branch.
func requotePGReservedIdents(expr string) string {
	return exprident.RequoteIdentifiers(expr, pgRequoteConfig)
}
