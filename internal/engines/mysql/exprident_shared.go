// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Thin delegations to the shared internal/translate/exprident package
// (ADR-0045). The scan primitives and the identifier-requote mechanism
// were byte-identical between the mysql and postgres engine packages;
// they now live in one place. These unexported aliases keep this
// package's existing call sites unchanged, which is what makes the
// relocation a verified behaviour-preserving move.

package mysql

import "github.com/orware/sluice/internal/translate/exprident"

func isIdentifierByte(b byte) bool { return exprident.IsIdentifierByte(b) }

func scanStringLiteral(expr string, start int) int {
	return exprident.ScanStringLiteral(expr, start)
}

func scanParenGroup(expr string, open int) (int, bool) {
	return exprident.ScanParenGroup(expr, open)
}

func splitTopLevelArgs(s string) []string { return exprident.SplitTopLevelArgs(s) }

// mysqlRequoteConfig is the engine-owned [exprident.Config] for MySQL:
// backtick quote char, the MySQL reserved-word set, the expression-
// grammar exclusion set, and no whitespace-before-paren skip (MySQL's
// historical helper required the '(' to immediately follow the token).
// The reserved/grammar sets stay in this package — they are dialect
// definitions (see reserved_idents.go).
var mysqlRequoteConfig = exprident.Config{
	QuoteByte:         '`',
	Reserved:          mysqlReservedWords,
	GrammarExclusions: exprGrammarKeywords,
	SkipWSBeforeParen: false,
}

// requoteMySQLReservedIdents re-applies backtick quoting to bare
// identifiers in an expression body that are MySQL reserved words used
// as column references. Thin wrapper over the shared mechanism with
// the MySQL dialect config.
//
// Background (validation-rig catalog #5): the MySQL schema reader
// strips backtick identifier quotes from generated-column / CHECK /
// index expressions at the read boundary so the IR holds text portable
// to either engine's writer (Postgres rejects backticks). That strip
// is lossy for the narrow case of an identifier that is also a MySQL
// reserved word — e.g. a column named `order` or `key`. The fix is
// target-side, where MySQL's reserved-word set is known.
func requoteMySQLReservedIdents(expr string) string {
	return exprident.RequoteIdentifiers(expr, mysqlRequoteConfig)
}
