// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"strings"

	gomysql "github.com/go-sql-driver/mysql"
)

// This file holds the engine's ONE "the table isn't there" classifier, in two
// strengths. It exists as its own file because the same question was previously
// answered in two places with two different answers: a code-based one in
// diagnose.go and a text-based one in control_table.go, and the text-based one
// was the one gating a refusal (audit backlog C-1).
//
// # Classify on the CODE, not the message
//
// A server's error TEXT is not a stable interface. Three measured ways it moves
// under sluice's own supported configurations, none of which changes the code:
//
//   - **Locale.** MySQL renders errors through `lc_messages`. Ground-truthed on
//     a stock `mysql:8`: with `lc_messages='fr_FR'`, a missing table is still
//     errno 1146 but reads `La table 'app.nosuch' n'existe pas` — the string
//     `doesn't exist` is simply absent.
//   - **Vitess / PlanetScale.** vtgate's own unknown-table errors are worded
//     `table '%s' does not exist` (VT05004) and
//     `table '%s' does not exist in keyspace '%s'` (VT05005) — "does not",
//     never "doesn't". A substring check on "doesn't exist" therefore matched
//     NEITHER, despite a comment here that claimed it matched both. They arrive
//     with errnos 1109 (`ERUnknownTable`) and 1146 (`ERNoSuchTable`)
//     respectively, per `vterrors.UnknownTable` / `vterrors.NoSuchTable` in the
//     pinned vitess.io/vitess v0.24.2 (`go/mysql/sqlerror/sql_error.go`).
//   - **Proxies.** Anything that re-wraps the wire error can rewrite the text;
//     the errno is what it must preserve to stay protocol-compatible.
//
// So the codes are the contract, and both of the two that mean "no such table"
// are accepted.
const (
	// errNoSuchTable is ER_NO_SUCH_TABLE — vanilla MySQL/MariaDB's
	// "Table 'db.tbl' doesn't exist", and vtgate's VT05005.
	errNoSuchTable = 1146
	// errUnknownTable is ER_UNKNOWN_TABLE. Vanilla MySQL raises it for DDL
	// shapes a plain SELECT cannot reach, so accepting it costs nothing there;
	// it is here because vtgate's VT05004 ("table '%s' does not exist") uses
	// it, and a Vitess target whose table is simply not created yet must read
	// as absent rather than as an engine failure.
	errUnknownTable = 1109
)

// isNoSuchTableErr reports whether err is a MySQL-protocol error whose CODE
// means "that table does not exist". Structural only, deliberately: this is the
// strength used where the answer gates a REFUSAL (the cold-start populated-
// target preflight — see [RowWriter.IsTableEmpty]), and there the safe direction
// for an error nobody can classify is to surface it, not to guess "absent".
func isNoSuchTableErr(err error) bool {
	var mErr *gomysql.MySQLError
	if !errors.As(err, &mErr) {
		return false
	}
	return mErr.Number == errNoSuchTable || mErr.Number == errUnknownTable
}

// isMySQLMissingTableErr is the DEGRADE-path strength: the same structural
// answer, plus a text fallback used only when the error carries no MySQL errno
// at all (a wrapped shape that lost the driver type). The twin of the vanilla-PG
// engine's isUndefinedColumnErr, and it is deliberately weaker than
// [isNoSuchTableErr]: every caller of this one turns "missing" into "behave as
// if empty / skip the cleanup" for sluice's OWN control tables, where guessing
// wrong costs a redundant statement rather than a bypassed refusal.
func isMySQLMissingTableErr(err error) bool {
	if err == nil {
		return false
	}
	if isNoSuchTableErr(err) {
		return true
	}
	var mErr *gomysql.MySQLError
	if errors.As(err, &mErr) {
		// The server DID answer with a code, and it is not a missing-table
		// code. Trusting the text over the code is exactly the C-1 defect.
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "doesn't exist") || strings.Contains(msg, "Error 1146")
}
