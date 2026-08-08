// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
)

// These pin the CLASSIFICATION, not the happy path (audit backlog C-1). The
// defect being closed is that the answer used to come from the error's TEXT,
// and every case below is a shape where the text and the code disagree.
//
// Every message string is ground truth, not invention:
//
//   - the English and French 1146 texts were produced on a stock `mysql:8`
//     container (`SET SESSION lc_messages='fr_FR'` for the second),
//   - the two vtgate wordings are `VT05004` / `VT05005` verbatim from
//     `go/vt/vterrors/code.go` in the pinned vitess.io/vitess v0.24.2, and
//     their errnos are that module's own `vterrors.UnknownTable` → 1109 /
//     `vterrors.NoSuchTable` → 1146 mapping (`go/mysql/sqlerror/sql_error.go`),
//   - 1049 and 1814 were produced on the same container.
func TestIsNoSuchTableErr_ClassifiesOnTheCodeNotTheText(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "1146 en_US — the shape the old substring matched",
			err:  &gomysql.MySQLError{Number: 1146, Message: "Table 'app.nosuch' doesn't exist"},
			want: true,
		},
		{
			name: "1146 fr_FR — same code, no English phrase (lc_messages)",
			err:  &gomysql.MySQLError{Number: 1146, Message: "La table 'app.nosuch' n'existe pas"},
			want: true,
		},
		{
			name: "vtgate VT05005 — 'does not', never 'doesn't'",
			err:  &gomysql.MySQLError{Number: 1146, Message: "table 'orders' does not exist in keyspace 'commerce'"},
			want: true,
		},
		{
			name: "vtgate VT05004 — ER_UNKNOWN_TABLE (1109)",
			err:  &gomysql.MySQLError{Number: 1109, Message: "table 'orders' does not exist"},
			want: true,
		},
		{
			name: "wrapped 1146 still classifies (errors.As)",
			err: fmt.Errorf("mysql: probe %q for emptiness: %w", "orders",
				&gomysql.MySQLError{Number: 1146, Message: "Table 'app.orders' doesn't exist"}),
			want: true,
		},

		// --- the controls: these must NOT read as "the table is absent" ---
		{
			name: "1049 unknown database — a real failure, not an absent table",
			err:  &gomysql.MySQLError{Number: 1049, Message: "Unknown database 'nodb'"},
			want: false,
		},
		{
			name: "1814 discarded tablespace — the table EXISTS and is unreadable",
			err:  &gomysql.MySQLError{Number: 1814, Message: "Tablespace has been discarded for table 'disc'"},
			want: false,
		},
		{
			// Constructed, not observed: the point is the PROPERTY that a
			// structured error carrying the fooling TEXT under a code that does
			// not mean "absent" is refused. The old substring returned true here
			// and would have reported a table sluice cannot read as empty.
			name: "some other numbered error whose text would have fooled the substring",
			err:  &gomysql.MySQLError{Number: 1815, Message: "Internal error: Table 'app.t' doesn't exist in engine"},
			want: false,
		},
		{
			name: "no MySQL errno at all — unclassifiable, so not 'absent'",
			err:  errors.New("Table 'app.orders' doesn't exist"),
			want: false,
		},
		{name: "nil", err: nil, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isNoSuchTableErr(tc.err); got != tc.want {
				t.Errorf("isNoSuchTableErr(%v) = %v; want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestIsMySQLMissingTableErr_StructuralFirstThenText pins the DEGRADE-path
// classifier's deliberately different shape: it accepts the same codes, still
// refuses a structured error carrying a different code (the C-1 property), and
// keeps the legacy text match ONLY for a value that lost the driver type.
//
// Stated narrowly on purpose: this helper reaches the control-table /
// migrate-state / query-timeout-recovery sites, where "missing" means "skip a
// cleanup". It is NOT the one gating the populated-target refusal — that is
// [isNoSuchTableErr], which has no text fallback at all.
func TestIsMySQLMissingTableErr_StructuralFirstThenText(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"1146 fr_FR", &gomysql.MySQLError{Number: 1146, Message: "La table 'x' n'existe pas"}, true},
		{"1109 vtgate", &gomysql.MySQLError{Number: 1109, Message: "table 'x' does not exist"}, true},
		{
			"structured non-missing code with fooling text is refused",
			&gomysql.MySQLError{Number: 1815, Message: "Internal error: Table 'x' doesn't exist in engine"},
			false,
		},
		{"1049 unknown database", &gomysql.MySQLError{Number: 1049, Message: "Unknown database 'nodb'"}, false},
		{"untyped legacy text still degrades", errors.New(`mysql: Error 1146: Table 'x' doesn't exist`), true},
		{"untyped unrelated text does not", errors.New("connection refused"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isMySQLMissingTableErr(tc.err); got != tc.want {
				t.Errorf("isMySQLMissingTableErr(%v) = %v; want %v", tc.err, got, tc.want)
			}
		})
	}
}
