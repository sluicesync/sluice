// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// TestSchemaWriter_IsTransientError pins the ADR-0114 DDL-phase retry
// verdict the orchestrator reads via [ir.TransientClassifier]: the live
// Track-C reparent shapes that killed the index build (57P01 / 57P03 /
// disk-full grow class) must classify transient, and a real DDL fault
// (undefined column, unique violation) must NOT — so a broken DDL still
// fails loudly instead of looping. Delegates to classifyApplierError, so
// this also guards that the SchemaWriter never grew a second classifier.
func TestSchemaWriter_IsTransientError(t *testing.T) {
	w := &SchemaWriter{}
	transient := []error{
		&pgconn.PgError{Code: "57P01", Message: "terminating connection due to administrator command"},
		&pgconn.PgError{Code: "57P03", Message: "the database system is not accepting connections"},
		&pgconn.PgError{Code: "53100", Message: `could not extend file "base/5/16634": No space left on device`},
	}
	for _, e := range transient {
		if !w.IsTransientError(e) {
			t.Errorf("IsTransientError(%v) = false; want true (a reparent/grow transient must retry)", e)
		}
	}
	// NOTE: 42703/42P01 are classified retriable schema-drift (self-heals
	// when the operator adds the column/table), so they are deliberately NOT
	// in this terminal set — assert only the genuinely-terminal shapes.
	terminal := []error{
		&pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"},
		&pgconn.PgError{Code: "42601", Message: `syntax error at or near "FOO"`},
		errors.New("some random non-transient failure"),
	}
	for _, e := range terminal {
		if w.IsTransientError(e) {
			t.Errorf("IsTransientError(%v) = true; want false (a real DDL fault must fail loudly)", e)
		}
	}
	if w.IsTransientError(nil) {
		t.Error("IsTransientError(nil) = true; want false")
	}
}

// TestClassifyApplierError_NilInNilOut — boundary case the pipeline
// relies on. Wrapping every applier return site MUST pass nil
// through unchanged or success becomes a typed error.
func TestClassifyApplierError_NilInNilOut(t *testing.T) {
	if got := classifyApplierError(nil); got != nil {
		t.Errorf("classifyApplierError(nil) = %v; want nil", got)
	}
}

// TestClassifyApplierError_NonRetriableUnchanged — ADR-0038's
// default-deny invariant. Unmatched errors return verbatim.
func TestClassifyApplierError_NonRetriableUnchanged(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"plain error", errors.New("some random failure")},
		{"wrapped error", fmt.Errorf("wrapping: %w", errors.New("inner"))},
		// Bug 200 negative pins: ConnectError-shaped text WITHOUT a transient
		// network shape stays terminal — the dial leg keys on shape, never on
		// "failed to connect" alone.
		{"dial to a typo'd host (no such host) stays terminal", errors.New("failed to connect: dial tcp: lookup db.exmple.com: no such host")},
		{"auth failure inside a connect error stays terminal", errors.New(`failed to connect to ` + "`user=app database=app`" + `: failed SASL auth: FATAL: password authentication failed for user "app" (SQLSTATE 28P01)`)},
		{"unique violation (explicit non-retriable per ADR-0038)", &pgconn.PgError{Code: "23505", Message: `duplicate key value violates unique constraint "users_pkey"`}},
		{"foreign key violation", &pgconn.PgError{Code: "23503", Message: "insert or update on table violates foreign key constraint"}},
		{"check violation", &pgconn.PgError{Code: "23514", Message: "new row violates check constraint"}},
		{"syntax error", &pgconn.PgError{Code: "42601", Message: "syntax error at or near \"FOO\""}},
		{"428C9 (generated column non-DEFAULT)", &pgconn.PgError{Code: "428C9", Message: `cannot insert a non-DEFAULT value into column "margin"`}},
		// XX000 is generic internal_error — a non-read-only XX000 must stay
		// terminal (the pg_readonly arm matches the message, not the bare code).
		{"XX000 non-read-only (generic internal_error stays terminal)", &pgconn.PgError{Code: "XX000", Message: "internal error: something unexpected"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := classifyApplierError(c.err)
			// Identity comparison is deliberate (mirror of the
			// MySQL test) — classifier must return input verbatim
			// for non-retriable shapes so the pipeline's
			// errors.As gate stays closed.
			//nolint:errorlint // identity not equivalence
			if got != c.err {
				t.Errorf("classifyApplierError should return non-retriable errors verbatim; got wrapped %T", got)
			}
			var re ir.RetriableError
			if errors.As(got, &re) {
				t.Errorf("non-retriable error matched ir.RetriableError via errors.As — default-deny is meant to prevent this")
			}
		})
	}
}

// TestClassifyApplierError_TerminalCodeShield pins the terminal-code
// shield (audit 2026-07-23 D0-8, folded into the D0-3/G-3 fix family):
// when the error chain carries a structured *pgconn.PgError, the server
// RESPONDED — classification is decided by its SQLSTATE alone, and the
// transport-text legs (both the dial-shape leg and the connection-shape
// leg) must be UNREACHABLE. Pre-shield the dial-shape leg ran BEFORE the
// PgError switch under a "dial-time, pre-byte by construction"
// justification that is false for any PgError: a P0001 raise_exception
// quoting stored text like "connection timed out" (a target-side audit
// trigger is routine) classified RETRIABLE, and 23505 fell through the
// switch's empty case into the second text leg like MySQL's 1062 —
// retrying a deterministically-failing batch through the full ADR-0038
// budget and delaying the loud terminal by many minutes.
//
// Structure mirrors the MySQL twin: the audit's OBSERVED cells first,
// then the G-3 cross-product (structurally-terminal SQLSTATE × every
// transient substring from both text legs) — all verbatim/non-retriable.
// The one documented message-gated exception (XX000 + read-only wording)
// is asserted to survive.
func TestClassifyApplierError_TerminalCodeShield(t *testing.T) {
	observed := []struct {
		name string
		err  error
	}{
		// The audit's OBSERVED cells (throwaway test, 2026-07-23),
		// re-pinned permanently.
		{
			"P0001 raise_exception quoting 'connection timed out'",
			&pgconn.PgError{Code: "P0001", Message: `audit trigger rejected row: reason "connection timed out"`},
		},
		{
			"23505 with transient wording in the message",
			&pgconn.PgError{Code: "23505", Message: `duplicate key value violates unique constraint "incidents_title_key": stored value 'connection refused'`},
		},
	}

	terminalCodes := []struct {
		code  string
		label string
	}{
		{"23505", "unique violation"},
		{"23503", "fk violation"},
		{"23514", "check violation"},
		{"42601", "syntax error"},
		{"P0001", "raise_exception"},
		{"22P02", "invalid text representation"},
		{"XX000", "generic internal_error (non-read-only)"},
	}
	// Both text legs' substring sets, duplicated deliberately (a pin that
	// reads the value it guards cannot detect the value changing). The
	// XX000 read-only wordings are NOT here — that AND-gate is the
	// documented exception, asserted below.
	transientSubstrings := []string{
		// dial-shape leg
		"connection refused", "connectex:", "actively refused", "connection timed out",
		// connection-shape leg
		"connection reset by peer", "broken pipe", "i/o timeout", "unexpected EOF",
		"use of closed network connection",
		"the database system is starting up", "the database system is shutting down",
	}

	cases := observed
	for _, code := range terminalCodes {
		for _, sub := range transientSubstrings {
			cases = append(cases, struct {
				name string
				err  error
			}{
				fmt.Sprintf("%s (%s) quoting %q", code.code, code.label, sub),
				&pgconn.PgError{Code: code.code, Message: fmt.Sprintf("server error quoting stored text: '%s'", sub)},
			})
		}
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := classifyApplierError(c.err)
			var re ir.RetriableError
			if errors.As(got, &re) {
				t.Fatalf("structurally-terminal PgError classified RETRIABLE via text over-match (D0-8): %v", c.err)
			}
			//nolint:errorlint // identity not equivalence — terminal errors return verbatim
			if got != c.err {
				t.Errorf("terminal error not returned verbatim; got %T", got)
			}
		})
	}

	// The documented message-gated exception survives the shield: XX000 +
	// the PlanetScale read-only wording stays retriable (structured code +
	// message AND-gate — the safe shape).
	roErr := &pgconn.PgError{Code: "XX000", Message: "pg_readonly: invalid statement because cluster is read-only"}
	got := classifyApplierError(roErr)
	var re ir.RetriableError
	if !errors.As(got, &re) || !re.Retriable() {
		t.Errorf("XX000 read-only AND-gate exception must stay retriable; got %T (%v)", got, got)
	}
}

// safeToRetryErr stands in for pgconn's private connLockError: an error
// whose SafeToRetry() contract says it occurred before any data reached the
// server. Pins the classifier's structured SafeToRetry leg without reaching
// into pgx internals.
type safeToRetryErr struct{}

func (safeToRetryErr) Error() string     { return "conn closed" }
func (safeToRetryErr) SafeToRetry() bool { return true }

// TestClassifyApplierError_RetriableShapes — each documented PG
// transient SQLSTATE from the ADR-0038 classifier table.
func TestClassifyApplierError_RetriableShapes(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"serialization_failure (40001)", &pgconn.PgError{Code: "40001", Message: "could not serialize access due to concurrent update"}},
		{"deadlock_detected (40P01)", &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}},
		{"admin_shutdown (57P01)", &pgconn.PgError{Code: "57P01", Message: "terminating connection due to administrator command"}},
		{"crash_shutdown (57P02)", &pgconn.PgError{Code: "57P02", Message: "terminating connection due to crash of another server process"}},
		{"cannot_connect_now (57P03)", &pgconn.PgError{Code: "57P03", Message: "the database system is starting up"}},
		// Class 53 — insufficient_resources (roadmap item 38). 53100 is the
		// live #94 storage-grow face: a streaming COPY into a PlanetScale PG
		// volume that is auto-growing under the write. 53000 / 53200 share the
		// transient-resource-squeeze shape.
		{"disk_full 53100 (could not extend file — item 38, live #94)", &pgconn.PgError{Code: "53100", Message: `could not extend file "base/16384/24576": No space left on device`}},
		{"insufficient_resources 53000", &pgconn.PgError{Code: "53000", Message: "insufficient resources"}},
		{"out_of_memory 53200", &pgconn.PgError{Code: "53200", Message: "out of memory"}},
		// PlanetScale PG serving-transition read-only window (XX000 + message;
		// PG twin of MySQL 1290, item 38 re-validation 2026-06-23).
		{"pg_readonly XX000 (cluster is read-only — PS reparent)", &pgconn.PgError{Code: "XX000", Message: "pg_readonly: invalid statement because cluster is read-only. See planetscale.com/docs/postgres/troubleshooting/readonly"}},
		{"connection_exception 08000", &pgconn.PgError{Code: "08000", Message: "connection_exception"}},
		{"connection_does_not_exist 08003", &pgconn.PgError{Code: "08003", Message: "connection does not exist"}},
		{"connection_failure 08006", &pgconn.PgError{Code: "08006", Message: "connection failure"}},
		{"sqlclient_unable_to_establish_sqlconnection 08001", &pgconn.PgError{Code: "08001", Message: "sqlclient unable to establish"}},
		{"schema drift: undefined_column 42703 (Bug F8)", &pgconn.PgError{Code: "42703", Message: `column "soak_extra" of relation "soak_events" does not exist`}},
		{"schema drift: undefined_table 42P01 (Bug F8)", &pgconn.PgError{Code: "42P01", Message: `relation "new_table" does not exist`}},
		{"driver.ErrBadConn", driver.ErrBadConn},
		// Bug 199b (v0.99.288 regression cycle): a severed pool conn picked
		// up at a checkpoint boundary surfaces pgconn's connLockError "conn
		// closed" — SafeToRetry-by-construction (no bytes reached the
		// server), previously unclassified → zero-retry terminal exit.
		{"pgconn.ErrConnClosed sentinel (Bug 199b)", pgconn.ErrConnClosed},
		// Bug 200: dial-time refusal at apply (pool acquire during a target
		// restart's refused window) — the pgx v5 ConnectError text shape,
		// Windows winsock wording and POSIX wording both.
		{"lane pool acquire, Windows refused dial (Bug 200)", errors.New(`pipelined acquire conn: failed to connect to ` + "`user=app database=app`" + `: dial error: dial tcp 127.0.0.1:5443: connectex: No connection could be made because the target machine actively refused it`)},
		{"pool acquire, POSIX refused dial (Bug 200)", errors.New("failed to connect: dial tcp 127.0.0.1:5432: connect: connection refused")},
		{"dial connection timed out (Bug 200)", errors.New("failed to connect: dial tcp 10.0.0.9:5432: connect: connection timed out")},
		{"wrapped pgconn.ErrConnClosed (checkpoint begin)", fmt.Errorf("postgres: applier: checkpoint begin: %w", pgconn.ErrConnClosed)},
		{"pgconn SafeToRetry contract (connLockError stand-in)", fmt.Errorf("checkpoint begin: %w", safeToRetryErr{})},
		{"io.EOF", io.EOF},
		{"io.ErrUnexpectedEOF sentinel (reparent conn drop)", io.ErrUnexpectedEOF},
		{"unexpected EOF string form (pgx mid-COPY conn sever)", errors.New(`copy chunk into "customers" (0 of 50000 rows copied before error): unexpected EOF`)},
		{"wrapped driver.ErrBadConn", fmt.Errorf("query: %w", driver.ErrBadConn)},
		{"context.DeadlineExceeded (GitHub #23 per-exec timeout)", context.DeadlineExceeded},
		{"wrapped context.DeadlineExceeded (GitHub #23)", fmt.Errorf("postgres: applier: insert: %w", context.DeadlineExceeded)},
		{"connection reset by peer", errors.New("write tcp: connection reset by peer")},
		{"connection refused", errors.New("dial tcp: connection refused")},
		{"broken pipe", errors.New("write tcp: broken pipe")},
		{"i/o timeout", errors.New("read tcp: i/o timeout")},
		// The control-read (ReadPosition/ListStreams) transient shape: a
		// degraded pooled connection surfaces pgx's cached-statement
		// cleanup timing out. ReadPosition/ListStreams route through
		// classifyApplierError so this rides the same retriable backoff as
		// the apply path (rather than a hard startup/status fault).
		{"cached-statement deallocate i/o timeout (control-read shape)", errors.New(`read position: failed to deallocate cached statement(s): timeout: read tcp 127.0.0.1:53482->127.0.0.1:32769: i/o timeout`)},
		{"database starting up (server-side)", errors.New("the database system is starting up")},
		{"database shutting down (server-side)", errors.New("the database system is shutting down")},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := classifyApplierError(c.err)
			var re ir.RetriableError
			if !errors.As(got, &re) {
				t.Fatalf("classifyApplierError did not produce ir.RetriableError; got %T (%v)", got, got)
			}
			if !re.Retriable() {
				t.Errorf("classified error's Retriable() = false; want true")
			}
			if !errors.Is(got, c.err) {
				t.Errorf("Unwrap chain broken: errors.Is(classified, original) = false")
			}
		})
	}
}

// TestClassifyApplierError_UnknownSQLSTATENotRetriable covers the
// default-deny: SQLSTATEs we haven't explicitly listed stay
// non-retriable so a previously-fail-fast error stays fail-fast.
func TestClassifyApplierError_UnknownSQLSTATENotRetriable(t *testing.T) {
	cases := []string{
		"42501", // insufficient_privilege
		"22P02", // invalid_text_representation
		"54000", // program_limit_exceeded
		"P0001", // raise_exception (PL/pgSQL custom)
		// Class-53 members deliberately EXCLUDED from item 38's retriable set:
		// these are config/operator faults that do NOT self-heal by retrying,
		// so they must stay terminal even though 53100/53000/53200 are now
		// retriable (don't over-match the class).
		"53300", // too_many_connections
		"53400", // configuration_limit_exceeded
	}
	for _, code := range cases {
		code := code
		t.Run(code, func(t *testing.T) {
			err := &pgconn.PgError{Code: code, Message: "sample"}
			got := classifyApplierError(err)
			var re ir.RetriableError
			if errors.As(got, &re) {
				t.Errorf("SQLSTATE %s wrongly classified as retriable", code)
			}
		})
	}
}
