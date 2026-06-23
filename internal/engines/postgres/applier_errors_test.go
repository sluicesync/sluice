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
