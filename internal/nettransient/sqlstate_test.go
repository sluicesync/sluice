// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package nettransient

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestIsConnectionAvailabilitySQLState pins the single-homed SQLSTATE set
// (Bug 203) both ways, through a REAL *pgconn.PgError — the structural
// SQLState() match is the load-bearing seam that keeps this package free of
// the pgx import while still catching the driver's actual error type, so
// the pin must exercise the real type, not just a local stub.
func TestIsConnectionAvailabilitySQLState(t *testing.T) {
	pg := func(code string) error {
		return fmt.Errorf("postgres: ping: %w", &pgconn.PgError{Code: code, Severity: "FATAL", Message: "x"})
	}
	for _, code := range []string{"57P01", "57P02", "57P03", "08000", "08003", "08006", "08007", "08P01"} {
		if !IsConnectionAvailabilitySQLState(pg(code)) {
			t.Errorf("IsConnectionAvailabilitySQLState(%s) = false; want true (connection-availability transient)", code)
		}
	}
	for _, code := range []string{"28P01", "28000", "3D000", "42P01", "42703", "40001", "53100", "53300", "57014", "0A000"} {
		if IsConnectionAvailabilitySQLState(pg(code)) {
			t.Errorf("IsConnectionAvailabilitySQLState(%s) = true; want false (must stay terminal)", code)
		}
	}
	if IsConnectionAvailabilitySQLState(nil) {
		t.Error("nil must not classify")
	}
	if IsConnectionAvailabilitySQLState(errors.New("dial tcp: connection refused")) {
		t.Error("a non-SQLSTATE error must not classify here (that is IsTransientShape's job)")
	}
}

// TestIsConnectionAvailabilitySQLState_TextIndependence pins the
// separation of concerns: the structured predicate reads ONLY the code —
// never message text — and the text corpus deliberately excludes PG
// server-lifecycle wordings, so a 57P03 chain classifies through the
// SQLSTATE leg alone. If either half drifts (a lifecycle wording creeps
// into TextShapes, or the predicate starts consulting messages), this pin
// fails and the D0-3/D0-8 data-echo shield rationale must be revisited.
func TestIsConnectionAvailabilitySQLState_TextIndependence(t *testing.T) {
	startingUp := fmt.Errorf("postgres: ping: %w",
		&pgconn.PgError{Code: "57P03", Severity: "FATAL", Message: "the database system is starting up"})
	if IsTransientShape(startingUp) {
		t.Error("IsTransientShape matched a structured PG lifecycle error; lifecycle wordings must stay OUT of the text corpus (the SQLSTATE leg owns them)")
	}
	if !IsConnectionAvailabilitySQLState(startingUp) {
		t.Error("the SQLSTATE leg must classify 57P03 regardless of message text")
	}
	// Code decides, not text: a 28P01 whose message ECHOES a corpus shape
	// stays terminal on the structured leg.
	authEcho := fmt.Errorf("postgres: ping: %w",
		&pgconn.PgError{Code: "28P01", Severity: "FATAL", Message: "password authentication failed (connection refused by policy)"})
	if IsConnectionAvailabilitySQLState(authEcho) {
		t.Error("IsConnectionAvailabilitySQLState consulted message text; the code alone must decide")
	}
}
