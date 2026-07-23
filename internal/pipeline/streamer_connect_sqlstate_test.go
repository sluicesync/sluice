// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 203 pins: the connect-phase classifier's structured SQLSTATE leg,
// exercised per cell through the REAL entry point (isRetriableConnectFailure
// over a connectHint-marked chain carrying a real *pgconn.PgError — the
// exact shape a retry re-establish sees when its ping lands in a restarting
// server's `57P03 database system is starting up` window). Pre-fix the
// classifier delegated to the network-shape matcher only, so every one of
// the retriable cells below exited TERMINAL — while the applier classifier
// and the trigger-CDC poll classifier both classified the same SQLSTATE
// retriable (QUAL-1's "fixed the representative, missed the sibling", one
// seam up).

package pipeline

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// connectPingErr builds the marked connect-phase chain a re-establish
// failure produces: the connectHint marker wrapping the engine's ping
// wrapping the structured server error.
func connectPingErr(code string) error {
	return connectHint(fmt.Errorf(
		"pipeline: engage add-column-forward: open source schema reader: postgres: ping: %w",
		&pgconn.PgError{Code: code, Severity: "FATAL", Message: "x"},
	))
}

func TestIsRetriableConnectFailure_SQLStateCells(t *testing.T) {
	// The PG connection-availability set: a restarting/promoting server.
	retriable := []string{
		"57P01", // admin_shutdown
		"57P02", // crash_shutdown
		"57P03", // cannot_connect_now — the starting-up window (the Bug 203 field shape)
		"08000", // connection_exception
		"08003", // connection_does_not_exist
		"08006", // connection_failure
		"08007", // transaction_resolution_unknown
		"08P01", // protocol_violation
	}
	for _, code := range retriable {
		code := code
		t.Run("retriable/"+code, func(t *testing.T) {
			if !isRetriableConnectFailure(connectPingErr(code)) {
				t.Errorf("SQLSTATE %s at the connect phase classified terminal; want retriable — a restarting server's availability window must ride the connect-phase budget (Bug 203)", code)
			}
		})
	}

	// Everything else stays terminal — retrying these masks a real fault.
	terminal := []string{
		"28P01", // invalid_password — credentials are an operator fault
		"28000", // invalid_authorization_specification
		"3D000", // invalid_catalog_name — wrong database
		"42P01", // undefined_table — setup fault
		"42703", // undefined_column
		"57014", // query_canceled — not an availability shape
		"40001", // serialization_failure — no connect-phase retry semantics
		"53300", // too_many_connections — config fault, does not self-heal
		"0A000", // feature_not_supported
	}
	for _, code := range terminal {
		code := code
		t.Run("terminal/"+code, func(t *testing.T) {
			if isRetriableConnectFailure(connectPingErr(code)) {
				t.Errorf("SQLSTATE %s classified retriable at the connect phase; want terminal — widening past the connection-availability set masks operator faults", code)
			}
		})
	}

	t.Run("marker required: an unmarked 57P03 stays with the engine classifiers", func(t *testing.T) {
		unmarked := fmt.Errorf("postgres: ping: %w", &pgconn.PgError{Code: "57P03", Severity: "FATAL", Message: "the database system is starting up"})
		if isRetriableConnectFailure(unmarked) {
			t.Error("isRetriableConnectFailure fired without the connect-phase marker; the SQLSTATE leg must not widen retry semantics the engines own")
		}
	})
}
