// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Corpus-parity change-detector for the PG applier classifier's
// transport-text leg (audit 2026-07-23 QUAL-1 / gate G-9): with NO
// structured *pgconn.PgError in the chain, every shared
// internal/nettransient corpus shape must classify retriable — and the
// terminal-code shield (D0-8) must keep every one of them TERMINAL when
// a structured terminal SQLSTATE is present, so the shared corpus can
// never weaken the shield.

package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/nettransient"
)

func TestClassifyApplierError_NetTransientCorpusParity(t *testing.T) {
	for _, shape := range nettransient.TextShapes {
		shape := shape
		t.Run(shape, func(t *testing.T) {
			// (a) No structured server error: the shared transport shape
			// rides the retry loop.
			plain := fmt.Errorf("postgres: applier: checkpoint begin: %w",
				errors.New("driver: "+shape+" (framed)"))
			got := classifyApplierError(plain)
			var re ir.RetriableError
			if !errors.As(got, &re) || !re.Retriable() {
				t.Errorf("shared corpus shape %q not retriable without a structured error — the site drifted from internal/nettransient", shape)
			}

			// (b) Shield preservation (D0-8): the SAME wording quoted inside
			// a structured terminal SQLSTATE stays terminal — the corpus must
			// only ever be consulted when no server response is present.
			shielded := &pgconn.PgError{
				Code:    "23505",
				Message: "duplicate key value violates unique constraint: stored value '" + shape + "'",
			}
			var re2 ir.RetriableError
			if errors.As(classifyApplierError(shielded), &re2) {
				t.Errorf("structured 23505 quoting corpus shape %q classified RETRIABLE — the shared corpus weakened the terminal-code shield (D0-8)", shape)
			}
		})
	}
	// The shared exclusions hold at this site too.
	if got := classifyApplierError(errors.New("failed to connect: dial tcp: lookup db.exmple.com: no such host")); got != nil {
		var re ir.RetriableError
		if errors.As(got, &re) {
			t.Error("'no such host' must stay terminal (operator error) at the PG applier site")
		}
	}
}

// TestClassifyApplierError_NetTransientSQLStateParity is the CODE-shape twin
// of the corpus-parity test above (audit 2026-07-26 QUAL-2).
//
// The existing parity test iterates nettransient.TextShapes only, so it could
// not see the real drift risk: the connection-availability SQLSTATE set was
// duplicated inline in this file while nettransient's own doc claimed to be
// its SINGLE HOME. A new managed-PG shape added to the shared predicate would
// reach the pgtrigger poll classifier and the connect-phase retry, but not the
// CDC apply classifier — the one that decides whether hours of streamed
// changes ride out an outage. Iterating the shared set is what makes the
// delegation checkable rather than merely intended.
func TestClassifyApplierError_NetTransientSQLStateParity(t *testing.T) {
	// Every SQLSTATE the shared predicate calls connection-availability must
	// be retriable here too.
	codes := []string{
		"57P01", "57P02", "57P03", // admin shutdown / crash shutdown / cannot connect now
		"08000", "08003", "08006", "08007", "08P01", // class 08 connection_exception
	}
	for _, code := range codes {
		t.Run(code, func(t *testing.T) {
			pgErr := &pgconn.PgError{Code: code, Message: "server said so"}
			if !nettransient.IsConnectionAvailabilitySQLState(pgErr) {
				t.Fatalf("the shared predicate does not consider %s a connection-availability code; this test's "+
					"list has drifted from nettransient", code)
			}
			got := classifyApplierError(fmt.Errorf("postgres: applier: %w", pgErr))
			var re ir.RetriableError
			if !errors.As(got, &re) || !re.Retriable() {
				t.Errorf("SQLSTATE %s is retriable per the shared predicate but TERMINAL here — the apply "+
					"classifier has drifted from internal/nettransient, so an outage shape that the poll "+
					"classifier rides out would kill a stream mid-apply (audit QUAL-2)", code)
			}
		})
	}

	// The shield still holds: a terminal SQLSTATE stays terminal even though
	// the delegation now runs in the same function.
	terminal := &pgconn.PgError{Code: "23505", Message: "duplicate key"}
	got := classifyApplierError(fmt.Errorf("postgres: applier: %w", terminal))
	var re ir.RetriableError
	if errors.As(got, &re) && re.Retriable() {
		t.Error("a terminal SQLSTATE became retriable after the delegation — the code shield regressed")
	}
}
