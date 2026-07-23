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
