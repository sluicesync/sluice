// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Corpus-parity change-detector for the MySQL applier classifier's
// transport-text leg (audit 2026-07-23 QUAL-1 / gate G-9): with NO
// structured *MySQLError in the chain, every shared
// internal/nettransient corpus shape must classify retriable — and the
// terminal-code shield (D0-3) must keep every one of them TERMINAL when
// a structured terminal code is present, so the shared corpus can never
// weaken the shield.

package mysql

import (
	"errors"
	"fmt"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/nettransient"
)

func TestClassifyApplierError_NetTransientCorpusParity(t *testing.T) {
	for _, shape := range nettransient.TextShapes {
		shape := shape
		t.Run(shape, func(t *testing.T) {
			// (a) No structured driver error: the shared transport shape
			// rides the retry loop.
			plain := fmt.Errorf("mysql: flush table %q: %w", "users",
				errors.New("driver: "+shape+" (framed)"))
			got := classifyApplierError(plain)
			var re ir.RetriableError
			if !errors.As(got, &re) || !re.Retriable() {
				t.Errorf("shared corpus shape %q not retriable without a structured error — the site drifted from internal/nettransient", shape)
			}

			// (b) Shield preservation (M0-4): the SAME wording echoed inside
			// a structured terminal code stays terminal — the corpus must
			// only ever be consulted when no server response is present.
			shielded := &gomysql.MySQLError{
				Number:  1062,
				Message: "Duplicate entry '" + shape + "' for key 'jobs.name'",
			}
			var re2 ir.RetriableError
			if errors.As(classifyApplierError(shielded), &re2) {
				t.Errorf("structured 1062 echoing corpus shape %q classified RETRIABLE — the shared corpus weakened the terminal-code shield (D0-3)", shape)
			}
		})
	}
	// The shared exclusions hold at this site too.
	if got := classifyApplierError(errors.New("dial tcp: lookup db.example.com: no such host")); got != nil {
		var re ir.RetriableError
		if errors.As(got, &re) {
			t.Error("'no such host' must stay terminal (operator error) at the MySQL applier site")
		}
	}
}
