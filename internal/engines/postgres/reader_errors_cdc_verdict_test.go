// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCDCReaderKeepsTheRetriableSchemaDriftVerdict holds the CDC side of the
// Bug-285 split, which is the half nothing was watching.
//
// The split's whole premise is that schema drift means different things on a
// cold copy and on a long-running stream, so the two get different verdicts.
// That makes the assignment of each classifier to each lane a DECISION — and a
// decision that can be reversed by a one-word edit with no test noticing is not
// being made, it is being defaulted.
//
// It was in fact reversed, silently: the Bug-285 commit changed
// [classifyReaderError] to delegate to [classifyCopyError], which made schema
// drift TERMINAL for the PG CDC pump's five walreceiver call sites. The commit
// is titled "schema drift is retriable for CDC and TERMINAL for a copy", and
// the CDC reader is CDC — so the change contradicted the commit's own stated
// scope, and no test in either direction said anything. Found by the pre-tag
// value-fidelity review of v0.152.1, reverted, and pinned here.
//
// If someone later decides the CDC reader SHOULD take the terminal verdict —
// there is a real argument, since its errors arrive from the SOURCE where
// "an operator is about to create the missing relation" is a much weaker
// premise — this test is what makes that a deliberate change with a reason
// attached rather than an unnoticed side effect.
func TestCDCReaderKeepsTheRetriableSchemaDriftVerdict(t *testing.T) {
	t.Parallel()

	for _, code := range []string{"42P01", "42703"} {
		t.Run(code, func(t *testing.T) {
			t.Parallel()

			drift := &pgconn.PgError{Code: code, Message: "schema drift"}

			readerVerdict := classifyReaderError(drift)
			if ir.IsTerminal(readerVerdict) {
				t.Fatalf("the CDC reader classifies %s as TERMINAL. The Bug-285 split gives the copy path "+
					"the terminal verdict and leaves CDC retriable — a long-running stream can meet a "+
					"relation the target does not have yet, and riding it out beats exiting into a "+
					"supervisor restart loop. Terminal here kills a stream on a condition that self-heals",
					code)
			}

			var re ir.RetriableError
			if !errors.As(readerVerdict, &re) || !re.Retriable() {
				t.Fatalf("the CDC reader's %s verdict is not retriable (%v)", code, readerVerdict)
			}

			// The independent expected value: the COPY classifier must give the
			// OPPOSITE answer for the identical input. Without this the test
			// would pass against a build where the split had collapsed in the
			// other direction — both lanes retriable — and it would be grading
			// nothing but a tautology.
			copyVerdict := classifyCopyError(drift)
			if !ir.IsTerminal(copyVerdict) {
				t.Fatalf("the COPY classifier does NOT rule %s terminal (%v), so the reader answering "+
					"retriable proves nothing about the split — both lanes agree and this test is vacuous",
					code, copyVerdict)
			}
		})
	}

	// And a shared transient must still be transient on BOTH, so the split is
	// narrow: it reverses exactly one verdict, not a whole classifier.
	transient := &pgconn.PgError{Code: "40001", Message: "serialization failure"}
	for name, got := range map[string]error{
		"reader": classifyReaderError(transient),
		"copy":   classifyCopyError(transient),
	} {
		var re ir.RetriableError
		if !errors.As(got, &re) || !re.Retriable() {
			t.Fatalf("the %s classifier no longer rules 40001 retriable (%v) — the Bug-285 split is "+
				"supposed to reverse ONE verdict, not diverge the classifiers wholesale", name, got)
		}
	}
}
