// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRawCopyCommitInDoubtIsTerminal pins the one place in the copy path where
// "retriable" is actively UNSAFE rather than merely wasteful.
//
// A dropped connection is the failure the raw-copy retry exists for, and
// `08006` is classified transient precisely so the copy can ride it out. That
// is correct for a failure DURING the COPY stream — the server aborts the
// transaction when the client goes away, so the attempt leaves zero rows.
//
// It is wrong for the identical error on the COMMIT's response, which is IN
// DOUBT: the server may have durably committed and lost the reply. Retrying
// there re-exports the whole chunk on top of rows that may already be present.
// On a chunked copy the duplicates fail loudly when the primary key is added;
// on the WHOLE-TABLE raw lane, which needs no primary key at all, they do not
// fail on anything — 2N rows at exit 0.
//
// So the two cases carry the SAME SQLSTATE and must classify differently, and
// the only thing separating them is where the error was raised. This test is
// what keeps that distinction from being flattened back by a future change to
// the transient allow-list.
//
// SCOPE, established by mutation rather than assumed: this test builds the
// terminal error itself, so it grades ir.IsTerminal and the wrapping contract
// — NOT whether withCopySessionPins produces one. Reverting the carve-out at
// the COMMIT leaves this file green. The gate that actually holds the call
// site is TestWithCopySessionPins_CommitFailureIsTerminal (integration), which
// takes the error from the function under test.
func TestRawCopyCommitInDoubtIsTerminal(t *testing.T) {
	t.Parallel()

	// The wire error both cases produce: a broken connection carrying 08006.
	connBroken := &pgconn.PgError{Code: "08006", Message: "write: broken pipe"}

	t.Run("mid-stream 08006 stays retriable", func(t *testing.T) {
		t.Parallel()
		// The shape the retry exists for: the same error, NOT wrapped by the
		// commit carve-out. It must remain transient, or the fix for the
		// in-doubt window would have disabled the retry entirely — which is
		// the over-correction this cell exists to catch.
		err := fmt.Errorf("copy chunk: %w", connBroken)
		if ir.IsTerminal(err) {
			t.Fatal("a mid-stream 08006 classified TERMINAL — the raw-copy retry is now disabled for the " +
				"exact failure it was built to survive")
		}
		var re ir.RetriableError
		if classified := classifyApplierError(err); !errors.As(classified, &re) || !re.Retriable() {
			t.Fatalf("a mid-stream 08006 is not retriable (%v); the storage-grow ride-out depends on it", classified)
		}
	})

	t.Run("the same 08006 on COMMIT is terminal", func(t *testing.T) {
		t.Parallel()
		// Exactly what withCopySessionPins returns when the COMMIT's response
		// does not arrive.
		err := &terminalPGError{err: fmt.Errorf(
			"commit raw-copy transaction (IN DOUBT — the commit may have succeeded on the server; "+
				"this table is not retried automatically because re-copying could duplicate rows "+
				"on a table with no primary key to catch them): %w", connBroken,
		)}

		if !ir.IsTerminal(err) {
			t.Fatal("an IN-DOUBT commit failure is not TERMINAL. The retry will re-export the chunk on top " +
				"of a commit that may have landed; on the keyless whole-table lane that is silent " +
				"duplication at exit 0")
		}
		// And it must survive being wrapped by the layers between the commit
		// and the retry loop — the classifiers text-match through %w, which is
		// why ir.TerminalError exists at all.
		wrapped := fmt.Errorf("postgres: ImportRawCopy: %w",
			fmt.Errorf("pipeline: copy table %q (raw copy): %w", "t", err))
		if !ir.IsTerminal(wrapped) {
			t.Fatal("the IN-DOUBT marker does not survive wrapping; by the time the retry loop sees the " +
				"error it would read as an ordinary transient 08006 again")
		}
		// The operator has to be able to tell this apart from an ordinary
		// connection drop, because the recovery differs (re-run the table
		// rather than assume nothing landed).
		if got := wrapped.Error(); !strings.Contains(got, "IN DOUBT") {
			t.Errorf("the refusal does not say the commit is in doubt, so an operator cannot know the "+
				"target may hold a partial copy: %s", got)
		}
	})
}
