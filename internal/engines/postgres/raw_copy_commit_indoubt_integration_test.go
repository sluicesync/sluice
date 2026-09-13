//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestWithCopySessionPins_CommitFailureIsTerminal drives the REAL commit path
// and is the gate that actually holds the in-doubt carve-out.
//
// Its unit-test sibling (TestRawCopyCommitInDoubtIsTerminal) constructs the
// terminal error itself, so it grades ir.IsTerminal's plumbing and says nothing
// about whether [withCopySessionPins] produces one. That was proven by
// mutation: reverting the carve-out at the COMMIT left the unit test green,
// which is the self-referential-fixture shape — it showed the binary agreeing
// with itself about a value no other code path had to produce.
//
// This test takes the error from the function under test. It forces a genuine
// commit failure by closing the connection inside fn, AFTER the pins and the
// body have succeeded and BEFORE withCopySessionPins issues its COMMIT — which
// is precisely the window a dropped connection hits in production, and the one
// whose outcome is IN DOUBT.
func TestWithCopySessionPins_CommitFailureIsTerminal(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "commit_indoubt_db")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire conn: %v", err)
	}
	// The conn is deliberately destroyed by this test; Close on an already
	// broken conn is a no-op error we do not care about.
	defer func() { _ = conn.Close() }()

	var pinErr error
	rawErr := conn.Raw(func(driverConn any) error {
		pgConn, perr := pgConnFromDriver(driverConn)
		if perr != nil {
			return perr
		}
		// Premise check: the connection is healthy and OUTSIDE a transaction,
		// so withCopySessionPins takes the ownTx branch that issues its own
		// BEGIN and COMMIT. Without that, this test would exercise the
		// ambient-transaction path, which has no COMMIT of its own and could
		// never produce the error under test.
		if st := pgConn.TxStatus(); st != 'I' {
			t.Fatalf("connection TxStatus = %q, want 'I' (idle, outside a transaction) — "+
				"withCopySessionPins would join an ambient transaction and never issue the COMMIT "+
				"this test grades", st)
		}

		pinErr = withCopySessionPins(ctx, pgConn, false, func() error {
			// The body SUCCEEDS. Then the connection dies — the exact shape of
			// a server that drops a saturated client between the last COPY
			// byte and the commit acknowledgement.
			if cerr := pgConn.Close(ctx); cerr != nil {
				t.Logf("closing the pinned connection returned %v (not fatal; the close is the point)", cerr)
			}
			return nil
		})
		return nil
	})
	// A destroyed connection can make database/sql report an error out of Raw;
	// that is expected and is not what this test grades.
	if rawErr != nil {
		t.Logf("conn.Raw returned %v (expected for a deliberately destroyed connection)", rawErr)
	}

	if pinErr == nil {
		t.Fatal("withCopySessionPins returned nil after its COMMIT could not be sent — the in-doubt " +
			"window would pass as a successful copy")
	}
	if !ir.IsTerminal(pinErr) {
		t.Fatalf("the commit failure is NOT terminal: %v\n\n"+
			"This is the silent-duplication path. The error carries a connection SQLSTATE the transient "+
			"classifier accepts, so runRawCopyChunkWithRetry will re-export the chunk on top of a commit "+
			"that may have landed on the server. On the whole-table raw lane — engaged for tables below "+
			"the split threshold, which needs no primary key — nothing downstream can catch the "+
			"duplicates, so the table ends up holding 2N rows at exit 0.", pinErr)
	}
}
