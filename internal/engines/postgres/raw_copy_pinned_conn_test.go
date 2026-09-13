// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"database/sql"
	"errors"
	"io"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// TestIsDeadPinnedConnErr pins the asymmetry that a chunk-level retry cannot
// see for itself, and that cost ~22 minutes of pointless replay to find.
//
// The raw exporter runs on one of two sources. On a POOLED source (*sql.DB) a
// dead connection is transient: the next attempt checks out a fresh one and
// succeeds. On a SNAPSHOT-PINNED source (*sql.Conn) the exported snapshot
// lives in a transaction on that exact connection, so when it dies the
// snapshot dies with it and every replay re-enters the same corpse.
//
// Measured 2026-09-12: a cold copy whose pinned source conn dropped replayed
// three chunks ~135 times at a 30s backoff cap, none of which could ever have
// worked, while the storage condition that triggered the first failure had
// already cleared. The retry loop was doing exactly what it was told; nothing
// had told it the resource could not be refreshed.
//
// The SERVER-RESPONSE cell is the one that keeps this narrow. A *pgconn.PgError
// means the server answered, so the connection was alive — those must keep
// flowing to classifyApplierError, or this predicate would silently convert
// every retriable server-side transient (53100, 25006, NK205) into a terminal
// failure on the snapshot path, which is the exact opposite of the day's work.
func TestIsDeadPinnedConnErr(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			// The shape observed in the field.
			name: "pgconn ErrConnClosed",
			err:  pgconn.ErrConnClosed,
			want: true,
		},
		{
			name: "database/sql ErrConnDone",
			err:  sql.ErrConnDone,
			want: true,
		},
		{
			name: "bare EOF",
			err:  io.EOF,
			want: true,
		},
		{
			name: "unexpected EOF",
			err:  io.ErrUnexpectedEOF,
			want: true,
		},
		{
			// THE NARROWING CELL. The server responded, so the conn was
			// alive. This must stay false or every server-side transient
			// becomes terminal on the pinned path.
			name: "a server-side transient (53100) is NOT a dead conn",
			err:  &pgconn.PgError{Code: "53100", Message: "could not extend file: No space left on device"},
			want: false,
		},
		{
			name: "a server-side transient (25006 low disk) is NOT a dead conn",
			err:  &pgconn.PgError{Code: "25006", Message: "shard is read-only (disk space low)"},
			want: false,
		},
		{
			name: "an ordinary error is not a dead conn",
			err:  errors.New("something else went wrong"),
			want: false,
		},
		{
			name: "nil",
			err:  nil,
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isDeadPinnedConnErr(tc.err); got != tc.want {
				if tc.want {
					t.Fatalf("isDeadPinnedConnErr(%v) = false, want true — a replay on the same pinned "+
						"connection can never succeed, so this must fail fast instead of spinning", tc.err)
				}
				t.Fatalf("isDeadPinnedConnErr(%v) = true, want false — treating a live-connection error as "+
					"a dead connection makes a RETRIABLE transient terminal on the snapshot path", tc.err)
			}
		})
	}
}

// TestDeadPinnedConnRefusalIsNotRetriable pins that the refusal the pinned
// branch returns cannot be mistaken for a transient by any retry loop above
// it. If it were wrapped as [ir.RetriableError], the replay storm this fix
// exists to stop would simply continue.
func TestDeadPinnedConnRefusalIsNotRetriable(t *testing.T) {
	t.Parallel()

	// The refusal is built by rawConn's pinned branch; construct the same
	// shape here rather than reaching for a live *sql.Conn.
	refusal := deadPinnedConnRefusal(pgconn.ErrConnClosed)

	var re ir.RetriableError
	if errors.As(refusal, &re) && re.Retriable() {
		t.Fatal("the dead-pinned-connection refusal is classified RETRIABLE — the retry loop will keep " +
			"replaying a connection that can never come back")
	}
	// The cause must stay reachable so an operator sees what actually died.
	if !errors.Is(refusal, pgconn.ErrConnClosed) {
		t.Error("the refusal dropped its cause; the operator cannot see which connection error occurred")
	}
}

// TestMapPinnedConnErr gates the WIRING, not just the predicates.
//
// The first cut of this fix tested isDeadPinnedConnErr and
// deadPinnedConnRefusal and nothing that joined them — so deleting the branch
// in rawConn broke no test at all. This is that gate.
func TestMapPinnedConnErr(t *testing.T) {
	t.Parallel()

	t.Run("nil passes through", func(t *testing.T) {
		t.Parallel()
		if got := mapPinnedConnErr(nil); got != nil {
			t.Fatalf("nil in must give nil out, got %v", got)
		}
	})

	t.Run("a dead pinned conn becomes the TERMINAL refusal", func(t *testing.T) {
		t.Parallel()
		got := mapPinnedConnErr(pgconn.ErrConnClosed)
		var re ir.RetriableError
		if errors.As(got, &re) && re.Retriable() {
			t.Fatal("the branch produced a RETRIABLE error — the replay storm continues")
		}
		if !errors.Is(got, pgconn.ErrConnClosed) {
			t.Error("the cause was dropped")
		}
		// Identity, deliberately: errors.Is would be true for the wrapped
		// form too, and "was it wrapped at all" is the assertion.
		if got == pgconn.ErrConnClosed { //nolint:errorlint // identity IS the assertion
			t.Error("the branch returned the raw error — the operator gets no explanation of why the " +
				"snapshot cannot resume")
		}
	})

	t.Run("a server-side transient passes through UNCHANGED", func(t *testing.T) {
		t.Parallel()
		in := &pgconn.PgError{Code: "53100", Message: "could not extend file: No space left on device"}
		// Identity again: the transient must be returned AS-IS, not merely
		// be findable in a chain, or its classification changes.
		if got := mapPinnedConnErr(in); got != error(in) { //nolint:errorlint // identity IS the assertion
			t.Fatalf("a server-side transient was rewritten (%v) — it must keep its identity so the "+
				"classifier can still see it is retriable", got)
		}
	})
}

// TestIsDeadPinnedConnErr_ServerResponseWinsInAJoinedChain makes the
// server-response guard LOAD-BEARING.
//
// Without this cell the guard was untestable-by-construction: pgconn's
// SafeToRetry is already false for a bare *pgconn.PgError, so every simple
// server-error case passed with or without it. Removing the guard broke
// nothing, which is the definition of a vacuous gate.
//
// This is the shape where it matters. An error chain can carry BOTH a server
// response and an EOF — errgroup combines the export and import legs, and the
// raw-copy retry joins a cause with ctx.Err() — and then errors.Is(err, io.EOF)
// is true even though the server plainly answered. Without the guard that
// chain reads as a dead connection, and a RETRIABLE storage transient becomes
// permanently terminal on the snapshot path: the exact inversion of the day's
// work.
func TestIsDeadPinnedConnErr_ServerResponseWinsInAJoinedChain(t *testing.T) {
	t.Parallel()

	pgErr := &pgconn.PgError{Code: "53100", Message: "could not extend file: No space left on device"}
	joined := errors.Join(pgErr, io.EOF)

	// Sanity: the chain really is ambiguous, or this cell proves nothing.
	if !errors.Is(joined, io.EOF) {
		t.Fatal("fixture is wrong: the chain does not carry io.EOF, so the guard is not under test")
	}

	if isDeadPinnedConnErr(joined) {
		t.Fatal("a chain carrying a SERVER RESPONSE was judged a dead connection because it also " +
			"carries io.EOF. The server answered, so the conn was alive — this turns a retriable " +
			"storage transient into a permanent failure on the snapshot-pinned path")
	}
}
