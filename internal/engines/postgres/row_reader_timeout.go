// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"log/slog"
)

// The SOURCE-side half of the statement_timeout pin.
//
// [withCopySessionPins] pins `statement_timeout = 0` for the COPY lanes,
// and its doc comment carries the full argument for why a bulk copy must
// not be governed by a server-side statement timeout. That fix landed on
// the WRITE side and on the raw lane's byte-pipe; it did nothing for the
// typed IR lane's own SELECT, so copying OUT of a server with a low
// statement_timeout still died. On PlanetScale Neki — which ships
// `statement_timeout = 30s` as a platform default — a single-reader copy
// of any table that takes longer than 30 seconds to stream is killed with
// 57014, having produced nothing, and re-running hits the same wall
// deterministically.
//
// WHICH READS ARE PINNED, enumerated rather than implied. The pin covers
// every read whose cost scales with the TABLE, which is the whole set that
// can cross a wall-clock timeout:
//
//   - [RowReader.ReadRows] — the unbounded single-reader full-table
//     stream. The longest statement sluice issues on the typed lane, and
//     the one the pin exists for: it is the path a table takes when
//     within-table chunking is declined (no usable PK, chunking off), so
//     it is also the path that cannot resume from a partial read.
//   - [RowReader.sampleKeysetOn] — ROW_NUMBER() OVER (ORDER BY pk) plus
//     COUNT(*) OVER () across the WHOLE table, to pick chunk boundaries.
//     A full scan plus a sort, before a single row has been copied.
//   - [RowReader.exactCountOn] — COUNT(*), the ADR-0042 N1 fallback taken
//     exactly when the table has never been ANALYZEd, which is the normal
//     shape of a freshly loaded migrate source.
//
// All three are ONCE PER TABLE, which is why the pin can afford to open a
// transaction of its own.
//
// NOT pinned, and each for a stated reason rather than by omission:
//
//   - [RowReader.readRowsBatch] — the keyset page behind ReadRowsBatch /
//     ReadRowsBatchBounded, and so behind every within-table chunk. It is
//     bounded by LIMIT (5000 rows by default) and driven by an index scan
//     on the PK, so the server would have to spend 30 seconds producing
//     5000 rows for a timeout to reach it. Pinning it would cost a BEGIN,
//     a SET and a ROLLBACK on EVERY page of every chunk — three
//     round-trips per ~200ms of work against a remote server, which is a
//     real throughput tax paid against a risk that does not arrive. If a
//     bounded page ever does time out, this is the first place to look.
//   - [RowReader.RangeBounds] — MIN/MAX over an indexed PK, two index
//     probes.
//   - [RowReader.reltuplesEstimate] — one pg_class row.
//
// The last two cannot grow with the table at all, so no table size can
// push them past a timeout.
//
// WHAT THIS COSTS IN CANCELLATION RESPONSIVENESS, stated because
// [withCopySessionPins] makes the same argument and it is weaker here. Its
// safety rests on the backend continuously touching the socket, so that
// pgx's DeadlineContextWatcherHandler — which breaks the socket rather
// than sending a CancelRequest — stops the statement at once. A streaming
// SELECT qualifies: it writes DataRows as it goes. COUNT(*) and the
// keyset-sample window do NOT — they produce nothing until the scan
// finishes — so on those two, a cancelled context is noticed only when the
// scan completes. That is a bounded delay on a preflight, and it is the
// behaviour every stock PostgreSQL already has (PG's own
// statement_timeout default is 0); the pin only changes servers that set
// one.
//
// WHY TRANSACTION-SCOPED rather than an [afterConnectSessionPins] entry:
// the same reason the copy pin is. A GUC set once per physical connection
// does not survive a TRANSACTION-mode pooler, where the backend can change
// between statements — and a Neki router is exactly such a layer. An
// explicit transaction is what pins a server backend, and SET LOCAL scopes
// the pin to that transaction so nothing leaks back into the pool. The
// leak is not hypothetical: pgx's ResetSession issues no DISCARD ALL, so a
// session-level SET on a pooled connection would silently disable
// statement_timeout for whatever ran on it next.
const pinReadStatementTimeout = "SET LOCAL statement_timeout = 0"

// noopRelease is the release function for a read session that owns
// nothing (a pinned snapshot connection, or an unrecognized querier).
func noopRelease() {}

// pinReadSession pins statement_timeout=0 for one table-scale read and
// returns the querier that read MUST run on, plus a release to call when
// its rows are closed. See the file header for scope and rationale.
//
// The returned querier is not always the one passed in: on a pool it is a
// transaction on one checked-out connection, which is the only form that
// survives a transaction-mode pooler.
//
// It never fails the read for want of a pin. A server that refuses to set
// statement_timeout is not a reason to refuse to copy — the pre-pin
// behaviour (run unpinned, and cross the timeout only if the table is big
// enough to) is strictly better than not running at all — so a pin error
// degrades to the original querier with a DEBUG log.
func pinReadSession(ctx context.Context, q querier, op string) (readOn querier, release func()) {
	switch src := q.(type) {
	case *sql.Conn:
		// A pinned reader's connection is already inside the
		// REPEATABLE READ transaction that holds its exported snapshot,
		// so SET LOCAL joins that transaction: correctly scoped, and
		// with no ownership to give back. Beginning our own transaction
		// here would be unsafe — the snapshot transaction is started
		// with a raw BEGIN that database/sql does not track, so a
		// BeginTx would nest silently and our Rollback would destroy
		// the caller's snapshot.
		//
		// The off-snapshot estimator connection
		// ([RowReader.reltuplesOffConn]) is the case where that premise
		// does NOT hold: it is a fresh connection outside any
		// transaction, where SET LOCAL is a no-op with a warning. That
		// is why the pin is verified rather than assumed — a pin that
		// silently did nothing is the failure this whole file exists to
		// stop being invisible.
		if _, err := src.ExecContext(ctx, pinReadStatementTimeout); err != nil {
			logUnpinnedRead(op, err)
			return q, noopRelease
		}
		if pinnedOnConn(ctx, src) {
			return src, noopRelease
		}
		// Not in a transaction, so SET LOCAL did nothing. This
		// connection is ours for the read's lifetime and is closed (or
		// belongs to a stream that closes it) afterwards, so a
		// session-scoped SET is in scope here and nowhere else.
		if _, err := src.ExecContext(ctx, "SET statement_timeout = 0"); err != nil {
			logUnpinnedRead(op, err)
		}
		return src, noopRelease

	case *sql.DB:
		conn, err := src.Conn(ctx)
		if err != nil {
			logUnpinnedRead(op, err)
			return q, noopRelease
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			_ = conn.Close()
			logUnpinnedRead(op, err)
			return q, noopRelease
		}
		if _, err := tx.ExecContext(ctx, pinReadStatementTimeout); err != nil {
			_ = tx.Rollback()
			_ = conn.Close()
			logUnpinnedRead(op, err)
			return q, noopRelease
		}
		return tx, func() {
			// A read transaction: rolling back is the cheap, correct
			// close. Both errors are ignored on purpose — the rows are
			// already drained, so there is no outcome left to report.
			_ = tx.Rollback()
			_ = conn.Close()
		}

	default:
		// A test double or a future querier. Unpinned, as before.
		return q, noopRelease
	}
}

// pinnedOnConn reports whether statement_timeout actually reads back as 0
// on conn. It is the premise check for the SET LOCAL above: "this
// connection is inside a transaction" is a fact about a caller several
// files away, and the project's rule is that a safety argument citing such
// a fact owes it a check rather than a comment.
//
// A read that cannot be performed is treated as NOT pinned, so the caller
// takes the session-scoped fallback: over-applying the fallback costs one
// redundant SET, under-applying it costs the copy.
func pinnedOnConn(ctx context.Context, conn *sql.Conn) bool {
	var v string
	if err := conn.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&v); err != nil {
		return false
	}
	// PostgreSQL renders the zero value as the bare string "0".
	return v == "0"
}

// logUnpinnedRead records that a read is proceeding WITHOUT the timeout
// pin. Deliberately DEBUG and deliberately not an error: on a stock
// PostgreSQL (statement_timeout = 0) an unpinned read behaves identically,
// so warning every operator about a condition that harms only those on a
// timeout-setting server would be noise. When it does matter, the read
// fails loudly on its own with 57014 and this line is in the debug log
// that explains why.
func logUnpinnedRead(op string, err error) {
	slog.Debug("source read left unpinned; a low server statement_timeout can still stop this read",
		slog.String("op", op), slog.String("err", err.Error()))
}
