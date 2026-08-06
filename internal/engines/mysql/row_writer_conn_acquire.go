// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Cold-copy connection ACQUISITION retry (roadmap item 139)
//
// ADR-0108 taught the per-batch flush to ride a transient target error,
// and item 122 taught the classifier vtgate's own wording for a dropped
// connection. Both landed on the OPERATION. Neither reached the
// CONNECTION the operation needs.
//
// Every bulk-write core in this package pins a *sql.Conn before it runs
// a single statement — for a session-scoped reason (SHOW WARNINGS /
// @@warning_count are per-session, so the Vector-B probe must read the
// same session that wrote). That acquire was a bare `w.db.Conn(ctx)`
// whose error was wrapped `pin connection: %w` and returned, so the
// field report of 2026-08-05 (run 2) died on:
//
//	mysql: batched insert into "audits": pin connection: Error 1105
//	(HY000): internal: vtgate connection error: read tcp …:15999:
//	read: connection reset by peer
//
// on a run whose 78 IDENTICAL drops one line later were all absorbed by
// [RowWriter.flushWithReparentRetry]. The classifier already knew that
// error; nothing consulted it at the acquire.
//
// # Why acquisition needs its own helper rather than the flush's
//
// [RowWriter.flushWithReparentRetry] cannot be reused as-is: it takes an
// attempt closure that RUNS A STATEMENT on a conn it is handed, which is
// the thing we do not have yet. What it CAN share is the policy — the
// same classifier verdict, the same backoff shape, the same wall-clock
// bound, the same grow-gate Await/Trip — and this helper deliberately
// reads those from the same package vars so the two loops cannot drift.
//
// # The one place the two loops legitimately DIVERGE
//
// The flush loop carries the keyless carve-out (audit B-9): re-sending a
// batch is only safe when a committed-but-unacked prior attempt would
// announce itself as a duplicate-key collision. Acquiring a connection
// WRITES NOTHING, so there is no prior attempt to be ambiguous about and
// no row that could be doubled — a re-acquire is idempotent by
// construction. That is why this helper has no [irbackup.TableReplayIdempotent]
// gate and MUST NOT grow one: refusing a keyless table's acquire would
// be a pure regression (the flush that follows still gets the carve-out).

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// acquireConnWithRetry checks a connection out of the pool with the SAME
// bounded transient-retry ADR-0108 gives the per-batch flush, so a target
// drop DURING acquisition is ridden rather than returned raw (item 139).
//
// It returns the raw acquire error on a TERMINAL failure — a bad DSN, an
// auth refusal, a closed pool — so each caller keeps wrapping it in its
// own `pin connection` message exactly as before; the operator-facing
// text for a genuine configuration error is unchanged. Only the
// CLASSIFIED-TRANSIENT case behaves differently, and only by retrying.
//
// table is used for the log/refusal text and for the ADR-0113
// reparent-touched notification; it is rendered through [tableNameOf], so
// a nil table cannot panic this path.
//
// The grow gate is engaged on both halves, matching the flush loop: Await
// before every attempt (park with the sibling lanes rather than hammer a
// target they are all backing off from) and Trip on a classified
// transient (tell the siblings this lane hit one). Item 143 asked what that
// trip SIGNAL actually means, for both loops at once, and answered it in one
// place: [growEvidenceOf], reached through [RowWriter.tripGrowGate]. This
// helper still does not invent a second answer — it now shares the first one.
func (w *RowWriter) acquireConnWithRetry(ctx context.Context, table *ir.Table) (*sql.Conn, error) {
	tableName := tableNameOf(table)
	if err := w.awaitGrowGate(ctx); err != nil {
		return nil, err
	}
	conn, err := w.db.Conn(ctx)
	if err == nil {
		return conn, nil
	}

	// WALL-CLOCK BOUND, same reasoning as the flush loop: the ADR-0110
	// gate's fast probe cycles consume attempts far faster than
	// wall-clock, so the terminal bound is elapsed time and the attempt
	// count is only a runaway backstop.
	deadline := time.Now().Add(coldCopyReparentMaxWallVar)

	for try := 1; ; try++ {
		var re ir.RetriableError
		if !errors.As(classifyApplierError(err), &re) || !re.Retriable() {
			return nil, err
		}
		w.tripGrowGate("mysql cold-copy connection acquire transient: "+err.Error(), err)
		// ADR-0113: a target that cannot hand out a connection is a target
		// mid-transition, which is exactly what the restore reconciler wants
		// to hear about. Notifying here can only ever over-report (the
		// reconciler re-derives the table from its chunks), never under-.
		w.notifyReparent(tableName)
		if time.Now().After(deadline) || try >= coldCopyReparentRetryAttemptsVar {
			return nil, fmt.Errorf(
				"mysql: cold-copy into %q: still could not acquire a connection after riding the reparent-retry "+
					"window (%s wall-clock, %d attempts; the target may be undergoing a prolonged reparent/failover "+
					"or be genuinely down): %w",
				tableName, coldCopyReparentMaxWallVar, try, err,
			)
		}

		backoff := coldCopyReparentBackoff(try)
		// Sibling of the flush loop's WARN, corrected in the same pass (item
		// 143): the verdict is derived from this error, not asserted about it.
		slog.WarnContext(
			ctx, "mysql: cold-copy could not acquire a connection because of a transient target error; retrying",
			slog.String("table", tableName),
			slog.String("evidence", growEvidenceOf(err).String()),
			slog.Int("attempt", try),
			slog.Duration("elapsed", time.Since(deadline.Add(-coldCopyReparentMaxWallVar))),
			slog.Duration("max_wall", coldCopyReparentMaxWallVar),
			slog.Duration("backoff", backoff),
			slog.String("err", err.Error()),
		)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		if aerr := w.awaitGrowGate(ctx); aerr != nil {
			return nil, aerr
		}
		conn, err = w.db.Conn(ctx)
		if err == nil {
			return conn, nil
		}
	}
}
