// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Overlapped index-build reparent-retry (ADR-0114 parity, audit N-15b)
//
// The MySQL analog of the PG overlap retry
// (postgres/schema_writer_index_overlap.go buildOneIndexWithReparentRetry):
// a PlanetScale storage-grow reparent landing mid-`ALTER … ADD INDEX` on the
// overlapped index path would otherwise abort the whole migration (loud;
// `--resume` recovers) on exactly the platform where reparents happen. The
// pipeline-level DDL-phase retry (migcore.ddl_phase_retry, gated on this
// writer's own IsTransientError) can't reach these per-table builds — they
// run interleaved with the copy, inside the engine — so the ride-out lives
// here, wrapping [SchemaWriter.buildTableIndexes] on BOTH overlap paths (the
// vanilla concurrent workers and the VStream serial build-as-copied branch).
//
// It deliberately reuses the SAME envelope (coldCopyReparent*Var +
// coldCopyReparentBackoff, ADR-0108) and classifier (classifyApplierError)
// as the cold-copy flush retry, so an index build and a row batch ride a
// storage-grow/reparent identically — no second policy to drift. Replaying
// buildTableIndexes after a killed attempt is clean: it re-probes every
// index (detect-then-skip, Bug 131) before emitting any ALTER, so a
// partially-applied combined ALTER — or one that committed server-side but
// died unacknowledged — is skipped, never double-created.
//
// Zero-value-safe by construction (the v0.99.51 lesson): the bounds are the
// ADR-0108 package vars, not config fields, so every construction path gets
// the same envelope; production never mutates them.

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

// buildTableIndexesWithReparentRetry runs one overlapped per-table index
// build on a PINNED worker connection with the bounded reparent-retry around
// it, mirroring the PG overlap wrapper of the same role. On a classified
// storage-grow/reparent transient it closes the dead pinned connection,
// backs off, re-acquires a FRESH connection (the pool reconnects to the
// reparented primary), and replays the build — safe because
// buildTableIndexes re-probes each index before emitting DDL. A
// non-transient (a real DDL fault) returns unchanged; budget exhaustion
// returns a LOUD terminal error. Returns the connection the caller should
// keep using (possibly the fresh one, or nil if a re-acquire ultimately
// failed) so the worker's nil-guarded deferred close and subsequent jobs
// target the live conn.
func (w *SchemaWriter) buildTableIndexesWithReparentRetry(ctx context.Context, conn *sql.Conn, job indexBuildJob) (*sql.Conn, error) {
	// attempt runs one build on the CURRENT conn; reacquire closes the dead
	// pinned conn and refreshes it (the reparented primary). conn is captured
	// by reference so the worker keeps using whatever connection survives.
	attempt := func(ctx context.Context) error { return w.buildTableIndexes(ctx, conn, job) }
	reacquire := func(ctx context.Context) error {
		if conn != nil {
			_ = conn.Close()
		}
		conn = nil
		fresh, err := w.db.Conn(ctx)
		if err != nil {
			return err
		}
		conn = fresh
		return nil
	}
	err := retryIndexBuildWithReparent(ctx, fmt.Sprintf("indexes on %q", job.tableName), attempt, reacquire)
	return conn, err
}

// buildTableIndexesOnPoolWithReparentRetry is the serial (VStream) path's
// wrapper: the build runs on the pooled *sql.DB, so there is no pinned
// connection to refresh — after a reparent kills the in-flight session,
// database/sql discards the dead conn and the next ExecContext checks out a
// fresh one (which the pool dials against the reparented primary). reacquire
// is therefore a documented no-op; the retry policy, classifier, and budget
// are identical to the worker path.
func (w *SchemaWriter) buildTableIndexesOnPoolWithReparentRetry(ctx context.Context, job indexBuildJob) error {
	attempt := func(ctx context.Context) error { return w.buildTableIndexes(ctx, w.db, job) }
	reacquire := func(context.Context) error { return nil } // pooled *sql.DB re-acquires implicitly
	return retryIndexBuildWithReparent(ctx, fmt.Sprintf("indexes on %q", job.tableName), attempt, reacquire)
}

// retryIndexBuildWithReparent is the PURE retry policy behind the overlapped
// MySQL index builds — no SQL, so it is unit-testable with fake
// attempt/reacquire closures. Same name and shape as the PG overlap policy;
// it reuses the ADR-0108 cold-copy envelope (coldCopyReparent*Var +
// coldCopyReparentBackoff) and classifier so the two ride a PlanetScale
// storage-grow/reparent identically:
//
//   - attempt runs one per-table index build on the current connection.
//   - on a CLASSIFIED transient (ir.RetriableError via classifyApplierError —
//     reparent / "not serving" / connection-lost / disk-full / read-only
//     window) it backs off (honoring ctx), calls reacquire to refresh the
//     connection, and attempts again. buildTableIndexes is detect-then-skip
//     per index, so a replay after a killed or committed-but-unacked ALTER
//     is clean — never a double-create, never a 1061.
//   - a NON-transient (a real DDL fault) returns unchanged — terminal, no
//     retry, exactly as before this fix.
//   - a reacquire error is itself classified on the next iteration (a still-
//     unreachable target surfaces the same transient shape), riding the
//     budget.
//   - budget exhaustion (wall-clock or runaway-attempt backstop) returns a
//     LOUD terminal error wrapping the most recent transient — never silent,
//     never infinite.
func retryIndexBuildWithReparent(
	ctx context.Context,
	label string,
	attempt func(ctx context.Context) error,
	reacquire func(ctx context.Context) error,
) error {
	err := attempt(ctx)
	if err == nil {
		return nil
	}
	deadline := time.Now().Add(coldCopyReparentMaxWallVar)
	for try := 1; ; try++ {
		var re ir.RetriableError
		if !errors.As(classifyApplierError(err), &re) || !re.Retriable() {
			return err
		}
		if time.Now().After(deadline) || try >= coldCopyReparentRetryAttemptsVar {
			return fmt.Errorf(
				"mysql: overlapped index build %s still failing after riding the reparent-retry window "+
					"(%s wall-clock, %d attempts; the target may be undergoing a prolonged storage-grow/reparent): %w",
				label, coldCopyReparentMaxWallVar, try, err,
			)
		}

		backoff := coldCopyReparentBackoff(try)
		slog.WarnContext(
			ctx, "mysql: overlapped index build hit a transient target error (likely a storage auto-grow / reparent); "+
				"re-acquiring a fresh connection and retrying the index build (ADR-0114 overlap path)",
			slog.String("build", label),
			slog.Int("attempt", try),
			slog.Duration("max_wall", coldCopyReparentMaxWallVar),
			slog.Duration("backoff", backoff),
			slog.String("err", err.Error()),
		)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}

		if rerr := reacquire(ctx); rerr != nil {
			err = rerr
			continue
		}
		err = attempt(ctx)
		if err == nil {
			return nil
		}
	}
}
