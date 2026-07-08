// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/ir"
)

// OpenSnapshotStream opens a TRIGGER-NATIVE consistent snapshot + CDC
// handoff (Bug 94). It does NOT use a replication slot or pgoutput —
// the whole point of the trigger engine is the slot-less managed-PG
// tier (ADR-0066). The previous implementation delegated to the
// composed [postgres.Engine].OpenSnapshotStream, which is the slot-
// based pgoutput path; under the orchestrator's engine-neutral
// coldStart that silently created a replication slot the target tier
// forbids and never engaged the capture-log poller. The same-engine /
// cross-engine congruence tests masked this because they drive the
// trigger reader via the MANUAL path (Setup → Migrator → OpenCDCReader)
// rather than through the Streamer.
//
// The stream provides a consistent bulk-copy snapshot plus a gapless,
// idempotent handoff to the trigger CDC poller:
//
//   - A dedicated *sql.DB and one pinned *sql.Conn running
//     `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` for the
//     snapshot read.
//   - Position = the CONTIGUOUS COMMITTED-PREFIX anchor of the capture
//     log, captured in the SAME transaction as the snapshot (see
//     [readChangeLogAnchor] for the gap-freedom proof). Everything ≤
//     anchor is in the snapshot (copied by Rows); everything > anchor
//     is replayed by Changes. Over-replay is safe (idempotent applier,
//     ADR-0010); a gap is forbidden. Transactions still in flight at
//     snapshot time — whose change-log rows are MVCC-invisible to the
//     anchor query — are waited out (bounded; loud refusal naming the
//     stuck txids on timeout) and the anchor clamped below their ids
//     (see [waitForPreSnapshotTxnsToSettle] for the invariant).
//   - Rows = a snapshot-pinned [postgres.RowReader] over the USER
//     tables (the orchestrator drives it per-table from the filtered
//     IR schema; the engine-managed capture tables are excluded from
//     ReadSchema by the postgres reader's bookkeeping-table filter,
//     Bug 93).
//   - Changes = the trigger [CDCReader] (the poller), which resumes
//     from the anchor and never creates or uses a slot.
//   - ReleaseRows commits the snapshot tx + closes the pinned conn so
//     it doesn't linger as `idle in transaction` for the CDC lifetime.
//   - Close stops the CDC poller and closes the snapshot pool.
func (e Engine) OpenSnapshotStream(ctx context.Context, dsn string) (*ir.SnapshotStream, error) {
	cfg, err := parseDSNCompat(dsn)
	if err != nil {
		return nil, err
	}

	// Dedicated pool for the snapshot read. The CDC poller opens its
	// OWN pool (via openCDCReader below) so the snapshot pool can be
	// released by ReleaseRows independently of the CDC lifetime.
	db, err := postgres.OpenPgxDB(cfg.dsn, e.appID)
	if err != nil {
		return nil, fmt.Errorf("pgtrigger: snapshot: open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pgtrigger: snapshot: ping: %w", err)
	}

	// Refuse loudly when the capture log is absent — the operator
	// forgot to run `sluice trigger setup`. Same refusal the CDC reader
	// surfaces at open time; firing it here too means the streamer's
	// cold-start aborts before any data moves rather than mid-stream.
	if exists, err := changeLogTableExists(ctx, db, cfg.schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pgtrigger: snapshot: preflight: %w", err)
	} else if !exists {
		_ = db.Close()
		return nil, fmt.Errorf(
			"pgtrigger: %s.%s does not exist on the source — run `sluice trigger setup --dsn=...` before starting the stream",
			cfg.schema, ChangeLogTable,
		)
	}

	// Perf-parity gap 3: the trigger-CDC cold-start copies tables SERIALLY on
	// the one snapshot-pinned connection below — there is no parallel option
	// on this flavor (the anchor + snapshot consistency argument is bound to
	// that single REPEATABLE READ view; parallelizing it is a separate L-effort
	// item recorded in docs/dev/perf-parity-matrix.md). Say so up front rather
	// than leaving a silently slower-than-migrate cold start unexplained.
	slog.InfoContext(ctx, "pgtrigger: snapshot: cold-start bulk copy runs SERIAL on the single snapshot connection (trigger-CDC design; no parallel knob on this flavor)")

	// Pin a single connection and open the REPEATABLE READ snapshot.
	// READ ONLY documents intent (the snapshot path never writes) and
	// lets the server skip assigning a real xid to the reader.
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pgtrigger: snapshot: pin sql conn: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("pgtrigger: snapshot: BEGIN: %w", err)
	}

	// Capture the CDC anchor at the same point / in the same tx as the
	// snapshot used for Rows. Running this query inside the open
	// REPEATABLE READ tx is what makes `pg_current_snapshot()` reflect
	// the very snapshot the bulk copy reads, so the contiguous-prefix
	// computation is consistent with what Rows will and won't see (see
	// readChangeLogAnchor for the gap-freedom proof + worked example).
	anchor, err := readChangeLogAnchor(ctx, conn, cfg.schema)
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("pgtrigger: snapshot: read CDC anchor: %w", err)
	}

	// The anchor query alone cannot see change-log rows INSERTed by
	// transactions still in flight at snapshot time (MVCC) — and such a
	// txn may hold an already-allocated id BELOW the anchor (the
	// invisible in-flight low-id window; live-confirmed 2026-07-08).
	// Export the snapshot's visibility horizon from the SAME pinned
	// conn the anchor used, assign a txid upper bound on the pool, wait
	// until every pre-bound transaction settles (fresh snapshot per
	// poll, on the pool — the pinned conn can never observe them
	// settling), then clamp the anchor below the now-visible change-log
	// ids that are NOT in the copy's snapshot. See
	// waitForPreSnapshotTxnsToSettle for the gap-freedom invariant. A
	// quiescent source pays three extra round-trips and no waiting.
	snapText, err := captureSnapshotText(ctx, conn)
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("pgtrigger: snapshot: export snapshot horizon: %w", err)
	}
	upperBound, err := captureTxidUpperBound(ctx, db)
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("pgtrigger: snapshot: assign settle-wait txid bound: %w", err)
	}
	if err := waitForPreSnapshotTxnsToSettle(ctx, db, upperBound, anchorSettleTimeout); err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("pgtrigger: snapshot: %w", err)
	}
	minID, found, err := minChangeLogIDForInvisibleTxns(ctx, db, cfg.schema, snapText, upperBound)
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("pgtrigger: snapshot: clamp anchor below snapshot-invisible change-log ids: %w", err)
	}
	if found && minID <= anchor {
		anchor = minID - 1
	}

	position, err := encodePos(pgTriggerPos{LastID: anchor})
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("pgtrigger: snapshot: encode position: %w", err)
	}

	// Rows: reuse the composed postgres engine's RowReader value-decode
	// + buildSelect machinery on our pinned snapshot conn. The reader
	// does not own the conn lifecycle — ReleaseRows/Close below do.
	rowReader := postgres.NewSnapshotRowReader(conn, cfg.schema)

	// Changes: the trigger poller, resuming from the anchor. It opens
	// its OWN *sql.DB pool and NEVER creates or uses a replication slot
	// (it scans sluice_change_log via the §2 txid safety-lag predicate).
	cdcReader, err := openCDCReader(ctx, dsn, e.appID)
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("pgtrigger: snapshot: build cdc reader: %w", err)
	}

	stream := &ir.SnapshotStream{
		Position: position,
		Rows:     rowReader,
		Changes:  cdcReader,
	}

	// rowsReleased guards ReleaseRowsFn / CloseFn against a double
	// commit/close. The orchestrator calls ReleaseRows after bulk-copy,
	// then Close on unwind; Close also calls releaseRows as a safety
	// net. Not mutex-guarded: the orchestrator never calls these
	// concurrently (bulk-copy is single-goroutine for the snapshot tx;
	// the streamer's defer chain serialises Close). Mirrors the
	// postgres engine's cdc_snapshot.go closure shape.
	rowsReleased := false
	releaseRows := func() error {
		if rowsReleased {
			return nil
		}
		rowsReleased = true
		// COMMIT (not ROLLBACK) so the read-only snapshot tx exits
		// cleanly — nothing was written; COMMIT is the project
		// convention and matches the postgres ReleaseRows. context.
		// Background so a cancelled parent ctx doesn't prevent cleanup.
		var firstErr error
		if _, err := conn.ExecContext(context.Background(), "COMMIT"); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	stream.ReleaseRowsFn = releaseRows
	stream.CloseFn = func() error {
		// Order: stop the CDC poller first (cancels its pump + closes
		// its own pool), then release the snapshot conn if not already,
		// then close the snapshot DB pool.
		var firstErr error
		if c, ok := cdcReader.(interface{ Close() error }); ok {
			if err := c.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := releaseRows(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	return stream, nil
}
