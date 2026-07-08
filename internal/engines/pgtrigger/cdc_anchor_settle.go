// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

// The settle wait closes the anchor query's MVCC blind spot (the
// invisible in-flight low-id window — see [readChangeLogAnchor] and the
// gap-freedom invariant on [waitForPreSnapshotTxnsToSettle]).
//
// The timeout bounds cold-start against a wedged writer transaction: a
// txn that never settles must surface as a loud refusal naming the
// stuck txids, not hang `sync start` forever. Five minutes tolerates
// legitimate long batch writes spanning the handoff while still failing
// within an operator's attention span; slot-based
// CREATE_REPLICATION_SLOT performs the same all-running-txns wait, but
// unbounded — bounding it is this engine's own loud-failure posture.
// Hardcoded like the poll defaults in cdc_reader.go (ADR-0066 §6); no
// existing operator knob fits, and a too-small configurable value would
// be a correctness footgun.
const (
	anchorSettleTimeout       = 5 * time.Minute
	anchorSettlePollInterval  = 1 * time.Second
	anchorSettleProgressEvery = 30 * time.Second
)

// settleQuerier is the database/sql slice the settle-wait helpers need.
// Both *sql.DB (the fresh-snapshot polls and the clamp query) and a
// snapshot-pinned *sql.Conn (the snapshot-text capture) satisfy it.
type settleQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// snapshotTextQuery exports the current snapshot's textual form
// (xmin:xmax:xip). Run on the PINNED snapshot connection it captures
// the exact visibility horizon of the bulk copy, replayable later via
// `$n::pg_snapshot` + pg_visible_in_snapshot on any connection.
const snapshotTextQuery = `SELECT pg_current_snapshot()::text`

// txidUpperBoundQuery assigns a REAL txid to its (implicit,
// immediately-committed) transaction and returns it in the 64-bit
// epoch-carrying bigint domain ([pollQuery]'s xid8 rationale). Run on a
// fresh connection AFTER the snapshot is established, the returned
// value strictly exceeds every txid assigned before the snapshot — the
// sound upper bound for the settle wait.
//
// A snapshot's own xmax is NOT that bound (the trap, live-confirmed on
// PG 16, 2026-07-08): xmax is latestCompletedXid+1, so a still-RUNNING
// transaction can sit at or above xmax — outside the xip list too —
// while already holding an invisible low change-log id. Only a freshly
// ASSIGNED xid bounds every earlier assignment; this is the same
// own-xid bound logical-decoding slot creation waits on. Costs one xid
// of wraparound budget per cold start; needs no privilege.
const txidUpperBoundQuery = `SELECT pg_current_xact_id()::text::bigint`

// settleWaitQuery lists the still-running txids assigned BEFORE the
// captured upper bound ($1), from a FRESH snapshot: its xmin (PG keeps
// snapshot xmin = the oldest still-running txid, so it sees runners at
// or above xmax that the xip list misses) unioned with its xip members.
// An empty result is exactly "every pre-bound transaction has settled".
// The `/* sluice-anchor-settle-wait */` marker makes the wait
// observable in pg_stat_activity (tests key on it; operators see what
// cold-start is blocked on).
const settleWaitQuery = `SELECT /* sluice-anchor-settle-wait */ x FROM (
  SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint AS x
  UNION
  SELECT xip::text::bigint FROM pg_snapshot_xip(pg_current_snapshot()) AS xip
) running WHERE x < $1 ORDER BY x`

// captureSnapshotText exports the current snapshot's text form. For the
// CDC-handoff clamp it MUST run on the same pinned connection as
// [readChangeLogAnchor], after the REPEATABLE READ snapshot is
// established (any prior query in the tx does that), so the captured
// horizon is exactly what the bulk copy will and won't see.
func captureSnapshotText(ctx context.Context, q settleQuerier) (string, error) {
	var snap string
	if err := q.QueryRowContext(ctx, snapshotTextQuery).Scan(&snap); err != nil {
		return "", err
	}
	return snap, nil
}

// captureTxidUpperBound assigns and returns the settle wait's txid
// upper bound (see [txidUpperBoundQuery]). Run it on the POOL — never
// on the pinned snapshot connection — after the snapshot is
// established.
func captureTxidUpperBound(ctx context.Context, q settleQuerier) (int64, error) {
	var bound int64
	if err := q.QueryRowContext(ctx, txidUpperBoundQuery).Scan(&bound); err != nil {
		return 0, err
	}
	return bound, nil
}

// waitForPreSnapshotTxnsToSettle blocks until every transaction whose
// txid was assigned before upperBound has settled (committed or
// aborted), polling a FRESH snapshot each round — the caller's pinned
// snapshot connection can never observe them settling, by the very MVCC
// rules that make this wait necessary.
//
// GAP-FREEDOM INVARIANT (the snapshot→CDC handoff contract): with S the
// bulk copy's REPEATABLE READ snapshot, the CDC anchor must satisfy
//
//	anchor ≤ min{ id : the change-log row's allocating txn is not
//	              visible in S } − 1
//
// (equivalently: every change NOT reflected in the copy replays through
// `id > anchor`). Rows invisible in S split by the allocating txn's
// txid against the upper bound U (assigned just after S):
//
//   - txid < U — assigned before U, possibly in flight at snapshot time
//     with an already-allocated change-log id LOWER than visible ids.
//     This wait settles ALL of them; [minChangeLogIDForInvisibleTxns]
//     then clamps the anchor below their now-visible rows. Aborted ones
//     left only dead rows — permanent id gaps CDC never needs.
//   - txid ≥ U — first wrote after S, so their change-log ids were
//     allocated after S and exceed every id the anchor query or the
//     clamp can return (the id sequence is a plain BIGSERIAL, CACHE 1:
//     nextval is monotonic in allocation order). Safe by construction;
//     the steady-state [pollQuery] hold-back emits them in order once
//     they settle.
//
// The wait covers ALL pre-bound transactions, not just change-log
// writers: an uncommitted txn's writes are invisible (the same MVCC
// fact that creates the hole), so "did it write to the change log?" is
// unknowable before it settles. CREATE_REPLICATION_SLOT's
// all-running-txns wait is the slot-path precedent. A quiescent source
// settles on the FIRST poll — no interval sleep, no added cold-start
// latency beyond the poll round-trip.
//
// A wedged writer refuses LOUDLY at timeout, naming the stuck txids and
// the operator action, rather than hanging cold-start forever.
func waitForPreSnapshotTxnsToSettle(ctx context.Context, q settleQuerier, upperBound int64, timeout time.Duration) error {
	start := time.Now()
	var announced bool
	lastProgress := start
	timer := time.NewTimer(0) // poll immediately: the quiescent path exits without sleeping
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		remaining, err := querySettleTxids(ctx, q, settleWaitQuery, upperBound)
		if err != nil {
			return fmt.Errorf("settle-wait poll: %w", err)
		}
		if len(remaining) == 0 {
			if announced {
				slog.InfoContext(
					ctx, "pgtrigger: snapshot: in-flight source transactions settled",
					slog.Duration("waited", time.Since(start).Round(time.Millisecond)),
				)
			}
			return nil
		}
		if !announced {
			announced = true
			slog.InfoContext(
				ctx, "pgtrigger: snapshot: waiting for in-flight source transactions to settle before anchoring the CDC handoff (gap-freedom)",
				slog.Int("txns", len(remaining)),
				slog.Any("txids", remaining),
				slog.Duration("budget", timeout),
			)
		}
		if time.Since(start) >= timeout {
			return fmt.Errorf(
				"cold-start CDC anchor: %d source transaction(s) still in flight after %s (txids: %v) — a transaction open since before the bulk-copy snapshot may hold an unsettled change-log id below the anchor, and proceeding could silently gap its changes; commit or roll back the stuck transaction(s) (find them: SELECT pid, state, xact_start, query FROM pg_stat_activity WHERE xact_start IS NOT NULL ORDER BY xact_start; last resort: SELECT pg_terminate_backend(<pid>)), then re-run `sluice sync start`",
				len(remaining), timeout, remaining,
			)
		}
		if time.Since(lastProgress) >= anchorSettleProgressEvery {
			lastProgress = time.Now()
			slog.WarnContext(
				ctx, "pgtrigger: snapshot: still waiting for in-flight source transactions to settle",
				slog.Any("txids", remaining),
				slog.Duration("waited", time.Since(start).Round(time.Second)),
				slog.Duration("budget", timeout),
			)
		}
		timer.Reset(anchorSettlePollInterval)
	}
}

// minChangeLogIDForInvisibleTxns returns the lowest change-log id whose
// allocating transaction (txid < upperBound) is NOT visible in the bulk
// copy's snapshot (snapText, the [captureSnapshotText] export), or
// found=false when no such row exists. Run AFTER
// [waitForPreSnapshotTxnsToSettle] on a fresh (post-settle) snapshot so
// formerly in-flight committed rows are visible; the caller clamps the
// CDC anchor to min−1 so those rows replay. The txid column round-trips
// through its text form back into the xid8 domain for
// pg_visible_in_snapshot — never the row's 32-bit epoch-less xmin
// ([pollQuery]'s rationale).
func minChangeLogIDForInvisibleTxns(ctx context.Context, q settleQuerier, schema, snapText string, upperBound int64) (minID int64, found bool, err error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(ChangeLogTable)
	var got sql.NullInt64
	if err := q.QueryRowContext(
		ctx,
		"SELECT MIN(id) FROM "+tableRef+
			" WHERE txid < $1 AND NOT pg_visible_in_snapshot(txid::text::xid8, $2::pg_snapshot)",
		upperBound, snapText,
	).Scan(&got); err != nil {
		return 0, false, err
	}
	return got.Int64, got.Valid, nil
}

// querySettleTxids runs a query projecting a single bigint txid column
// and collects the results.
func querySettleTxids(ctx context.Context, q settleQuerier, query string, args ...any) ([]int64, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []int64
	for rows.Next() {
		var txid int64
		if err := rows.Scan(&txid); err != nil {
			return nil, err
		}
		out = append(out, txid)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
