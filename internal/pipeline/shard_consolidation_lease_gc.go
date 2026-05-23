// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// ADR-0054 Shape A Phase 2 — v0.76.0 closure: lease GC sweep (task #21).
//
// ADR-0054 ships v1 without GC. The
// sluice_shard_consolidation_lease control table accumulates one row
// per (consolidated target table) per recognized-shape DDL boundary —
// on a long-lived deployment, that's bounded by the cumulative
// distinct-DDL count, not by event volume, so the operational impact is
// modest. But unbounded growth is still a maintenance hazard the v1 ADR
// explicitly defers ("Out of scope"); this file closes that follow-up.
//
// # Safety semantics
//
// A row is safe to delete when BOTH:
//
//  1. State = APPLIED — the lease's applied_at IS NOT NULL, i.e. the
//     boundary's DDL has been durably recorded on the target. HELD or
//     EXPIRED rows are never deleted regardless of position (a HELD row
//     is by definition a peer's in-flight apply we must not race with;
//     an EXPIRED row is a takeover candidate the GC must not pre-empt).
//
//  2. Every stream in sluice_cdc_state has a persisted source_position
//     at-or-after the lease row's anchor_position under the engine's
//     [ir.PositionOrderer]. The "every stream" check INCLUDES streams
//     in the stop-requested state — their persisted position is still
//     authoritative for "where would this stream resume from?", so a
//     paused stream's position is still a constraint on GC.
//
// Rows that lack an anchor (v0.75.0 legacy rows that pre-date the
// additive `anchor_position` migration) are defensively retained — the
// sweeper has no way to compute their safety and the loud-failure tenet
// rules out guessing.
//
// # Engine boundary
//
// The sweeper takes three engine surfaces it dispatches through:
//
//   - [ir.ShardConsolidationLeaseLister] enumerates every row.
//   - [ir.ShardConsolidationLeaseDeleter] removes one row by primary key.
//   - The applier's [ir.ChangeApplier] (cast to ListStreams) enumerates
//     every persisted stream's position.
//
// Engines that don't implement the deleter surface inherit the no-GC
// default — sluice is robust to a missing surface because the engine's
// type-assertion at engagement time returns nil, and the sweeper's
// caller in heartbeatLoop is guarded by the same check.
//
// # Loud-failure tenet
//
// GC errors are LOGGED at WARN but DO NOT propagate up to the
// heartbeat caller. Lease GC failure (e.g. transient DB error reading
// a stream's position) must not crash an otherwise-healthy stream;
// retention is a maintenance operation, not a correctness one. The
// sweeper returns its accumulated error so callers that explicitly
// want to observe it can — the heartbeat-piggyback path discards.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/orware/sluice/internal/ir"
)

// LeaseGCDeps bundles the engine surfaces the sweeper dispatches
// through. Keeping them on a small struct keeps the public function
// signature legible and makes mock-injection trivial in tests.
type LeaseGCDeps struct {
	// Lister enumerates every row in sluice_shard_consolidation_lease.
	Lister ir.ShardConsolidationLeaseLister

	// Deleter removes one row by TargetTableFullName. Engines that
	// don't implement the deleter surface make this nil, in which case
	// [SweepConsolidationLeases] returns a no-op with a debug log.
	Deleter ir.ShardConsolidationLeaseDeleter

	// PosReader enumerates every persisted stream's source position
	// (via [ir.ChangeApplier.ListStreams]). The reader is the same
	// ChangeApplier the lease store + deleter live on; we keep the
	// surface separate so unit tests can inject a fake without
	// constructing a full engine.
	PosReader ir.ChangeApplier

	// Orderer compares two positions under the engine's partial-order
	// semantics. Required — the safety check is a position-comparison.
	Orderer ir.PositionOrderer
}

// SweepConsolidationLeases runs one lease GC sweep against deps. Returns
// the number of rows deleted plus an accumulated error (joined across
// all per-row failures) — callers that want best-effort GC discard the
// error, callers that want to observe it inspect both return values.
//
// Per-row failures are non-fatal: the sweeper moves on to the next row.
// Top-level failures (lister error, stream-list error) DO short-circuit
// — we can't safely classify any row when the inputs are themselves
// unknown.
//
// Safe to call from multiple goroutines concurrently (each call
// computes its own snapshot of streams + leases and deletes only rows
// it owns the safety classification for; concurrent DELETEs of the
// same primary key are idempotent — DELETE on a missing row is a
// no-op on both PG and MySQL).
func SweepConsolidationLeases(ctx context.Context, deps LeaseGCDeps) (deleted int, err error) {
	if deps.Lister == nil {
		return 0, errors.New("pipeline: sweep consolidation leases: lister is nil")
	}
	if deps.PosReader == nil {
		return 0, errors.New("pipeline: sweep consolidation leases: position reader is nil")
	}
	if deps.Orderer == nil {
		return 0, errors.New("pipeline: sweep consolidation leases: orderer is nil " +
			"(engine does not implement ir.PositionOrderer; loud-failure tenet)")
	}
	if deps.Deleter == nil {
		// Engine without the deleter surface — sweep no-op. Log at
		// DEBUG because this is a routine "engine doesn't support GC"
		// path rather than a misconfiguration.
		slog.DebugContext(ctx,
			"shard consolidation lease GC: engine has no deleter; skipping")
		return 0, nil
	}

	leases, err := deps.Lister.ListLeases(ctx)
	if err != nil {
		return 0, fmt.Errorf("pipeline: sweep consolidation leases: list leases: %w", err)
	}
	if len(leases) == 0 {
		return 0, nil
	}

	streams, err := deps.PosReader.ListStreams(ctx)
	if err != nil {
		return 0, fmt.Errorf("pipeline: sweep consolidation leases: list streams: %w", err)
	}
	// No streams = no safety constraint on the second condition — but
	// that also means no stream is consuming events past these
	// boundaries, so DELETE-ing the rows would still be correct
	// post-hoc. However: an operator-facing dry-run / inspection
	// path could legitimately hit "no streams" against a target that
	// previously had streams (e.g. ClearStream was used). The
	// conservative call is to do NOTHING — preserve the rows until at
	// least one stream re-attaches and provides a real constraint
	// boundary. This matches the loud-failure tenet's "when in doubt,
	// don't delete" framing.
	if len(streams) == 0 {
		slog.DebugContext(ctx,
			"shard consolidation lease GC: no streams; skipping (conservative)")
		return 0, nil
	}

	var errs []error
	for _, lease := range leases {
		ok, why := canGCLease(lease, streams, deps.Orderer)
		if !ok {
			slog.DebugContext(
				ctx,
				"shard consolidation lease GC: retain",
				"table", lease.TargetTableFullName,
				"reason", why,
			)
			continue
		}
		if delErr := deps.Deleter.DeleteLease(ctx, lease.TargetTableFullName); delErr != nil {
			// Per-row failure: log + accumulate, keep going. Don't
			// abort the sweep — the next row might succeed.
			slog.WarnContext(
				ctx,
				"shard consolidation lease GC: delete failed",
				"table", lease.TargetTableFullName,
				"error", delErr,
			)
			errs = append(errs, fmt.Errorf("delete %q: %w", lease.TargetTableFullName, delErr))
			continue
		}
		deleted++
		slog.DebugContext(
			ctx,
			"shard consolidation lease GC: deleted",
			"table", lease.TargetTableFullName,
		)
	}

	if deleted > 0 {
		slog.InfoContext(
			ctx,
			"shard consolidation lease GC swept",
			"deleted", deleted,
			"retained", len(leases)-deleted,
		)
	}
	if len(errs) > 0 {
		return deleted, errors.Join(errs...)
	}
	return deleted, nil
}

// canGCLease returns (true, "") when a row is safe to delete per the
// two-condition rule, and (false, reason) otherwise. The reason string
// is for DEBUG logging — it documents which condition retained the
// row so an operator inspecting the log can see why GC retained
// what.
func canGCLease(lease ir.ShardConsolidationLeaseRow, streams []ir.StreamStatus, orderer ir.PositionOrderer) (eligible bool, reason string) {
	if !lease.HasAppliedAt {
		// Condition 1: not yet APPLIED (HELD or EXPIRED) — never GC.
		return false, "not applied"
	}
	if !lease.HasAnchor {
		// Defensive retention of legacy v0.75.0 rows: no anchor to
		// compare against. The next time a stream observes a boundary
		// on this table, the post-v0.76.0 path will rewrite the row
		// with an anchor (via FinalizeLeaseApply) and a later sweep
		// can act on it. Until then, retain.
		return false, "no anchor (legacy row)"
	}
	// Condition 2: every stream past the anchor under the engine's
	// position partial-order. "Past" = the stream's persisted source
	// position is at-or-after the lease's anchor_position.
	for _, s := range streams {
		atOrAfter, err := orderer.PositionAtOrAfter(s.Position, lease.AnchorPosition)
		if err != nil {
			// Position parse error — retain defensively. Comparison
			// errors are themselves a Bug-74-class silent-loss hazard
			// (a malformed position could mis-classify a row as
			// safe-to-delete); the loud-failure tenet wins.
			return false, fmt.Sprintf("orderer error on stream %q: %v", s.StreamID, err)
		}
		if !atOrAfter {
			return false, fmt.Sprintf("stream %q has not reached anchor", s.StreamID)
		}
	}
	return true, ""
}
