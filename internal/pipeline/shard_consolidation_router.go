// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// ADR-0054 Shape A Phase 2c — boundary router.
//
// The router glues the lease primitive (state machine + heartbeat) to
// the IR-delta classifier + per-shape applier + per-shape probe. For
// each observed SchemaSnapshot boundary on each per-shard stream:
//
//   1. The router computes the IR delta (pre, post) for the affected
//      table and asks ClassifyShape for the recognized-shape verdict.
//   2. For an unrecognized shape, refuses loudly with the drained-
//      model recovery hint (loud-failure tenet).
//   3. For ShapeKindNone (no structural change), records a no-op
//      boundary and returns.
//   4. Otherwise calls LeaseManager.Acquire(table, ddl-text):
//      a. On Acquire success WITHOUT takeover → apply the shape via
//         ir.ShapeDeltaApplier, then LeaseManager.Apply.
//      b. On Acquire success WITH takeover → dispatch the probe; if
//         Applied, LeaseManager.Apply (record only); if NotApplied,
//         apply the shape + LeaseManager.Apply; if Inconsistent,
//         refuse loudly.
//      c. On Acquire contention (peer holds the lease) → observe-
//         until-applied loop with a checksum-mismatch refusal on
//         divergent peer DDL.
//
// Caller: ADR-0054 Phase 2d wires this into the streamer's
// SchemaSnapshot dispatch path (one BoundaryRouter call per snapshot
// per stream). The router itself owns no state — it's a pure-function
// orchestration over LeaseManager + ShapeDeltaApplier + Prober.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// BoundaryRouter coordinates a single SchemaSnapshot boundary's
// lease-and-apply against a consolidated target table. One router
// instance per Streamer; methods are safe to call sequentially per
// snapshot (the source CDC reader emits snapshots one at a time per
// table).
type BoundaryRouter struct {
	mgr     *LeaseManager
	applier ir.ShapeDeltaApplier
	prober  ShardConsolidationProber

	// observePollInterval controls how often the observer loop polls
	// the lease row when a peer holds the lease. Default 2 seconds;
	// tests can shrink it via NewBoundaryRouter's option-arg path
	// (today we keep it as a private field for forward-compat).
	observePollInterval time.Duration

	// observeTimeout is the upper bound on the observer-wait. After
	// this, refuse loudly — a peer stream stuck holding the lease is
	// an operator-actionable condition (the peer may have crashed
	// with the lease still HELD, in which case the takeover path will
	// pick up via the lease's TTL — but we don't block this stream
	// forever waiting for that to happen).
	observeTimeout time.Duration
}

// NewBoundaryRouter constructs a router around the supplied lease
// manager + per-shape applier + prober. Returns an error if any
// dependency is nil — these are non-optional for live coordination.
//
// observeTimeout defaults to 2 × LeaseDuration when zero — the same
// observer-wait cap ADR-0054 §3 recommends. observePollInterval
// defaults to 2 seconds.
func NewBoundaryRouter(mgr *LeaseManager, applier ir.ShapeDeltaApplier, prober ShardConsolidationProber) (*BoundaryRouter, error) {
	if mgr == nil {
		return nil, errors.New("pipeline: NewBoundaryRouter: lease manager is nil")
	}
	if applier == nil {
		return nil, errors.New("pipeline: NewBoundaryRouter: shape applier is nil")
	}
	if prober == nil {
		return nil, errors.New("pipeline: NewBoundaryRouter: prober is nil")
	}
	return &BoundaryRouter{
		mgr:                 mgr,
		applier:             applier,
		prober:              prober,
		observePollInterval: 2 * time.Second,
		observeTimeout:      2 * mgr.cfg.LeaseDuration,
	}, nil
}

// RouteBoundary handles a single observed DDL boundary for tableName.
// (pre, post) are the affected table's IR schema before and after the
// DDL. ddlText is a human-readable rendering of the shape (used for
// the lease's ddl_text + checksum); the router computes the checksum
// internally via [ChecksumDDLText].
//
// schemaVersion is the boundary's monotonically-increasing version
// number from ADR-0049's schema-history (the value that ends up in
// the lease's applied_schema_version field).
//
// Returns nil on success (whether this stream applied the DDL itself
// or observed a peer's apply). Returns a wrapped error on:
//
//   - Unrecognized shape (ShapeKindUnrecognized) → refuse loudly.
//   - Probe outcome Inconsistent on takeover → refuse loudly.
//   - DDL-checksum mismatch on peer observation → refuse loudly
//     ([ErrLeaseChecksumMismatch]).
//   - Apply error from the engine's ShapeDeltaApplier → propagate.
//   - Observer timeout (peer never finalized within observeTimeout).
//
// anchor is the source-side CDC position at which this boundary's DDL
// was observed (the SchemaSnapshot's Position). Persisted into the
// lease row on Apply so the v0.76.0 lease GC sweep (task #21) can
// compare against every stream's persisted position. A zero-value
// Position is permitted (callers without CDC context); the row stores
// NULL and the GC sweep defensively retains it.
func (r *BoundaryRouter) RouteBoundary(
	ctx context.Context,
	tableName string,
	pre, post *ir.Table,
	ddlText string,
	schemaVersion int64,
	anchor ir.Position,
) error {
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		return fmt.Errorf("pipeline: route boundary: %w. %s", err, RecoveryHint(tableName))
	}
	if shape.Kind == ShapeKindNone {
		slog.DebugContext(
			ctx, "shard consolidation boundary: no-op (no structural change)",
			"table", tableName,
			"stream_id", r.mgr.streamID,
		)
		return nil
	}

	checksum := ChecksumDDLText(ddlText)

	lease, err := r.mgr.Acquire(ctx, tableName, ddlText)
	switch {
	case err == nil:
		return r.handleHeldLease(ctx, lease, post, shape, ddlText, checksum, schemaVersion, anchor)
	case errors.Is(err, ErrLeaseContended):
		// A peer holds the lease (HELD) or has already finalized
		// (APPLIED). Observe until APPLIED, verify checksum, return.
		return r.observeUntilApplied(ctx, tableName, checksum, ddlText)
	default:
		return fmt.Errorf("pipeline: route boundary: acquire: %w", err)
	}
}

// handleHeldLease runs when this stream successfully acquired the
// lease (either ABSENT → HELD or EXPIRED takeover → HELD). It
// dispatches the right apply/probe path based on the takeover flag.
func (r *BoundaryRouter) handleHeldLease(
	ctx context.Context,
	lease *Lease,
	post *ir.Table,
	shape Shape,
	ddlText, checksum string,
	schemaVersion int64,
	anchor ir.Position,
) (retErr error) {
	defer func() {
		if retErr != nil {
			// On any error, release the lease so it expires on TTL
			// rather than getting renewed indefinitely. The takeover
			// stream's probe will reconcile.
			r.mgr.Release(ctx, lease)
		}
	}()

	if !lease.Takeover() {
		// Normal lease-holder path: apply the shape, then finalize.
		if err := r.applyShape(ctx, post, shape); err != nil {
			return fmt.Errorf("pipeline: route boundary: apply shape %s: %w", shape.Kind, err)
		}
		return r.mgr.Apply(ctx, lease, schemaVersion, ddlText, checksum, anchor)
	}

	// Takeover path: probe the target schema for the prior holder's
	// recorded effect. Three outcomes per ADR-0054 §4.
	outcome, err := DispatchProbe(ctx, r.prober, post, shape)
	if err != nil {
		return fmt.Errorf("pipeline: route boundary: probe %s: %w. %s",
			shape.Kind, err, RecoveryHint(lease.tableName))
	}
	switch outcome {
	case ProbeOutcomeApplied:
		// Prior holder's ALTER landed — just record the finalize.
		slog.InfoContext(
			ctx, "shard consolidation takeover: probe Applied (record-only)",
			"table", lease.tableName,
			"stream_id", r.mgr.streamID,
			"shape", shape.Kind.String(),
		)
		return r.mgr.Apply(ctx, lease, schemaVersion, ddlText, checksum, anchor)
	case ProbeOutcomeNotApplied:
		// Prior holder crashed before applying — re-apply.
		slog.InfoContext(
			ctx, "shard consolidation takeover: probe NotApplied (re-applying)",
			"table", lease.tableName,
			"stream_id", r.mgr.streamID,
			"shape", shape.Kind.String(),
		)
		if err := r.applyShape(ctx, post, shape); err != nil {
			return fmt.Errorf("pipeline: route boundary: takeover apply shape %s: %w", shape.Kind, err)
		}
		return r.mgr.Apply(ctx, lease, schemaVersion, ddlText, checksum, anchor)
	case ProbeOutcomeInconsistent:
		return fmt.Errorf(
			"pipeline: route boundary: takeover probe Inconsistent for %s on %q — "+
				"target schema is in a partial state inconsistent with the recorded shape. %s",
			shape.Kind, lease.tableName, RecoveryHint(lease.tableName),
		)
	}
	return fmt.Errorf("pipeline: route boundary: unknown probe outcome %v", outcome)
}

// applyShape dispatches the IR-delta-derived shape to the engine's
// ir.ShapeDeltaApplier. Each branch maps Shape.Kind to the matching
// engine method.
func (r *BoundaryRouter) applyShape(ctx context.Context, post *ir.Table, shape Shape) error {
	switch shape.Kind {
	case ShapeKindAddColumn:
		return r.applier.AlterAddColumn(ctx, post, shape.AddedColumns)
	case ShapeKindDropColumn:
		return r.applier.AlterDropColumn(ctx, post, shape.DroppedColumns)
	case ShapeKindCreateIndex:
		return r.applier.CreateShapeIndex(ctx, post, shape.CreatedIndexes)
	case ShapeKindDropIndex:
		return r.applier.DropShapeIndex(ctx, post, shape.DroppedIndexes)
	case ShapeKindAlterColumnType:
		return r.applier.AlterColumnType(ctx, post, shape.AlteredColumn)
	case ShapeKindAlterColumnNullability:
		return r.applier.AlterColumnNullability(ctx, post, shape.AlteredColumn)
	case ShapeKindRenameColumn:
		if shape.RenamedColumnBefore == nil || shape.RenamedColumnAfter == nil {
			return errors.New("pipeline: apply shape: rename-column shape missing before/after column")
		}
		return r.applier.AlterRenameColumn(ctx, post, shape.RenamedColumnBefore.Name, shape.RenamedColumnAfter.Name)
	case ShapeKindAddCheck:
		return r.applier.AlterAddCheck(ctx, post, shape.AddedChecks)
	case ShapeKindDropCheck:
		return r.applier.AlterDropCheck(ctx, post, shape.DroppedChecks)
	case ShapeKindModifyCheck:
		if shape.ModifiedCheckBefore == nil || shape.ModifiedCheckAfter == nil {
			return errors.New("pipeline: apply shape: modify-check shape missing before/after constraint")
		}
		return r.applier.AlterModifyCheck(ctx, post, shape.ModifiedCheckBefore, shape.ModifiedCheckAfter)
	case ShapeKindNone:
		return nil
	}
	return fmt.Errorf("pipeline: apply shape: unrecognized shape %v", shape.Kind)
}

// observeUntilApplied polls the lease row until the holder finalizes
// (APPLIED), then verifies the recorded checksum matches this peer's
// own. On match → return nil (this stream advances its schema-version
// cursor and continues CDC against the migrated target). On mismatch
// → return ErrLeaseChecksumMismatch with diagnostic detail.
//
// Times out after observeTimeout — the lease's TTL means a crashed
// holder eventually expires and another stream takes over, but THIS
// stream isn't going to block on that forever. The operator's
// recovery flow (drained model) is the loud-failure path.
func (r *BoundaryRouter) observeUntilApplied(ctx context.Context, tableName, ourChecksum, ourDDLText string) error {
	deadline := time.Now().Add(r.observeTimeout)
	for {
		obs, err := r.mgr.Observe(ctx, tableName)
		if err != nil {
			return fmt.Errorf("pipeline: observe lease %q: %w", tableName, err)
		}
		switch obs.State {
		case LeaseStateApplied:
			if obs.DDLChecksum == ourChecksum {
				slog.InfoContext(
					ctx, "shard consolidation peer-applied (checksum match)",
					"table", tableName,
					"stream_id", r.mgr.streamID,
					"holder", obs.HolderStreamID,
					"version", obs.AppliedSchemaVersion,
				)
				return nil
			}
			return fmt.Errorf(
				"%w: peer holder %q applied DDL with checksum %q; this stream observed checksum %q. "+
					"Recorded DDL: %q. Our DDL: %q. %s",
				ErrLeaseChecksumMismatch,
				obs.HolderStreamID, obs.DDLChecksum, ourChecksum,
				obs.DDLText, ourDDLText, RecoveryHint(tableName),
			)
		case LeaseStateAbsent, LeaseStateHeld, LeaseStateExpired:
			// Wait + retry.
		}
		if time.Now().After(deadline) {
			return fmt.Errorf(
				"pipeline: observe lease %q timed out after %s (last state %s, holder %q). %s",
				tableName, r.observeTimeout, obs.State, obs.HolderStreamID, RecoveryHint(tableName),
			)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.observePollInterval):
		}
	}
}
