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

	// sourceEngine / targetEngine name the engine pair the router
	// forwards between. Every boundary's post IR is the SOURCE's CDC
	// projection — the source's column dialect and the source's
	// namespace (a MySQL database name, a PG schema) — and the target
	// SchemaWriter / prober cannot consume that directly: qualifyTable
	// honours Table.Schema over its own bound schema, so an unscrubbed
	// post lands the DDL in a namespace named after the SOURCE (Bug 262:
	// `schema "source_db" does not exist` on every forwarded DDL of a
	// MySQL→PG Shape A stream). The router runs [retargetShapeForTarget]
	// — the same resolver the single-stream forwarder and chain restore
	// use — before the apply and before the takeover probe, so the DDL
	// resolves through the writer's bound namespace exactly as the cold-
	// start CREATE and the row applier's appliershared.Schema do.
	sourceEngine string
	targetEngine string

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
// manager + per-shape applier + prober for the named engine pair
// ([ir.Engine.Name] of the source and of the target — the same pair the
// single-stream forwarder's schemaForwardDeps carries). Returns an
// error if any dependency is nil — these are non-optional for live
// coordination. A same-engine pair is a type pass-through; the
// namespace scrub applies to every pair.
//
// observeTimeout defaults to 2 × LeaseDuration when zero — the same
// observer-wait cap ADR-0054 §3 recommends. observePollInterval
// defaults to 2 seconds.
func NewBoundaryRouter(mgr *LeaseManager, applier ir.ShapeDeltaApplier, prober ShardConsolidationProber, sourceEngine, targetEngine string) (*BoundaryRouter, error) {
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
		sourceEngine:        sourceEngine,
		targetEngine:        targetEngine,
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
	// SLM-1: the session-zone cast door, BEFORE the lease is touched. Shape
	// A forwards ALTER COLUMN TYPE regardless of --schema-changes, and the
	// first boundary it classifies against the cold-start seed is one the
	// reader may have had no prior for; this door is what stands between
	// that boundary and applyShape on every path out of this function.
	if err := refuseSessionZoneSwap(tableName, shape, RecoveryHint(tableName)); err != nil {
		return fmt.Errorf("pipeline: route boundary: %w", err)
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
//
// post is the boundary's comparison-form IR (source dialect, source
// namespace). It is resolved to the target's shape ONCE here, and every
// path out of this function — the held-lease apply, the takeover probe,
// the takeover re-apply — consumes the resolved table, so the three
// arms cannot diverge on which namespace they address (Bug 262).
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

	target, targetShape := retargetShapeForTarget(post, shape, r.sourceEngine, r.targetEngine)

	if !lease.Takeover() {
		// Normal lease-holder path: apply the shape, then finalize.
		if err := r.applyShape(ctx, target, targetShape); err != nil {
			return fmt.Errorf("pipeline: route boundary: apply shape %s: %w", shape.Kind, err)
		}
		return r.mgr.Apply(ctx, lease, schemaVersion, ddlText, checksum, anchor)
	}

	// Takeover path: probe the target schema for the prior holder's
	// recorded effect. Three outcomes per ADR-0054 §4. The probe reads
	// the target catalog through the same resolved namespace the apply
	// writes to — the engine probers mirror qualifyTable's Table.Schema
	// precedence, so an unscrubbed post would probe the SOURCE's name.
	outcome, err := DispatchProbe(ctx, r.prober, target, targetShape)
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
		if err := r.applyShape(ctx, target, targetShape); err != nil {
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
// ir.ShapeDeltaApplier. Thin method wrapper over the shared
// [applyShapeDelta] free function so the Shape A boundary router and
// the single-stream ADR-0091 forwarding intercept use the identical
// proven dispatch. target / shape are the [retargetShapeForTarget]
// outputs — handleHeldLease resolves them once per boundary.
func (r *BoundaryRouter) applyShape(ctx context.Context, target *ir.Table, shape Shape) error {
	return applyShapeDelta(ctx, r.applier, target, shape)
}

// applyShapeDelta maps a classified [Shape] to the matching
// [ir.ShapeDeltaApplier] method against post. It is the single source
// of truth for per-shape DDL dispatch, shared by the Shape A boundary
// router (ADR-0054) and the single-stream forwarding intercept
// (ADR-0091). All branches are catalog-only DDL and idempotent on the
// post-state (the applier methods use IF [NOT] EXISTS / detect-then-
// emit), so a retry that replays the boundary is safe.
//
// post must already be resolved to the target engine's shape through
// [retargetShapeForTarget] — dialect retargeted, Schema scrubbed so the
// target SchemaWriter's qualifyTable falls back to its bound namespace.
// Both live callers do this (the single-stream forwarder in
// applyShapeForward; the Shape A router in handleHeldLease). This
// precondition used to be documented as already holding for Shape A
// because its tables were "manifest-derived" — they never were: the
// router's post is the raw CDC projection, and that false premise was
// Bug 262. TestRouteBoundary_Bug262_EveryShapeAndArmResolvesToTheTarget
// is the gate on the router side.
//
// ShapeKindRenameColumn is dispatched here for the Shape A path (whose
// lease + catalog make the rename unambiguous); the single-stream
// caller refuses rename BEFORE reaching this function (ADR-0091 §3 —
// it cannot prove rename-vs-drop+add without a stable column id).
func applyShapeDelta(ctx context.Context, applier ir.ShapeDeltaApplier, post *ir.Table, shape Shape) error {
	switch shape.Kind {
	case ShapeKindAddColumn:
		return applier.AlterAddColumn(ctx, post, shape.AddedColumns)
	case ShapeKindDropColumn:
		return applier.AlterDropColumn(ctx, post, shape.DroppedColumns)
	case ShapeKindCreateIndex:
		return applier.CreateShapeIndex(ctx, post, shape.CreatedIndexes)
	case ShapeKindDropIndex:
		return applier.DropShapeIndex(ctx, post, shape.DroppedIndexes)
	case ShapeKindAlterColumnType:
		return applier.AlterColumnType(ctx, post, shape.AlteredColumn)
	case ShapeKindAlterColumnNullability:
		return applier.AlterColumnNullability(ctx, post, shape.AlteredColumn)
	case ShapeKindRenameColumn:
		if shape.RenamedColumnBefore == nil || shape.RenamedColumnAfter == nil {
			return errors.New("pipeline: apply shape: rename-column shape missing before/after column")
		}
		return applier.AlterRenameColumn(ctx, post, shape.RenamedColumnBefore.Name, shape.RenamedColumnAfter.Name)
	case ShapeKindAddCheck:
		return applier.AlterAddCheck(ctx, post, shape.AddedChecks)
	case ShapeKindDropCheck:
		return applier.AlterDropCheck(ctx, post, shape.DroppedChecks)
	case ShapeKindModifyCheck:
		if shape.ModifiedCheckBefore == nil || shape.ModifiedCheckAfter == nil {
			return errors.New("pipeline: apply shape: modify-check shape missing before/after constraint")
		}
		return applier.AlterModifyCheck(ctx, post, shape.ModifiedCheckBefore, shape.ModifiedCheckAfter)
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
