// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// ADR-0054 Shape A Phase 2 — streamer-side live-coordination
// engagement.
//
// The Streamer engages live cross-shard DDL coordination when ALL of
// the following hold:
//
//   1. The operator has set --inject-shard-column NAME=VALUE
//      (ShardColumnSpec.Engaged()).
//   2. The operator has NOT passed --no-coordinate-live-ddl
//      (Streamer.CoordinateLiveDDL == true).
//   3. The target applier implements [ir.ShardConsolidationLeaseStore]
//      AND [ir.ShardConsolidationProber] (the shipping PG and MySQL
//      engines both do; an unknown engine refuses loudly).
//
// Engagement constructs a [LeaseManager] keyed on the streamer's
// StreamID, opens a SchemaWriter for the per-shape DDL apply path,
// and constructs a [BoundaryRouter] that ties the three together.
// The streamer's SchemaSnapshot intercept (Phase 2d) consumes the
// router on each observed DDL boundary.

import (
	"context"
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// engageShardCoordination constructs the LeaseManager + BoundaryRouter
// on the Streamer when live-coordination conditions are met, or
// refuses loudly when the flags say "engage" but the target engine
// can't satisfy the contract.
//
// No-op (returns nil) when Shape A is not engaged or
// CoordinateLiveDDL is false. When live-coordination is engaged but
// the applier doesn't implement the lease store / prober, refuses
// with a clear "engine X does not support live cross-shard DDL
// coordination" message naming `--no-coordinate-live-ddl` as the
// recovery flag.
//
// Called by [Streamer.openApplier] AFTER applier construction and
// AFTER the Shape A support probe (checkShardColumnSupport).
func (s *Streamer) engageShardCoordination(ctx context.Context, applier ir.ChangeApplier) error {
	if !s.InjectShardColumn.Engaged() {
		return nil
	}
	if !s.CoordinateLiveDDL {
		return nil
	}
	store, ok := applier.(ir.ShardConsolidationLeaseStore)
	if !ok {
		return s.refuseEngineMissingCoordination("lease store")
	}
	prober, ok := applier.(ir.ShardConsolidationProber)
	if !ok {
		return s.refuseEngineMissingCoordination("probe surface")
	}
	cfg := s.ShardCoordinationLease
	mgr, err := NewLeaseManager(store, s.StreamID, cfg)
	if err != nil {
		return fmt.Errorf("pipeline: engage shard consolidation: %w", err)
	}
	s.leaseMgr = mgr

	// Bug 85.b fix: wire the v0.76.0 lease GC sweep into the LeaseManager
	// so its heartbeat loop's GC-trigger guard sees non-nil gcDeps.
	//
	// Three surfaces, TWO sources:
	//   - Lister + Deleter live on the applier (ChangeApplier interface
	//     extensions, implemented by PG + MySQL applier structs).
	//   - PositionOrderer lives on the SOURCE ENGINE
	//     (e.g. internal/engines/postgres/position_orderer.go:33's
	//     `func (Engine) PositionAtOrAfter(...)` — it's a method on
	//     the engine factory value, NOT on *ChangeApplier).
	//
	// v0.77.0's fix attempt (Bug 85.a) wrongly type-asserted the
	// orderer on `applier`, which silently failed on every real
	// engine — gcDeps stayed nil, sweep stayed dead. Bug 85.b: assert
	// on s.Source, not applier.
	//
	// Engines that don't implement any of the three surfaces inherit
	// the no-GC default — the sweep is a maintenance op, never
	// load-bearing on the apply path.
	if lister, ok := applier.(ir.ShardConsolidationLeaseLister); ok {
		if deleter, ok := applier.(ir.ShardConsolidationLeaseDeleter); ok {
			if orderer, ok := s.Source.(ir.PositionOrderer); ok {
				mgr.WithGC(&LeaseGCDeps{
					Lister:    lister,
					Deleter:   deleter,
					PosReader: applier,
					Orderer:   orderer,
				})
			}
		}
	}

	// Open a SchemaWriter for the per-shape DDL apply path. The
	// writer's lifetime is owned by the Streamer; closed via
	// closeShardCoordination on Run exit.
	if s.Target == nil {
		return fmt.Errorf("pipeline: engage shard consolidation: nil target engine")
	}
	sw, err := s.Target.OpenSchemaWriter(ctx, s.TargetDSN)
	if err != nil {
		return fmt.Errorf("pipeline: engage shard consolidation: open schema writer: %w", err)
	}
	shapeApplier, ok := sw.(ir.ShapeDeltaApplier)
	if !ok {
		_ = closeIfErrIgnored(sw)
		return s.refuseEngineMissingCoordination("shape delta applier")
	}
	// Honor --target-schema if set so DDL emits to the right namespace.
	if s.TargetSchema != "" {
		if setter, ok := sw.(ir.SchemaSetter); ok {
			setter.SetSchema(s.TargetSchema)
		}
	}
	s.shapeWriter = sw

	router, err := NewBoundaryRouter(mgr, shapeApplier, prober)
	if err != nil {
		_ = closeIfErrIgnored(sw)
		return fmt.Errorf("pipeline: engage shard consolidation: %w", err)
	}
	s.boundaryRouter = router
	return nil
}

// refuseEngineMissingCoordination is the shared shape of the
// "engine doesn't implement X" refusal — names the missing surface +
// the recovery flag.
func (s *Streamer) refuseEngineMissingCoordination(missingSurface string) error {
	engineName := ""
	if s.Target != nil {
		engineName = s.Target.Name()
	}
	return fmt.Errorf(
		"pipeline: target engine %q does not implement live cross-shard DDL coordination "+
			"(missing %s, ADR-0054). Recovery: pass --no-coordinate-live-ddl to use the drained "+
			"model (stop every shard with 'sluice sync stop --wait', run one cross-shard schema "+
			"migrate, then resume every shard with 'sluice sync start --resume')",
		engineName, missingSurface,
	)
}

// closeShardCoordination releases the SchemaWriter opened by
// engageShardCoordination. Idempotent — safe to call on streams that
// never engaged.
func (s *Streamer) closeShardCoordination() {
	if s == nil {
		return
	}
	if s.shapeWriter != nil {
		_ = closeIfErrIgnored(s.shapeWriter)
		s.shapeWriter = nil
	}
	s.boundaryRouter = nil
	s.leaseMgr = nil
}

// closeIfErrIgnored is a tiny helper that closes anything implementing
// Close() error and swallows the error (defer-friendly cleanup path).
// Mirrors the existing closeIf helper without colliding on signature.
func closeIfErrIgnored(v any) error {
	if c, ok := v.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

// ShardConsolidationLeaseManager returns the streamer's lease manager
// when live-coordination has been engaged, or nil otherwise. Exposed
// for tests and for the operator-status surface.
func (s *Streamer) ShardConsolidationLeaseManager() *LeaseManager {
	if s == nil {
		return nil
	}
	return s.leaseMgr
}

// ShardConsolidationBoundaryRouter returns the streamer's boundary
// router when live-coordination has been engaged, or nil otherwise.
// Exposed for tests.
func (s *Streamer) ShardConsolidationBoundaryRouter() *BoundaryRouter {
	if s == nil {
		return nil
	}
	return s.boundaryRouter
}
