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
//      (the shipping PG and MySQL engines both do; an unknown engine
//      would refuse loudly).
//
// Engagement constructs a [LeaseManager] keyed on the streamer's
// StreamID and stashes it on the Streamer for the Phase 2c SchemaSnap-
// shot routing to use. Refusal is loud: the operator's combination of
// flags + target engine is a misconfiguration, not a silent skip.

import (
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// engageShardCoordination constructs the LeaseManager on the Streamer
// when live-coordination conditions are met, or refuses loudly when
// the flags say "engage" but the target engine can't satisfy the
// contract.
//
// No-op (returns nil) when Shape A is not engaged or
// CoordinateLiveDDL is false (operator opted into the drained
// model). When live-coordination is engaged but the applier doesn't
// implement the lease store, refuses with a clear "engine X does not
// support live cross-shard DDL coordination" message naming
// `--no-coordinate-live-ddl` as the recovery flag.
//
// Called by the Streamer's [openApplier] AFTER applier construction
// and AFTER the Shape A support probe (checkShardColumnSupport).
func (s *Streamer) engageShardCoordination(applier ir.ChangeApplier) error {
	if !s.InjectShardColumn.Engaged() {
		return nil
	}
	if !s.CoordinateLiveDDL {
		return nil
	}
	store, ok := applier.(ir.ShardConsolidationLeaseStore)
	if !ok {
		engineName := ""
		if s.Target != nil {
			engineName = s.Target.Name()
		}
		return fmt.Errorf(
			"pipeline: target engine %q does not implement live cross-shard DDL coordination "+
				"(ADR-0054). Recovery: pass --no-coordinate-live-ddl to use the drained model "+
				"(stop every shard with 'sluice sync stop --wait', run one cross-shard schema "+
				"migrate, then resume every shard with 'sluice sync start --resume')",
			engineName,
		)
	}
	cfg := s.ShardCoordinationLease
	mgr, err := NewLeaseManager(store, s.StreamID, cfg)
	if err != nil {
		return fmt.Errorf("pipeline: engage shard consolidation: %w", err)
	}
	s.leaseMgr = mgr
	return nil
}

// ShardConsolidationLeaseManager returns the streamer's lease manager
// when live-coordination has been engaged, or nil otherwise. Exposed
// for tests and for the Phase 2d operator-status surface; production
// code reaches the manager via the Streamer's own SchemaSnapshot
// routing.
func (s *Streamer) ShardConsolidationLeaseManager() *LeaseManager {
	if s == nil {
		return nil
	}
	return s.leaseMgr
}
