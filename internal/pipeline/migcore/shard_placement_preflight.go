// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// ShardPlacementProber is the OPTIONAL target-side surface for the
// shard-placement preflight. A target that can be sharded implements it and
// reports whether any in-scope table's ROUTING disagrees with where its rows
// physically ARE.
//
// Only the PostgreSQL writer implements it today, and only against a
// PlanetScale Neki endpoint; every other target either does not implement the
// surface or reports nothing, and [PreflightShardPlacement] is then a no-op.
type ShardPlacementProber interface {
	// ShardPlacementMismatch returns the name of the first table whose
	// routing and placement disagree, or "" when none do. An error means the
	// probe could not RUN and must never be read as a mismatch.
	ShardPlacementMismatch(ctx context.Context, tables []*ir.Table) (mismatched string, err error)
}

// PreflightShardPlacement refuses a target whose sharded tables route to a
// different place than their rows live.
//
// # Why this is a refusal
//
// In that state the target's PRIMARY KEY is not globally enforced. sluice's
// CDC applier and its idempotent bulk-copy writer both write with
// `INSERT … ON CONFLICT (pk) DO UPDATE`; the conflict is looked for on the
// shard the write ROUTES to, which does not hold the original row, so the
// statement INSERTS. Every replayed change adds a duplicate, at exit 0,
// against a constraint the operator believes is enforced. That is the
// silent-divergence class, which the project's tenets rank above everything
// else — so it stops the run rather than warning.
//
// Measured on a live PlanetScale Neki cluster on 2026-09-10 and written up at
// C:\code\neki-issues as NEKI-006, including the side-by-side against a table
// sharded through the supported reshard workflow, which behaves correctly.
//
// # What it does NOT cover, stated rather than implied
//
//   - A target that is not sharded, or not Neki: no-op.
//   - An EMPTY table: nothing can be mis-placed, because rows sluice writes
//     go to the routed shard by construction. A fresh cold start into an
//     empty database is therefore uncovered and does not need covering; the
//     hazard needs a PRE-EXISTING populated table (a populated target under
//     --force-cold-start, a warm resume, or a sync against a database
//     somebody sharded underneath).
//   - A table with no PRIMARY KEY: there is no keyed read to compare and no
//     upsert for the hazard to ride in on.
//   - It samples ONE row per table. Routing-versus-placement is a property of
//     the table's topology, not of individual rows.
//
// A probe that cannot run is surfaced as an error, never as a mismatch, so a
// transient read failure cannot manufacture a refusal.
func PreflightShardPlacement(ctx context.Context, schema *ir.Schema, handle any) error {
	if schema == nil || len(schema.Tables) == 0 {
		return nil
	}
	prober, ok := handle.(ShardPlacementProber)
	if !ok {
		return nil
	}
	mismatched, err := prober.ShardPlacementMismatch(ctx, schema.Tables)
	if err != nil {
		return WrapWithHint(PhaseConnect, fmt.Errorf(
			"pipeline: shard-placement preflight: %w", err,
		))
	}
	if mismatched == "" {
		return nil
	}
	return sluicecode.Wrap(
		sluicecode.CodeTargetShardPlacementMismatch,
		"run the platform's reshard workflow so the rows are moved to the shards the topology routes to "+
			"(on PlanetScale Neki: __neki.reshard_create followed by __neki.workflow_switch_traffic), then "+
			"re-run; or point sluice at a target whose tables are not in this state",
		fmt.Errorf(
			"pipeline: target table %q routes to a different shard than its rows are on: a scattering read "+
				"returns rows that an equality-routed read on the PRIMARY KEY cannot find. In that state the "+
				"PRIMARY KEY is NOT globally enforced, so sluice's idempotent upsert would INSERT duplicate "+
				"rows instead of updating them — silently, at exit 0. Nothing has been written. This usually "+
				"means the table was assigned to a shard group without a reshard workflow to move its data",
			mismatched,
		),
	)
}
