// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Cross-shard-collision preflight (Bug 152). A Vitess/PlanetScale
// keyspace fronted by vtgate transparently MERGES every shard into one
// logical stream, so a `sluice migrate` / `sync` that points at a
// multi-shard source WITHOUT a discriminator writes every shard's rows
// into one target table. If that table has a primary key or any UNIQUE
// constraint, rows from different shards that share a key value
// SILENTLY OVERWRITE each other on the target (per-shard auto-increment
// ranges and tenant-local ids collide across shards) — exit-0,
// fewer-rows-than-source data loss, the worst class for this project.
//
// ADR-0048's --inject-shard-column already solves this by appending a
// per-shard discriminator and rewriting the PK to lead with it. This
// preflight closes the GAP when that flag is NOT set: it refuses a
// multi-shard source feeding a collision-capable (PK / UNIQUE) target
// table, before any data moves. The operator's two ways forward are
// named in the error: add the discriminator (--inject-shard-column), or
// opt in explicitly (--allow-cross-shard-merge) when the key is known to
// be globally unique across shards (Vitess sequences / UUID keys).
//
// Scope (the B1 "targeted" decision): the refusal fires ONLY for a
// table with a PK or UNIQUE — a keyless table is already at-least-once
// (ADR-0010) with no overwrite, so merging shards into it loses nothing
// the operator hasn't already accepted. Single-shard / non-sharded
// sources are a no-op (one logical source, no merge), as are engines
// that don't implement [ir.ShardDiscoverer] (vanilla MySQL, Postgres).

import (
	"context"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// errCrossShardCollisionRefused is the sentinel cause for a Bug 152
// preflight refusal. Wrapped with the per-table detail; tests assert via
// errors.Is without coupling to the message text.
var errCrossShardCollisionRefused = errors.New("pipeline: cross-shard merge refused")

// preflightCrossShardCollision refuses a multi-shard source merging into
// a single non-discriminated target table that can collide on a key.
// Returns nil when:
//
//   - The operator engaged --inject-shard-column (shardColumnEngaged) —
//     the discriminator keeps per-shard rows disjoint (the
//     [preflightShardConsolidation] three-point check governs that path
//     instead), OR
//   - The operator passed --allow-cross-shard-merge (allowMerge) — an
//     explicit "my keys are globally unique across shards" override, OR
//   - The source doesn't implement [ir.ShardDiscoverer] (non-sharded
//     engine — vanilla MySQL, Postgres), OR
//   - The source reports <= 1 shard (unsharded keyspace; no merge), OR
//   - No table in the schema has a PK or UNIQUE (nothing can collide).
//
// Returns a wrapped [errCrossShardCollisionRefused] naming the first
// collision-capable table otherwise. A shard-discovery error is
// fail-CLOSED (refused): this is a silent-loss guard, so an inability to
// confirm the shard count must not silently proceed — the operator can
// pass --allow-cross-shard-merge to override a discovery that can't run.
func preflightCrossShardCollision(
	ctx context.Context,
	source ir.Engine,
	sourceDSN string,
	schema *ir.Schema,
	shardColumnEngaged bool,
	allowMerge bool,
) error {
	if shardColumnEngaged || allowMerge {
		return nil
	}
	if schema == nil || len(schema.Tables) == 0 {
		return nil
	}
	discoverer, ok := source.(ir.ShardDiscoverer)
	if !ok {
		// Non-sharded engine (vanilla MySQL / Postgres): single logical
		// source, nothing to merge. Same opportunistic posture as the
		// other preflights.
		return nil
	}
	shards, err := discoverer.DiscoverShards(ctx, sourceDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf(
			"%w: could not determine the source's shard layout (%v) — refusing rather than "+
				"risk a silent cross-shard overwrite. If the source is unsharded or its keys are "+
				"globally unique across shards, pass --allow-cross-shard-merge to proceed; if it "+
				"is sharded, pass --inject-shard-column NAME=VALUE per shard (ADR-0048)",
			errCrossShardCollisionRefused, err,
		))
	}
	if len(shards) <= 1 {
		// Unsharded keyspace (single shard) or non-sharded source: no
		// merge happens, so no cross-shard collision is possible.
		return nil
	}
	for _, table := range schema.Tables {
		if table == nil {
			continue
		}
		kind, collides := collisionKey(table)
		if !collides {
			// Keyless table: at-least-once with no overwrite (ADR-0010);
			// merging shards into it loses nothing.
			continue
		}
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf(
			"%w: the source is a sharded keyspace with %d shards (%v) that vtgate merges into one "+
				"logical stream, but no shard discriminator is set — every shard's rows would be "+
				"written into the single target table %q, whose %s means rows from different shards "+
				"sharing a key value would SILENTLY OVERWRITE each other (per-shard id ranges collide "+
				"across shards). Choose one: "+
				"(a) pass --inject-shard-column NAME=VALUE per shard so sluice adds a discriminator and "+
				"a composite PK that keeps cross-shard rows disjoint (ADR-0048); or "+
				"(b) pass --allow-cross-shard-merge if you have verified the key is globally unique "+
				"across shards (e.g. Vitess sequences or UUID keys), so no overwrite can occur",
			errCrossShardCollisionRefused, len(shards), shards, table.Name, kind,
		))
	}
	return nil
}

// collisionKey reports whether a table has a key that can collide across
// merged shards (a primary key or any UNIQUE index/constraint) and a
// short human description of it for the refusal message. A table with no
// such key returns ("", false): merging shards into it cannot overwrite
// (at-least-once, ADR-0010).
func collisionKey(table *ir.Table) (string, bool) {
	if table.PrimaryKey != nil && len(table.PrimaryKey.Columns) > 0 {
		return "PRIMARY KEY", true
	}
	for _, idx := range table.Indexes {
		if idx != nil && idx.Unique && len(idx.Columns) > 0 {
			return "UNIQUE index", true
		}
	}
	return "", false
}
