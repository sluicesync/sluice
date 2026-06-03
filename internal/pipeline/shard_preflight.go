// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Shape-A populated-target preflight (ADR-0048 Decision 3 /
// DP-2 resolved 2026-05-16). Replaces the silent skip of
// `--force-cold-start` with a loud three-point assertion when the
// operator has set `--inject-shard-column NAME=VALUE` and the
// target is non-empty:
//
//   1. The discriminator column EXISTS on the target and every
//      existing row has it NOT NULL (a NULL ⇒ a prior non-shard-
//      aware load contaminated the table and the operator must
//      reconcile before this run can extend it).
//   2. The incoming stream's VALUE is NOT already present among
//      the target's DISTINCT discriminator values (presence ⇒
//      shard is already loaded or the operator reused a shard
//      value → double-load / cross-shard collision).
//   3. The target's composite PK structurally LEADS with the
//      discriminator (otherwise the disjointness guarantee the
//      bypass rests on is void; a CDC update targeting a PK from
//      a sibling shard could mis-target this shard's row).
//
// All three checks run against the target via the optional
// [shardPreflightProber] surface — engines that expose it (both
// shipping engines) drive the actual SQL probes; engines that
// don't fall back silently the same way preflightColdStart does
// for [ir.TableEmptyChecker].
//
// DP-2 is owner-confirmed as discriminator-value-presence-only:
// composite-PK disjointness covers correctness regardless of
// resume status, so this preflight does NOT consult
// `sluice_cdc_state` / `sluice_migrate_state`. The composite-PK
// check above is the disjointness invariant the bypass rests on.
//
// DP-3 is owner-confirmed as drained-model-for-v1: cross-shard
// schema migration is operator-driven via `sync stop --wait`
// → schema migrate → `sync start --resume` (ADR-0030 Strategy
// A). This preflight does NOT lease, coordinate, or detect
// in-flight cross-shard DDL; the existing schema-history
// machinery is the loud-failure path if shards diverge mid-stream.

import (
	"context"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// errShardConsolidationRefused is the sentinel cause for a Shape-A
// preflight refusal. Wrapped with the per-table per-check detail;
// tests use errors.Is to assert without coupling to the message
// text.
var errShardConsolidationRefused = errors.New("pipeline: shard consolidation refused")

// shardPreflightProber is the optional surface a [ir.RowWriter]
// (or any handle to the target) can implement to drive the
// three-point Shape-A populated-target check. Each method maps
// 1:1 to one assertion the preflight makes; the engine's
// implementation issues the appropriate engine-specific SQL.
//
// All three methods are read-only — preflight refuses BEFORE any
// data moves, so an engine implementation should never write.
// The contract is structurally identical to [ir.TableEmptyChecker]:
// the orchestrator probes via type-assertion and skips silently
// on engines that don't implement it (the existing v0.3.0
// "opportunistic" preflight posture, ADR-0048 Decision 3).
//
// Defined in the pipeline package rather than `ir` because it is
// orchestrator-private: only the Shape-A populated-target branch
// consults it, and it doesn't belong to the cross-cutting IR
// contract every engine ships with.
type shardPreflightProber interface {
	// HasNullShardColumn returns true if the named discriminator
	// column exists on the target table and at least one existing
	// row has it NULL. Returns (false, nil) when the column is
	// absent OR every row is non-NULL (the legal-Shape-A shape).
	// An engine error (table missing on the target, column missing,
	// permission denied) returns it verbatim — the orchestrator
	// wraps with operator-actionable context.
	HasNullShardColumn(ctx context.Context, table *ir.Table, column string) (bool, error)

	// ShardValuePresent returns true if the named discriminator
	// column exists on the target table and AT LEAST one row
	// already carries the supplied `value`. Used to refuse the
	// "shard 2 starting on top of itself" / "operator reused a
	// shard value" cases — presence ⇒ double-load hazard.
	ShardValuePresent(ctx context.Context, table *ir.Table, column string, value any) (bool, error)

	// CompositePKLeadsWith returns true if the target table's
	// primary key exists, is composite (>1 column), AND its
	// leading column is the named discriminator. False ⇒ the
	// disjointness guarantee the Shape-A bypass rests on is void;
	// must refuse.
	CompositePKLeadsWith(ctx context.Context, table *ir.Table, column string) (bool, error)
}

// preflightShardConsolidation runs the three-point Shape-A
// populated-target check on every table in the schema. Returns
// nil when:
//
//   - The operator has NOT supplied `--inject-shard-column`
//     (shardName == ""), OR
//   - The target writer doesn't implement [shardPreflightProber]
//     (engine doesn't surface the probes — same opportunistic
//     posture as preflightColdStart), OR
//   - Every table is either empty (first-shard cold-start) OR
//     passes all three assertions.
//
// Returns a wrapped [errShardConsolidationRefused] naming the
// first offending table and the specific failed check otherwise.
//
// The first-shard cold-start path is detected per-table via
// [ir.TableEmptyChecker]: an empty target table skips the three-
// point check (there's nothing to collide with). When the writer
// doesn't expose IsTableEmpty either, the preflight refuses
// loudly across the board — the operator's `--inject-shard-column`
// asserts populated-target semantics; without an empty-check we
// can't safely choose between "shard 1 cold-start" and "shard N
// populated extend."
func preflightShardConsolidation(
	ctx context.Context,
	schema *ir.Schema,
	rw ir.RowWriter,
	shardName string,
	shardValue any,
) error {
	if shardName == "" {
		return nil
	}
	if schema == nil || len(schema.Tables) == 0 {
		return nil
	}
	prober, ok := rw.(shardPreflightProber)
	if !ok {
		// Engine doesn't expose the probes — fall back to the
		// existing v0.3.0 cold-start preflight (called separately
		// by the orchestrator). The Shape-A loud refusal is
		// opportunistic exactly like preflightColdStart.
		return nil
	}
	emptyChecker, _ := rw.(ir.TableEmptyChecker)

	for _, table := range schema.Tables {
		if table == nil {
			continue
		}
		// Per-table empty-fast-path: shard 1 cold-start lands here
		// (target table empty ⇒ no rows to collide with, no
		// existing discriminator values to compare against).
		if emptyChecker != nil {
			empty, err := emptyChecker.IsTableEmpty(ctx, table)
			if err != nil {
				return wrapWithHint(PhaseSchemaApply, fmt.Errorf(
					"pipeline: shard preflight: probe %q empty: %w", table.Name, err,
				))
			}
			if empty {
				continue
			}
		}
		// Check (1): no NULL discriminator on existing rows.
		hasNull, err := prober.HasNullShardColumn(ctx, table, shardName)
		if err != nil {
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf(
				"pipeline: shard preflight: probe %q NULL %q: %w", table.Name, shardName, err,
			))
		}
		if hasNull {
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf(
				"%w: target table %q has rows with NULL %q — a previous non-shard-aware load contaminated the table; "+
					"reconcile by either backfilling the discriminator (UPDATE %s SET %s = <shard_value> WHERE %s IS NULL) "+
					"or by passing --reset-target-data + --yes to wipe the table and start clean. "+
					"Shape A (ADR-0048) refuses to extend a populated-target whose existing rows lack the discriminator",
				errShardConsolidationRefused, table.Name, shardName, table.Name, shardName, shardName,
			))
		}
		// Check (2): incoming shard's VALUE not already present.
		present, err := prober.ShardValuePresent(ctx, table, shardName, shardValue)
		if err != nil {
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf(
				"pipeline: shard preflight: probe %q value %v on %q: %w",
				table.Name, shardValue, shardName, err,
			))
		}
		if present {
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf(
				"%w: target table %q already has rows with %s = %v — this shard is already loaded "+
					"or the operator reused a shard value (cross-shard collision risk). "+
					"Recovery: (a) pick a fresh VALUE for --inject-shard-column NAME=VALUE if a sibling shard "+
					"already used it; (b) if this is a re-attempt of THIS shard, use --resume to pick up "+
					"where the previous run left off; (c) --reset-target-data + --yes to wipe and restart",
				errShardConsolidationRefused, table.Name, shardName, shardValue,
			))
		}
		// Check (3): composite PK leads with the discriminator.
		leads, err := prober.CompositePKLeadsWith(ctx, table, shardName)
		if err != nil {
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf(
				"pipeline: shard preflight: probe %q PK lead %q: %w", table.Name, shardName, err,
			))
		}
		if !leads {
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf(
				"%w: target table %q does not have a composite PRIMARY KEY leading with %q — "+
					"the Shape A disjointness guarantee rests on (discriminator, …source PK); without it "+
					"a CDC update from a sibling shard could silently mis-target this shard's row. "+
					"Recovery: re-create the target table with the composite PK "+
					"(ALTER TABLE %s DROP PRIMARY KEY; ALTER TABLE %s ADD PRIMARY KEY (%s, …)), "+
					"or pass --exclude-table=%s to skip the table from the consolidated stream",
				errShardConsolidationRefused, table.Name, shardName, table.Name, table.Name, shardName, table.Name,
			))
		}
	}
	return nil
}
