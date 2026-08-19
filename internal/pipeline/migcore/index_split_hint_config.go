// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// ApplyIndexSplitSizeHint threads per-table SOURCE byte-size estimates to a
// target [ir.SchemaWriter] via the optional [ir.IndexSplitSizeHintSetter], so a
// writer's large-table index-build decision (MySQL's ADR-0184 per-index ALTER
// split) sizes off the accurate long-lived SOURCE rather than the
// freshly-copied target's stale post-copy information_schema stats — the
// PlanetScale-leg bug where a 36 MB target table reported DATA_LENGTH 16 KB, so
// the split never fired on the one platform it exists for.
//
// Engine-neutral, and a no-op unless BOTH the TARGET writer wants the hint AND
// the SOURCE reader can supply byte sizes ([ir.TableByteSizeEstimator]). A
// source that cannot estimate — a snapshot-pinned reader, or a source engine
// without the surface — yields no map, so the writer keeps its safe default (the
// combined ALTER). Estimate failures and zero/absent sizes are skipped, never
// fatal. Must be called BEFORE the index phase.
//
// It reaches the single [SchemaWriter.buildTableIndexes] chokepoint, so both
// migrate and sync cold-start (which set it at their shared copy-phase entry
// points) get the source-driven decision from the same code.
func ApplyIndexSplitSizeHint(ctx context.Context, target any, rows ir.RowReader, schema *ir.Schema) {
	setter, ok := target.(ir.IndexSplitSizeHintSetter)
	if !ok {
		return
	}
	est, ok := rows.(ir.TableByteSizeEstimator)
	if !ok || schema == nil {
		return
	}
	sizes := make(map[string]int64, len(schema.Tables))
	for _, t := range schema.Tables {
		n, err := est.EstimateTableBytes(ctx, t)
		if err != nil {
			slog.DebugContext(ctx, "index-split size hint: source byte-size estimate failed for a table; ignoring it",
				slog.String("table", t.Name), slog.String("error", err.Error()))
			continue
		}
		if n <= 0 {
			continue // no estimate for this table (0 = unknown, never "empty").
		}
		sizes[t.Name] = n
	}
	if len(sizes) == 0 {
		return // nothing to hint; leave the writer at its safe default.
	}
	setter.SetIndexSplitSizeHint(sizes)
}
