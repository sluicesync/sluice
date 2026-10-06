// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// sourceEngineComparator is the position order of the chain's recorded
// SOURCE engine, resolved from the engine registry — what `backup compact
// --smart-compaction` needs to judge the severed-transaction door's shape (B)
// before and after it rewrites change chunks (backup.CompactOpts.Comparator).
// The pipeline may not name engines (archgate), so the lookup lives here, the
// way restore and the broker get theirs from the TARGET engine. nil when the
// chain records no source engine, the engine is not registered, or it does
// not order positions (only Postgres does today).
func sourceEngineComparator(ctx context.Context, store irbackup.Store) ir.PositionMonotonicChecker {
	cat, err := lineage.ResolveLineage(ctx, store)
	if err != nil || cat.SourceEngine == "" {
		return nil
	}
	eng, ok := engines.Get(cat.SourceEngine)
	if !ok {
		return nil
	}
	return lineage.SameEngineComparator(ctx, store, eng)
}
