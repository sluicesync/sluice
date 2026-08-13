// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/ir"
)

// Compile-time declarations of the ir interfaces this engine's
// concrete types intentionally implement.
//
// Why this file exists: the orchestrator discovers optional surfaces
// by runtime type-assertion, so a method-set break doesn't fail the
// build — the assertion quietly stops matching and the pipeline
// silently downgrades to the fallback path. The blank-var assertions
// below turn that silent downgrade into a compile error here.
//
// The trigger engine's surface is intentionally NARROW. It composes
// [postgres.Engine] by delegation (NOT embedding-with-promotion —
// see the Engine type's doc comment), precisely so it does NOT
// satisfy the slot-flavoured optional openers the orchestrator
// type-asserts on: [ir.SlotManagerOpener], [ir.CDCReaderWithSlotOpener],
// [ir.SnapshotStreamWithSlotOpener]. A slot-less managed-PG tier is
// the engine's reason to exist; inheriting those surfaces would
// silently route operators through slot management the server forbids.
// Do NOT "fix" a missing-interface compile error by widening this
// engine — that narrowness is load-bearing.
var (
	_ ir.Engine            = Engine{}
	_ ir.ConnectionLabeler = Engine{}
	_ ir.CDCReader         = (*CDCReader)(nil)
	// audit-2026-07-11 M-3: ChangeLogPruner drives change-log pruning; a
	// method-set drift here silently stops pruning → the trigger change-log
	// grows unbounded (a silent-loss-adjacent resource leak).
	_ ir.ChangeLogPruner = (*CDCReader)(nil)
	// Roadmap item 115: the consumer-registry COMPANION is what makes the
	// auto-prune safe on a change log shared by several streams. The sidecar
	// fails CLOSED without it — a method-set drift here does not silently
	// re-enable an unscoped prune, it stops pruning entirely — but it would
	// still stop this engine from registering, making its stream invisible to
	// a peer's pruner. Pin it.
	_ ir.ChangeLogConsumerRegistry = (*CDCReader)(nil)
	// Audit 2026-08-11 D-1: `migrate --where` works on this engine because
	// [Engine.OpenRowReader] delegates to the composed postgres engine,
	// whose reader implements the predicate pushdown. The pin lives HERE
	// (asserting the DELEGATED type) so the docsync engine-list derivation
	// attributes the capability to `postgres-trigger`, matching the
	// runtime truth — verified behaviourally by
	// TestPostgresTriggerRowReaderHonorsWhereFilters (internal/pipeline).
	// This is a delegated-reader fact, not a widening of this engine's own
	// surface; the narrowness note above is about the slot openers.
	_ ir.RowFilterSetter = (*postgres.RowReader)(nil)
)
