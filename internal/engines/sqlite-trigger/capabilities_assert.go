// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import "sluicesync.dev/sluice/internal/ir"

// Compile-time declarations of the ir interfaces this engine's concrete types
// intentionally implement.
//
// Why this file exists: the orchestrator discovers optional surfaces by runtime
// type-assertion, so a method-set break doesn't fail the build — the assertion
// quietly stops matching and the pipeline silently downgrades to a fallback
// path. The blank-var assertions below turn that silent downgrade into a compile
// error here.
//
// The trigger engine's surface is intentionally NARROW. It composes
// [sqlite.Engine] by delegation (NOT embedding-with-promotion — see the Engine
// type's doc comment), precisely so it does NOT inherit any optional opener the
// orchestrator type-asserts on that SQLite cannot honour (slot management, the
// writer/target surfaces). SQLite has no replication slots and the trigger
// engine is a CDC SOURCE only; do NOT "fix" a missing-interface error by widening
// this engine — that narrowness is load-bearing.
var (
	_ ir.Engine    = Engine{}
	_ ir.CDCReader = (*CDCReader)(nil)
	// audit-2026-07-11 M-3: ChangeLogPruner drives change-log pruning; a
	// method-set drift here silently stops pruning → the trigger change-log
	// grows unbounded (a silent-loss-adjacent resource leak).
	_ ir.ChangeLogPruner = (*CDCReader)(nil)
	// Roadmap item 115: the consumer-registry COMPANION is what makes the
	// auto-prune safe on a change log shared by several streams. The sidecar
	// fails CLOSED without it — a method-set drift here does not silently
	// re-enable an unscoped prune, it stops pruning entirely — but it would
	// still stop this engine (and its d1-trigger transport sibling, whose CDC
	// reader IS this type) from registering, making its stream invisible to a
	// peer's pruner. Pin it.
	_ ir.ChangeLogConsumerRegistry = (*CDCReader)(nil)
)
