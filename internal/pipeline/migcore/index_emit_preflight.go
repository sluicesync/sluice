// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// PreflightIndexEmit asks the TARGET engine whether it can represent every
// index in schema, and refuses before any data moves if it cannot (roadmap
// item 118).
//
// # The problem it closes
//
// sluice's phase order is create tables → bulk-copy → create indexes →
// create constraints, and every "this index cannot be represented on the
// target" refusal lives in an engine's DDL emitter for SECONDARY indexes —
// reached only in that third phase. Each such refusal was loud, zero-loss
// and correct, and each arrived after the entire table had been copied:
// MySQL's partial UNIQUE (its ordinary path is the deferred ADD-clause
// builder), SQLite's index prefix length (whose only call site is
// CREATE INDEX, with no inline path at all), and Postgres's index prefix
// length on the deferred ADD CONSTRAINT … UNIQUE path.
//
// # Why this is one call and not three moved checks
//
// Moving each check individually re-runs the sibling sweep every time a new
// target-unrepresentable index shape appears, and the two release-note
// claims that this already fired "before any data moves" are the evidence
// that the sweep is not reliably run. Asking the engine the question keeps
// the policy in the engine (the IR-first tenet) with no second copy to
// drift.
//
// # Scope, stated rather than implied
//
// This reaches exactly the refusals the implementing engines route through
// their own index-representability check helpers. It is NOT a general
// "would the whole schema apply" preflight — column-type refusals are
// [CheckCrossEngineSupportable]'s job, and an engine-side emit error raised
// somewhere other than those helpers still surfaces at its own phase.
//
// An engine without the optional surface (every source-only engine) is
// skipped silently: there is no target-side DDL to preflight.
//
// mode labels the run ("migrate", "sync cold-start", "restore: …") and is
// logged rather than folded into the refusal, so the engine's message — the
// one an operator acts on — stays byte-identical to the one the emitter
// would have produced later.
func PreflightIndexEmit(ctx context.Context, target ir.Engine, schema *ir.Schema, mode string) error {
	pf, ok := target.(ir.IndexEmitPreflighter)
	if !ok || schema == nil {
		return nil
	}
	if err := pf.PreflightIndexes(schema); err != nil {
		return fmt.Errorf("%s: %w", mode, err)
	}
	slog.DebugContext(ctx, "index-emit preflight clean",
		slog.String("mode", mode),
		slog.String("target_engine", target.Name()),
		slog.Int("tables", len(schema.Tables)))
	return nil
}
