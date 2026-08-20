// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
)

// PreflightSourceBoolRanges runs the source engine's optional
// [ir.BoolRangePreflighter] fail-fast against the in-scope schema, opening a
// short-lived source SchemaReader to do it. It is the shared pre-copy check
// both `migrate` and the `sync` cold-start run so a source TINYINT(1) column
// holding a value outside {0,1} — which the boolean mapping would silently
// collapse to true and lose (SLUICE-E-VALUE-TINYINT1-RANGE) — refuses at
// planning time, before any data moves, on BOTH entry points (the item-118
// shared-copy-phase parity: a refusal that fires before any data moves on one
// must fire before any data moves on the other).
//
// It is a fail-FAST LAYER, not the guarantee: the per-read-path decode-time
// refusal is the correctness floor. Accordingly this is deliberately forgiving
// of everything except a confirmed out-of-range value:
//
//   - a source whose reader does not implement the surface (every non-MySQL
//     source) is a no-op;
//   - a reader that will not open is a no-op here — the cold-start / migrate
//     opens its own reader next and will surface that error loudly there;
//   - inside the implementation, a probe that cannot COMPLETE (a clean huge
//     table that times out or hits a statement-time wall) WARNs and returns nil.
//
// Only a genuinely out-of-range value returns the coded refusal.
func PreflightSourceBoolRanges(ctx context.Context, source ir.Engine, sourceDSN string, schema *ir.Schema, rowFilters map[string]string) error {
	if schema == nil {
		return nil
	}
	sr, err := source.OpenSchemaReader(ctx, sourceDSN)
	if err != nil {
		return nil // the real open happens next; let it surface the error loudly
	}
	defer CloseIf(sr)

	pf, ok := sr.(ir.BoolRangePreflighter)
	if !ok {
		return nil
	}
	// rowFilters (the operator's per-table --where) is threaded so the probe
	// never refuses on a row the copy would not move (F-1). Same map the
	// cold-start copy honors; nil/empty probes every row.
	return pf.PreflightBoolRanges(ctx, schema, rowFilters)
}
