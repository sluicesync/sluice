// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// RunViewsPhase emits CREATE VIEW for every view in schema.Views with
// a retry policy that handles view-to-view dependency ordering without
// implementing a full SQL parser. The policy: emit views in declared
// order; on failure, accumulate the failed view in a retry list; after
// the first pass, retry the failed views up to 2 more times. If the
// retry list is non-empty after the third pass, surface the
// accumulated errors.
//
// Why retry rather than topological sort: parsing the view's SELECT
// body to extract referenced views requires a real SQL parser, which
// is out of scope for Phase 1 (and arguably ever — different engines
// have different SELECT grammars). Real-world view dependency depths
// are shallow (typically 1-2 levels of view-on-view); two retry
// passes covers the common cases. Operators with deeper dependency
// graphs (>2 levels of view-on-view chains) get a clear error
// pointing at the still-failing views and can manually reorder
// `--include-view` invocations to bootstrap the dependency chain.
//
// No-op on schemas without views; cheap when none fail.
func RunViewsPhase(ctx context.Context, schema *ir.Schema, sw ir.SchemaWriter) error {
	if schema == nil || len(schema.Views) == 0 {
		return nil
	}

	// First pass: try every view. retry collects views that failed.
	pending := append([]*ir.View(nil), schema.Views...)
	var lastErrs []error

	const maxPasses = 3 // 1 initial + 2 retries
	for pass := 0; pass < maxPasses && len(pending) > 0; pass++ {
		var nextPending []*ir.View
		lastErrs = nil
		for _, v := range pending {
			single := &ir.Schema{Views: []*ir.View{v}}
			// ADR-0114: ride a storage-grow/reparent that lands on the view
			// DDL, atop the dependency-pass retry below. A non-transient
			// failure (e.g. an unresolved view-on-view dependency) returns
			// promptly and is handled by the pass-retry exactly as before.
			cv := func(ctx context.Context) error { return sw.CreateViews(ctx, single) }
			if err := RunDDLPhaseWithReparentRetry(ctx, "views", sw, cv); err != nil {
				if pass == maxPasses-1 {
					// Last pass — accumulate the error for the caller.
					lastErrs = append(lastErrs, fmt.Errorf("view %q: %w", v.Name, err))
				} else {
					slog.DebugContext(
						ctx, "view create failed, will retry",
						slog.String("view", v.Name),
						slog.Int("pass", pass+1),
						slog.String("error", err.Error()),
					)
				}
				nextPending = append(nextPending, v)
			}
		}
		if len(nextPending) == len(pending) && pass < maxPasses-1 {
			// No progress this pass — abort early. Trying again wouldn't
			// help (no view-create succeeded to unblock the rest). Force
			// the next iteration to be the last so the caller gets the
			// accumulated errors.
			slog.DebugContext(
				ctx, "no progress in views phase; bailing to error report",
				slog.Int("pending", len(nextPending)),
				slog.Int("pass", pass+1),
			)
			pass = maxPasses - 2 // next iteration is the last (records errors)
		}
		pending = nextPending
	}

	if len(pending) > 0 {
		// Build a single combined error so the operator sees every
		// still-failing view at once rather than just the first.
		names := make([]string, 0, len(pending))
		for _, v := range pending {
			names = append(names, v.Name)
		}
		base := fmt.Errorf("pipeline: create views failed after %d retries (%d still failing: %v); "+
			"view-to-view dependency depth may exceed retry budget — review and reorder declared view list",
			maxPasses-1, len(pending), names)
		if len(lastErrs) > 0 {
			return errors.Join(append([]error{base}, lastErrs...)...)
		}
		return base
	}

	slog.InfoContext(ctx, "views created", slog.Int("count", len(schema.Views)))
	return nil
}
