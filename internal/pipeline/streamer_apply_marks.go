// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// clearApplyMarksForColdStart deletes the stream's ADR-0190 apply marks
// before a cold start copies anything. Every cold-start entry point calls it
// first — [Streamer.coldStart], [Streamer.coldStartMultiDatabase] and the
// stopped-cold-start resume ([Streamer.resumeStoppedColdStart]), which
// together are every path that re-seeds the target (the fresh stream,
// --reset-target-data, --restart-from-scratch, the automatic re-snapshot, the
// interrupted-COPY resume). A cold start's copy re-seeds the target and its
// CDC begins at a new anchor, so no transaction an existing mark names is
// ever re-delivered; see [ir.ApplyMarksClearer] for why the clear is still
// load-bearing. An applier without the surface has no marks; a failure is a
// target error like any other on this path.
func clearApplyMarksForColdStart(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	clearer, ok := applier.(ir.ApplyMarksClearer)
	if !ok {
		return nil
	}
	if err := clearer.ClearApplyMarks(ctx, streamID); err != nil {
		return connectHint(fmt.Errorf("pipeline: clear the stream's apply marks before the cold-start copy: %w", err))
	}
	return nil
}
