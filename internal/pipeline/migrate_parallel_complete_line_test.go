// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestLogParallelCopyComplete_Shape pins the item-69c table-level
// completion line for the within-table-parallel path: message
// identical to the single-reader ticker's ("bulk copy complete"),
// carrying the whole-table row total, the lane count as `chunks`, and
// a duration. Before this line a parallel table finished silently at
// INFO (per-chunk lines only; the raw-copy lane logged nothing above
// DEBUG) — only verify proved the copy.
func TestLogParallelCopyComplete_Shape(t *testing.T) {
	logs := captureSlog(t)

	logParallelCopyComplete(context.Background(), "orders", 250000, 8, time.Now().Add(-3*time.Second))

	out := logs.String()
	if !strings.Contains(out, "bulk copy complete") {
		t.Errorf("expected the single-reader-sibling message; got: %q", out)
	}
	for _, want := range []string{"table=orders", "rows=250000", "chunks=8", "duration="} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in the completion line; got: %q", want, out)
		}
	}
	if !strings.Contains(out, "level=INFO") {
		t.Errorf("completion line must be INFO; got: %q", out)
	}
}
