// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"sync/atomic"

	"sluicesync.dev/sluice/internal/ir"
)

// PKTracker captures the PK column values of the last row passing
// through a batch. Used by the copy tees to extract the cursor for
// the next iteration without changing the writer interface.
//
// The tracker is intentionally simple: it overwrites lastValues on
// every row, and lastValues is a fresh slice per call so the writer's
// downstream consumer can't mutate the captured values out from under
// the orchestrator. Concurrent reads of [PKTracker.LastPK] are not
// supported — the only caller is the orchestrator, which reads after
// the writer has returned.
type PKTracker struct {
	pkCols []string
	last   atomic.Pointer[[]any]
}

// NewPKTracker returns a tracker for the given PK column names.
func NewPKTracker(pkCols []string) *PKTracker {
	return &PKTracker{pkCols: pkCols}
}

// Observe records the PK column values of row. nil row is a no-op
// (defensive — should not happen in practice). Missing PK columns
// produce a slice with nil entries; the next batch's WHERE predicate
// would be incorrect, but classifyTableForResume rejects no-PK tables
// upstream so the situation shouldn't arise.
func (t *PKTracker) Observe(row ir.Row) {
	if row == nil || len(t.pkCols) == 0 {
		return
	}
	pk := make([]any, len(t.pkCols))
	for i, c := range t.pkCols {
		pk[i] = row[c]
	}
	t.last.Store(&pk)
}

// LastPK returns the PK values of the most recently observed row,
// plus a flag indicating whether any rows were seen. Returns (nil,
// false) when no rows passed through.
func (t *PKTracker) LastPK() ([]any, bool) {
	p := t.last.Load()
	if p == nil {
		return nil, false
	}
	return *p, true
}
