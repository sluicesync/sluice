// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import "sync"

// ReparentTracker is the thread-safe set of reparent-touched tables fed by
// the writers' [ir.ReparentObserverSetter] callback and drained by the
// restore reconciliation / migrate reconciliation phase (ADR-0113/ADR-0141).
// It is shared by pipeline-root's restore and migrate cold-copy paths — the
// single definition that lets the restore domain be carved cleanly (3.7b).
type ReparentTracker struct {
	mu      sync.Mutex
	touched map[string]bool
}

// NewReparentTracker returns an empty tracker ready for concurrent marks.
func NewReparentTracker() *ReparentTracker {
	return &ReparentTracker{touched: map[string]bool{}}
}

// Mark records table as reparent-touched. Safe for concurrent calls from
// every restore/copy writer.
func (t *ReparentTracker) Mark(table string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.touched[table] = true
}

// Drain returns the current touched set and clears it, so a reconciliation
// round can re-derive those tables while a concurrent redo that itself hits
// a reparent re-marks for the next round.
func (t *ReparentTracker) Drain() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.touched) == 0 {
		return nil
	}
	out := make([]string, 0, len(t.touched))
	for k := range t.touched {
		out = append(out, k)
	}
	t.touched = map[string]bool{}
	return out
}
