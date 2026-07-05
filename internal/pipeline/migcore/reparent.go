// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"sync"

	"sluicesync.dev/sluice/internal/ir"
)

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

// ApplyReparentObserver wires the run's reparent-touched observer (ADR-0113)
// onto a freshly-opened writer that opts in via [ir.ReparentObserverSetter].
// nil observe (no tracker constructed) or an engine that doesn't implement
// the setter is a no-op — pre-ADR-0113 behaviour, byte-for-byte. Called
// alongside ApplyGrowGate, on the single openTargetRowWriter path, so every
// restore/migrate writer reports through the same tracker. Shared by
// pipeline-root's migrate cold-copy and the carved-out backup/restore domain.
func ApplyReparentObserver(target any, observe func(table string)) {
	if observe == nil {
		return
	}
	if setter, ok := target.(ir.ReparentObserverSetter); ok {
		setter.SetReparentObserver(observe)
	}
}

// ReconcileMaxRounds bounds the ADR-0113 reconciliation loop: a target that
// reparents on EVERY serial redo is wedged (not a transient grow), so after
// this many rounds the migrate/restore reconciliation surfaces loudly rather
// than looping forever. In practice one round suffices — by reconciliation
// time the volume has grown to its final size, so a redo writes into an
// already-grown volume and triggers no fresh reparent.
const ReconcileMaxRounds = 10
