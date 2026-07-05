// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// withFastGrowGate shrinks migcore's grow-gate time envelope to near-zero
// for the duration of a test so the FSM cycles fast and deterministically,
// while the STRUCTURE (open/closed transitions, coalescing, max-hold bound)
// is exactly as production. Restores the production values on cleanup.
//
// The envelope globals are read only at gate CONSTRUCTION (NewGrowGate
// snapshots them into per-instance fields — the v0.99.100 -race property),
// so a test that mutates them before constructing its gate never races a
// running owner. A pipeline-root copy of migcore's identical test helper:
// migcore's tuning vars are exported for exactly this cross-package
// test-envelope shrink.
func withFastGrowGate(t *testing.T) {
	t.Helper()
	base, capDur, maxHold := migcore.GrowGateBackoffBase, migcore.GrowGateBackoffCap, migcore.GrowGateMaxHold
	migcore.GrowGateBackoffBase = time.Millisecond
	migcore.GrowGateBackoffCap = time.Millisecond
	migcore.GrowGateMaxHold = 5 * time.Second
	t.Cleanup(func() {
		migcore.GrowGateBackoffBase = base
		migcore.GrowGateBackoffCap = capDur
		migcore.GrowGateMaxHold = maxHold
	})
}
