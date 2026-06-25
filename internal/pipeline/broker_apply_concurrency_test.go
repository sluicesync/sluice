// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import "testing"

// TestResolveBrokerApplyConcurrency pins the ADR-0106 raw-int contract for the
// broker's incremental-replay lane count: 0 (unset) → the fast adaptive
// default, <0 → defensive serial, 1 → explicit serial opt-out, N>1 honored.
// The zero value resolving to the fast default — not the Go-zero-meaning-serial
// trap (v0.99.51) — is the whole point: every broker construction path (CLI,
// tests, future callers) gets concurrent replay unless it opts out with 1.
func TestResolveBrokerApplyConcurrency(t *testing.T) {
	cases := []struct {
		name string
		in   int
		want int
	}{
		{"unset resolves to fast default", 0, defaultApplyConcurrency},
		{"negative clamps to serial", -1, 1},
		{"explicit serial opt-out", 1, 1},
		{"operator override honored", 8, 8},
		{"two lanes honored", 2, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := resolveReplayApplyConcurrency(c.in); got != c.want {
				t.Errorf("resolveReplayApplyConcurrency(%d) = %d; want %d", c.in, got, c.want)
			}
		})
	}
}

// TestBrokerApplyConcurrency_PlumbEngages pins that the broker's resolved lane
// count flows through the shared applyApplyConcurrency helper onto the applier:
// the fast default (from an unset field) engages the setter, while an explicit
// 1 (serial opt-out) leaves it untouched. This is the broker-side counterpart
// to the streamer's openApplier plumb (streamer_run_phases.go) — the same
// helper, so exactly-once and lane-routing semantics are identical.
func TestBrokerApplyConcurrency_PlumbEngages(t *testing.T) {
	// Unset broker field (0) → fast default → setter engaged with W = default.
	rec := &recordingConcurrencySetter{}
	applyApplyConcurrency(rec, resolveReplayApplyConcurrency(0))
	if rec.calls != 1 || rec.lanes != defaultApplyConcurrency {
		t.Errorf("unset: setter calls=%d lanes=%d; want calls=1 lanes=%d", rec.calls, rec.lanes, defaultApplyConcurrency)
	}

	// Explicit serial opt-out (1) → no-op, applier stays on the serial path.
	recSerial := &recordingConcurrencySetter{}
	applyApplyConcurrency(recSerial, resolveReplayApplyConcurrency(1))
	if recSerial.calls != 0 {
		t.Errorf("serial opt-out engaged the setter %d times; want 0", recSerial.calls)
	}
}
