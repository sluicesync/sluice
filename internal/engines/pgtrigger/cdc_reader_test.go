// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"testing"
	"time"
)

// TestCDCReader_SetPollInterval_OverridesDefault pins the roadmap
// item 18(c) / ADR-0066 §6 setter contract: an operator-supplied
// `--poll-interval=DUR` flows through Streamer.PollInterval to this
// reader via the streamer's pollIntervalSetter type assertion.
func TestCDCReader_SetPollInterval_OverridesDefault(t *testing.T) {
	r := &CDCReader{pollInterval: defaultPollInterval}
	if r.pollInterval != defaultPollInterval {
		t.Fatalf("precondition: default poll interval = %v; want %v", r.pollInterval, defaultPollInterval)
	}
	r.SetPollInterval(250 * time.Millisecond)
	if r.pollInterval != 250*time.Millisecond {
		t.Errorf("after SetPollInterval(250ms): pollInterval = %v; want 250ms", r.pollInterval)
	}
}

// TestCDCReader_SetPollInterval_ZeroIsNoop pins the "0 means leave
// the default in place" contract so the streamer's `if s.PollInterval
// > 0` gate isn't load-bearing alone — a future caller bypassing that
// gate and calling SetPollInterval(0) directly must NOT collapse the
// reader to a busy-loop.
func TestCDCReader_SetPollInterval_ZeroIsNoop(t *testing.T) {
	r := &CDCReader{pollInterval: defaultPollInterval}
	r.SetPollInterval(0)
	if r.pollInterval != defaultPollInterval {
		t.Errorf("after SetPollInterval(0): pollInterval = %v; want default %v (zero must NOT collapse the poll loop)",
			r.pollInterval, defaultPollInterval)
	}
}

// TestCDCReader_SetPollInterval_NegativeIsNoop mirrors the zero case
// — a negative duration is meaningless for a polling cadence; the
// setter rejects it rather than letting it propagate to time.Timer.
func TestCDCReader_SetPollInterval_NegativeIsNoop(t *testing.T) {
	r := &CDCReader{pollInterval: defaultPollInterval}
	r.SetPollInterval(-1 * time.Second)
	if r.pollInterval != defaultPollInterval {
		t.Errorf("after SetPollInterval(-1s): pollInterval = %v; want default %v (negative must NOT propagate)",
			r.pollInterval, defaultPollInterval)
	}
}
