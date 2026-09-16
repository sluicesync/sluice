// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/logcapture"
)

// captureSlog redirects the default slog logger to a mutex-guarded buffer
// for the duration of the test, restoring it on cleanup. Returns the
// buffer so a test can assert on emitted log lines.
func captureSlog(t *testing.T) *logcapture.Buffer {
	t.Helper()
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	buf := &logcapture.Buffer{}
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return buf
}

// fakeGrowClock drives a [GrowGate]'s two time seams so that a hold lasts
// EXACTLY as long as the gate asked for, with no OS scheduler in the loop.
//
// # Why this exists (TESTFRAGILE-2)
//
// The grow gate's ladder rungs are small multiples of a base backoff, and
// tests scale that base down so a six-rung ladder finishes quickly. At
// sub-50ms rungs a wall-clock assertion is measuring the runner, not the
// gate: Windows's default timer granularity is ~15.6ms, so one missed tick is
// most of a 20ms budget. Two tests in this package failed release-tag Windows
// legs that way, on different tags, for the same reason.
//
// Raising the base only buys headroom. This removes the dependency instead.
// [GrowGate.runOwner] is written entirely in terms of `g.now()` and
// `g.after()`, so a clock whose After ADVANCES the clock by the requested
// duration and fires immediately makes `onWindowClosed` report the gate's
// intended hold exactly — turning tolerance bands into equalities, which are
// strictly stronger, and making the tests run in microseconds.
//
// It is safe for the gate's concurrency: After is called from the owner
// goroutine while Now is called from both it and the test, so the clock is
// mutex-guarded.
type fakeGrowClock struct {
	mu  sync.Mutex
	now time.Time
}

// newFakeGrowClock starts at a fixed, non-zero instant. Non-zero matters:
// the gate distinguishes a zero time.Time from a real one in several places
// (lastReopen, windowStart), so starting at the zero value would exercise
// different branches than production ever takes.
func newFakeGrowClock() *fakeGrowClock {
	return &fakeGrowClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeGrowClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After advances the clock by d and fires at once, so the gate observes
// exactly d of elapsed time having actually waited none of it.
func (c *fakeGrowClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	if d > 0 {
		c.now = c.now.Add(d)
	}
	at := c.now
	c.mu.Unlock()

	ch := make(chan time.Time, 1)
	ch <- at
	return ch
}

// Advance moves the clock forward WITHOUT the gate having asked to wait, which
// is how a test simulates a healthy stretch — time passing while the gate is
// open and idle.
//
// The real-time form of this is `time.Sleep(idle + slack)`, which costs the
// wall-clock duration and then needs a scheduler allowance on top because the
// sleep can overshoot. Advancing a fake clock costs nothing and overshoots by
// nothing, so an episode-idle boundary can be crossed exactly rather than
// approximately.
func (c *fakeGrowClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
