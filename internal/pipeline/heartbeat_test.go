// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a goroutine-safe bytes.Buffer for capturing slog
// output in concurrent tests. The bare bytes.Buffer is not
// goroutine-safe; using it directly as the io.Writer behind a slog
// handler that's invoked from a background goroutine while the test
// main goroutine reads via String() trips the race detector (the
// v0.48.0 CI surfaced this — the local CGO_ENABLED=0 Windows build
// silently disables -race, so the failure only fires on CI's Linux
// runner). Mutex-protect the Write + String paths to close the
// window.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestStartHeartbeat_EmitsOnTick pins the GitHub #23 Phase A
// invariant: when interval > 0, the heartbeat goroutine MUST emit
// `stream: heartbeat` lines at the cadence. The operator-visible
// outcome (a stalled stream is distinguishable from a wedge) hinges
// on this; if the goroutine silently exits or skips ticks, the
// diagnostic is useless.
func TestStartHeartbeat_EmitsOnTick(t *testing.T) {
	var buf syncBuffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	orig := slog.Default()
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(orig)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startHeartbeat(ctx, "test-stream", 50*time.Millisecond)

	// Poll until >= 2 ticks land instead of sleeping a fixed window: the
	// cadence is real-clock, so a starved CI runner (the Windows
	// unit-leg class) can deliver fewer ticks in a fixed window than the
	// cadence promises. >= 2 still pins the goroutine-dies-after-first-
	// tick regression; the deadline is generous because it only bounds
	// the FAILURE path.
	if !pollLogCount(&buf, "stream: heartbeat", 2, 10*time.Second) {
		t.Fatalf("heartbeat fired only %d times within 10s at 50ms cadence; want >= 2",
			strings.Count(buf.String(), "stream: heartbeat"))
	}
	cancel()

	got := buf.String()
	if !strings.Contains(got, "stream: heartbeat") {
		t.Errorf("heartbeat log missing; got log = %q", got)
	}
	if !strings.Contains(got, "test-stream") {
		t.Errorf("heartbeat log missing stream_id; got = %q", got)
	}
}

// pollLogCount polls buf until substr appears at least n times, or the
// deadline passes. The poll-until shape replaces the fixed sleep-then-
// count windows that under-counted ticks on starved runners.
func pollLogCount(buf *syncBuffer, substr string, n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Count(buf.String(), substr) >= n {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// TestStartHeartbeat_ZeroIntervalDisables covers the off path:
// interval=0 is the operator's "I don't want heartbeats" signal.
// Goroutine must not start; no log lines emitted.
func TestStartHeartbeat_ZeroIntervalDisables(t *testing.T) {
	var buf syncBuffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	orig := slog.Default()
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(orig)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startHeartbeat(ctx, "test-stream", 0)

	// Benign real-clock sleep: interval=0 starts NO goroutine at all, so
	// this can only false-pass (never false-fail) — not a flake vector.
	time.Sleep(100 * time.Millisecond)
	cancel()

	if strings.Contains(buf.String(), "stream: heartbeat") {
		t.Errorf("interval=0 should disable heartbeat; got log lines: %q", buf.String())
	}
}

// TestStartHeartbeat_ExitsOnCtxCancel pins the goroutine-leak guard:
// when ctx cancels, the goroutine MUST exit. Tested indirectly: after
// cancel, poll until the tick count holds still for 10x the cadence. A
// leaked goroutine keeps producing a tick every 20ms and can never show
// a 200ms-quiet window; a healthy exit flat-lines right after the at-
// most-one raced tick (ticker.C and ctx.Done ready in the same select —
// Go picks randomly — is in-spec). The poll-until-stable shape replaces
// the old fixed grace + fixed second window, whose real-clock bounds
// false-failed when a starved runner delivered the raced tick late.
func TestStartHeartbeat_ExitsOnCtxCancel(t *testing.T) {
	var buf syncBuffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	orig := slog.Default()
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(orig)

	ctx, cancel := context.WithCancel(context.Background())
	startHeartbeat(ctx, "test-stream", 20*time.Millisecond)
	// Prove the goroutine is actually ticking before the cancel, so the
	// leak check below is exercising a live goroutine.
	if !pollLogCount(&buf, "stream: heartbeat", 1, 10*time.Second) {
		t.Fatal("heartbeat never ticked before cancel")
	}
	cancel()

	const stableFor = 200 * time.Millisecond // 10x the 20ms cadence
	deadline := time.Now().Add(10 * time.Second)
	last := strings.Count(buf.String(), "stream: heartbeat")
	lastChange := time.Now()
	for time.Now().Before(deadline) {
		if n := strings.Count(buf.String(), "stream: heartbeat"); n != last {
			last, lastChange = n, time.Now()
		} else if time.Since(lastChange) >= stableFor {
			return // flat line — the goroutine exited
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("heartbeat count still rising %s after cancel (count=%d) — goroutine leaked", stableFor, last)
}
