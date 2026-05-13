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

	// Wait long enough for ~3 ticks to land.
	time.Sleep(200 * time.Millisecond)
	cancel()

	got := buf.String()
	if !strings.Contains(got, "stream: heartbeat") {
		t.Errorf("heartbeat log missing; got log = %q", got)
	}
	if !strings.Contains(got, "test-stream") {
		t.Errorf("heartbeat log missing stream_id; got = %q", got)
	}
	// Expect at least 2 ticks in 200ms at 50ms cadence (allowing
	// scheduling slop). A flaky count >= 1 would mask the goroutine-
	// dies-after-first-tick regression we care about.
	count := strings.Count(got, "stream: heartbeat")
	if count < 2 {
		t.Errorf("heartbeat fired only %d times in 200ms at 50ms cadence; want >= 2", count)
	}
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

	time.Sleep(100 * time.Millisecond)
	cancel()

	if strings.Contains(buf.String(), "stream: heartbeat") {
		t.Errorf("interval=0 should disable heartbeat; got log lines: %q", buf.String())
	}
}

// TestStartHeartbeat_ExitsOnCtxCancel pins the goroutine-leak guard:
// when ctx cancels, the goroutine MUST exit. Tested indirectly: take
// a count snapshot some time AFTER cancel (allowing a grace window
// in case ticker.C and ctx.Done both fire in the same select — Go
// picks one randomly, so a single extra tick is in-spec), then
// confirm the count stays stable across a second window. A leaked
// goroutine would keep accumulating ticks across the second window;
// a healthy exit produces a flat line.
func TestStartHeartbeat_ExitsOnCtxCancel(t *testing.T) {
	var buf syncBuffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	orig := slog.Default()
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(orig)

	ctx, cancel := context.WithCancel(context.Background())
	startHeartbeat(ctx, "test-stream", 20*time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	cancel()

	// Grace window: allow any in-flight tick that raced with cancel
	// to land before we snapshot the count.
	time.Sleep(40 * time.Millisecond)
	postCancelCount := strings.Count(buf.String(), "stream: heartbeat")

	// Second window: a healthy-exit goroutine produces no further
	// ticks here. A leak would add ~10+ ticks at 20ms cadence.
	time.Sleep(200 * time.Millisecond)
	finalCount := strings.Count(buf.String(), "stream: heartbeat")

	if finalCount != postCancelCount {
		t.Errorf("heartbeat goroutine leaked: post-cancel-grace=%d final=%d (cancel didn't exit the goroutine)",
			postCancelCount, finalCount)
	}
}
