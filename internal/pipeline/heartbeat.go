// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"
	"time"
)

// # Stream heartbeat (GitHub #23 Phase A)
//
// A periodic INFO log line emitted while a stream is alive so the
// silent-stall failure mode (process alive, no apply, no log) is
// distinguishable from a wedge (process alive, no apply, no
// heartbeat either).
//
// Phase A intent: don't try to *fix* the stall (that's Phase B, after
// we have ground truth from operator-collected goroutine dumps via
// the --pprof-listen endpoint). Just give the operator a positive
// liveness signal at default log level. When a stall fires next, the
// log shows heartbeats stopping AND the operator can hit
// /debug/pprof/goroutine?debug=2 to dump every stack — exactly the
// data needed to localise the wedge point.
//
// The heartbeat is per-stream (stream_id tagged) so an operator
// running multiple `sluice sync start` instances sees which is alive.
// It's NOT tied to apply activity — the goroutine wakes on its own
// timer and logs unconditionally. That's deliberate: a stalled apply
// loop wouldn't produce an activity-tied heartbeat at all, defeating
// the diagnostic purpose. The user reads "heartbeat present + no
// apply lines" as "source is quiet"; "no heartbeat + no apply lines"
// as "process wedged."

// DefaultHeartbeatInterval is the wall-clock cadence the per-stream
// heartbeat goroutine emits at when [Streamer.HeartbeatInterval] is
// left zero. 60 seconds is the smallest value that doesn't flood
// default log destinations during a 24h+ overnight run while still
// surfacing a stall within a minute.
const DefaultHeartbeatInterval = 60 * time.Second

// startHeartbeat spawns a goroutine that logs an INFO line every
// interval until ctx is cancelled. interval <= 0 disables the
// goroutine (no-op return). Caller does NOT need to track the
// goroutine — it exits on its own when ctx cancels.
func startHeartbeat(ctx context.Context, streamID string, interval time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				slog.InfoContext(ctx, "stream: heartbeat",
					slog.String("stream_id", streamID),
					slog.Time("at", t.UTC()),
					slog.Duration("interval", interval),
				)
			}
		}
	}()
}
