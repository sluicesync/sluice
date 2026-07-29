// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Tiny slog-wrapper helpers shared between stream.go and stream_state.go.
// Pulled into a separate file so stream_state.go (which is otherwise
// stdlib-only) doesn't pull slog into a file whose primary concern is
// JSON marshalling.

import (
	"context"
	"log/slog"
	"time"
)

func warnConcurrentWriterOverride(ctx context.Context, prior *streamState, conflict string) {
	slog.WarnContext(
		ctx, "stream: --force bypassing concurrent-writer check",
		slog.Int("prior_pid", prior.PID),
		slog.String("prior_host", prior.Host),
		slog.Time("prior_last_rollover_at", prior.LastRolloverAt),
		slog.String("conflict", conflict),
	)
}

func warnConcurrentWriterTakeover(ctx context.Context, prior *streamState) {
	slog.WarnContext(
		ctx, "stream: prior stream_state is stale; taking over destination",
		slog.Int("prior_pid", prior.PID),
		slog.String("prior_host", prior.Host),
		slog.Time("prior_last_rollover_at", prior.LastRolloverAt),
	)
}

// logConcurrentWriterHandoff is INFO, not WARN, on purpose: a recorded
// handoff is the supervisor-restart happy path, not an anomaly the
// operator should go look at. exitedAt comes from [priorHandedOff]
// rather than being re-derived here — it is the evidence that admitted
// the takeover.
func logConcurrentWriterHandoff(ctx context.Context, prior *streamState, exitedAt time.Time) {
	slog.InfoContext(
		ctx, "stream: prior stream recorded a clean exit; taking over destination",
		slog.Int("prior_pid", prior.PID),
		slog.String("prior_host", prior.Host),
		slog.Time("prior_stopped_at", exitedAt),
	)
}

// warnCleanExitNotRecorded reports a `stopped_at` stamp that did not
// land. Best-effort by design (the chain, not this file, is the source
// of truth) — the only cost is that the next stream against this
// destination waits out the staleness window.
func warnCleanExitNotRecorded(ctx context.Context, path, reason string) {
	slog.WarnContext(
		ctx, "stream: could not record clean exit in stream_state; next start may have to wait out the staleness window",
		slog.String("path", path),
		slog.String("reason", reason),
	)
}
