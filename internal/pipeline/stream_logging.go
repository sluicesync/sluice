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
