// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package progress

import (
	"context"
	"log/slog"
)

// LogSink is migrate's structured-log [Sink]: it reproduces the exact
// slog records `migrate` emitted before ADR-0155, so the --log-format=json
// ingestion path and every non-TTY/CI/piped migrate invocation are
// byte-for-byte unchanged. It is the zero value, the [FromContext]
// default, and the nil-Migrator.Progress fallback.
//
// It is migrate-specific: its lines say "migration: ...". The other
// commands (verify/backup/restore) do NOT use it on the non-TTY path —
// they use [Nop], because their historical output is their own
// report/slog, not routed through this sink.
//
// It holds no state — every method routes to [slog.Default] at call time
// (matching the pre-ADR-0155 call-sites, which also read the default
// handler each call), so a single shared value is safe to share across
// goroutines and runs.
//
// A note on context: the historical call-sites used the *Context slog
// variants (InfoContext/WarnContext) to carry any trace/request attrs a
// handler might pull from ctx. sluice's own handlers (the stderr Text and
// JSON handlers in cmd/sluice) do not read ctx, so the emitted bytes are
// identical with a background context; the golden test in logsink_test.go
// pins that. If a future handler starts extracting ctx attrs, the Sink
// methods would grow a ctx parameter — cheap, and the test would catch
// the drift.
type LogSink struct{}

// compile-time assertion that LogSink satisfies Sink.
var _ Sink = LogSink{}

// PhaseStarted is a no-op: the pre-ADR-0155 orchestrator logged nothing
// at phase start, so byte-identity means emitting nothing.
func (LogSink) PhaseStarted(Phase) {}

// PhaseCompleted emits the historical per-phase completion line. The
// phase's Key is the `phase=` attr — migrate's Keys are the
// [ir.MigrationPhase] strings, so the line is byte-identical.
func (LogSink) PhaseCompleted(phase Phase) {
	slog.InfoContext(context.Background(), "migration: phase complete", slog.String("phase", phase.Key))
}

// PhaseCompletedEarly emits the --upfront-indexes variant of the
// completion line, preserving its distinct "(upfront)" marker.
func (LogSink) PhaseCompletedEarly(phase Phase) {
	slog.InfoContext(context.Background(), "migration: phase complete (upfront)", slog.String("phase", phase.Key))
}

// TableProgress is a deliberate no-op — see the [Sink.TableProgress] doc:
// the pipeline's progressTicker still emits its own rich "bulk copy
// progress" records on this path, which this narrow signal must not
// double.
func (LogSink) TableProgress(string, int64, int64) {}

// Warn re-emits an operator warning as the WARN record it always was.
func (LogSink) Warn(msg string, attrs ...any) {
	slog.WarnContext(context.Background(), msg, attrs...)
}

// Summary emits the historical end-of-run completion line. Only the
// table count is carried — the pre-ADR-0155 line never had a row count.
func (LogSink) Summary(r Result) {
	slog.InfoContext(context.Background(), "migration complete", slog.Int("tables", r.Tables))
}
