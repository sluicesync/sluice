// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package progress is sluice's TTY-aware presentation layer (ADR-0155).
//
// A one-shot command (migrate first; verify/backup/restore later) emits
// its phase/progress events to a [Sink] instead of calling [log/slog]
// directly at each call-site. The concrete sink is chosen once, at
// startup, by the environment:
//
//   - [LogSink] reproduces the exact structured-log records sluice has
//     always emitted — byte-for-byte unchanged. It is the default
//     whenever stdout is not a terminal, whenever --log-format=json is
//     requested, and whenever --no-progress is set, so every automation,
//     the JSON ingestion path, and `sluice ... | tee` keep working
//     identically. It is ALSO the nil default: a [Migrator] that never
//     had its Progress field set behaves exactly as before.
//   - [TTYSink] drives a bubbletea/lipgloss live view for an operator at
//     an interactive terminal. It is the ONLY writer to the TTY while
//     active (the CLI silences slog for the render's duration), so the
//     live phase checklist and per-table progress bar never corrupt.
//
// The interface deliberately stays small: a command's presentation is a
// handful of typed events, not a logging API. See ADR-0155 for the
// decision record and the rollout order (migrate, then verify, then
// backup/restore onto this same shared sink).
package progress

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
)

// Sink receives a command's progress events. A command emits to a Sink
// instead of calling slog directly so its presentation (structured logs
// vs a live TTY view) is a startup choice, not baked into every
// call-site.
//
// Every method must be safe to call from multiple goroutines: the
// bulk-copy phase drives [Sink.TableProgress] from per-table/per-chunk
// goroutines while the orchestrator drives the phase methods from the
// main goroutine.
type Sink interface {
	// PhaseStarted marks a phase as in progress. It has no historical
	// slog line (the pre-ADR-0155 orchestrator logged only on
	// completion), so [LogSink] treats it as a no-op — nothing to keep
	// byte-identical. The TTY view uses it to render the "[..]" mark.
	PhaseStarted(phase ir.MigrationPhase)

	// PhaseCompleted marks a phase finished. [LogSink] emits the exact
	// `"migration: phase complete" phase=<phase>` record the orchestrator
	// has always emitted.
	PhaseCompleted(phase ir.MigrationPhase)

	// PhaseCompletedEarly is PhaseCompleted for a phase that finished
	// ahead of its usual slot — today only the --upfront-indexes index
	// build, which runs before the bulk copy. It exists solely to
	// preserve that path's distinct historical line,
	// `"migration: phase complete (upfront)"`, byte-for-byte. A named,
	// tested wart (per the loud-failure / clean-code tenets) rather than a
	// silent reuse of PhaseCompleted that would drop the "(upfront)"
	// marker from the log stream operators grep.
	PhaseCompletedEarly(phase ir.MigrationPhase)

	// TableProgress reports a bulk-copy table's advancing row count.
	// total is 0 until the async row-count estimate returns (the ETA
	// stays unknown in that window). It is driven by the pipeline's
	// bulk-copy ticker.
	//
	// [LogSink] is a deliberate no-op here: the pipeline's progressTicker
	// still emits its own rich `"bulk copy progress"` records (rows,
	// bytes, rate, ETA, chunk) on the non-TTY path, which this narrow
	// (done,total) signal cannot reproduce and must not double. Only the
	// TTY view consumes it — for the per-table bar — where the ticker's
	// slog lines are silenced.
	TableProgress(table string, done, total int64)

	// Warn surfaces an operator-facing warning (degraded FKs, dropped
	// collations, ...). attrs follow the slog convention: alternating
	// key/value pairs or [log/slog.Attr] values. [LogSink] re-emits it as
	// the WARN record it always was; the TTY view collects it for the
	// final summary panel.
	Warn(msg string, attrs ...any)

	// Summary renders the terminal, end-of-run result. [LogSink] emits
	// the `"migration complete" tables=N` record; the TTY view replaces
	// the live view with a compact static summary panel.
	Summary(r Result)
}

// Result is the end-of-run summary a command hands to [Sink.Summary].
type Result struct {
	// Tables is the number of tables migrated — the only field the
	// historical `"migration complete"` log line carries, so it is the
	// only field [LogSink] renders.
	Tables int

	// Rows is a best-effort total row count for the TTY summary panel.
	// The bulk-copy path counts per chunk and never aggregates a
	// per-table total (see the Migrator's "migration complete"
	// bookkeeping note), so this is 0 / unknown today; the TTY view
	// omits it when zero. [LogSink] ignores it — the historical line
	// never carried a row count and must not start.
	Rows int64
}

// sinkKey is the unexported context key under which a run's Sink travels.
type sinkKey struct{}

// defaultSink is the process-wide [LogSink] returned by [FromContext]
// when no sink was attached — the byte-identical structured-log path
// every pre-ADR-0155 caller (sync cold-start, tests, broker) implicitly
// gets. Stateless, so a single shared value is safe.
var defaultSink = LogSink{}

// NewContext returns ctx carrying sink, so call-sites deep in the
// orchestrator (the bulk-copy ticker especially) can reach it via
// [FromContext] without threading a parameter through every concurrent
// copy function. A nil sink is ignored — the context is returned
// unchanged so [FromContext] falls back to the [LogSink] default.
func NewContext(ctx context.Context, sink Sink) context.Context {
	if sink == nil {
		return ctx
	}
	return context.WithValue(ctx, sinkKey{}, sink)
}

// FromContext returns the [Sink] attached to ctx, or the shared
// [LogSink] default when none is present. It never returns nil, so
// call-sites need no nil check. The LogSink default is exactly the
// pre-ADR-0155 behaviour, which is what keeps every un-wired path (sync,
// tests, library embedders) byte-identical.
func FromContext(ctx context.Context) Sink {
	if s, ok := ctx.Value(sinkKey{}).(Sink); ok {
		return s
	}
	return defaultSink
}
