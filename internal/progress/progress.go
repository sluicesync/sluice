// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package progress is sluice's TTY-aware presentation layer (ADR-0155).
//
// A one-shot command (migrate, verify, backup, restore) emits its
// phase/progress events to a [Sink] instead of calling [log/slog]
// directly at each call-site. The concrete sink is chosen once, at
// startup, by the environment:
//
//   - [LogSink] reproduces the exact structured-log records `migrate`
//     has always emitted — byte-for-byte unchanged. It is the migrate
//     default whenever stdout is not a terminal, whenever
//     --log-format=json is requested, and whenever --no-progress is set,
//     so every automation, the JSON ingestion path, and `sluice ... |
//     tee` keep working identically. It is ALSO the nil-Migrator.Progress
//     default: a [Migrator] that never had its Progress field set behaves
//     exactly as before.
//   - [Nop] is the do-nothing sink the OTHER commands (verify, backup,
//     restore) use on the non-TTY path. Their historical output is their
//     own report (verify writes a text/JSON report to its own writer) or
//     their own direct slog lines (backup/restore) — NOT routed through a
//     sink — so the presentation layer must add nothing there. Only the
//     TTY path drives a live view for them.
//   - [TTYSink] drives a bubbletea/lipgloss live view for an operator at
//     an interactive terminal. It is command-parameterized by a [Spec]
//     (title + ordered [Phase] checklist) and is the ONLY writer to the
//     TTY while active (the CLI silences slog for the render's duration),
//     so the live phase checklist never corrupts.
//
// The interface deliberately stays small: a command's presentation is a
// handful of typed events, not a logging API. See ADR-0155 for the
// decision record and the rollout order (migrate phase 1; verify, backup,
// restore phase 2 — onto this same shared, command-parameterized sink).
package progress

import (
	"context"
)

// Phase is a command-agnostic checklist step (ADR-0155 phase 2): a stable
// Key and a human display Label. The Key identifies the phase across the
// sink boundary (the model matches checklist rows by Key; migrate's
// [LogSink] also emits it as the `phase=` attr, so migrate's Keys are the
// [ir.MigrationPhase] strings — keeping its log lines byte-identical). The
// Label is what the TTY checklist renders. Each command declares its own
// ordered phase list in a [Spec].
type Phase struct {
	Key   string
	Label string
}

// Spec parameterizes the pretty view for one command: its title, its
// ordered phase checklist, which phase (by Key) carries the inline
// per-table progress bar, and the label-column width of the summary
// panel. This is what makes the model render "whatever list it's given"
// rather than the migrate-hardcoded checklist of phase 1.
type Spec struct {
	// Title is the brand line above the checklist ("sluice migrate") and
	// the stem of the summary header ("sluice migrate - complete").
	Title string
	// Phases is the ordered checklist. Rows fill in as their Key's
	// PhaseCompleted arrives; an out-of-display-order completion still
	// fills the right row (matched by Key).
	Phases []Phase
	// ProgressKey names the phase whose row shows the inline per-table
	// bar while active (migrate's bulk copy). Empty means no phase in
	// this Spec drives a per-table bar; the active row just shows a
	// "(working...)" hint.
	ProgressKey string
	// LabelWidth is the summary panel's label column width. Zero falls
	// back to [defaultLabelWidth].
	LabelWidth int
}

// Sink receives a command's progress events. A command emits to a Sink
// instead of calling slog directly so its presentation (structured logs
// vs a live TTY view) is a startup choice, not baked into every
// call-site.
//
// Every method must be safe to call from multiple goroutines: migrate's
// bulk-copy phase drives [Sink.TableProgress] from per-table/per-chunk
// goroutines while the orchestrator drives the phase methods from the
// main goroutine.
type Sink interface {
	// PhaseStarted marks a phase as in progress. It has no historical
	// slog line (the pre-ADR-0155 orchestrator logged only on
	// completion), so [LogSink] treats it as a no-op — nothing to keep
	// byte-identical. The TTY view uses it to render the "[..]" mark.
	PhaseStarted(phase Phase)

	// PhaseCompleted marks a phase finished. migrate's [LogSink] emits
	// the exact `"migration: phase complete" phase=<key>` record the
	// orchestrator has always emitted.
	PhaseCompleted(phase Phase)

	// PhaseCompletedEarly is PhaseCompleted for a phase that finished
	// ahead of its usual slot — today only migrate's --upfront-indexes
	// index build, which runs before the bulk copy. It exists solely to
	// preserve that path's distinct historical line,
	// `"migration: phase complete (upfront)"`, byte-for-byte. A named,
	// tested wart (per the loud-failure / clean-code tenets) rather than a
	// silent reuse of PhaseCompleted that would drop the "(upfront)"
	// marker from the log stream operators grep.
	PhaseCompletedEarly(phase Phase)

	// TableProgress reports a bulk-copy table's advancing row count.
	// total is 0 until the async row-count estimate returns (the ETA
	// stays unknown in that window). It is driven by migrate's bulk-copy
	// ticker.
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
	// key/value pairs or [log/slog.Attr] values. migrate's [LogSink]
	// re-emits it as the WARN record it always was; the TTY view collects
	// it for the final summary panel.
	Warn(msg string, attrs ...any)

	// Summary renders the terminal, end-of-run result. migrate's
	// [LogSink] emits the `"migration complete" tables=N` record; the TTY
	// view replaces the live view with a compact static summary panel.
	Summary(r Result)
}

// Result is the end-of-run summary a command hands to [Sink.Summary].
type Result struct {
	// Tables is the number of tables migrated — the only field migrate's
	// historical `"migration complete"` log line carries, so it is the
	// only field migrate's [LogSink] renders. The other commands use the
	// [Nop] sink on the non-TTY path (they emit their own output), so
	// they leave Tables 0 and drive the panel entirely through Fields.
	Tables int

	// Fields are the summary-panel rows the TTY view renders, in order —
	// the per-command summary contract (verify: tables checked/clean/
	// mismatched/skipped; backup: chunks/rows/encrypted/signed/EndPosition;
	// restore: tables/rows). [LogSink] ignores them (same as migrate's Rows
	// in phase 1: the panel-only extras never enter the structured stream).
	Fields []Field
}

// Field is one label:value row of the TTY summary panel.
type Field struct {
	Label string
	Value string
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
