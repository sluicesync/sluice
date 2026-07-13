// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package progress

import (
	"io"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// LiveTTYSink is the interactive driver for the CONTINUOUS `sync start` live
// panel (ADR-0156). It is the sibling of [TTYSink]: it drives a
// bubbletea/lipgloss [livePanel] on the terminal, but with a continuous
// contract — the view is persistent for the process lifetime and never renders
// a terminal summary.
//
// It satisfies [Sink] so the shared bulk-copy phase's existing [FromContext]
// events (phase checklist + per-table bar) feed the panel's initial-copy
// section with NO orchestrator changes. The continuous CDC-mode signals
// (EnterCDC / Status / Health / Event) are driven by the CLI-side status poller
// and slog gate.
//
// Renderer isolation (ADR-0156, load-bearing): the sync goroutine and this
// renderer are SEPARATE. The program runs in its own goroutine; a panic inside
// it is recovered and reported via onRendererPanic (the CLI restores slog and
// keeps the stream on the structured-log path) — a renderer failure never
// aborts the sync.
type LiveTTYSink struct {
	prog *tea.Program
	w    io.Writer
	done chan struct{}

	// onRendererPanic (may be nil) is invoked once if the bubbletea program
	// panics, so the CLI can restore structured logging and keep the sync
	// running on the log path instead of the (now-dead) panel.
	onRendererPanic func(any)
}

// compile-time assertion that *LiveTTYSink satisfies Sink.
var _ Sink = (*LiveTTYSink)(nil)

// NewLiveTTYSink starts a bubbletea program rendering the continuous panel to w
// and returns a sink that feeds it. spec parameterizes the initial-copy
// checklist (reuse [MigrateProgressSpec]); header is the static identity;
// stopCmd is the drain-and-stop side effect returned on q/ctrl+c (may be nil);
// onRendererPanic (may be nil) is called if the renderer panics.
//
// The program runs inline (no alternate screen) so the final line stays in
// scrollback, mirroring [NewTTYSink].
func NewLiveTTYSink(w io.Writer, spec Spec, header LiveHeader, stopCmd tea.Cmd, onRendererPanic func(any)) *LiveTTYSink {
	return startLiveSink(w, newLivePanel(spec, header, stopCmd, time.Now), onRendererPanic)
}

// startLiveSink runs panel as a bubbletea program on w in its own goroutine
// and wraps it in a [LiveTTYSink]. Shared by [NewLiveTTYSink] (the phase-1
// `sync start` panel) and [NewLiveReadoutTTYSink] (the phases-2/3 readout
// panels) so the renderer-isolation contract is written once: a panic inside
// the program is recovered HERE (never unwinding into the command's
// goroutine) and reported via onRendererPanic.
func startLiveSink(w io.Writer, panel livePanel, onRendererPanic func(any)) *LiveTTYSink {
	p := tea.NewProgram(panel, tea.WithOutput(w))
	s := &LiveTTYSink{prog: p, w: w, done: make(chan struct{}), onRendererPanic: onRendererPanic}
	go func() {
		defer close(s.done)
		defer func() {
			if r := recover(); r != nil {
				// Renderer isolation: swallow the panic HERE so it cannot
				// unwind into (and kill) the command's goroutine. The CLI's
				// handler restores slog and keeps the command on the
				// structured-log path.
				if s.onRendererPanic != nil {
					s.onRendererPanic(r)
				}
			}
		}()
		_, _ = p.Run()
	}()
	return s
}

// NewLiveReadoutTTYSink starts a bubbletea program rendering the GENERIC
// readout panel (ADR-0156 phases 2/3) to w and returns a sink that feeds it.
// It is the constructor for the broker / backup-stream / metrics-watch
// panels: same header / events / footer chrome + drain-and-stop as
// [NewLiveTTYSink], but the body is the ordered label/value list pushed via
// [LiveTTYSink.Readout] rather than the `sync start` initial-copy checklist /
// CDC body. header carries the mode word + identity line; stopCmd is the
// drain-and-stop side effect returned on q/ctrl+c; onRendererPanic (may be
// nil) is called if the renderer panics.
func NewLiveReadoutTTYSink(w io.Writer, header LiveHeader, stopCmd tea.Cmd, onRendererPanic func(any)) *LiveTTYSink {
	return startLiveSink(w, newReadoutPanel(header, stopCmd, time.Now), onRendererPanic)
}

// Readout pushes one refresh of the GENERIC label/value readout body (ADR-0156
// phases 2/3). Safe to call from the command's tick/sample loop; the panel
// replaces its body with the supplied fields on the next frame.
func (s *LiveTTYSink) Readout(fields []Field) { s.prog.Send(readoutMsg{fields: fields}) }

// --- Sink (initial-copy feed) ---

func (s *LiveTTYSink) PhaseStarted(phase Phase) {
	s.prog.Send(phaseStartedMsg{key: phase.Key})
}

func (s *LiveTTYSink) PhaseCompleted(phase Phase) {
	s.prog.Send(phaseCompletedMsg{key: phase.Key})
}

func (s *LiveTTYSink) PhaseCompletedEarly(phase Phase) {
	s.prog.Send(phaseCompletedMsg{key: phase.Key})
}

func (s *LiveTTYSink) TableProgress(table string, done, total int64) {
	s.prog.Send(tableProgressMsg{table: table, done: done, total: total})
}

func (s *LiveTTYSink) Warn(msg string, attrs ...any) {
	s.prog.Send(warnMsg{text: warnLine(msg, attrs...)})
}

// Summary is a no-op for the continuous panel: a `sync start` cold-start hands
// off to CDC rather than ending in a summary, and the streamer never drives
// Summary on this path. Present only to satisfy [Sink].
func (s *LiveTTYSink) Summary(Result) {}

// --- continuous CDC-mode feed (driven by the CLI poller / slog gate) ---

// EnterCDC flips the panel to CDC mode (the snapshot->CDC handoff). Idempotent.
func (s *LiveTTYSink) EnterCDC() { s.prog.Send(enterCDCMsg{}) }

// Status reports the latest CDC apply position + freshness reading.
func (s *LiveTTYSink) Status(st LiveStatus) { s.prog.Send(statusMsg{status: st}) }

// HealthConnected marks the stream connected (a successful status read),
// carrying the cumulative reconnect count.
func (s *LiveTTYSink) HealthConnected(restarts int) {
	s.prog.Send(healthMsg{state: healthConnected, restarts: restarts})
}

// HealthReconnecting marks the stream as reconnecting (a failed status read),
// carrying the cumulative reconnect count. The enum stays unexported; these
// named wrappers are the CLI-facing surface.
func (s *LiveTTYSink) HealthReconnecting(restarts int) {
	s.prog.Send(healthMsg{state: healthReconnecting, restarts: restarts})
}

// Event forwards a WARN/ERROR record into the bounded recent-events ring.
func (s *LiveTTYSink) Event(level, text string) {
	s.prog.Send(eventMsg{level: level, text: text})
}

// Quit ends the program (a no-op if the operator already force-quit) and waits
// for the render goroutine to return. Safe to call exactly once.
func (s *LiveTTYSink) Quit() {
	s.prog.Quit()
	<-s.done
}

// Wait blocks until the render goroutine returns — i.e. until the operator
// force-quits (a second q/ctrl+c) or [Quit] is called. Used by the CLI to
// select against the streamer's exit.
func (s *LiveTTYSink) Wait() { <-s.done }

// NewStopResultMsg builds the message a drain-and-stop [tea.Cmd] returns so the
// CLI can report the RequestStop outcome (nil on success) into the panel's
// footer without the unexported message type leaking out of this package.
func NewStopResultMsg(err error) tea.Msg { return stopRequestedMsg{err: err} }
