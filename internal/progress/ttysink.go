// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package progress

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"sluicesync.dev/sluice/internal/ir"
)

// TTYSink is the interactive [Sink]: it drives a bubbletea/lipgloss live
// view (the phase checklist + per-table bar + final summary panel) on the
// terminal. Each Sink call is translated into a message and handed to the
// running program via Send, which is goroutine-safe — the bulk-copy ticker
// calls TableProgress from copy goroutines while the orchestrator drives
// the phase methods from the main goroutine.
//
// bubbletea owns stdout for the render's duration; the CLI silences slog
// (which writes stderr) while a TTYSink is active so the two never
// interleave on a shared terminal (ADR-0155: the pretty sink is the ONLY
// writer to the TTY). On completion the live view is replaced by a compact
// static summary so scrollback stays clean.
//
// Lifecycle: the CLI constructs the sink (starting the program in a
// goroutine), sets it as [Migrator.Progress], runs the migration, then
// calls [TTYSink.Close] to finalize. [Sink.Summary] is the terminal event
// — it quits the program, which prints the final summary View.
type TTYSink struct {
	prog *tea.Program
	w    io.Writer
	done chan struct{}
	// onInterrupt is invoked once, after the program returns, iff the
	// operator interrupted (ctrl+c) or the program was killed by a
	// signal. The CLI wires it to cancel the migration context so an
	// abort at the pretty view actually stops the run.
	onInterrupt func()
}

// compile-time assertion that *TTYSink satisfies Sink.
var _ Sink = (*TTYSink)(nil)

// NewTTYSink starts a bubbletea program rendering to w and returns a sink
// that feeds it. onInterrupt (may be nil) is called after the program
// exits if the operator aborted the view. The program runs inline (no
// alternate screen) so the final summary stays in scrollback.
func NewTTYSink(w io.Writer, onInterrupt func()) *TTYSink {
	m := newModel(time.Now(), time.Now)
	p := tea.NewProgram(m, tea.WithOutput(w))
	s := &TTYSink{prog: p, w: w, done: make(chan struct{}), onInterrupt: onInterrupt}
	go func() {
		defer close(s.done)
		final, err := p.Run()
		fm, _ := final.(model)
		if (err != nil || fm.interrupted) && s.onInterrupt != nil {
			s.onInterrupt()
		}
		// Print the summary panel HERE — after bubbletea has released the
		// terminal — as a plain write, so the inline renderer can't clip the
		// box's bottom border on quit (View returns "" once done, clearing
		// the live checklist first). Only when the run reached a summary
		// (fm.done); an abort/interrupt leaves nothing printed, and its
		// error surfaces through the CLI's error path.
		if fm.done && s.w != nil {
			_, _ = io.WriteString(s.w, fm.summaryView()+"\n")
		}
	}()
	return s
}

func (s *TTYSink) PhaseStarted(phase ir.MigrationPhase) {
	s.prog.Send(phaseStartedMsg{phase: phase})
}

func (s *TTYSink) PhaseCompleted(phase ir.MigrationPhase) {
	s.prog.Send(phaseCompletedMsg{phase: phase})
}

// PhaseCompletedEarly marks the same phase done; the "(upfront)"
// distinction is a log-stream concern (see [LogSink]) with no bearing on
// the checklist, which just fills the row in.
func (s *TTYSink) PhaseCompletedEarly(phase ir.MigrationPhase) {
	s.prog.Send(phaseCompletedMsg{phase: phase})
}

func (s *TTYSink) TableProgress(table string, done, total int64) {
	s.prog.Send(tableProgressMsg{table: table, done: done, total: total})
}

func (s *TTYSink) Warn(msg string, attrs ...any) {
	s.prog.Send(warnMsg{text: warnLine(msg, attrs...)})
}

func (s *TTYSink) Summary(r Result) {
	s.prog.Send(summaryMsg{result: r})
}

// Close finalizes the view: it quits the program (a no-op if [Sink.Summary]
// already did) and waits for the render goroutine to return, so the final
// summary is fully flushed before the CLI restores slog and prints
// anything else. Safe to call exactly once at the end of the run.
func (s *TTYSink) Close() {
	s.prog.Quit()
	<-s.done
}

// warnLine renders a warning message plus its slog-style attrs into a
// single compact human line for the summary panel. attrs follow slog's
// convention: [slog.Attr] values (what the degraded-FK reporter passes)
// or alternating key/value pairs. Attrs are rendered "key=value" and
// space-joined after the message; the message alone is used when there
// are none.
func warnLine(msg string, attrs ...any) string {
	pairs := attrPairs(attrs)
	if len(pairs) == 0 {
		return msg
	}
	return msg + " (" + strings.Join(pairs, " ") + ")"
}

// attrPairs flattens slog-style args into "key=value" strings. It accepts
// both [slog.Attr] values and alternating key/value pairs; a trailing
// unpaired key is rendered "key=!MISSING" (mirroring slog's own bad-key
// handling) so a mis-call is visible rather than dropped.
func attrPairs(args []any) []string {
	var out []string
	for i := 0; i < len(args); i++ {
		switch a := args[i].(type) {
		case slog.Attr:
			out = append(out, a.Key+"="+a.Value.String())
		case string:
			if i+1 < len(args) {
				out = append(out, a+"="+fmt.Sprint(args[i+1]))
				i++
			} else {
				out = append(out, a+"=!MISSING")
			}
		default:
			out = append(out, fmt.Sprint(a))
		}
	}
	return out
}
