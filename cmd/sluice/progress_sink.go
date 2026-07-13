// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/mattn/go-isatty"

	"sluicesync.dev/sluice/internal/progress"
)

// wantPrettyProgress reports whether the interactive TTY progress view
// should drive a migrate run (ADR-0155). It is the pretty view ONLY when
// every condition holds:
//
//   - stdout is a terminal (isatty) — piped/redirected/CI stdout gets the
//     structured-log sink so automation is unchanged;
//   - --log-format=text — --log-format=json always forces the log sink
//     (structured wins when explicitly requested);
//   - --no-progress is not set — the operator's explicit escape hatch;
//   - the run is not the `--format json` result-envelope path (that owns
//     stdout for its single JSON object), not a --dry-run (which prints a
//     plan, not phase progress), and not a multi-namespace fan-out (which
//     emits a per-database summary the single live view can't represent).
//
// Any of those falling out selects the byte-identical [progress.LogSink].
func wantPrettyProgress(g *Globals, jsonEnvelope, dryRun, multiNamespace bool) bool {
	if g.NoProgress {
		return false
	}
	if !strings.EqualFold(g.LogFormat, "text") {
		return false
	}
	if jsonEnvelope || dryRun || multiNamespace {
		return false
	}
	return isatty.IsTerminal(os.Stdout.Fd())
}

// silenceSlogForTTY makes bubbletea the ONLY writer to the terminal while
// the pretty view renders (ADR-0155): it swaps slog.Default with a gate
// handler that DROPS records below WARN and, for WARN/ERROR, does two
// things — (1) FORWARDS them to sink.Warn so they render inside the live
// view's Warnings section (e.g. the postgres writer's collation-drop
// warning), and (2) BUFFERS them for the failure path. Neither writes to
// stderr during the render, so the pipeline's stray slog lines can't
// interleave with — and corrupt — the box.
//
// The returned restore(flush) reinstalls the previous handler and, ONLY
// when flush is true (the failure path, where sink.Summary never rendered
// the box so the warnings would otherwise be lost), writes the buffered
// records to stderr after a blank-line separator. On the success path the
// warnings are already in the summary box, so nothing is flushed and no
// raw line collides with the render. The pretty view only runs under
// --log-format=text, so the buffered records keep their normal formatting.
func silenceSlogForTTY(sink progress.Sink) func(flush bool) {
	prev := slog.Default()
	// slog serialises writes to the underlying io.Writer internally, and by
	// the time restore runs every pipeline goroutine has quiesced (Run has
	// returned and the TTYSink program has been Closed), so a plain buffer
	// is safe here.
	var buf bytes.Buffer
	gate := &warnGateHandler{
		sink:   sink,
		buffer: slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}),
	}
	slog.SetDefault(slog.New(gate))
	return func(flush bool) {
		slog.SetDefault(prev)
		if flush && buf.Len() > 0 {
			_, _ = os.Stderr.WriteString("\n")
			_, _ = os.Stderr.Write(buf.Bytes())
		}
	}
}

// warnGateHandler is the slog.Handler installed while the pretty view owns
// the terminal. It suppresses records below WARN entirely; WARN/ERROR
// records are forwarded to the presentation sink (so they land in the live
// view's Warnings section) AND buffered (so the failure path, where the
// summary box never renders, can still surface them on stderr).
type warnGateHandler struct {
	sink   progress.Sink
	buffer slog.Handler
}

func (h *warnGateHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelWarn
}

func (h *warnGateHandler) Handle(ctx context.Context, r slog.Record) error {
	attrs := make([]any, 0, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a)
		return true
	})
	h.sink.Warn(r.Message, attrs...)
	return h.buffer.Handle(ctx, r)
}

func (h *warnGateHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &warnGateHandler{sink: h.sink, buffer: h.buffer.WithAttrs(attrs)}
}

func (h *warnGateHandler) WithGroup(name string) slog.Handler {
	return &warnGateHandler{sink: h.sink, buffer: h.buffer.WithGroup(name)}
}

// runMigrateWithProgress runs fn (the migration) under the chosen
// presentation sink. When pretty is false it just runs fn — the sink is
// the [progress.LogSink] default, byte-identical to before. When pretty is
// true it starts a [progress.TTYSink] on stdout, silences slog for the
// render (forwarding WARN/ERROR into the view), sets the sink on the
// migrator via setSink, runs, then finalizes: quit the view, restore slog,
// and flush the buffered warnings/error to stderr ONLY on failure (when the
// summary box never rendered). cancel is the migration context's cancel,
// wired as the view's ctrl+c handler so an abort at the pretty view
// actually stops the run.
func runMigrateWithProgress(pretty bool, cancel func(), setSink func(progress.Sink), fn func() error) error {
	if !pretty {
		return fn()
	}
	sink := progress.NewTTYSink(os.Stdout, cancel)
	setSink(sink)
	restore := silenceSlogForTTY(sink)
	err := fn()
	sink.Close()
	restore(err != nil)
	return err
}
