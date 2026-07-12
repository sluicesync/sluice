// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
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
// the pretty view renders (ADR-0155): it swaps slog.Default with a handler
// that DROPS records below WARN and BUFFERS WARN/ERROR, so the pipeline's
// stray slog lines (the progress ticker, row-count probe warnings, ...)
// can't interleave with — and corrupt — the live render on a shared
// terminal. The returned restore func reinstalls the previous handler and
// flushes the buffered WARN/ERROR records to stderr, so no warning is lost;
// it simply surfaces after the static summary. The pretty view only runs
// under --log-format=text, so a text handler reproduces the buffered
// records' normal formatting.
func silenceSlogForTTY() func() {
	prev := slog.Default()
	// slog serialises writes to the underlying io.Writer internally, and by
	// the time restore runs every pipeline goroutine has quiesced (Run has
	// returned and the TTYSink program has been Closed), so a plain buffer
	// is safe here.
	var buf bytes.Buffer
	gate := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})
	slog.SetDefault(slog.New(gate))
	return func() {
		slog.SetDefault(prev)
		if buf.Len() > 0 {
			_, _ = os.Stderr.Write(buf.Bytes())
		}
	}
}

// runMigrateWithProgress runs fn (the migration) under the chosen
// presentation sink. When pretty is false it just runs fn — the sink is
// the [progress.LogSink] default, byte-identical to before. When pretty is
// true it starts a [progress.TTYSink] on stdout, silences slog for the
// render, sets the sink on the migrator via setSink, runs, then finalizes
// (quit the view, restore slog, flush buffered warnings). cancel is the
// migration context's cancel, wired as the view's ctrl+c handler so an
// abort at the pretty view actually stops the run.
func runMigrateWithProgress(pretty bool, cancel func(), setSink func(progress.Sink), fn func() error) error {
	if !pretty {
		return fn()
	}
	sink := progress.NewTTYSink(os.Stdout, cancel)
	setSink(sink)
	restore := silenceSlogForTTY()
	err := fn()
	sink.Close()
	restore()
	return err
}
