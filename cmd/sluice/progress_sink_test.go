// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/mattn/go-isatty"

	"sluicesync.dev/sluice/internal/progress"
)

// recordingSink is a progress.Sink that records Warn calls, for asserting
// that WARN records emitted during a pretty render are forwarded into the
// view rather than written raw to the terminal.
type recordingSink struct {
	progress.LogSink // no-op embeds for the methods we don't assert on
	warns            []string
}

func (s *recordingSink) Warn(msg string, _ ...any) {
	s.warns = append(s.warns, msg)
}

// TestSilenceSlogForTTY_ForwardsWarnToSink pins Fix 2: while the pretty
// view owns the terminal, a WARN record (e.g. the postgres writer's
// collation drop) is forwarded to the sink so it lands in the summary box's
// Warnings section — not written raw to stderr where it would collide with
// the render. INFO/DEBUG records are dropped entirely.
func TestSilenceSlogForTTY_ForwardsWarnToSink(t *testing.T) {
	sink := &recordingSink{}
	restore := silenceSlogForTTY(sink)
	slog.InfoContext(context.Background(), "phase progress that must be suppressed")
	slog.WarnContext(context.Background(), "dropped collation on column",
		slog.String("column", "name"), slog.String("collation", "en_US"))
	restore(false) // success path: no raw flush

	if len(sink.warns) != 1 {
		t.Fatalf("want exactly 1 forwarded warn, got %d: %v", len(sink.warns), sink.warns)
	}
	if sink.warns[0] != "dropped collation on column" {
		t.Errorf("forwarded warn = %q, want the collation message", sink.warns[0])
	}
}

// TestWantPrettyProgress_NonTerminalSelectsLogSink pins the ADR-0155
// contract that a non-terminal stdout selects the structured-log sink even
// under --log-format=text. Under `go test`, os.Stdout is a pipe (not a
// TTY), so this asserts the real isatty gate rejects the pretty view.
func TestWantPrettyProgress_NonTerminalSelectsLogSink(t *testing.T) {
	if isatty.IsTerminal(os.Stdout.Fd()) {
		t.Skip("test stdout is a terminal; the non-TTY assertion needs a piped stdout")
	}
	g := &Globals{LogFormat: "text"} // the pretty-eligible format
	if wantPrettyProgress(g, false, false, false) {
		t.Error("non-terminal stdout must select the log sink even with --log-format=text")
	}
}

// TestWantPrettyProgress_Guards pins the forcing conditions that select the
// log sink regardless of TTY: --no-progress, --log-format=json, the JSON
// result-envelope, --dry-run, and multi-namespace fan-out.
func TestWantPrettyProgress_Guards(t *testing.T) {
	cases := []struct {
		name           string
		g              *Globals
		jsonEnvelope   bool
		dryRun         bool
		multiNamespace bool
	}{
		{"no-progress", &Globals{LogFormat: "text", NoProgress: true}, false, false, false},
		{"log-format json", &Globals{LogFormat: "json"}, false, false, false},
		{"json envelope", &Globals{LogFormat: "text"}, true, false, false},
		{"dry-run", &Globals{LogFormat: "text"}, false, true, false},
		{"multi-namespace", &Globals{LogFormat: "text"}, false, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if wantPrettyProgress(tc.g, tc.jsonEnvelope, tc.dryRun, tc.multiNamespace) {
				t.Errorf("%s must force the log sink", tc.name)
			}
		})
	}
}
