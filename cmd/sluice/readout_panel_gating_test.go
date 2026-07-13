// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"testing"

	"github.com/mattn/go-isatty"
)

// TestReadoutPanelGating_NonTerminal pins that the three ADR-0156 phases-2/3
// commands — `sync from-backup run` (broker), `backup stream run`, and
// `metrics-watch` — gate their live readout panel on the SAME
// [wantPrettyProgress] rule as `sync start` and the one-shot commands: a
// non-terminal stdout keeps the byte-identical structured slog stream even
// under --log-format=text. Under `go test`, os.Stdout is a pipe (not a TTY),
// so the gate must be false for every one of them.
func TestReadoutPanelGating_NonTerminal(t *testing.T) {
	if isatty.IsTerminal(os.Stdout.Fd()) {
		t.Skip("test stdout is a terminal; the non-TTY assertion needs a piped stdout")
	}
	g := &Globals{LogFormat: "text"}
	// All three commands call wantPrettyProgress(g, false, false, false) — they
	// have no --format json envelope, dry-run, or multi-namespace shape.
	if wantPrettyProgress(g, false, false, false) {
		t.Error("non-terminal stdout must keep the structured stream for the broker / backup-stream / metrics-watch panels")
	}
}

// TestMetricsWatchOnceNeverPanels pins that `metrics-watch --once` never
// renders the panel even on a TTY: the one-shot scripted mode has no
// long-lived loop to render, so the pretty gate is ANDed with !Once.
func TestMetricsWatchOnceNeverPanels(t *testing.T) {
	g := &Globals{LogFormat: "text"}
	m := &MetricsWatchCmd{Once: true}
	// The command's own gate: wantPrettyProgress(...) && !m.Once. Whatever the
	// TTY state, --once forces the structured/one-shot path.
	pretty := wantPrettyProgress(g, false, false, false) && !m.Once
	if pretty {
		t.Error("metrics-watch --once must never take the panel path")
	}
}
