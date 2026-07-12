// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"testing"

	"github.com/mattn/go-isatty"
)

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
