// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/alecthomas/kong"
)

// TestTriggerSetup_CaptureReplicatedWritesFlag pins the ADR-0185 opt-in
// THROUGH the real kong parser — the Bug-180 lesson: a flag-gated branch
// pinned only by direct SetupOptions construction can be unreachable from
// the CLI (a default, an enum collapse, a missed field thread), and every
// engine-level test here constructs SetupOptions directly. The parse-level
// pin plus the field thread in TriggerSetupCmd.Run (which copies the field
// verbatim into pgtrigger.SetupOptions) is what makes the engine tests'
// coverage reachable by an operator.
func TestTriggerSetup_CaptureReplicatedWritesFlag(t *testing.T) {
	parse := func(t *testing.T, args ...string) *CLI {
		t.Helper()
		cli := &CLI{}
		parser, err := kong.New(cli, kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
		if err != nil {
			t.Fatalf("kong.New: %v", err)
		}
		if _, err := parser.Parse(args); err != nil {
			t.Fatalf("parse %v: %v", args, err)
		}
		return cli
	}

	base := []string{"trigger", "setup", "--dsn=postgres://localhost/db", "--tables=orders"}

	t.Run("omitted → false (plain triggers, the zero-value-safe default)", func(t *testing.T) {
		cli := parse(t, base...)
		if cli.Trigger.Setup.CaptureReplicatedWrites {
			t.Fatal("omitted --capture-replicated-writes parsed true; the default must be the plain (origin-only) posture")
		}
	})

	t.Run("explicit flag → true", func(t *testing.T) {
		cli := parse(t, append(append([]string{}, base...), "--capture-replicated-writes")...)
		if !cli.Trigger.Setup.CaptureReplicatedWrites {
			t.Fatal("--capture-replicated-writes parsed false; the opt-in branch is unreachable from the CLI (Bug 180 shape)")
		}
	})
}
