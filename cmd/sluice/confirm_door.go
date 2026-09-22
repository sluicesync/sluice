// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/mattn/go-isatty"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// The destructive-prompt door (audit GC-13).
//
// AGENTS.md promises agents that no sluice command blocks on a prompt. Six
// sites in this package read a confirmation from os.Stdin, and on a pipe,
// an EOF or a CI runner every one of them read "" as a decline. Five of
// them returned the non-zero errConfirmDeclined; `trigger teardown`
// printed "aborted" and returned nil — exit 0 with the trigger engine
// still installed. `slot drop` had already been rebuilt to refuse instead
// of prompting for exactly this reason, and its sibling was never
// enumerated.
//
// The policy, applied to every prompt site uniformly: a destructive prompt
// may only fire when stdin is a real terminal. Anywhere else — an agent, a
// script, a CI job, a closed or silent pipe — the command refuses with
// SLUICE-E-CONFIRMATION-REQUIRED (exit 3) BEFORE printing the prompt and
// before touching either database, naming --yes as the remedy. On a
// terminal the interactive friction tiers stay exactly as designed (the
// typed `reset` token of ADR-0023, the table name for add-table, y/N for
// teardown), and a decline there is errConfirmDeclined (exit 1), never a
// nil return.
//
// `slot drop` and `sync decommission` refuse without --yes on a terminal
// too; that is a stricter subset of the same rule, not a divergence.
//
// TestStdinPromptRoster_EveryReadIsDoored (internal/docsync) is the gate:
// it derives every os.Stdin argument in this package from the AST and
// fails on one that is not rostered, or whose enclosing function does not
// call refuseUnlessTerminal before it.

// stdinIsTerminal is the door's one environmental probe. It is a
// variable so the command tests can pin BOTH branches of the door without
// owning a pseudo-terminal: withCommandIO feeds stdin from a pipe, which
// is exactly the non-terminal shape, and asTerminalStdin flips the probe
// for the prompt-path tests.
var stdinIsTerminal = defaultStdinIsTerminal

// defaultStdinIsTerminal reports whether the process's stdin is a
// terminal. The only os.Stdin use in this package that reads nothing.
func defaultStdinIsTerminal() bool {
	return isatty.IsTerminal(os.Stdin.Fd())
}

// refuseUnlessTerminal is the door every destructive prompt walks
// through first. action names what the command was about to ask
// permission for, so the refusal reads as a sentence about THIS command
// rather than a generic policy line.
func refuseUnlessTerminal(action string) error {
	if stdinIsTerminal() {
		return nil
	}
	return &sluicecode.CodedError{
		Code: sluicecode.CodeConfirmationRequired,
		Hint: "pass --yes (or -y) to confirm; sluice only prompts when stdin is a terminal",
		Err:  fmt.Errorf("%s is destructive and needs confirmation, but stdin is not a terminal so sluice cannot ask", action),
	}
}

// confirmResetTargetData is the --reset-target-data tier (ADR-0023): the
// door, then the typed `reset` prompt. Shared by migrate, sync start and
// sync from-backup run — the three used to carry a copy each. out is
// where the prompt lands (stderr under a --format json envelope, so
// stdout keeps its single JSON object). Returns nil to proceed, the
// coded refusal off a non-terminal stdin, or errConfirmDeclined.
func confirmResetTargetData(ctx context.Context, out io.Writer) error {
	if err := refuseUnlessTerminal("--reset-target-data (DROP tables on the target)"); err != nil {
		return err
	}
	ok, err := confirmTypedDestructive(ctx, os.Stdin, out,
		"This will DROP tables on the target. Type 'reset' to confirm: ", "reset")
	if err != nil {
		return err
	}
	if !ok {
		return errConfirmDeclined
	}
	return nil
}
