// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"os"

	"sluicesync.dev/sluice/internal/diagnose"
)

// DiagnoseCmd implements `sluice diagnose` — the operator-bundle
// assembler. ADR-0056.
//
// Two invocation paths exist:
//
//  1. Operator-initiated: `sluice diagnose --stream-id X --output
//     bundle.zip --privacy=standard`. Produces a ZIP suitable for
//     attaching to a GitHub issue.
//  2. Automatic on crash: the long-running subcommands (sync start,
//     migrate, sync from-backup run) expose a `--diagnose-on-crash-dir`
//     flag that, when set, installs an [diagnose.CrashHook] writing a
//     bundle to the named directory whenever the subcommand exits with
//     an error. Opt-in only (default off).
//
// **Exit codes.** Mirror `sluice verify`/`sluice diff`:
//
//   - 0 — bundle written successfully.
//   - 2 — operational error (couldn't open the target, couldn't write
//     the ZIP, etc.).
//
// The diagnose subcommand does NOT have a "drift" / threshold-style
// exit code 1; the bundle either assembles or it doesn't.
type DiagnoseCmd struct {
	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`

	SourceDriver string `help:"Source engine name (optional). When set together with --source, the bundle also includes a source-side engine snapshot + cross-engine health probe." placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN (optional). See --source-driver." env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`
	SlotName     string `help:"Replication-slot name on the source (PG-only, requires --source). Defaults to 'sluice_slot' on PG sources." placeholder:"NAME" group:"source"`

	StreamID string `help:"Stream identifier the bundle is scoped to. Required — the bundle without a stream ID would be meaningless." required:"" placeholder:"ID"`
	Output   string `help:"Path to write the bundle ZIP to." required:"" short:"o" placeholder:"PATH"`

	Privacy string `help:"Privacy level (basic|standard|verbose). basic: state-table dumps only — no version, no DSN, no logs. standard: + redacted CLI args, sluice version, engine health probes, capabilities. verbose: + per-table COUNT(*) on the target (slow path on large tables) + last 200 lines of --log-file. ADR-0056 has the full inclusion/exclusion contract." default:"standard" enum:"basic,standard,verbose" placeholder:"LEVEL"`

	LogFile string `help:"Path to sluice's slog output file. PrivacyVerbose includes the last 200 lines in the bundle. Empty disables." placeholder:"PATH"`
}

// Run implements `sluice diagnose`.
func (d *DiagnoseCmd) Run(_ *Globals) error {
	target, err := resolveEngine(d.TargetDriver)
	if err != nil {
		return operationalError{err: fmt.Errorf("--target-driver: %w", err)}
	}
	req := diagnose.Request{
		StreamID:        d.StreamID,
		TargetEngine:    target,
		TargetDSN:       d.Target,
		SluiceVersion:   version,
		SluiceCommit:    commit,
		SluiceBuildDate: date,
		CLIArgs:         os.Args[1:],
		LogFile:         d.LogFile,
		SlotName:        d.SlotName,
	}
	if d.SourceDriver != "" {
		source, err := resolveEngine(d.SourceDriver)
		if err != nil {
			return operationalError{err: fmt.Errorf("--source-driver: %w", err)}
		}
		req.SourceEngine = source
		req.SourceDSN = d.Source
	} else if d.Source != "" {
		return operationalError{err: errors.New("--source given without --source-driver")}
	}

	level, err := diagnose.ParsePrivacyLevel(d.Privacy)
	if err != nil {
		return operationalError{err: err}
	}
	req.PrivacyLevel = level

	f, err := os.Create(d.Output) //nolint:gosec // operator-supplied output path
	if err != nil {
		return operationalError{err: fmt.Errorf("create output %q: %w", d.Output, err)}
	}
	defer func() { _ = f.Close() }()

	ctx := kongContext()
	if err := diagnose.Write(ctx, f, req); err != nil {
		// Clean up the partial file — a half-written zip is just
		// noise on the operator's disk. Best-effort; if removal
		// fails we still surface the underlying error.
		_ = f.Close()
		_ = os.Remove(d.Output)
		return operationalError{err: fmt.Errorf("assemble bundle: %w", err)}
	}
	fmt.Fprintf(os.Stdout, "diagnose bundle written to %s (privacy=%s, stream=%s)\n", d.Output, level, d.StreamID)
	return nil
}
