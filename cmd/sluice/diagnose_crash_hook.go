// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"sluicesync.dev/sluice/internal/diagnose"
	"sluicesync.dev/sluice/internal/ir"
)

// CrashHookFlags is the embeddable flag group long-running
// subcommands (sync start, migrate, sync from-backup run) embed to
// opt into the ADR-0056 auto-on-crash bundle hook.
//
// **Default-OFF**: Dir is empty unless the operator passes the flag
// explicitly. An unattended bundle landing on disk is a privacy risk
// that requires opt-in; the assembler refuses to default-on.
type CrashHookFlags struct {
	DiagnoseOnCrashDir     string `help:"Auto-write a diagnose bundle to this directory if the subcommand exits with an error (ADR-0056). Off by default. Opt-in only — an unattended bundle landing on disk is a privacy risk." placeholder:"DIR"`
	DiagnoseOnCrashPrivacy string `help:"Privacy level for auto-on-crash bundles (basic|standard|verbose). ADR-0056 defaults to 'basic' (safest) so an unattended bundle never carries DSN locators, version metadata, or logs without explicit opt-up. Only consulted when --diagnose-on-crash-dir is set." default:"basic" enum:"basic,standard,verbose" placeholder:"LEVEL"`
}

// installCrashHook wires the auto-on-crash bundle writer for a long-
// running subcommand. Returns a function the caller defers + invokes
// with the subcommand's exit error (the hook's Wrap method); when
// crash-hook flags are unset, the returned function is a no-op that
// returns runErr unchanged.
//
// Refuses loudly if the operator passed --diagnose-on-crash-dir but
// the directory doesn't exist or isn't writable; failing fast here
// beats failing mid-crash with a useless secondary error.
func installCrashHook(flags CrashHookFlags, req diagnose.Request) (func(error) error, error) {
	if flags.DiagnoseOnCrashDir == "" {
		return func(err error) error { return err }, nil
	}
	level, err := diagnose.ParsePrivacyLevel(flags.DiagnoseOnCrashPrivacy)
	if err != nil {
		return nil, fmt.Errorf("--diagnose-on-crash-privacy: %w", err)
	}
	hook, ok, err := diagnose.Install(diagnose.CrashHookConfig{
		Dir:             flags.DiagnoseOnCrashDir,
		PrivacyLevel:    level,
		RequestTemplate: req,
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		return func(err error) error { return err }, nil
	}
	fmt.Fprintf(os.Stderr, "sluice: --diagnose-on-crash-dir=%s active (privacy=%s)\n",
		flags.DiagnoseOnCrashDir, level)

	return func(runErr error) error {
		return hook.Wrap(kongContext(), runErr)
	}, nil
}

// crashHookRequestForStreamer builds the [diagnose.Request] template
// for the auto-on-crash hook from a sync-start invocation. The
// PrivacyLevel + CrashContext fields are filled in at hook fire-time
// by [diagnose.CrashHook.Wrap].
func crashHookRequestForStreamer(streamID string, source, target ir.Engine, sourceDSN, targetDSN, slotName string) diagnose.Request {
	return diagnose.Request{
		StreamID:        streamID,
		SourceEngine:    source,
		SourceDSN:       sourceDSN,
		TargetEngine:    target,
		TargetDSN:       targetDSN,
		SlotName:        slotName,
		SluiceVersion:   version,
		SluiceCommit:    commit,
		SluiceBuildDate: date,
		CLIArgs:         os.Args[1:],
	}
}
