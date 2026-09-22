// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// promptSite is one destructive-confirmation site driven through its real
// Run(). Every site the AST roster in internal/docsync enumerates has a
// row here, so the policy is pinned per site, not per representative:
// the door is one function, but each caller wires it in by hand and a
// site that forgot would pass a single-site test.
type promptSite struct {
	name string
	// run executes the command with the given --yes value and --format
	// ("" for commands without a --format flag).
	run func(yes bool, format string) error
	// formats lists the --format values the command accepts; "" means
	// the command has no --format flag and only the text shape applies.
	formats []string
	// promptText is a fragment of the prompt the site prints on a
	// terminal; the non-terminal refusal must never print it.
	promptText string
	// accept is the stdin line that passes the prompt on a terminal.
	accept string
}

// destructivePromptSites builds the roster. Every command targets an
// unreachable port-1 DSN (or a malformed one) so a run that clears the
// door fails fast, locally, and dial-free on a DOWNSTREAM error —
// proving the door was passed rather than short-circuited.
func destructivePromptSites(t *testing.T) []promptSite {
	t.Helper()
	return []promptSite{
		{
			name:       "migrate --reset-target-data",
			formats:    []string{"text", "json"},
			promptText: confirmPromptText,
			accept:     "reset\n",
			run: func(yes bool, format string) error {
				cmd := &MigrateCmd{}
				cmd.SourceDriver = "postgres"
				cmd.Source = "postgres://u:pw@127.0.0.1:1/src?sslmode=disable"
				cmd.TargetDriver = "postgres"
				cmd.Target = "postgres://u:pw@127.0.0.1:1/dst?sslmode=disable"
				cmd.Format = format
				cmd.ResetTargetData = true
				cmd.Yes = yes
				cmd.InjectShardColumn = "bogus-no-equals"
				return cmd.Run(&Globals{})
			},
		},
		{
			name:       "sync start --reset-target-data",
			formats:    []string{"text", "json"},
			promptText: confirmPromptText,
			accept:     "reset\n",
			run: func(yes bool, format string) error {
				cmd := &SyncStartCmd{}
				cmd.SourceDriver = "postgres"
				cmd.Source = "postgres://u:pw@127.0.0.1:1/src?sslmode=disable"
				cmd.TargetDriver = "postgres"
				cmd.Target = "postgres://u:pw@127.0.0.1:1/dst?sslmode=disable"
				cmd.StreamID = "s1"
				cmd.Format = format
				cmd.ResetTargetData = true
				cmd.Yes = yes
				cmd.ApplyBatchSize = "bogus"
				cmd.ApplyRetryAttempts = 8
				cmd.ApplyRetryBackoffBase = 100 * time.Millisecond
				cmd.ApplyRetryBackoffCap = 5 * time.Second
				return cmd.Run(&Globals{})
			},
		},
		{
			name:       "sync from-backup run --reset-target-data",
			formats:    []string{""},
			promptText: confirmPromptText,
			accept:     "reset\n",
			run: func(yes bool, _ string) error {
				cmd := &SyncFromBackupCmd{}
				cmd.BackupDir = t.TempDir()
				cmd.TargetDriver = "postgres"
				cmd.Target = "postgres://u:pw@127.0.0.1:1/dst?sslmode=disable"
				cmd.StreamID = "s1"
				cmd.ResetTargetData = true
				cmd.Yes = yes
				return cmd.Run(&Globals{})
			},
		},
		{
			name:       "schema add-table",
			formats:    []string{""},
			promptText: `Type "users_v2" to confirm`,
			accept:     "users_v2\n",
			run: func(yes bool, _ string) error {
				cmd := &SchemaAddTableCmd{}
				cmd.SourceDriver = "postgres"
				cmd.Source = "postgres://u:pw@127.0.0.1:1/src?sslmode=disable"
				cmd.TargetDriver = "postgres"
				cmd.Target = "postgres://u:pw@127.0.0.1:1/dst?sslmode=disable"
				cmd.Table = "users_v2"
				cmd.StreamID = "s1"
				cmd.Yes = yes
				return cmd.Run(&Globals{})
			},
		},
		{
			// The GC-13 site: on a pipe this used to print "aborted" and
			// return nil — exit 0 with the trigger engine still installed.
			name:       "trigger teardown (postgres-trigger)",
			formats:    []string{""},
			promptText: "Tear down the sluice trigger engine",
			accept:     "y\n",
			run: func(yes bool, _ string) error {
				cmd := &TriggerTeardownCmd{
					SourceDriver: "postgres-trigger",
					// Malformed on purpose: pgtrigger.Teardown parses the
					// DSN before dialing, so the downstream failure is
					// local and immediate.
					DSN: "::not-a-postgres-dsn::",
					Yes: yes,
				}
				return cmd.Run(&Globals{})
			},
		},
		{
			// Same command, the SQLite-family branch: it reaches the door
			// through runSQLiteLike, a different call path than the PG
			// branch, so it is pinned on its own.
			name:       "trigger teardown (sqlite-trigger)",
			formats:    []string{""},
			promptText: "Tear down the sluice trigger engine",
			accept:     "y\n",
			run: func(yes bool, _ string) error {
				cmd := &TriggerTeardownCmd{
					SourceDriver: "sqlite-trigger",
					// A file inside a directory that does not exist: the
					// open fails locally instead of creating a database.
					DSN: filepath.Join(t.TempDir(), "missing-dir", "src.db"),
					Yes: yes,
				}
				return cmd.Run(&Globals{})
			},
		},
	}
}

// TestDestructivePromptDoor pins the GC-13 policy through the real Run()
// of every prompt site, in three shapes each:
//
//   - stdin NOT a terminal, no --yes: the coded SLUICE-E-CONFIRMATION-
//     REQUIRED refusal (exit 3), the prompt is never printed, and under
//     --format json a single stdout envelope with status "refused"
//     carrying the code. Nothing downstream runs.
//   - stdin NOT a terminal, --yes: the door is bypassed and the run
//     fails on a DOWNSTREAM error — not the refusal, not the declined
//     sentinel.
//   - stdin a terminal, no --yes: the prompt fires; the accept token
//     passes the door (downstream error again) and any other answer is
//     errConfirmDeclined (exit 1) — never a nil return.
func TestDestructivePromptDoor(t *testing.T) {
	for _, site := range destructivePromptSites(t) {
		for _, format := range site.formats {
			label := site.name
			if format != "" {
				label += "/" + format
			}

			t.Run(label+"/piped stdin refuses before prompting", func(t *testing.T) {
				asPipedStdin(t)
				var err error
				stdout, stderr := withCommandIO(t, "y\nreset\nusers_v2\n", func() {
					err = site.run(false, format)
				})
				ce, ok := sluicecode.FromError(err)
				if !ok || ce.Code != sluicecode.CodeConfirmationRequired {
					t.Fatalf("piped stdin without --yes must refuse with %s; got %v", sluicecode.CodeConfirmationRequired, err)
				}
				if ce.ExitCode() != sluicecode.ExitRefusal {
					t.Errorf("exit code = %d; want %d (ExitRefusal)", ce.ExitCode(), sluicecode.ExitRefusal)
				}
				if !strings.Contains(ce.Hint, "--yes") {
					t.Errorf("hint must name --yes; got %q", ce.Hint)
				}
				if strings.Contains(stdout, site.promptText) || strings.Contains(stderr, site.promptText) {
					t.Errorf("the refusal must fire BEFORE the prompt is printed; stdout=%q stderr=%q", stdout, stderr)
				}
				if format == "json" {
					var doc map[string]any
					if jerr := json.Unmarshal([]byte(stdout), &doc); jerr != nil {
						t.Fatalf("stdout is not one JSON object: %v\n%s", jerr, stdout)
					}
					if doc["status"] != envelopeStatusRefused {
						t.Errorf("envelope status = %v; want %q", doc["status"], envelopeStatusRefused)
					}
					errObj, _ := doc["error"].(map[string]any)
					if errObj["code"] != string(sluicecode.CodeConfirmationRequired) {
						t.Errorf("envelope error.code = %v; want %s", errObj["code"], sluicecode.CodeConfirmationRequired)
					}
				}
			})

			t.Run(label+"/piped stdin with --yes proceeds", func(t *testing.T) {
				asPipedStdin(t)
				var err error
				_, _ = withCommandIO(t, "", func() {
					err = site.run(true, format)
				})
				requireDownstreamFailure(t, err)
			})

			t.Run(label+"/terminal accept proceeds", func(t *testing.T) {
				asTerminalStdin(t)
				var err error
				stdout, stderr := withCommandIO(t, site.accept, func() {
					err = site.run(false, format)
				})
				if !strings.Contains(stdout+stderr, site.promptText) {
					t.Errorf("a terminal must see the prompt; stdout=%q stderr=%q", stdout, stderr)
				}
				requireDownstreamFailure(t, err)
			})

			t.Run(label+"/terminal decline exits non-zero", func(t *testing.T) {
				asTerminalStdin(t)
				var err error
				_, _ = withCommandIO(t, "no\n", func() {
					err = site.run(false, format)
				})
				if !errors.Is(err, errConfirmDeclined) {
					t.Fatalf("a decline must return errConfirmDeclined (exit 1), never nil; got %v", err)
				}
			})
		}
	}
}

// requireDownstreamFailure asserts the run got PAST the door: it failed
// (every site targets an unreachable or malformed DSN), and on neither
// of the door's two errors.
func requireDownstreamFailure(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected a downstream failure past the door (no real database in unit tests); got nil")
	}
	if errors.Is(err, errConfirmDeclined) {
		t.Fatalf("run must pass the door; got the declined sentinel: %v", err)
	}
	if ce, ok := sluicecode.FromError(err); ok && ce.Code == sluicecode.CodeConfirmationRequired {
		t.Fatalf("run must pass the door; got the confirmation refusal: %v", err)
	}
}

// TestDefaultStdinIsTerminal_PipeIsNot binds the door's real probe to the
// environment the policy is about: an os.Pipe on stdin — what an agent,
// a CI runner, or `cmd | sluice` hands the process — is NOT a terminal.
// The command tests above override the probe, so without this one the
// default wiring would be pinned by nothing.
func TestDefaultStdinIsTerminal_PipeIsNot(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer func() { _ = r.Close(); _ = w.Close() }()
	orig := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = orig }()
	if defaultStdinIsTerminal() {
		t.Fatal("a pipe on stdin reported as a terminal — the door would prompt an agent")
	}
}

// TestRefuseUnlessTerminal pins the door function itself: nil on a
// terminal; on anything else a ClassRefusal coded error (exit 3) that
// names the action and the --yes remedy.
func TestRefuseUnlessTerminal(t *testing.T) {
	asTerminalStdin(t)
	if err := refuseUnlessTerminal("x"); err != nil {
		t.Fatalf("terminal stdin must open the door; got %v", err)
	}
	asPipedStdin(t)
	err := refuseUnlessTerminal("tearing down the trigger engine")
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeConfirmationRequired || ce.ExitCode() != sluicecode.ExitRefusal {
		t.Fatalf("piped stdin must refuse with %s / exit %d; got %v", sluicecode.CodeConfirmationRequired, sluicecode.ExitRefusal, err)
	}
	if !strings.Contains(err.Error(), "tearing down the trigger engine") {
		t.Errorf("the refusal must name the action; got %q", err.Error())
	}
}
