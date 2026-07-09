// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// withCommandIO runs fn with os.Stdin fed from stdin and os.Stdout /
// os.Stderr captured, restoring the real streams afterwards. The
// destructive-confirm command paths read and write the process
// streams directly (that IS the surface under test — the prompt must
// land on the right stream), so the swap happens at the os level
// rather than through an injected writer.
func withCommandIO(t *testing.T, stdin string, fn func()) (stdout, stderr string) {
	t.Helper()
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	if _, err := io.WriteString(inW, stdin); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	_ = inW.Close()

	origIn, origOut, origErr := os.Stdin, os.Stdout, os.Stderr
	os.Stdin, os.Stdout, os.Stderr = inR, outW, errW
	defer func() {
		os.Stdin, os.Stdout, os.Stderr = origIn, origOut, origErr
	}()
	fn()
	os.Stdin, os.Stdout, os.Stderr = origIn, origOut, origErr

	_ = outW.Close()
	_ = errW.Close()
	outB, _ := io.ReadAll(outR)
	errB, _ := io.ReadAll(errR)
	return string(outB), string(errB)
}

const confirmPromptText = "Type 'reset' to confirm"

// TestResetTargetDataConfirm_CommandPaths pins the destructive-confirm
// gate through the real Run() of all three embedding commands, across
// {json, text} × {confirm, abort} (sync from-backup run has no
// --format flag, so only text applies there):
//
//   - An abort returns errConfirmDeclined — non-zero exit, and under
//     --format json a single stdout envelope with status "aborted"
//     (the old nil-return rendered status "completed").
//   - In json mode the prompt goes to stderr; stdout carries ONLY the
//     envelope (the one-JSON-object contract).
//   - Typing the token passes the gate: the run proceeds and fails on
//     a DIFFERENT, downstream error (no DB in unit tests), proving the
//     gate consumed the answer rather than short-circuiting.
func TestResetTargetDataConfirm_CommandPaths(t *testing.T) {
	// Each command's runner returns the Run error and plants a fast,
	// dial-free failure point directly AFTER the prompt (bogus
	// --inject-shard-column / --apply-batch-size / an empty chain
	// dir), so the confirm path proves the gate consumed the answer —
	// the run must fail on the PLANTED error, not something earlier —
	// and no test ever touches a network.
	type commandCase struct {
		run            func(format string) error
		formats        []string
		wantDownstream string // substring of the planted post-prompt error
	}
	cases := map[string]commandCase{
		"migrate": {
			formats:        []string{"text", "json"},
			wantDownstream: "--inject-shard-column",
			run: func(format string) error {
				cmd := &MigrateCmd{}
				cmd.SourceDriver = "postgres"
				cmd.Source = "postgres://u:pw@127.0.0.1:1/src?sslmode=disable"
				cmd.TargetDriver = "postgres"
				cmd.Target = "postgres://u:pw@127.0.0.1:1/dst?sslmode=disable"
				cmd.Format = format
				cmd.ResetTargetData = true
				cmd.InjectShardColumn = "bogus-no-equals"
				return cmd.Run(&Globals{})
			},
		},
		"sync start": {
			formats:        []string{"text", "json"},
			wantDownstream: "--apply-batch-size",
			run: func(format string) error {
				cmd := &SyncStartCmd{}
				cmd.SourceDriver = "postgres"
				cmd.Source = "postgres://u:pw@127.0.0.1:1/src?sslmode=disable"
				cmd.TargetDriver = "postgres"
				cmd.Target = "postgres://u:pw@127.0.0.1:1/dst?sslmode=disable"
				cmd.StreamID = "s1"
				cmd.Format = format
				cmd.ResetTargetData = true
				cmd.ApplyBatchSize = "bogus"
				// Direct construction skips kong's defaults; the retry
				// dials are validated (in range) BEFORE the prompt, so
				// give them their kong defaults to reach it.
				cmd.ApplyRetryAttempts = 8
				cmd.ApplyRetryBackoffBase = 100 * time.Millisecond
				cmd.ApplyRetryBackoffCap = 5 * time.Second
				return cmd.Run(&Globals{})
			},
		},
		"sync from-backup run": {
			formats: []string{"text"}, // no --format flag on this command
			// An empty chain dir is tolerated up front (fresh-chain
			// shape), so the first post-prompt failure is the broker's
			// applier open against the unreachable port-1 target — an
			// immediate local connection-refused, still dial-free in
			// the "no real database" sense.
			wantDownstream: "open target change applier",
			run: func(string) error {
				cmd := &SyncFromBackupCmd{}
				cmd.BackupDir = t.TempDir()
				cmd.TargetDriver = "postgres"
				cmd.Target = "postgres://u:pw@127.0.0.1:1/dst?sslmode=disable"
				cmd.StreamID = "s1"
				cmd.ResetTargetData = true
				return cmd.Run(&Globals{})
			},
		},
	}

	for name, tc := range cases {
		for _, format := range tc.formats {
			t.Run(name+"/"+format+"/abort", func(t *testing.T) {
				var err error
				stdout, stderr := withCommandIO(t, "no\n", func() {
					err = tc.run(format)
				})
				if !errors.Is(err, errConfirmDeclined) {
					t.Fatalf("aborted confirm must return errConfirmDeclined; got %v", err)
				}
				if format == "json" {
					if strings.Contains(stdout, confirmPromptText) {
						t.Errorf("json mode leaked the prompt onto stdout:\n%s", stdout)
					}
					if !strings.Contains(stderr, confirmPromptText) {
						t.Errorf("json mode must prompt on stderr; stderr = %q", stderr)
					}
					var doc map[string]any
					if jerr := json.Unmarshal([]byte(stdout), &doc); jerr != nil {
						t.Fatalf("stdout is not one JSON object: %v\n%s", jerr, stdout)
					}
					if doc["status"] != "aborted" {
						t.Errorf("envelope status = %v; want aborted", doc["status"])
					}
				} else {
					if !strings.Contains(stdout, confirmPromptText) {
						t.Errorf("text mode must prompt on stdout; stdout = %q", stdout)
					}
					if strings.Contains(stdout, `"status"`) {
						t.Errorf("text mode must not emit an envelope:\n%s", stdout)
					}
				}
			})

			t.Run(name+"/"+format+"/confirm", func(t *testing.T) {
				var err error
				stdout, _ := withCommandIO(t, "reset\n", func() {
					err = tc.run(format)
				})
				if err == nil {
					t.Fatal("expected the planted downstream error after a confirmed prompt")
				}
				if errors.Is(err, errConfirmDeclined) {
					t.Fatalf("typed token must pass the gate; got the declined sentinel: %v", err)
				}
				if !strings.Contains(err.Error(), tc.wantDownstream) {
					t.Fatalf("run must fail on the PLANTED post-prompt error (%q) — a different error means the prompt was never reached; got: %v",
						tc.wantDownstream, err)
				}
				if format == "json" {
					var doc map[string]any
					if jerr := json.Unmarshal([]byte(stdout), &doc); jerr != nil {
						t.Fatalf("stdout is not one JSON object: %v\n%s", jerr, stdout)
					}
					if s := doc["status"]; s == "aborted" || s == "completed" {
						t.Errorf("downstream failure must not render status %v", s)
					}
				}
			})
		}
	}
}

// TestConfirmTypedDestructive_CtxCancelAbortsPrompt pins the
// Ctrl-C-at-the-prompt fix: once signal.NotifyContext is installed, a
// SIGINT cancels the context instead of killing the process, and the
// old blocking stdin read swallowed it — the prompt just sat there.
// Both cancellation orders must abort the prompt with the context's
// error.
func TestConfirmTypedDestructive_CtxCancelAbortsPrompt(t *testing.T) {
	t.Run("pre-canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		blockingIn, w := io.Pipe() // no writer activity: a read would block forever
		defer func() { _ = w.Close(); _ = blockingIn.Close() }()
		ok, err := confirmTypedDestructive(ctx, blockingIn, io.Discard, "confirm: ", "reset")
		if ok || !errors.Is(err, context.Canceled) {
			t.Fatalf("got (%v, %v); want (false, context.Canceled)", ok, err)
		}
	})
	t.Run("canceled mid-prompt", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		blockingIn, w := io.Pipe() // the read never completes, so only the cancel can win
		defer func() { _ = w.Close(); _ = blockingIn.Close() }()
		go cancel()
		ok, err := confirmTypedDestructive(ctx, blockingIn, io.Discard, "confirm: ", "reset")
		if ok || !errors.Is(err, context.Canceled) {
			t.Fatalf("got (%v, %v); want (false, context.Canceled)", ok, err)
		}
	})
}

// TestForceExitAfterSecondSignal pins the second-SIGINT escape hatch:
// the first signal is left to the graceful NotifyContext path (no
// exit), the second exits immediately with 130 (128+SIGINT). The
// unbuffered channel makes the assertions race-free: a completed send
// proves the watcher consumed the first signal, and exit can only be
// called after the second receive.
func TestForceExitAfterSecondSignal(t *testing.T) {
	sigs := make(chan os.Signal)
	var mu sync.Mutex
	var codes []int
	done := make(chan struct{})
	go forceExitAfterSecondSignal(sigs, func(code int) {
		mu.Lock()
		codes = append(codes, code)
		mu.Unlock()
		close(done)
	})

	sigs <- os.Interrupt // first: consumed, graceful path — must not exit
	mu.Lock()
	if len(codes) != 0 {
		mu.Unlock()
		t.Fatalf("exit called after the FIRST signal: %v", codes)
	}
	mu.Unlock()

	sigs <- os.Interrupt // second: the operator insists
	<-done
	mu.Lock()
	defer mu.Unlock()
	if len(codes) != 1 || codes[0] != forceExitCode {
		t.Fatalf("exit calls = %v; want exactly one with %d", codes, forceExitCode)
	}
}
