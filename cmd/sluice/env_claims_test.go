// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/alecthomas/kong"

	"sluicesync.dev/sluice/internal/config"
)

// TestClaimedEnvVarNames_ParsedCLI pins the claim collection through the
// REAL kong parser, not a hand-built struct: the `envvar:` marker has to
// survive alongside kong's own tags on the same field, and the values it
// reads have to be the ones kong actually assigned. A direct-call unit
// test would green even if kong rejected the tag or the flag never
// reached the field.
func TestClaimedEnvVarNames_ParsedCLI(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "encryption-passphrase-env names the variable directly",
			args: []string{
				"backup", "full",
				"--source-driver", "postgres", "--source", "postgres://u:p@h/db",
				"--output-dir", t.TempDir(),
				"--encrypt", "--encryption-passphrase-env", "SLUICE_PASS",
			},
			want: []string{"SLUICE_PASS"},
		},
		{
			name: "env: specs on the sibling key flags are claimed too",
			args: []string{
				"backup", "full",
				"--source-driver", "postgres", "--source", "postgres://u:p@h/db",
				"--output-dir", t.TempDir(),
				"--sign-key", "env:SLUICE_SIGN_KEY",
				"--keyset-source", "env:SLUICE_KEYSET",
			},
			want: []string{"SLUICE_KEYSET", "SLUICE_SIGN_KEY"},
		},
		{
			name: "verify-key on the restore surface is claimed",
			args: []string{
				"backup", "verify",
				"--from-dir", t.TempDir(),
				"--verify-key", "env:SLUICE_VERIFY_KEY",
			},
			want: []string{"SLUICE_VERIFY_KEY"},
		},
		{
			// The same flags in their NON-env forms name no variable —
			// a claim on a file path would be meaningless, and a claim
			// on "" would swallow the warning for every unnamed var.
			name: "path and kms:// forms claim nothing",
			args: []string{
				"backup", "full",
				"--source-driver", "postgres", "--source", "postgres://u:p@h/db",
				"--output-dir", t.TempDir(),
				"--sign-key", "/etc/sluice/sign.pem",
				"--keyset-source", "file:/etc/sluice/keyset.yaml",
			},
			want: nil,
		},
		{
			name: "a command with no secret flags claims nothing",
			args: []string{"engines"},
			want: nil,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cli := &CLI{}
			parser, err := kong.New(cli, kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
			if err != nil {
				t.Fatalf("kong.New: %v", err)
			}
			if _, err := parser.Parse(c.args); err != nil {
				t.Fatalf("Parse: %v", err)
			}
			got := claimedEnvVarNames(cli)
			if len(got) == 0 && len(c.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("claimedEnvVarNames = %v; want %v", got, c.want)
			}
		})
	}
}

// TestRegisterClaimedEnvVars_SilencesTheUnknownKeyWarning is the
// end-to-end gate for the whole chain — parse, register, then the real
// [config.Load] a subcommand runs. The halves each have their own pin;
// this is the one that fails if the wiring between them is dropped.
// SLUICE_PASS is the natural name for the variable the
// --encryption-passphrase-env help text recommends, and pre-fix it made
// every backup command warn that it was a typo.
func TestRegisterClaimedEnvVars_SilencesTheUnknownKeyWarning(t *testing.T) {
	t.Setenv("SLUICE_PASS", "hunter2")
	t.Setenv("SLUICE_TPYO_KEY", "1")
	t.Cleanup(func() { config.SetClaimedEnvVars(nil) })

	cli := &CLI{}
	parser, err := kong.New(cli, kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	if _, err := parser.Parse([]string{
		"backup", "full",
		"--source-driver", "postgres", "--source", "postgres://u:p@h/db",
		"--output-dir", t.TempDir(),
		"--encrypt", "--encryption-passphrase-env", "SLUICE_PASS",
	}); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	registerClaimedEnvVars(cli)

	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	if _, err := config.Load(""); err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	logs := logBuf.String()
	if strings.Contains(logs, "SLUICE_PASS") {
		t.Errorf("config.Load warned about the flag-named passphrase var: %s", logs)
	}
	if !strings.Contains(logs, "SLUICE_TPYO_KEY") {
		t.Errorf("config.Load lost the genuine typo warning: %s", logs)
	}
}
