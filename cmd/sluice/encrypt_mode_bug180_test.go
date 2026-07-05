// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/alecthomas/kong"
)

// TestEncryptMode_OmittedIsEmpty_Bug180 pins the CLI-PATH behavior that the
// Bug 179 fix depends on and that Bug 180 exposed as broken: omitting
// --encrypt-mode must resolve to "" so the backup orchestrator can inherit an
// existing chain's mode. A kong default of "per-chain" (or a
// buildBackupEncryption collapse of ""→per-chain) makes omission
// indistinguishable from an explicit per-chain, leaving the inherit branch
// unreachable — so an incremental of a per-chunk chain that omits the flag is
// refused with a hint ("omit --encrypt-mode to inherit") that cannot be
// satisfied.
//
// This tests THROUGH kong (not by setting the field directly) — the Bug-74
// lesson: the programmatic alignEncryption("", …) pin greened a path no CLI
// user can reach.
func TestEncryptMode_OmittedIsEmpty_Bug180(t *testing.T) {
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

	base := []string{"backup", "full", "--source-driver=postgres", "--source=postgres://localhost/db", "--output-dir=/tmp/b"}

	t.Run("omitted → empty (inherit-eligible)", func(t *testing.T) {
		cli := parse(t, base...)
		if got := cli.Backup.Full.EncryptMode; got != "" {
			t.Fatalf("omitted --encrypt-mode resolved to %q; want \"\" (else the inherit branch is unreachable — Bug 180)", got)
		}
	})

	t.Run("explicit per-chunk preserved", func(t *testing.T) {
		cli := parse(t, append(append([]string{}, base...), "--encrypt-mode=per-chunk")...)
		if got := cli.Backup.Full.EncryptMode; got != "per-chunk" {
			t.Fatalf("explicit --encrypt-mode=per-chunk = %q; want per-chunk", got)
		}
	})

	t.Run("explicit per-chain preserved", func(t *testing.T) {
		cli := parse(t, append(append([]string{}, base...), "--encrypt-mode=per-chain")...)
		if got := cli.Backup.Full.EncryptMode; got != "per-chain" {
			t.Fatalf("explicit --encrypt-mode=per-chain = %q; want per-chain", got)
		}
	})

	t.Run("invalid mode rejected by enum", func(t *testing.T) {
		cli := &CLI{}
		parser, err := kong.New(cli, kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
		if err != nil {
			t.Fatalf("kong.New: %v", err)
		}
		if _, err := parser.Parse(append(append([]string{}, base...), "--encrypt-mode=bogus")); err == nil {
			t.Fatal("--encrypt-mode=bogus parsed OK; want enum rejection")
		}
	})
}

// TestBuildBackupEncryption_OmittedModeFlowsThrough_Bug180 pins that
// buildBackupEncryption does NOT collapse an omitted mode to per-chain — the
// second Bug 180 site. The orchestrator (alignEncryption / setupChainEncryption)
// owns the inherit/default decision; the CLI builder must forward "".
func TestBuildBackupEncryption_OmittedModeFlowsThrough_Bug180(t *testing.T) {
	e := &EncryptionFlags{Encrypt: true, EncryptionPassphrase: "secret", EncryptMode: ""}
	got, err := e.buildBackupEncryption()
	if err != nil {
		t.Fatalf("buildBackupEncryption: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil encryption")
	}
	if got.Mode != "" {
		t.Errorf("omitted mode collapsed to %q; want \"\" (orchestrator resolves it — Bug 180)", got.Mode)
	}

	// Explicit mode still forwarded verbatim.
	e2 := &EncryptionFlags{Encrypt: true, EncryptionPassphrase: "secret", EncryptMode: "per-chunk"}
	got2, err := e2.buildBackupEncryption()
	if err != nil {
		t.Fatalf("buildBackupEncryption(per-chunk): %v", err)
	}
	if got2.Mode != "per-chunk" {
		t.Errorf("explicit mode = %q; want per-chunk", got2.Mode)
	}
}
