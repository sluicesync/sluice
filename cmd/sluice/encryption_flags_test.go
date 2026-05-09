// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEncryptionFlags_ResolvePassphrase_Direct(t *testing.T) {
	e := &EncryptionFlags{
		Encrypt:              true,
		EncryptionPassphrase: "secret",
	}
	got, err := e.resolvePassphrase()
	if err != nil {
		t.Fatalf("resolvePassphrase: %v", err)
	}
	if got != "secret" {
		t.Errorf("got %q want %q", got, "secret")
	}
}

func TestEncryptionFlags_ResolvePassphrase_Env(t *testing.T) {
	t.Setenv("SLUICE_TEST_PASS", "env-secret")
	e := &EncryptionFlags{
		Encrypt:                 true,
		EncryptionPassphraseEnv: "SLUICE_TEST_PASS",
	}
	got, err := e.resolvePassphrase()
	if err != nil {
		t.Fatalf("resolvePassphrase: %v", err)
	}
	if got != "env-secret" {
		t.Errorf("got %q want %q", got, "env-secret")
	}
}

func TestEncryptionFlags_ResolvePassphrase_EmptyEnv(t *testing.T) {
	t.Setenv("SLUICE_TEST_EMPTY", "")
	e := &EncryptionFlags{
		Encrypt:                 true,
		EncryptionPassphraseEnv: "SLUICE_TEST_EMPTY",
	}
	if _, err := e.resolvePassphrase(); err == nil {
		t.Fatalf("empty env expected to error; got nil")
	}
}

func TestEncryptionFlags_ResolvePassphrase_File(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "passphrase.txt")
	if err := os.WriteFile(path, []byte("file-secret\n"), 0o600); err != nil {
		t.Fatalf("write tempfile: %v", err)
	}
	e := &EncryptionFlags{
		Encrypt:                  true,
		EncryptionPassphraseFile: path,
	}
	got, err := e.resolvePassphrase()
	if err != nil {
		t.Fatalf("resolvePassphrase: %v", err)
	}
	if got != "file-secret" {
		t.Errorf("got %q want %q", got, "file-secret")
	}
}

func TestEncryptionFlags_ResolvePassphrase_NoSource(t *testing.T) {
	e := &EncryptionFlags{Encrypt: true}
	if _, err := e.resolvePassphrase(); err == nil {
		t.Fatalf("no source expected to error; got nil")
	}
}

func TestEncryptionFlags_ResolvePassphrase_MultipleSources(t *testing.T) {
	e := &EncryptionFlags{
		Encrypt:                 true,
		EncryptionPassphrase:    "a",
		EncryptionPassphraseEnv: "B",
	}
	if _, err := e.resolvePassphrase(); err == nil {
		t.Fatalf("multiple sources expected to error; got nil")
	} else if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("err message: %v", err)
	}
}

func TestEncryptionFlags_BuildBackupEncryption_Plaintext(t *testing.T) {
	e := &EncryptionFlags{}
	got, err := e.buildBackupEncryption()
	if err != nil {
		t.Fatalf("buildBackupEncryption: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for plaintext; got %+v", got)
	}
}

func TestEncryptionFlags_BuildBackupEncryption_PassphraseSetButNoEncryptFlag(t *testing.T) {
	e := &EncryptionFlags{
		EncryptionPassphrase: "secret",
	}
	if _, err := e.buildBackupEncryption(); err == nil {
		t.Fatalf("passphrase without --encrypt expected to error; got nil")
	}
}

func TestEncryptionFlags_BuildBackupEncryption_Encrypt(t *testing.T) {
	e := &EncryptionFlags{
		Encrypt:              true,
		EncryptionPassphrase: "secret",
		EncryptMode:          "per-chain",
	}
	got, err := e.buildBackupEncryption()
	if err != nil {
		t.Fatalf("buildBackupEncryption: %v", err)
	}
	if got == nil {
		t.Fatalf("expected non-nil; got nil")
	}
	if got.Mode != "per-chain" {
		t.Errorf("Mode: got %q", got.Mode)
	}
	if got.Envelope == nil {
		t.Errorf("Envelope: got nil")
	}
}

func TestEncryptionFlags_BuildReadEnvelope_NoEncrypt(t *testing.T) {
	e := &EncryptionFlags{}
	env, err := e.buildReadEnvelope(nil)
	if err != nil {
		t.Fatalf("buildReadEnvelope: %v", err)
	}
	if env != nil {
		t.Errorf("expected nil envelope when --encrypt not set; got non-nil")
	}
}

func TestEncryptionFlags_BuildReadEnvelope_Encrypt(t *testing.T) {
	e := &EncryptionFlags{
		Encrypt:              true,
		EncryptionPassphrase: "secret",
	}
	env, err := e.buildReadEnvelope(nil)
	if err != nil {
		t.Fatalf("buildReadEnvelope: %v", err)
	}
	if env == nil {
		t.Fatalf("expected non-nil envelope")
	}
	if env.Mode() != "passphrase-argon2id" {
		t.Errorf("Mode: got %q", env.Mode())
	}
}
