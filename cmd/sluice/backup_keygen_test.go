// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
)

// TestBackupKeygen_RoundTrip drives the CLI keygen through to a signer +
// verifier that agree — the on-disk key contract exercised at the CLI
// layer (the "pin the fix through the CLI layer" discipline).
func TestBackupKeygen_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	if err := (&BackupKeygenCmd{OutDir: dir}).Run(&Globals{}); err != nil {
		t.Fatalf("keygen: %v", err)
	}
	privPath := filepath.Join(dir, "sluice-sign-key.pem")
	pubPath := filepath.Join(dir, "sluice-verify-key.pem")

	// The private key file is 0600 (skip the perm bits on Windows, which
	// does not represent Unix mode — CI on Linux enforces it).
	fi, err := os.Stat(privPath)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if perm := fi.Mode().Perm(); perm != 0o600 {
			t.Errorf("private key perm = %o; want 0600", perm)
		}
	}

	// The generated keypair round-trips: build a signer from the private
	// key and a verify key from the public, and confirm the ids match.
	signer, err := (&EncryptionFlags{SignKey: privPath}).buildEd25519Signer()
	if err != nil || signer == nil {
		t.Fatalf("buildEd25519Signer: %v (nil=%v)", err, signer == nil)
	}
	pub, err := (&EncryptionFlags{VerifyKey: pubPath}).resolveVerifyKey()
	if err != nil || pub == nil {
		t.Fatalf("resolveVerifyKey: %v (nil=%v)", err, pub == nil)
	}
	if signer.KeyID != crypto.Ed25519KeyID(pub) {
		t.Fatalf("keygen signer/verifier key-id mismatch: %s != %s", signer.KeyID, crypto.Ed25519KeyID(pub))
	}
}

// TestBackupKeygen_RefusesClobber pins that keygen will not overwrite an
// existing private key without --force.
func TestBackupKeygen_RefusesClobber(t *testing.T) {
	dir := t.TempDir()
	if err := (&BackupKeygenCmd{OutDir: dir}).Run(&Globals{}); err != nil {
		t.Fatal(err)
	}
	if err := (&BackupKeygenCmd{OutDir: dir}).Run(&Globals{}); err == nil {
		t.Fatal("keygen clobbered an existing key without --force")
	}
	if err := (&BackupKeygenCmd{OutDir: dir, Force: true}).Run(&Globals{}); err != nil {
		t.Fatalf("keygen --force refused: %v", err)
	}
}

// TestBackupKeygen_PathValidation pins the flag-combination errors.
func TestBackupKeygen_PathValidation(t *testing.T) {
	cases := []struct {
		name string
		cmd  BackupKeygenCmd
		ok   bool
	}{
		{"out-dir only", BackupKeygenCmd{OutDir: "d"}, true},
		{"priv+pub", BackupKeygenCmd{Priv: "a", Pub: "b"}, true},
		{"out-dir + priv", BackupKeygenCmd{OutDir: "d", Priv: "a"}, false},
		{"priv without pub", BackupKeygenCmd{Priv: "a"}, false},
		{"none", BackupKeygenCmd{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := tc.cmd.resolvePaths()
			if (err == nil) != tc.ok {
				t.Fatalf("resolvePaths ok=%v, want %v (err=%v)", err == nil, tc.ok, err)
			}
		})
	}
}

// TestEncryptionFlags_KeyResolution_Env pins the `env:VAR` key-source form
// and that a bad key refuses loudly.
func TestEncryptionFlags_KeyResolution_Env(t *testing.T) {
	pub, priv, _ := crypto.GenerateEd25519Keypair()
	privPEM, _ := crypto.MarshalEd25519PrivateKeyPEM(priv)
	pubPEM, _ := crypto.MarshalEd25519PublicKeyPEM(pub)
	t.Setenv("TEST_SLUICE_SIGN_KEY", string(privPEM))
	t.Setenv("TEST_SLUICE_VERIFY_KEY", string(pubPEM))

	signer, err := (&EncryptionFlags{SignKey: "env:TEST_SLUICE_SIGN_KEY"}).buildEd25519Signer()
	if err != nil || signer == nil {
		t.Fatalf("env sign key: %v", err)
	}
	gotPub, err := (&EncryptionFlags{VerifyKey: "env:TEST_SLUICE_VERIFY_KEY"}).resolveVerifyKey()
	if err != nil || gotPub == nil {
		t.Fatalf("env verify key: %v", err)
	}

	// Empty env var → loud refusal.
	if _, err := (&EncryptionFlags{SignKey: "env:TEST_SLUICE_ABSENT"}).buildEd25519Signer(); err == nil {
		t.Fatal("absent env sign key did not error")
	}
	// Garbage PEM → loud refusal.
	t.Setenv("TEST_SLUICE_BAD", "not a pem")
	if _, err := (&EncryptionFlags{VerifyKey: "env:TEST_SLUICE_BAD"}).resolveVerifyKey(); err == nil {
		t.Fatal("garbage verify key did not error")
	}
	// Unset flags resolve to nil (no signing / no verify key).
	if s, err := (&EncryptionFlags{}).buildEd25519Signer(); err != nil || s != nil {
		t.Fatalf("empty sign-key: got signer=%v err=%v, want nil/nil", s, err)
	}
}
