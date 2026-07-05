// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
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

// TestEncryptionFlags_BuildBackupEncryption_RebuildForChain pins
// Bug 43's CLI surface: the lineage.BackupEncryption returned for an
// --encrypt run carries a non-nil RebuildForChain that, when called
// with a chain's recorded Argon2id params, returns an envelope whose
// KEK was derived against the chain's salt (not the cold-start
// envelope's freshly-minted salt). This is what lets the orchestrator
// successfully unwrap a parent chain's WrappedCEK when extending an
// encrypted chain.
func TestEncryptionFlags_BuildBackupEncryption_RebuildForChain(t *testing.T) {
	const passphrase = "correct horse battery staple"

	// Step 1: simulate a chain's full backup. Use distinct params
	// from whatever buildBackupEncryption mints internally.
	chainParams, err := crypto.DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("chain params: %v", err)
	}
	chainEnv, err := crypto.NewPassphraseEnvelope(passphrase, chainParams)
	if err != nil {
		t.Fatalf("chain envelope: %v", err)
	}
	cek, err := crypto.GenerateCEK()
	if err != nil {
		t.Fatalf("generate cek: %v", err)
	}
	wrapped, err := chainEnv.WrapCEK(cek)
	if err != nil {
		t.Fatalf("wrap cek: %v", err)
	}

	// Step 2: build the CLI-side lineage.BackupEncryption (cold-start salt).
	flags := &EncryptionFlags{
		Encrypt:              true,
		EncryptionPassphrase: passphrase,
		EncryptMode:          "per-chain",
	}
	enc, err := flags.buildBackupEncryption()
	if err != nil {
		t.Fatalf("buildBackupEncryption: %v", err)
	}
	if enc == nil {
		t.Fatal("buildBackupEncryption returned nil")
	}
	if enc.RebuildForChain == nil {
		t.Fatal("Bug 43 contract: RebuildForChain must be populated for encrypted runs")
	}

	// Step 3: cold-start envelope MUST fail to unwrap the chain's
	// CEK (the salt asymmetry the bug exposed).
	if _, err := enc.Envelope.UnwrapCEK(wrapped); err == nil {
		t.Fatal("Bug 43 contract: cold-start envelope must fail to unwrap chain-salt CEK")
	}

	// Step 4: invoke RebuildForChain with the chain's recorded
	// params; the returned envelope must unwrap cleanly.
	recordedParams := &irbackup.Argon2idParams{
		Salt:        chainParams.Salt,
		Memory:      chainParams.Memory,
		Iterations:  chainParams.Iterations,
		Parallelism: chainParams.Parallelism,
		KeyLen:      chainParams.KeyLen,
	}
	rebuilt, err := enc.RebuildForChain(recordedParams)
	if err != nil {
		t.Fatalf("RebuildForChain: %v", err)
	}
	if rebuilt == nil {
		t.Fatal("RebuildForChain returned nil envelope")
	}
	got, err := rebuilt.UnwrapCEK(wrapped)
	if err != nil {
		t.Fatalf("rebuilt envelope unwrap: %v", err)
	}
	if !bytes.Equal(got, cek) {
		t.Fatal("rebuilt envelope CEK mismatch")
	}
}

// TestEncryptionFlags_BuildBackupEncryption_RebuildForChain_NilParams
// ensures the builder rejects nil params loudly rather than minting a
// wrong-salt envelope silently.
func TestEncryptionFlags_BuildBackupEncryption_RebuildForChain_NilParams(t *testing.T) {
	flags := &EncryptionFlags{
		Encrypt:              true,
		EncryptionPassphrase: "secret",
	}
	enc, err := flags.buildBackupEncryption()
	if err != nil {
		t.Fatalf("buildBackupEncryption: %v", err)
	}
	if _, err := enc.RebuildForChain(nil); err == nil {
		t.Fatal("RebuildForChain(nil) must error; got nil")
	}
}

// TestEncryptionFlags_KMSAndPassphraseMutuallyExclusive pins the
// Phase 6.2 contract: --kms-key-arn and --encryption-passphrase{,-env,
// -file} can't be combined. The orchestrator only accepts one key
// material; mixing them is operator confusion that should fail loudly
// at flag-parse time, not silently pick a precedence rule.
func TestEncryptionFlags_KMSAndPassphraseMutuallyExclusive(t *testing.T) {
	cases := []struct {
		name  string
		flags EncryptionFlags
	}{
		{"with-passphrase-direct", EncryptionFlags{
			Encrypt:              true,
			KMSKeyARN:            "arn:aws:kms:us-east-1:1:key/x",
			EncryptionPassphrase: "secret",
		}},
		{"with-passphrase-env", EncryptionFlags{
			Encrypt:                 true,
			KMSKeyARN:               "arn:aws:kms:us-east-1:1:key/x",
			EncryptionPassphraseEnv: "VAR",
		}},
		{"with-passphrase-file", EncryptionFlags{
			Encrypt:                  true,
			KMSKeyARN:                "arn:aws:kms:us-east-1:1:key/x",
			EncryptionPassphraseFile: "/tmp/p",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.flags.buildBackupEncryption(); err == nil {
				t.Fatal("expected mutual-exclusion error; got nil")
			} else if !strings.Contains(err.Error(), "mutually exclusive") {
				t.Errorf("error should name mutual exclusion; got %v", err)
			}
		})
	}
}

// TestEncryptionFlags_KMSWithoutEncrypt pins the symmetry with the
// passphrase-without-encrypt sanity check: setting --kms-key-arn
// without --encrypt is operator confusion, not a silent disable.
func TestEncryptionFlags_KMSWithoutEncrypt(t *testing.T) {
	flags := &EncryptionFlags{
		KMSKeyARN: "arn:aws:kms:us-east-1:1:key/x",
	}
	if _, err := flags.buildBackupEncryption(); err == nil {
		t.Fatal("expected error when --kms-key-arn is set without --encrypt")
	} else if !strings.Contains(err.Error(), "--encrypt") {
		t.Errorf("error should name --encrypt; got %v", err)
	}
}

// TestEncryptionFlags_EncryptWithoutAnyKey pins the surface that
// --encrypt alone (no passphrase, no KMS) errors with a clear
// message naming the flag families that satisfy it.
func TestEncryptionFlags_EncryptWithoutAnyKey(t *testing.T) {
	flags := &EncryptionFlags{Encrypt: true}
	_, err := flags.buildBackupEncryption()
	if err == nil {
		t.Fatal("expected --encrypt-without-key to error")
	}
	for _, want := range []string{"encryption-passphrase", "kms-key-arn", "gcp-kms-key-resource", "azure-key-vault-id"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should name %q; got %v", want, err)
		}
	}
}

// TestEncryptionFlags_AllProvidersMutuallyExclusive pins Phase 6.3's
// broadened contract: AWS / GCP / Azure / passphrase are all
// pairwise mutually exclusive.
func TestEncryptionFlags_AllProvidersMutuallyExclusive(t *testing.T) {
	const (
		awsARN     = "arn:aws:kms:us-east-1:1:key/x"
		gcpRes     = "projects/p/locations/us/keyRings/r/cryptoKeys/k"
		azureURL   = "https://v.vault.azure.net/keys/k"
		passphrase = "secret"
	)
	cases := []struct {
		name  string
		flags EncryptionFlags
	}{
		{"passphrase+aws", EncryptionFlags{Encrypt: true, EncryptionPassphrase: passphrase, KMSKeyARN: awsARN}},
		{"passphrase+gcp", EncryptionFlags{Encrypt: true, EncryptionPassphrase: passphrase, GCPKMSKeyResource: gcpRes}},
		{"passphrase+azure", EncryptionFlags{Encrypt: true, EncryptionPassphrase: passphrase, AzureKeyVaultID: azureURL}},
		{"aws+gcp", EncryptionFlags{Encrypt: true, KMSKeyARN: awsARN, GCPKMSKeyResource: gcpRes}},
		{"aws+azure", EncryptionFlags{Encrypt: true, KMSKeyARN: awsARN, AzureKeyVaultID: azureURL}},
		{"gcp+azure", EncryptionFlags{Encrypt: true, GCPKMSKeyResource: gcpRes, AzureKeyVaultID: azureURL}},
		{"all-four", EncryptionFlags{
			Encrypt:              true,
			EncryptionPassphrase: passphrase,
			KMSKeyARN:            awsARN,
			GCPKMSKeyResource:    gcpRes,
			AzureKeyVaultID:      azureURL,
		}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.flags.buildBackupEncryption(); err == nil {
				t.Fatal("expected mutual-exclusion error; got nil")
			} else if !strings.Contains(err.Error(), "mutually exclusive") {
				t.Errorf("error should name mutual exclusion; got %v", err)
			}
		})
	}
}

// TestEncryptionFlags_GCPAzureWithoutEncrypt mirrors the
// passphrase-without-encrypt / kms-without-encrypt sanity checks for
// the two new Phase 6.3 providers.
func TestEncryptionFlags_GCPAzureWithoutEncrypt(t *testing.T) {
	cases := []struct {
		name  string
		flags EncryptionFlags
		want  string
	}{
		{
			"gcp",
			EncryptionFlags{GCPKMSKeyResource: "projects/p/locations/us/keyRings/r/cryptoKeys/k"},
			"--gcp-kms-key-resource",
		},
		{
			"azure",
			EncryptionFlags{AzureKeyVaultID: "https://v.vault.azure.net/keys/k"},
			"--azure-key-vault-id",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.flags.buildBackupEncryption(); err == nil {
				t.Fatal("expected error")
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error should name %q; got %v", tc.want, err)
			}
		})
	}
}
