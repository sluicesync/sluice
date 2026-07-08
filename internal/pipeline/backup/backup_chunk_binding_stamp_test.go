// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the ADR-0152 format-version stamp on the full-backup
// write side: encrypted runs stamp FormatVersionEncryptedChunkBinding;
// a RESUMED pre-binding encrypted run inherits the prior version (its
// kept chunks on the store are unbound — a v5 stamp would send the
// restore down the AAD path against them).

package backup

import (
	"bytes"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

func stampTestEncryption(t *testing.T) *lineage.BackupEncryption {
	t.Helper()
	p, err := crypto.DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("DefaultArgon2idParams: %v", err)
	}
	p.Memory, p.Iterations, p.Parallelism = 1024, 1, 1
	env, err := crypto.NewPassphraseEnvelope("stamp-pass", p)
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope: %v", err)
	}
	return &lineage.BackupEncryption{Envelope: env}
}

func TestSetupChainEncryption_FormatVersionStamp(t *testing.T) {
	t.Run("fresh encrypted run stamps the binding version", func(t *testing.T) {
		b := &Backup{Encryption: stampTestEncryption(t)}
		m := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy}
		if _, err := b.setupChainEncryption(m, nil); err != nil {
			t.Fatalf("setupChainEncryption: %v", err)
		}
		if m.FormatVersion != irbackup.FormatVersionEncryptedChunkBinding {
			t.Errorf("FormatVersion = %d; want %d — the chunks this run writes are AAD-bound and readers gate on the stamp",
				m.FormatVersion, irbackup.FormatVersionEncryptedChunkBinding)
		}
	})

	t.Run("plaintext run never stamps it", func(t *testing.T) {
		b := &Backup{}
		m := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy}
		if _, err := b.setupChainEncryption(m, nil); err != nil {
			t.Fatalf("setupChainEncryption: %v", err)
		}
		if m.FormatVersion != irbackup.FormatVersionLegacy {
			t.Errorf("plaintext FormatVersion = %d; want %d (Bug-116 proportionality)", m.FormatVersion, irbackup.FormatVersionLegacy)
		}
	})

	t.Run("resuming a PRE-binding encrypted run inherits its version + unbound shape", func(t *testing.T) {
		enc := stampTestEncryption(t)
		// The prior in-progress manifest as an old binary wrote it:
		// pre-v5 version, chain CEK wrapped UNBOUND under this
		// envelope's KEK.
		cek, err := crypto.GenerateCEK()
		if err != nil {
			t.Fatalf("GenerateCEK: %v", err)
		}
		wrapped, err := enc.Envelope.WrapCEK(cek)
		if err != nil {
			t.Fatalf("WrapCEK: %v", err)
		}
		prior := &irbackup.Manifest{
			FormatVersion: irbackup.FormatVersionSecurityMetadata,
			PartialState:  irbackup.BackupStateInProgress,
			ChainEncryption: &irbackup.ChainEncryption{
				Algorithm:  crypto.AlgorithmAESGCM,
				Mode:       crypto.EncryptModePerChain,
				KEKMode:    crypto.KEKModePassphrase,
				WrappedCEK: wrapped,
			},
		}
		b := &Backup{Encryption: enc}
		m := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionSecurityMetadata}
		got, err := b.setupChainEncryption(m, prior)
		if err != nil {
			t.Fatalf("setupChainEncryption resume: %v (the pre-binding prior's UNBOUND wrap must unwrap via the legacy path)", err)
		}
		if m.FormatVersion >= irbackup.FormatVersionEncryptedChunkBinding {
			t.Errorf("resumed FormatVersion = %d; the prior run's kept chunks are UNBOUND — a v5 stamp would break their restore", m.FormatVersion)
		}
		if !bytes.Equal(got, cek) {
			t.Errorf("resumed run recovered a different chain CEK")
		}
	})

	t.Run("resuming a v5 encrypted run keeps the v5 stamp + bound wrap", func(t *testing.T) {
		enc := stampTestEncryption(t)
		b := &Backup{Encryption: enc}
		// A prior run written by THIS binary: first build it fresh so
		// the wrap is bound to its identity.
		prior := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy, PartialState: irbackup.BackupStateInProgress}
		cek, err := b.setupChainEncryption(prior, nil)
		if err != nil {
			t.Fatalf("fresh setup: %v", err)
		}
		// The resumed manifest adopts the prior identity (CreatedAt et
		// al are zero on both here), so the bound unwrap must succeed.
		// Same envelope (same salt) — the CLI's fresh-salt rebind is
		// Bug 43's territory, covered elsewhere.
		m := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy}
		got, err := (&Backup{Encryption: enc}).setupChainEncryption(m, prior)
		if err != nil {
			t.Fatalf("v5 resume: %v (the bound wrap must unwrap under the prior manifest's identity)", err)
		}
		if m.FormatVersion != irbackup.FormatVersionEncryptedChunkBinding {
			t.Errorf("v5 resume FormatVersion = %d; want %d", m.FormatVersion, irbackup.FormatVersionEncryptedChunkBinding)
		}
		if !bytes.Equal(got, cek) {
			t.Errorf("v5 resume recovered a different chain CEK")
		}
	})
}
