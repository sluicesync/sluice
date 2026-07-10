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

// TestSetupChainEncryption_SignedTableBindingStamp pins the SEC-F1 write
// -side stamp: a FRESH signed encrypted run stamps
// [irbackup.FormatVersionChunkTableBinding] (v7) so its row chunks are
// written with — and restored against — the parent-table-bound AAD; a
// RESUMED pre-v7 signed run keeps [irbackup.FormatVersionSignedManifest]
// (v6) so its already-written untable-bound chunks still decrypt.
func TestSetupChainEncryption_SignedTableBindingStamp(t *testing.T) {
	t.Run("fresh signed encrypted run stamps the table-binding version", func(t *testing.T) {
		_, priv, err := crypto.GenerateEd25519Keypair()
		if err != nil {
			t.Fatalf("GenerateEd25519Keypair: %v", err)
		}
		b := &Backup{Encryption: stampTestEncryption(t), Ed25519Signer: lineage.NewEd25519Signer(priv)}
		m := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy}
		if _, err := b.setupChainEncryption(m, nil); err != nil {
			t.Fatalf("setupChainEncryption: %v", err)
		}
		if m.FormatVersion != irbackup.FormatVersionChunkTableBinding {
			t.Errorf("FormatVersion = %d; want %d — a fresh signed encrypted full binds its row chunks to their parent table",
				m.FormatVersion, irbackup.FormatVersionChunkTableBinding)
		}
	})

	t.Run("resuming a pre-v7 signed run keeps the prior (untable-bound) version", func(t *testing.T) {
		enc := stampTestEncryption(t)
		_, priv, err := crypto.GenerateEd25519Keypair()
		if err != nil {
			t.Fatalf("GenerateEd25519Keypair: %v", err)
		}
		// Build a prior run with a bound wrap, then stamp it v6 to stand in
		// for a signed encrypted full an older (pre-SEC-F1) binary wrote.
		prior := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy, PartialState: irbackup.BackupStateInProgress}
		cek, err := (&Backup{Encryption: enc}).setupChainEncryption(prior, nil)
		if err != nil {
			t.Fatalf("fresh prior setup: %v", err)
		}
		prior.FormatVersion = irbackup.FormatVersionSignedManifest

		b := &Backup{Encryption: enc, Ed25519Signer: lineage.NewEd25519Signer(priv)}
		m := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy}
		got, err := b.setupChainEncryption(m, prior)
		if err != nil {
			t.Fatalf("resume setup: %v", err)
		}
		if m.FormatVersion != irbackup.FormatVersionSignedManifest {
			t.Errorf("resumed FormatVersion = %d; want %d — the prior run's kept row chunks are NOT table-bound, so a v7 stamp would break their decrypt",
				m.FormatVersion, irbackup.FormatVersionSignedManifest)
		}
		if !bytes.Equal(got, cek) {
			t.Errorf("resumed run recovered a different chain CEK")
		}
	})
}

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
	t.Run("fresh encrypted (unsigned) run stamps the table-binding version", func(t *testing.T) {
		// SEC-1: a fresh encrypted run — even UNSIGNED — table-binds its
		// row-chunk AAD, so it stamps the table-binding version (v7), not the
		// old chunk-binding-only v5. GCM enforces the table AAD regardless of
		// signature, closing the same-column chunk-swap for unsigned backups.
		b := &Backup{Encryption: stampTestEncryption(t)}
		m := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy}
		if _, err := b.setupChainEncryption(m, nil); err != nil {
			t.Fatalf("setupChainEncryption: %v", err)
		}
		if m.FormatVersion != irbackup.FormatVersionChunkTableBinding {
			t.Errorf("FormatVersion = %d; want %d — a fresh encrypted run's chunks are TABLE-bound (SEC-1)",
				m.FormatVersion, irbackup.FormatVersionChunkTableBinding)
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

	t.Run("resuming a fresh (v7) encrypted run keeps the v7 stamp + bound wrap", func(t *testing.T) {
		enc := stampTestEncryption(t)
		b := &Backup{Encryption: enc}
		// A prior run written by THIS binary: first build it fresh so the
		// wrap is bound to its identity. Post-SEC-1, a fresh encrypted run is
		// stamped v7 (table-bound), so resuming it keeps v7.
		prior := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy, PartialState: irbackup.BackupStateInProgress}
		cek, err := b.setupChainEncryption(prior, nil)
		if err != nil {
			t.Fatalf("fresh setup: %v", err)
		}
		if prior.FormatVersion != irbackup.FormatVersionChunkTableBinding {
			t.Fatalf("fresh prior FormatVersion = %d; want %d (SEC-1)", prior.FormatVersion, irbackup.FormatVersionChunkTableBinding)
		}
		// The resumed manifest adopts the prior identity (CreatedAt et
		// al are zero on both here), so the bound unwrap must succeed.
		// Same envelope (same salt) — the CLI's fresh-salt rebind is
		// Bug 43's territory, covered elsewhere.
		m := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy}
		got, err := (&Backup{Encryption: enc}).setupChainEncryption(m, prior)
		if err != nil {
			t.Fatalf("v7 resume: %v (the bound wrap must unwrap under the prior manifest's identity)", err)
		}
		if m.FormatVersion != irbackup.FormatVersionChunkTableBinding {
			t.Errorf("v7 resume FormatVersion = %d; want %d", m.FormatVersion, irbackup.FormatVersionChunkTableBinding)
		}
		if !bytes.Equal(got, cek) {
			t.Errorf("v7 resume recovered a different chain CEK")
		}
	})

	t.Run("resuming a genuine v5/v6 encrypted chain keeps its pre-table-binding stamp", func(t *testing.T) {
		// SEC-1 backcompat: a chain written before the row-chunk TABLE-binding
		// (v5 unsigned, v6 signed) carries row chunks whose AAD lacks the
		// parent-table field. Resuming it must NOT jump to v7 — that would
		// send restore down the table-bound read path against unbound chunks.
		// Build a fresh (v7) prior, then rewind its stamp to simulate an
		// old-binary v5 chain: the identity-bound wrap is unchanged, only the
		// recorded FormatVersion differs.
		for _, priorFV := range []int{irbackup.FormatVersionEncryptedChunkBinding, irbackup.FormatVersionSignedManifest} {
			enc := stampTestEncryption(t)
			b := &Backup{Encryption: enc}
			prior := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy, PartialState: irbackup.BackupStateInProgress}
			cek, err := b.setupChainEncryption(prior, nil)
			if err != nil {
				t.Fatalf("fresh setup: %v", err)
			}
			prior.FormatVersion = priorFV // simulate a pre-SEC-1 chain

			m := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy}
			got, err := (&Backup{Encryption: enc}).setupChainEncryption(m, prior)
			if err != nil {
				t.Fatalf("v%d resume: %v", priorFV, err)
			}
			if m.FormatVersion >= irbackup.FormatVersionChunkTableBinding {
				t.Errorf("v%d resume FormatVersion = %d; must stay < %d — the prior chain's chunks are NOT table-bound",
					priorFV, m.FormatVersion, irbackup.FormatVersionChunkTableBinding)
			}
			if !bytes.Equal(got, cek) {
				t.Errorf("v%d resume recovered a different chain CEK", priorFV)
			}
		}
	})
}
