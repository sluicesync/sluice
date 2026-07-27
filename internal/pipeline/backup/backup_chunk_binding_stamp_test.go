// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the ADR-0152 format-version stamp on the full-backup
// write side: encrypted runs stamp the newest chunk-binding tier; a
// RESUMED run inherits the tier its already-written chunks were sealed
// under (a newer stamp would send the restore down an AAD shape those
// chunks do not carry).

package backup

import (
	"bytes"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// TestSetupChainEncryption_SignedTableBindingStamp pins the SEC-F1 /
// SEC-2 write-side stamp for a SIGNED encrypted run: a FRESH one stamps
// [irbackup.FormatVersionInjectiveChunkAAD] (v9) so its row chunks are
// written with — and restored against — the injective, parent-table-bound
// AAD; a RESUMED pre-v7 signed run keeps
// [irbackup.FormatVersionSignedManifest] (v6) so its already-written
// untable-bound chunks still decrypt.
func TestSetupChainEncryption_SignedTableBindingStamp(t *testing.T) {
	t.Run("fresh signed encrypted run stamps the injective-AAD version", func(t *testing.T) {
		_, priv, err := crypto.GenerateEd25519Keypair()
		if err != nil {
			t.Fatalf("GenerateEd25519Keypair: %v", err)
		}
		b := &Backup{Encryption: stampTestEncryption(t), Ed25519Signer: lineage.NewEd25519Signer(priv)}
		m := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy}
		if _, err := b.setupChainEncryption(m, nil); err != nil {
			t.Fatalf("setupChainEncryption: %v", err)
		}
		if m.FormatVersion != irbackup.FormatVersionInjectiveChunkAAD {
			t.Errorf("FormatVersion = %d; want %d — a fresh signed encrypted full seals its row chunks with the injective parent-table AAD",
				m.FormatVersion, irbackup.FormatVersionInjectiveChunkAAD)
		}
	})

	t.Run("resuming a pre-v7 signed run keeps the prior (untable-bound) version", func(t *testing.T) {
		enc := stampTestEncryption(t)
		_, priv, err := crypto.GenerateEd25519Keypair()
		if err != nil {
			t.Fatalf("GenerateEd25519Keypair: %v", err)
		}
		// A signed encrypted full as an older (pre-SEC-F1) binary wrote it.
		prior, cek := priorChainAtVersion(t, enc, irbackup.FormatVersionSignedManifest)

		b := &Backup{Encryption: enc, Ed25519Signer: lineage.NewEd25519Signer(priv)}
		m := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy}
		got, err := b.setupChainEncryption(m, prior)
		if err != nil {
			t.Fatalf("resume setup: %v", err)
		}
		if m.FormatVersion != irbackup.FormatVersionSignedManifest {
			t.Errorf("resumed FormatVersion = %d; want %d — the prior run's kept row chunks are NOT table-bound, so a v7+ stamp would break their decrypt",
				m.FormatVersion, irbackup.FormatVersionSignedManifest)
		}
		if !bytes.Equal(got, cek) {
			t.Errorf("resumed run recovered a different chain CEK")
		}
	})

	t.Run("resuming a pre-v9 signed run keeps the raw-concat table-bound version", func(t *testing.T) {
		// ADR-0181: a v7/v8 signed chain's kept chunks were sealed with the
		// RAW-CONCATENATION table binding. Stamping v9 would recompute the
		// length-prefixed AAD against them.
		enc := stampTestEncryption(t)
		_, priv, err := crypto.GenerateEd25519Keypair()
		if err != nil {
			t.Fatalf("GenerateEd25519Keypair: %v", err)
		}
		prior, cek := priorChainAtVersion(t, enc, irbackup.FormatVersionChunkTableBinding)

		b := &Backup{Encryption: enc, Ed25519Signer: lineage.NewEd25519Signer(priv)}
		m := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy}
		got, err := b.setupChainEncryption(m, prior)
		if err != nil {
			t.Fatalf("resume setup: %v", err)
		}
		if m.FormatVersion != irbackup.FormatVersionChunkTableBinding {
			t.Errorf("resumed FormatVersion = %d; want %d — the prior run's kept chunks carry the raw-concatenation AAD",
				m.FormatVersion, irbackup.FormatVersionChunkTableBinding)
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

// priorChainAtVersion builds an in-progress encrypted chain root as a
// binary at format version fv would have left it: the recorded stamp is
// fv AND the chain CEK is wrapped under fv's CEK binding.
//
// Re-wrapping (rather than rewinding a fresh run's stamp in place) is
// load-bearing since ADR-0181: the CEK binding's ENCODING now changes at
// v9 too, so a stamp rewound without a matching re-wrap would fail to
// unwrap for the encoding reason rather than the tier reason under test —
// a green-looking fixture that proves nothing about the tier ladder.
func priorChainAtVersion(t *testing.T, enc *lineage.BackupEncryption, fv int) (prior *irbackup.Manifest, cek []byte) {
	t.Helper()
	prior = &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy, PartialState: irbackup.BackupStateInProgress}
	cek, err := (&Backup{Encryption: enc}).setupChainEncryption(prior, nil)
	if err != nil {
		t.Fatalf("fresh prior setup: %v", err)
	}
	prior.FormatVersion = fv
	wrapped, err := lineage.WrapChainCEK(enc.Envelope, cek, prior)
	if err != nil {
		t.Fatalf("re-wrap prior CEK at v%d: %v", fv, err)
	}
	prior.ChainEncryption.WrappedCEK = wrapped
	return prior, cek
}

func TestSetupChainEncryption_FormatVersionStamp(t *testing.T) {
	t.Run("fresh encrypted (unsigned) run stamps the injective-AAD version", func(t *testing.T) {
		// SEC-1: a fresh encrypted run — even UNSIGNED — table-binds its
		// row-chunk AAD, and SEC-2 renders that binding injectively, so it
		// stamps v9. GCM enforces the AAD regardless of signature, closing the
		// same-column chunk-swap (and now its delimiter-forged variant) for
		// unsigned backups.
		b := &Backup{Encryption: stampTestEncryption(t)}
		m := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy}
		if _, err := b.setupChainEncryption(m, nil); err != nil {
			t.Fatalf("setupChainEncryption: %v", err)
		}
		if m.FormatVersion != irbackup.FormatVersionInjectiveChunkAAD {
			t.Errorf("FormatVersion = %d; want %d — a fresh encrypted run's chunks are TABLE-bound (SEC-1) with an INJECTIVE encoding (SEC-2)",
				m.FormatVersion, irbackup.FormatVersionInjectiveChunkAAD)
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

	t.Run("resuming a fresh (v9) encrypted run keeps the v9 stamp + bound wrap", func(t *testing.T) {
		enc := stampTestEncryption(t)
		b := &Backup{Encryption: enc}
		// A prior run written by THIS binary: first build it fresh so the
		// wrap is bound to its identity. Post-SEC-2, a fresh encrypted run is
		// stamped v9 (injective, table-bound), so resuming it keeps v9.
		prior := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy, PartialState: irbackup.BackupStateInProgress}
		cek, err := b.setupChainEncryption(prior, nil)
		if err != nil {
			t.Fatalf("fresh setup: %v", err)
		}
		if prior.FormatVersion != irbackup.FormatVersionInjectiveChunkAAD {
			t.Fatalf("fresh prior FormatVersion = %d; want %d (SEC-2)", prior.FormatVersion, irbackup.FormatVersionInjectiveChunkAAD)
		}
		// The resumed manifest adopts the prior identity (CreatedAt et
		// al are zero on both here), so the bound unwrap must succeed.
		// Same envelope (same salt) — the CLI's fresh-salt rebind is
		// Bug 43's territory, covered elsewhere.
		m := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy}
		got, err := (&Backup{Encryption: enc}).setupChainEncryption(m, prior)
		if err != nil {
			t.Fatalf("v9 resume: %v (the bound wrap must unwrap under the prior manifest's identity)", err)
		}
		if m.FormatVersion != irbackup.FormatVersionInjectiveChunkAAD {
			t.Errorf("v9 resume FormatVersion = %d; want %d", m.FormatVersion, irbackup.FormatVersionInjectiveChunkAAD)
		}
		if !bytes.Equal(got, cek) {
			t.Errorf("v9 resume recovered a different chain CEK")
		}
	})

	t.Run("resuming a genuine v5/v6 encrypted chain keeps its pre-table-binding stamp", func(t *testing.T) {
		// SEC-1 backcompat: a chain written before the row-chunk TABLE-binding
		// (v5 unsigned, v6 signed) carries row chunks whose AAD lacks the
		// parent-table field. Resuming it must NOT jump a tier — that would
		// send restore down the table-bound read path against unbound chunks.
		for _, priorFV := range []int{irbackup.FormatVersionEncryptedChunkBinding, irbackup.FormatVersionSignedManifest} {
			enc := stampTestEncryption(t)
			prior, cek := priorChainAtVersion(t, enc, priorFV)

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

	t.Run("resuming a genuine v7/v8 encrypted chain keeps its raw-concat AAD stamp", func(t *testing.T) {
		// ADR-0181 backcompat, one tier up: a v7/v8 chain's row chunks are
		// table-bound but sealed with the RAW-CONCATENATION encoding. Resuming
		// it must stay below v9 or the kept chunks stop decrypting.
		for _, priorFV := range []int{irbackup.FormatVersionChunkTableBinding, irbackup.FormatVersionCDCPositionBinding} {
			enc := stampTestEncryption(t)
			prior, cek := priorChainAtVersion(t, enc, priorFV)

			m := &irbackup.Manifest{FormatVersion: irbackup.FormatVersionLegacy}
			got, err := (&Backup{Encryption: enc}).setupChainEncryption(m, prior)
			if err != nil {
				t.Fatalf("v%d resume: %v", priorFV, err)
			}
			if m.FormatVersion >= irbackup.FormatVersionInjectiveChunkAAD {
				t.Errorf("v%d resume FormatVersion = %d; must stay < %d — the prior chain's chunks carry the raw-concatenation AAD",
					priorFV, m.FormatVersion, irbackup.FormatVersionInjectiveChunkAAD)
			}
			if !bytes.Equal(got, cek) {
				t.Errorf("v%d resume recovered a different chain CEK", priorFV)
			}
		}
	})
}
