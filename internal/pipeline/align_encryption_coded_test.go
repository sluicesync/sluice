// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestAlignEncryption_MismatchIsCoded pins the WRITE-side faces of
// SLUICE-E-BACKUP-ENCRYPTION-MISMATCH (audit-2026-07-15 M3, filed at the
// v0.99.258 merge): the read-side preflights (restore / chain restore /
// broker / export) got the coded refusal in v0.99.258, and the extending
// writers' alignEncryption — the exact same two operator mistakes, caught
// before a single chunk is written — must carry the same code so an
// operator's retry loop keys on one class for both directions. Both
// writers (incremental + stream) × both shapes (encrypted parent with no
// key; envelope KEK mode differing from the chain's recorded kek_mode).
func TestAlignEncryption_MismatchIsCoded(t *testing.T) {
	ctx := context.Background()
	// A parent that IS the chain root (carries ChainEncryption), recorded
	// under a KMS kek_mode so a passphrase envelope mismatches.
	encryptedParent := &irbackup.Manifest{
		Kind: "full",
		ChainEncryption: &irbackup.ChainEncryption{
			Algorithm: "aes-256-gcm",
			KEKMode:   crypto.KEKModeAWSKMS,
			KEKRef:    "arn:aws:kms:us-east-1:1:key/x",
		},
	}

	align := map[string]func(enc *lineage.BackupEncryption) ([]byte, error){
		"incremental": func(enc *lineage.BackupEncryption) ([]byte, error) {
			b := &IncrementalBackup{segStore: newMemStore(), Encryption: enc}
			return b.alignEncryption(ctx, encryptedParent)
		},
		"stream": func(enc *lineage.BackupEncryption) ([]byte, error) {
			b := &BackupStream{segStore: newMemStore(), Encryption: enc}
			return b.alignEncryption(ctx, encryptedParent)
		},
	}

	// A plaintext parent (no ChainEncryption anywhere in its chain) for
	// the reverse-direction conflict (audit 2026-07-16 M3.1).
	plaintextParent := &irbackup.Manifest{Kind: "full"}
	alignAgainst := map[string]func(parent *irbackup.Manifest, enc *lineage.BackupEncryption) ([]byte, error){
		"incremental": func(parent *irbackup.Manifest, enc *lineage.BackupEncryption) ([]byte, error) {
			b := &IncrementalBackup{segStore: newMemStore(), Encryption: enc}
			return b.alignEncryption(ctx, parent)
		},
		"stream": func(parent *irbackup.Manifest, enc *lineage.BackupEncryption) ([]byte, error) {
			b := &BackupStream{segStore: newMemStore(), Encryption: enc}
			return b.alignEncryption(ctx, parent)
		},
	}

	for name, alignFn := range align {
		alignFn := alignFn
		alignParent := alignAgainst[name]
		t.Run(name, func(t *testing.T) {
			t.Run("encrypted parent + no --encrypt → coded mismatch", func(t *testing.T) {
				_, err := alignFn(nil)
				assertCoded(t, err, sluicecode.CodeBackupEncryptionMismatch)
			})

			t.Run("envelope kek_mode differs from the chain's → coded mismatch", func(t *testing.T) {
				enc := &lineage.BackupEncryption{Envelope: cheapTestEnvelope(t)} // passphrase mode vs recorded aws-kms
				_, err := alignFn(enc)
				assertCoded(t, err, sluicecode.CodeBackupEncryptionMismatch)
			})

			t.Run("plaintext parent + --encrypt → coded mismatch (M3.1)", func(t *testing.T) {
				enc := &lineage.BackupEncryption{Envelope: cheapTestEnvelope(t)}
				_, err := alignParent(plaintextParent, enc)
				assertCoded(t, err, sluicecode.CodeBackupEncryptionMismatch)
			})
		})
	}
}
