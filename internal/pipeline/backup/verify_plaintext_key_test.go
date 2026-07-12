// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestVerifyBackupWith_KeyAgainstPlaintextChain_Refused pins audit-2026-07-11
// M-1: `backup verify --encrypt` against a chain that claims PLAINTEXT (root
// ChainEncryption == nil) must REFUSE, not return a false GREEN. The apply
// paths (restore / chain_restore / broker preflightEncryption) already refuse
// a key-vs-plaintext-claiming chain; verify did not, so a whole-chain
// encrypted→plaintext downgrade on an unsigned chain (adversary strips the
// ChainEncryption marker + every ChunkEncryption + forges plaintext chunks with
// matching SHAs) verified GREEN while restore refused. Without the key the same
// chain still verifies clean (control), so the refusal is specifically the
// key-vs-plaintext mismatch.
func TestVerifyBackupWith_KeyAgainstPlaintextChain_Refused(t *testing.T) {
	ctx := context.Background()
	plaintextFull := func() *irbackup.Manifest {
		m := &irbackup.Manifest{
			FormatVersion: irbackup.FormatVersionLegacy,
			CreatedAt:     time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC),
			SourceEngine:  "postgres",
			Kind:          irbackup.BackupKindFull,
			Schema:        &ir.Schema{Tables: []*ir.Table{{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}}},
			Tables:        []*irbackup.TableManifest{{Name: "t", RowCount: 0}},
			// ChainEncryption deliberately nil — the chain claims plaintext.
		}
		m.BackupID = irbackup.ComputeBackupID(m)
		return m
	}

	newStore := func(t *testing.T) irbackup.Store {
		t.Helper()
		dir := t.TempDir()
		store, _ := blobcodec.NewLocalStore(dir)
		full := plaintextFull()
		if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
			t.Fatalf("write full: %v", err)
		}
		lineage.UpdateLineageForManifestBestEffort(ctx, store, full, lineage.ManifestFileName, blobcodec.CodecGzip)
		return store
	}

	t.Run("no key: plaintext chain verifies clean (control)", func(t *testing.T) {
		if _, _, err := VerifyBackupWith(ctx, newStore(t), VerifyOptions{}); err != nil {
			t.Fatalf("plaintext chain without a key should verify clean; got: %v", err)
		}
	})

	t.Run("key supplied: plaintext-claiming chain REFUSED", func(t *testing.T) {
		p, err := crypto.DefaultArgon2idParams()
		if err != nil {
			t.Fatalf("DefaultArgon2idParams: %v", err)
		}
		p.Memory, p.Iterations, p.Parallelism = 1024, 1, 1
		env, err := crypto.NewPassphraseEnvelope("verify-pass", p)
		if err != nil {
			t.Fatalf("NewPassphraseEnvelope: %v", err)
		}
		_, _, err = VerifyBackupWith(ctx, newStore(t), VerifyOptions{Envelope: env})
		if err == nil {
			t.Fatal("key against a plaintext-claiming chain must be refused; got nil")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeBackupChunkAuthFailed {
			t.Fatalf("want %s, got %v", sluicecode.CodeBackupChunkAuthFailed, err)
		}
	})
}
