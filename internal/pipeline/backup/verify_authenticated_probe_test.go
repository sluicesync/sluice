// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// Bug 215's second defect, and the one that made the first a trap:
// `backup verify --encrypt` reported `all chunks OK … decrypt_probe=true`
// on a chain `restore` could not decrypt at all.
//
// The historical probe was [lineage.ProbeChunkDecrypt], which unwraps a
// chunk's OWN WrappedCEK — a field only PER-CHUNK mode populates. On the
// DEFAULT per-chain chain it returned nil for every chunk without
// touching a byte of ciphertext, so the total decryption performed by a
// whole verify run was one chain-root CEK unwrap. `decrypt_probe=true`
// meant "a key was supplied".
//
// These pins are deliberately unit-level and writer-independent: they
// forge the artifact directly, so they stay red if the verify probe
// regresses even after the writer-side fix is long settled.

// verifyProbeChain builds a minimal one-table encrypted chain whose
// single row chunk is sealed with sealCEK and bound with sealAAD, while
// the manifest advertises the chain CEK. Passing the chain's real CEK
// and the real AAD produces an honest chain; passing anything else
// produces exactly the shape restore refuses and verify used to bless.
func verifyProbeChain(t *testing.T, env crypto.EnvelopeEncryption, corrupt func(realCEK, realAAD []byte) (cek, aad []byte)) irbackup.Store {
	t.Helper()
	ctx := context.Background()
	store, _ := blobcodec.NewLocalStore(t.TempDir())

	cek, err := crypto.GenerateCEK()
	if err != nil {
		t.Fatalf("GenerateCEK: %v", err)
	}
	const file = "chunks/t/t-0.jsonl.gz"
	m := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Kind:          irbackup.BackupKindFull,
		Schema:        &ir.Schema{Tables: []*ir.Table{{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}}},
		PartialState:  irbackup.BackupStateComplete,
	}
	m.BackupID = irbackup.ComputeBackupID(m)
	wrapped, err := lineage.WrapChainCEK(env, cek, m)
	if err != nil {
		t.Fatalf("WrapChainCEK: %v", err)
	}
	m.ChainEncryption = &irbackup.ChainEncryption{
		Algorithm:  crypto.AlgorithmAESGCM,
		KEKMode:    env.Mode(),
		Mode:       crypto.EncryptModePerChain,
		WrappedCEK: wrapped,
	}
	chunk := &irbackup.ChunkInfo{
		File:     file,
		RowCount: 1,
		Encryption: &irbackup.ChunkEncryption{
			Algorithm: crypto.AlgorithmAESGCM, NonceLen: crypto.NonceLen, AuthTagLen: crypto.AuthTagLen,
		},
	}
	m.Tables = []*irbackup.TableManifest{{Name: "t", RowCount: 1, Chunks: []*irbackup.ChunkInfo{chunk}}}

	sealCEK, sealAAD := cek, irbackup.ChunkAADFor(m, chunk, "", "t")
	if corrupt != nil {
		sealCEK, sealAAD = corrupt(cek, irbackup.ChunkAADFor(m, chunk, "", "t"))
	}
	ct, err := crypto.EncryptChunkWithAAD([]byte("not really a codec stream, but GCM does not care"), sealCEK, sealAAD)
	if err != nil {
		t.Fatalf("EncryptChunkWithAAD: %v", err)
	}
	// The recorded digest is always HONEST about the bytes on disk, so
	// the SHA check can never be what fires — only the authenticated
	// open can.
	sum := sha256.Sum256(ct)
	chunk.SHA256 = hex.EncodeToString(sum[:])

	if err := store.Put(ctx, file, bytes.NewReader(ct)); err != nil {
		t.Fatalf("put chunk: %v", err)
	}
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, m); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, m, lineage.ManifestFileName, blobcodec.CodecGzip)
	return store
}

// verifyProbeEnvelope is a cheap (test-parameter) passphrase envelope.
func verifyProbeEnvelope(t *testing.T) crypto.EnvelopeEncryption {
	t.Helper()
	env, err := crypto.NewPassphraseEnvelope("probe-pass", crypto.Argon2idParams{
		Salt: []byte("0123456789abcdef"), Memory: 1024, Iterations: 1, Parallelism: 1, KeyLen: crypto.KEKLen,
	})
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope: %v", err)
	}
	return env
}

// TestVerifyBackup_AuthenticatesEveryChunk_Bug215 is the non-vacuity +
// detection pair for the per-chain decrypt probe.
func TestVerifyBackup_AuthenticatesEveryChunk_Bug215(t *testing.T) {
	ctx := context.Background()

	t.Run("honest chain: the probe actually OPENS the chunk", func(t *testing.T) {
		env := verifyProbeEnvelope(t)
		rep, err := VerifyBackupCodedReport(ctx, verifyProbeChain(t, env, nil), VerifyOptions{Envelope: env})
		if err != nil {
			t.Fatalf("honest encrypted chain must verify clean: %v", err)
		}
		if rep.Authenticated != rep.Chunks || rep.Chunks == 0 {
			t.Fatalf("authenticated %d of %d chunks; want every one — a per-chain chain whose chunks are never "+
				"opened is exactly the vacuous probe Bug 215 hid behind", rep.Authenticated, rep.Chunks)
		}
	})

	t.Run("wrong CEK: refused, coded AUTH-FAILED (Bug 215's artifact)", func(t *testing.T) {
		env := verifyProbeEnvelope(t)
		store := verifyProbeChain(t, env, func(_, aad []byte) ([]byte, []byte) {
			other, err := crypto.GenerateCEK()
			if err != nil {
				t.Fatalf("GenerateCEK: %v", err)
			}
			return other, aad // sealed under a key the chain does not advertise
		})
		rep, err := VerifyBackupCodedReport(ctx, store, VerifyOptions{Envelope: env})
		if err == nil {
			t.Fatalf("verify returned rc=0 (chunks=%d decrypted=%d) on a chunk sealed under the WRONG CEK — "+
				"restore cannot read it", rep.Chunks, rep.Authenticated)
		}
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupChunkAuthFailed {
			t.Errorf("err = %v; want coded %s", err, sluicecode.CodeBackupChunkAuthFailed)
		}
	})

	t.Run("wrong AAD: refused too (the binding, not just the key)", func(t *testing.T) {
		// The Bug-74-style half: a chunk whose key is right but whose
		// position/table binding is not — a splice, or a manifest
		// relabelled around an intact ciphertext. Restore refuses it;
		// so must verify.
		env := verifyProbeEnvelope(t)
		store := verifyProbeChain(t, env, func(cek, aad []byte) ([]byte, []byte) {
			return cek, append(append([]byte(nil), aad...), []byte("\nschema=elsewhere")...)
		})
		if _, err := VerifyBackupCodedReport(ctx, store, VerifyOptions{Envelope: env}); err == nil {
			t.Fatal("verify returned rc=0 on a chunk bound to a DIFFERENT position/table; restore refuses it")
		}
	})

	t.Run("no envelope: SHA-only, nothing claimed (control)", func(t *testing.T) {
		// The legacy posture must stay available and must NOT claim
		// decryption it did not do — the honest version of the
		// `decrypt_probe=true` line that started all this.
		env := verifyProbeEnvelope(t)
		store := verifyProbeChain(t, env, func(_, aad []byte) ([]byte, []byte) {
			other, err := crypto.GenerateCEK()
			if err != nil {
				t.Fatalf("GenerateCEK: %v", err)
			}
			return other, aad
		})
		rep, err := VerifyBackupCodedReport(ctx, store, VerifyOptions{})
		if err != nil {
			t.Fatalf("SHA-only verify of an intact-bytes chain: %v", err)
		}
		if rep.Authenticated != 0 {
			t.Errorf("Authenticated = %d without an envelope; want 0", rep.Authenticated)
		}
	})
}
