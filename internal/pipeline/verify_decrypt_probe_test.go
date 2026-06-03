// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
)

// TestVerifyBackupWith_DecryptProbe_Bug117 pins the Bug 117 (v0.94.1)
// closure: per-chunk-mode chains accept passphrase rotation silently
// pre-fix because SHA-256 verify hashes the on-disk bytes
// (compressed + encrypted) and a rotation-after-chunk re-wraps later
// chunks under the new envelope without changing the SHA observation
// surface. Post-fix, VerifyBackupWith unwraps each chunk's WrappedCEK
// under the supplied envelope and counts an unwrap failure as a
// verify failure with a clear chunk-naming error.
//
// Matrix:
//  1. Plaintext chain + nil envelope → SHA-only, all OK.
//  2. Per-chain mode + correct envelope → chain-CEK unwrap probe
//     passes; no per-chunk probe (per-chain WrappedCEK is empty).
//  3. Per-chain mode + WRONG envelope → up-front chain-CEK unwrap
//     fails fast with an irrecoverable error before chunk iteration.
//  4. Per-chunk mode + correct envelope → every chunk's WrappedCEK
//     unwrap probe passes; SHA-only would also pass; no failures.
//  5. Per-chunk mode + WRONG envelope → every chunk's WrappedCEK
//     unwrap probe fails; SHA-only would still pass; failed == total.
//  6. Per-chain mode + mismatched KEKMode → up-front mode check
//     refuses fast with a descriptive error.
func TestVerifyBackupWith_DecryptProbe_Bug117(t *testing.T) {
	schema := &ir.Schema{
		Tables: []*ir.Table{
			{
				Name: "users",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
					{Name: "name", Type: ir.Varchar{Length: 100}},
				},
			},
		},
	}
	rows := map[string][]ir.Row{
		"users": {
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
			{"id": int64(3), "name": "Carol"},
		},
	}

	runBackup := func(t *testing.T, mode string, env crypto.EnvelopeEncryption) ir.BackupStore {
		t.Helper()
		dir := t.TempDir()
		store, err := NewLocalStore(dir)
		if err != nil {
			t.Fatalf("NewLocalStore: %v", err)
		}
		src := newBackupRecorderEngine("postgres", schema, rows)
		b := &Backup{
			Source:    src,
			SourceDSN: "src",
			Store:     store,
			ChunkRows: 1, // force 3 chunks
		}
		if env != nil {
			b.Encryption = &BackupEncryption{Envelope: env, Mode: mode}
		}
		if err := b.Run(context.Background()); err != nil {
			t.Fatalf("Backup.Run: %v", err)
		}
		return store
	}

	newEnv := func(t *testing.T, pass string) crypto.EnvelopeEncryption {
		t.Helper()
		p, err := crypto.DefaultArgon2idParams()
		if err != nil {
			t.Fatalf("DefaultArgon2idParams: %v", err)
		}
		env, err := crypto.NewPassphraseEnvelope(pass, p)
		if err != nil {
			t.Fatalf("NewPassphraseEnvelope: %v", err)
		}
		return env
	}

	// Re-derive an envelope against a chain's recorded Argon2id params
	// (the same shape buildReadEnvelope uses). Required so the read
	// envelope unwraps the chain's WrappedCEK.
	rebindForChain := func(t *testing.T, store ir.BackupStore, pass string) crypto.EnvelopeEncryption {
		t.Helper()
		m, err := ReadRootManifest(context.Background(), store)
		if err != nil {
			t.Fatalf("ReadRootManifest: %v", err)
		}
		if m.ChainEncryption == nil || m.ChainEncryption.Argon2id == nil {
			t.Fatalf("chain root missing Argon2id params; cannot rebind envelope")
		}
		ap := m.ChainEncryption.Argon2id
		params := crypto.Argon2idParams{
			Salt:        ap.Salt,
			Memory:      ap.Memory,
			Iterations:  ap.Iterations,
			Parallelism: ap.Parallelism,
			KeyLen:      ap.KeyLen,
		}
		env, err := crypto.NewPassphraseEnvelope(pass, params)
		if err != nil {
			t.Fatalf("NewPassphraseEnvelope (rebind): %v", err)
		}
		return env
	}

	t.Run("plaintext chain + nil envelope → SHA-only OK", func(t *testing.T) {
		store := runBackup(t, "", nil)
		total, failed, err := VerifyBackupWith(context.Background(), store, VerifyOptions{})
		if err != nil {
			t.Fatalf("VerifyBackupWith: %v", err)
		}
		if total != 3 || failed != 0 {
			t.Errorf("total/failed = %d/%d; want 3/0", total, failed)
		}
	})

	t.Run("per-chain mode + correct envelope → chain probe passes", func(t *testing.T) {
		env := newEnv(t, "secret-pass")
		store := runBackup(t, crypto.EncryptModePerChain, env)
		readEnv := rebindForChain(t, store, "secret-pass")
		total, failed, err := VerifyBackupWith(context.Background(), store, VerifyOptions{Envelope: readEnv})
		if err != nil {
			t.Fatalf("VerifyBackupWith: %v", err)
		}
		if total != 3 || failed != 0 {
			t.Errorf("total/failed = %d/%d; want 3/0", total, failed)
		}
	})

	t.Run("per-chain mode + wrong envelope → up-front chain CEK unwrap fails fast", func(t *testing.T) {
		env := newEnv(t, "secret-pass")
		store := runBackup(t, crypto.EncryptModePerChain, env)
		wrong := rebindForChain(t, store, "WRONG-pass") // same params, different passphrase
		_, _, err := VerifyBackupWith(context.Background(), store, VerifyOptions{Envelope: wrong})
		if err == nil {
			t.Fatalf("VerifyBackupWith: want error for wrong passphrase; got nil")
		}
		if !strings.Contains(err.Error(), "unwrap chain cek") {
			t.Errorf("error = %q; want substring 'unwrap chain cek'", err.Error())
		}
	})

	t.Run("per-chunk mode + correct envelope → every chunk probe passes", func(t *testing.T) {
		env := newEnv(t, "rotation-pass")
		store := runBackup(t, crypto.EncryptModePerChunk, env)
		readEnv := rebindForChain(t, store, "rotation-pass")
		total, failed, err := VerifyBackupWith(context.Background(), store, VerifyOptions{Envelope: readEnv})
		if err != nil {
			t.Fatalf("VerifyBackupWith: %v", err)
		}
		if total != 3 || failed != 0 {
			t.Errorf("total/failed = %d/%d; want 3/0", total, failed)
		}
	})

	t.Run("per-chunk mode + wrong envelope → every chunk decrypt probe fails (Bug 117 signal)", func(t *testing.T) {
		env := newEnv(t, "rotation-pass")
		store := runBackup(t, crypto.EncryptModePerChunk, env)
		// Bug 117 scenario: operator rotated to a NEW passphrase
		// post-backup. SHA-only verify (legacy) passes silently.
		wrong := rebindForChain(t, store, "ROTATED-pass")
		total, failed, err := VerifyBackupWith(context.Background(), store, VerifyOptions{Envelope: wrong})
		if err != nil {
			t.Fatalf("VerifyBackupWith: unexpected irrecoverable err: %v", err)
		}
		if total != 3 {
			t.Errorf("total = %d; want 3", total)
		}
		if failed != total {
			t.Errorf("failed = %d; want %d (every per-chunk WrappedCEK should refuse the rotated passphrase)", failed, total)
		}

		// Belt-and-suspenders: confirm SHA-only verify on the SAME
		// store still reports 0 failed (the bug-class signature).
		shaTotal, shaFailed, shaErr := VerifyBackup(context.Background(), store)
		if shaErr != nil {
			t.Fatalf("legacy VerifyBackup: %v", shaErr)
		}
		if shaTotal != 3 || shaFailed != 0 {
			t.Errorf("SHA-only on per-chunk-mode chain: total/failed = %d/%d; want 3/0 (this is the Bug 117 silent-accept the decrypt probe fixes)", shaTotal, shaFailed)
		}
	})

	t.Run("envelope mode mismatch → up-front mode-check refuses fast", func(t *testing.T) {
		env := newEnv(t, "secret-pass")
		store := runBackup(t, crypto.EncryptModePerChain, env)
		// Hand a stub envelope whose Mode() lies about being KMS so the
		// up-front kek_mode check triggers without us needing a real
		// KMS-backed envelope. UnwrapCEK is unreachable on this path.
		stub := &modeStubEnvelope{mode: crypto.KEKModeAWSKMS}
		_, _, err := VerifyBackupWith(context.Background(), store, VerifyOptions{Envelope: stub})
		if err == nil {
			t.Fatalf("VerifyBackupWith: want envelope-mode mismatch error; got nil")
		}
		if !strings.Contains(err.Error(), "does not match chain's recorded kek_mode") {
			t.Errorf("error = %q; want substring about mode mismatch", err.Error())
		}
	})
}

// modeStubEnvelope satisfies [crypto.EnvelopeEncryption] for the mode
// mismatch test. Wrap/Unwrap are unreachable on the tested path so
// they return errors loudly if accidentally hit.
type modeStubEnvelope struct{ mode string }

func (s *modeStubEnvelope) Mode() string { return s.mode }
func (s *modeStubEnvelope) WrapCEK([]byte) ([]byte, error) {
	return nil, errors.New("stub: WrapCEK not implemented")
}

func (s *modeStubEnvelope) UnwrapCEK([]byte) ([]byte, error) {
	return nil, errors.New("stub: UnwrapCEK not implemented")
}

// TestProbeChunkDecrypt_NilSafe pins the small no-op branches.
func TestProbeChunkDecrypt_NilSafe(t *testing.T) {
	if err := probeChunkDecrypt(nil, nil); err != nil {
		t.Errorf("nil env + nil chunk: %v", err)
	}
	if err := probeChunkDecrypt(nil, &ir.ChunkInfo{}); err != nil {
		t.Errorf("nil env + non-nil chunk: %v", err)
	}
	env := &modeStubEnvelope{mode: "test"}
	if err := probeChunkDecrypt(env, &ir.ChunkInfo{}); err != nil {
		t.Errorf("env + plaintext chunk (Encryption nil): %v", err)
	}
	// Per-chain mode chunk: Encryption non-nil but WrappedCEK empty
	// → probe is a no-op (chain-level probe covers it).
	if err := probeChunkDecrypt(env, &ir.ChunkInfo{Encryption: &ir.ChunkEncryption{}}); err != nil {
		t.Errorf("env + per-chain-mode chunk (empty WrappedCEK): %v", err)
	}
}
