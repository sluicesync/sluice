// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
)

// TestIncrementalAlignEncryption_PerChunkDecryptProbe_Bug117_Ingestion
// pins the Bug 117 ingestion-path closure (v0.96.3): pre-fix per-chunk-
// mode incrementals silently accepted a rotated passphrase at incremental
// start because the per-chunk path returned nil for the chain CEK
// without probing any of the parent's existing chunk WrappedCEKs. The
// loud failure only surfaced later at restore. Post-fix,
// alignEncryption probes the parent's first per-chunk WrappedCEK
// against the operator's envelope and refuses loudly when it fails.
//
// This is the symmetric ingestion-path closure to v0.94.1's
// VerifyBackupWith decrypt probe (verify path).
//
// Matrix:
//  1. Per-chunk mode + correct envelope at incremental start → probe
//     passes, alignEncryption returns nil-CEK + nil-err (per-chunk-mode
//     contract: no chain CEK).
//  2. Per-chunk mode + ROTATED envelope at incremental start → probe
//     fails, alignEncryption returns a wrapped "passphrase rotated
//     mid-chain?" error naming the Bug 117 signal.
//  3. Per-chain mode + correct envelope → unchanged behaviour (chain
//     CEK probe passes; no per-chunk probe).
//  4. Per-chain mode + ROTATED envelope → existing chain-CEK probe
//     fires first, NOT the new per-chunk probe (load-bearing: the
//     pre-fix invariant is preserved for per-chain mode).
func TestIncrementalAlignEncryption_PerChunkDecryptProbe_Bug117_Ingestion(t *testing.T) {
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
			ChunkRows: 1,
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

	readRoot := func(t *testing.T, store ir.BackupStore) *ir.Manifest {
		t.Helper()
		m, err := ReadRootManifest(context.Background(), store)
		if err != nil {
			t.Fatalf("ReadRootManifest: %v", err)
		}
		return m
	}

	t.Run("per-chunk mode + correct envelope → probe passes, nil CEK", func(t *testing.T) {
		env := newEnv(t, "rotation-pass")
		store := runBackup(t, crypto.EncryptModePerChunk, env)
		parent := readRoot(t, store)

		readEnv := rebindForChain(t, store, "rotation-pass")
		inc := &IncrementalBackup{
			segStore:   store,
			Encryption: &BackupEncryption{Envelope: readEnv, Mode: crypto.EncryptModePerChunk},
		}
		cek, err := inc.alignEncryption(context.Background(), parent)
		if err != nil {
			t.Fatalf("alignEncryption: unexpected err: %v", err)
		}
		if cek != nil {
			t.Errorf("cek = %v; per-chunk mode contract says nil", cek)
		}
	})

	t.Run("per-chunk mode + ROTATED envelope → probe refuses loudly (Bug 117 ingestion signal)", func(t *testing.T) {
		env := newEnv(t, "rotation-pass")
		store := runBackup(t, crypto.EncryptModePerChunk, env)
		parent := readRoot(t, store)

		// Operator rotated to a NEW passphrase post-backup. Pre-fix
		// this passed silently and the incremental wrote new chunks
		// under the rotated envelope; restore would later trip on the
		// passphrase mismatch crossing the rotation boundary.
		wrong := rebindForChain(t, store, "ROTATED-pass")
		inc := &IncrementalBackup{
			segStore:   store,
			Encryption: &BackupEncryption{Envelope: wrong, Mode: crypto.EncryptModePerChunk},
		}
		_, err := inc.alignEncryption(context.Background(), parent)
		if err == nil {
			t.Fatalf("alignEncryption: want loud refuse for rotated passphrase; got nil — the Bug 117 ingestion-path hole is open again")
		}
		if !strings.Contains(err.Error(), "passphrase rotated mid-chain") {
			t.Errorf("error = %q; want substring 'passphrase rotated mid-chain' (the v0.94.1 probe-helper signal)", err.Error())
		}
	})

	t.Run("per-chain mode + correct envelope → unchanged behaviour", func(t *testing.T) {
		env := newEnv(t, "secret-pass")
		store := runBackup(t, crypto.EncryptModePerChain, env)
		parent := readRoot(t, store)

		readEnv := rebindForChain(t, store, "secret-pass")
		inc := &IncrementalBackup{
			segStore:   store,
			Encryption: &BackupEncryption{Envelope: readEnv, Mode: crypto.EncryptModePerChain},
		}
		cek, err := inc.alignEncryption(context.Background(), parent)
		if err != nil {
			t.Fatalf("alignEncryption: unexpected err: %v", err)
		}
		if len(cek) == 0 {
			t.Errorf("per-chain mode should return a non-empty CEK")
		}
	})

	t.Run("per-chain mode + ROTATED envelope → existing chain-CEK probe fires first", func(t *testing.T) {
		env := newEnv(t, "secret-pass")
		store := runBackup(t, crypto.EncryptModePerChain, env)
		parent := readRoot(t, store)

		wrong := rebindForChain(t, store, "WRONG-pass")
		inc := &IncrementalBackup{
			segStore:   store,
			Encryption: &BackupEncryption{Envelope: wrong, Mode: crypto.EncryptModePerChain},
		}
		_, err := inc.alignEncryption(context.Background(), parent)
		if err == nil {
			t.Fatalf("alignEncryption: want refuse on wrong passphrase; got nil")
		}
		if !strings.Contains(err.Error(), "unwrap parent chain cek") {
			t.Errorf("error = %q; want substring 'unwrap parent chain cek' (the existing per-chain probe path)", err.Error())
		}
	})
}

// TestFirstPerChunkProbe exercises the small helper directly. The
// alignEncryption probe is gated on this returning a non-nil chunk; if
// the helper regresses (e.g. forgets ChangeChunks or table iteration),
// the ingestion-path probe silently no-ops and Bug 117 reopens.
func TestFirstPerChunkProbe(t *testing.T) {
	chunk := func(wrapped string) *ir.ChunkInfo {
		return &ir.ChunkInfo{
			File: "chunks/_table_users/0.gz",
			Encryption: &ir.ChunkEncryption{
				WrappedCEK: []byte(wrapped),
			},
		}
	}

	t.Run("nil manifest", func(t *testing.T) {
		if got := firstPerChunkProbe(nil); got != nil {
			t.Errorf("firstPerChunkProbe(nil) = %v; want nil", got)
		}
	})

	t.Run("full manifest with per-chunk-mode table chunks", func(t *testing.T) {
		m := &ir.Manifest{
			Tables: []*ir.TableManifest{
				{Name: "users", Chunks: []*ir.ChunkInfo{chunk("wrapped-cek-bytes")}},
			},
		}
		got := firstPerChunkProbe(m)
		if got == nil || string(got.Encryption.WrappedCEK) != "wrapped-cek-bytes" {
			t.Errorf("firstPerChunkProbe missed the Tables[].Chunks probe candidate; got %v", got)
		}
	})

	t.Run("incremental manifest with ChangeChunks fallback", func(t *testing.T) {
		m := &ir.Manifest{
			ChangeChunks: []*ir.ChunkInfo{chunk("incremental-cek")},
		}
		got := firstPerChunkProbe(m)
		if got == nil || string(got.Encryption.WrappedCEK) != "incremental-cek" {
			t.Errorf("firstPerChunkProbe missed the ChangeChunks fallback; got %v", got)
		}
	})

	t.Run("per-chain-mode chunks (empty WrappedCEK) skipped", func(t *testing.T) {
		m := &ir.Manifest{
			Tables: []*ir.TableManifest{
				{Name: "users", Chunks: []*ir.ChunkInfo{{File: "x", Encryption: &ir.ChunkEncryption{WrappedCEK: nil}}}},
			},
		}
		if got := firstPerChunkProbe(m); got != nil {
			t.Errorf("firstPerChunkProbe should skip per-chain-mode chunks; got %v", got)
		}
	})

	t.Run("plaintext chunks (nil Encryption) skipped", func(t *testing.T) {
		m := &ir.Manifest{
			Tables: []*ir.TableManifest{
				{Name: "users", Chunks: []*ir.ChunkInfo{{File: "x"}}},
			},
		}
		if got := firstPerChunkProbe(m); got != nil {
			t.Errorf("firstPerChunkProbe should skip plaintext chunks; got %v", got)
		}
	})

	t.Run("empty manifest → nil (caller falls through)", func(t *testing.T) {
		if got := firstPerChunkProbe(&ir.Manifest{}); got != nil {
			t.Errorf("firstPerChunkProbe(empty) = %v; want nil", got)
		}
	})

	t.Run("Tables before ChangeChunks (full-manifest precedence)", func(t *testing.T) {
		m := &ir.Manifest{
			Tables: []*ir.TableManifest{
				{Name: "users", Chunks: []*ir.ChunkInfo{chunk("table-cek")}},
			},
			ChangeChunks: []*ir.ChunkInfo{chunk("change-cek")},
		}
		got := firstPerChunkProbe(m)
		if got == nil || string(got.Encryption.WrappedCEK) != "table-cek" {
			t.Errorf("firstPerChunkProbe should prefer Tables[].Chunks; got %v", got)
		}
	})
}
