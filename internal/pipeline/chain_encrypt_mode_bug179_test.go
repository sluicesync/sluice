// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestChainEncryptModeInheritAndRefuse_Bug179 pins the fix for Bug 179: a
// segment extending an encrypted chain must use the CHAIN's encryption mode,
// not this invocation's --encrypt-mode. Pre-fix, alignEncryption inherited
// the parent's mode for the chain-CEK derivation but the sibling
// resolveChunkCEK read b.Encryption.Mode (the operator flag, defaulting
// per-chain) for WRITING chunks — so a per-chunk chain + a mode-omitted
// incremental/stream built and verified fine yet was UN-RESTORABLE ("chain
// CEK is unset" at restore, only the full's rows land).
//
// Post-fix, alignEncryption is the single authority: it inherits the chain's
// mode when --encrypt-mode is omitted (so resolveChunkCEK agrees), and
// refuses LOUDLY at build time when an explicit --encrypt-mode conflicts.
//
// The matrix runs against BOTH chain-extending orchestrators (they share the
// identical mode-resolution code) × {parent per-chunk, parent per-chain} ×
// {omitted, explicit-matching, explicit-conflicting}.
func TestChainEncryptModeInheritAndRefuse_Bug179(t *testing.T) {
	schema := &ir.Schema{
		Tables: []*ir.Table{{
			Name: "users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
				{Name: "name", Type: ir.Varchar{Length: 100}},
			},
		}},
	}
	rows := map[string][]ir.Row{"users": {
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}}

	const pass = "chain-pass"

	newEnv := func(t *testing.T, params crypto.Argon2idParams) crypto.EnvelopeEncryption {
		t.Helper()
		env, err := crypto.NewPassphraseEnvelope(pass, params)
		if err != nil {
			t.Fatalf("NewPassphraseEnvelope: %v", err)
		}
		return env
	}

	// buildChain runs a full backup at `mode` and returns its store + parsed
	// root manifest (the chain the segment will extend).
	buildChain := func(t *testing.T, mode string) (irbackup.Store, *irbackup.Manifest) {
		t.Helper()
		store, err := blobcodec.NewLocalStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewLocalStore: %v", err)
		}
		params, err := crypto.DefaultArgon2idParams()
		if err != nil {
			t.Fatalf("DefaultArgon2idParams: %v", err)
		}
		b := &backup.Backup{
			Source:     newBackupRecorderEngine("postgres", schema, rows),
			SourceDSN:  "src",
			Store:      store,
			ChunkRows:  1,
			Encryption: &lineage.BackupEncryption{Envelope: newEnv(t, params), Mode: mode},
		}
		if err := b.Run(context.Background()); err != nil {
			t.Fatalf("Backup.Run(%s): %v", mode, err)
		}
		parent, err := lineage.ReadRootManifest(context.Background(), store)
		if err != nil {
			t.Fatalf("ReadRootManifest: %v", err)
		}
		return store, parent
	}

	// rebindEnv mints the read-side envelope against the chain's recorded salt
	// (CLI envelopes carry a fresh salt; alignEncryption rebinds internally,
	// but the test envelope must derive the same KEK to unwrap).
	rebindEnv := func(t *testing.T, parent *irbackup.Manifest) crypto.EnvelopeEncryption {
		t.Helper()
		if parent.ChainEncryption == nil || parent.ChainEncryption.Argon2id == nil {
			t.Fatal("chain root missing Argon2id params")
		}
		ap := parent.ChainEncryption.Argon2id
		return newEnv(t, crypto.Argon2idParams{
			Salt: ap.Salt, Memory: ap.Memory, Iterations: ap.Iterations,
			Parallelism: ap.Parallelism, KeyLen: ap.KeyLen,
		})
	}

	// A chain-extending orchestrator, reduced to the surface this pin needs:
	// set the segment's --encrypt-mode, align against the parent, and expose
	// the mode resolveChunkCEK will see afterward (b.Encryption.Mode).
	type extender struct {
		name    string
		align   func(store irbackup.Store, env crypto.EnvelopeEncryption, incMode string, parent *irbackup.Manifest) ([]byte, error)
		encMode func() string // b.Encryption.Mode after align — what resolveChunkCEK reads
		chunk   func(chainCEK []byte) (cek, wrapped []byte, err error)
	}

	newExtenders := func() []*extender {
		var inc *IncrementalBackup
		var bs *BackupStream
		return []*extender{
			{
				name: "IncrementalBackup",
				align: func(store irbackup.Store, env crypto.EnvelopeEncryption, incMode string, parent *irbackup.Manifest) ([]byte, error) {
					inc = &IncrementalBackup{segStore: store, Encryption: &lineage.BackupEncryption{Envelope: env, Mode: incMode}}
					return inc.alignEncryption(context.Background(), parent)
				},
				encMode: func() string { return inc.Encryption.Mode },
				chunk:   func(cek []byte) ([]byte, []byte, error) { return inc.resolveChunkCEK(cek) },
			},
			{
				name: "BackupStream",
				align: func(store irbackup.Store, env crypto.EnvelopeEncryption, incMode string, parent *irbackup.Manifest) ([]byte, error) {
					bs = &BackupStream{segStore: store, Encryption: &lineage.BackupEncryption{Envelope: env, Mode: incMode}}
					return bs.alignEncryption(context.Background(), parent)
				},
				encMode: func() string { return bs.Encryption.Mode },
				chunk:   func(cek []byte) ([]byte, []byte, error) { return bs.resolveChunkCEK(cek) },
			},
		}
	}

	for _, chainMode := range []string{crypto.EncryptModePerChunk, crypto.EncryptModePerChain} {
		store, parent := buildChain(t, chainMode)

		// (1) omitted --encrypt-mode → inherit the chain's mode; the segment
		// is restorable because resolveChunkCEK now writes in the chain's mode.
		t.Run("inherit/"+chainMode, func(t *testing.T) {
			for _, ex := range newExtenders() {
				cek, err := ex.align(store, rebindEnv(t, parent), "", parent)
				if err != nil {
					t.Fatalf("%s: align(omitted mode) err = %v; want inherit", ex.name, err)
				}
				if got := ex.encMode(); got != chainMode {
					t.Fatalf("%s: inherited mode = %q; want chain's %q", ex.name, got, chainMode)
				}
				// The mode-source split (Bug 179) is closed iff resolveChunkCEK
				// now behaves per the inherited chain mode:
				gotCEK, gotWrapped, cerr := ex.chunk(cek)
				if cerr != nil {
					t.Fatalf("%s: resolveChunkCEK err = %v", ex.name, cerr)
				}
				switch chainMode {
				case crypto.EncryptModePerChunk:
					// per-chunk: fresh CEK + wrap per chunk (NOT the nil chain CEK).
					if len(gotCEK) == 0 || len(gotWrapped) == 0 {
						t.Errorf("%s: per-chunk inherit → resolveChunkCEK gave cek=%d wrapped=%d; want both non-empty (the Bug 179 un-restorable path)", ex.name, len(gotCEK), len(gotWrapped))
					}
				case crypto.EncryptModePerChain:
					// per-chain: reuse the chain CEK, empty per-chunk wrap.
					if len(gotWrapped) != 0 {
						t.Errorf("%s: per-chain inherit → resolveChunkCEK wrapped=%d; want empty", ex.name, len(gotWrapped))
					}
				}
			}
		})

		// (2) explicit MATCHING --encrypt-mode → accepted (no false refusal).
		t.Run("explicit-match/"+chainMode, func(t *testing.T) {
			for _, ex := range newExtenders() {
				if _, err := ex.align(store, rebindEnv(t, parent), chainMode, parent); err != nil {
					t.Errorf("%s: align(explicit matching %q) err = %v; want accept", ex.name, chainMode, err)
				}
			}
		})

		// (3) explicit CONFLICTING --encrypt-mode → LOUD build-time refusal.
		conflicting := crypto.EncryptModePerChain
		if chainMode == crypto.EncryptModePerChain {
			conflicting = crypto.EncryptModePerChunk
		}
		t.Run("refuse-conflict/"+chainMode, func(t *testing.T) {
			for _, ex := range newExtenders() {
				_, err := ex.align(store, rebindEnv(t, parent), conflicting, parent)
				if err == nil {
					t.Fatalf("%s: chain=%q + --encrypt-mode=%q built OK; want LOUD refuse (Bug 179: un-restorable chain)", ex.name, chainMode, conflicting)
				}
				if !strings.Contains(err.Error(), "conflicts with the chain's encryption mode") {
					t.Errorf("%s: refuse error = %q; want 'conflicts with the chain's encryption mode'", ex.name, err.Error())
				}
				// Coded since audit 2026-07-16 M3.1: same class as the
				// other encryption-state conflicts.
				assertCoded(t, err, sluicecode.CodeBackupEncryptionMismatch)
			}
		})
	}
}
