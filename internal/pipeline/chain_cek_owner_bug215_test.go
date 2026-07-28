// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// Bug 215 — the unit half of the chain-CEK-owner pin. The integration
// matrix proves the round trip; this proves the DECISION, in
// milliseconds, at the exact function that made it wrongly.
//
// The shape a rotated encrypted chain has on disk, which is the whole
// reason the bug exists: the lineage root's `manifest.json` carries the
// chain CEK, and EVERY rotation-born segment full carries a DIFFERENT
// one (each runs the ordinary `backup full` orchestrator against an
// empty provisional dir, so `Backup.setupChainEncryption` takes its
// prior == nil arm and mints a fresh CEK). Two keys, one chain.
//
// The read path then uses them asymmetrically, deliberately:
// `ChainRestore.applyFull` scopes a `Restore` to the segment store, so a
// segment full's ROW chunks open under that segment's key; but every
// incremental's CHANGE chunks open under the CHAIN ROOT's, whatever
// segment they live in. So a chain-EXTENDING writer has exactly one
// correct answer, and pre-fix it resolved through the open segment and
// got the other one.

// bug215RotatedChainStore builds that shape: a root manifest wrapping
// rootCEK and a segment full at seg-1/ wrapping segCEK, both under the
// same passphrase envelope. Returns the store and the two CEKs.
func bug215RotatedChainStore(t *testing.T, env crypto.EnvelopeEncryption) (store *memStore, rootCEK, segCEK []byte) {
	t.Helper()
	ctx := context.Background()
	store = newMemStore()

	mk := func(dir string, createdAt time.Time) []byte {
		cek, err := crypto.GenerateCEK()
		if err != nil {
			t.Fatalf("GenerateCEK: %v", err)
		}
		m := &irbackup.Manifest{
			FormatVersion: irbackup.BackupFormatVersion,
			SourceEngine:  "postgres",
			Kind:          irbackup.BackupKindFull,
			CreatedAt:     createdAt,
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
		if err := lineage.WriteManifestAt(ctx, lineage.NewPrefixedStore(store, dir), lineage.ManifestFileName, m); err != nil {
			t.Fatalf("WriteManifestAt(%q): %v", dir, err)
		}
		return cek
	}
	base := time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)
	rootCEK = mk("", base)
	segCEK = mk("seg-1", base.Add(time.Hour))
	if bytes.Equal(rootCEK, segCEK) {
		t.Fatal("root and segment CEKs collided; the fixture cannot distinguish the two answers")
	}
	return store, rootCEK, segCEK
}

// TestChainCEKOwner_ExtendingWriterUsesTheChainRoot_Bug215 pins that
// BOTH chain-extending writers seal their chunks under the CHAIN ROOT's
// CEK when the open segment is a rotation-born one carrying its own.
//
// Pre-fix both resolved through `b.segStore`, so both returned segCEK,
// and every chunk they wrote was sealed with a key `ChainRestore` never
// tries: exit 3 at `chunk failed authenticated decryption` on a chain
// `backup verify` had just called healthy.
func TestChainCEKOwner_ExtendingWriterUsesTheChainRoot_Bug215(t *testing.T) {
	ctx := context.Background()
	env := cheapTestEnvelope(t)
	store, rootCEK, segCEK := bug215RotatedChainStore(t, env)

	// The parent an extending writer resolves on a rotation-born open
	// segment with no committed incremental: the SEGMENT's own full.
	segFull, err := lineage.ReadManifestAt(ctx, lineage.NewPrefixedStore(store, "seg-1"), lineage.ManifestFileName)
	if err != nil {
		t.Fatalf("read segment full: %v", err)
	}
	if segFull.ChainEncryption == nil {
		t.Fatal("fixture segment full carries no chain header")
	}

	writers := map[string]func() ([]byte, error){
		"IncrementalBackup": func() ([]byte, error) {
			b := &IncrementalBackup{
				Store:      store,
				segStore:   lineage.NewPrefixedStore(store, "seg-1"),
				Encryption: &lineage.BackupEncryption{Envelope: env},
			}
			return b.alignEncryption(ctx, segFull)
		},
		"BackupStream": func() ([]byte, error) {
			b := &BackupStream{
				Store:      store,
				segStore:   lineage.NewPrefixedStore(store, "seg-1"),
				Encryption: &lineage.BackupEncryption{Envelope: env},
			}
			return b.alignEncryption(ctx, segFull)
		},
	}
	for name, align := range writers {
		t.Run(name, func(t *testing.T) {
			got, err := align()
			if err != nil {
				t.Fatalf("alignEncryption: %v", err)
			}
			if bytes.Equal(got, segCEK) {
				t.Fatal("resolved the OPEN SEGMENT's CEK — Bug 215 is back: every chunk this writer seals " +
					"is unreadable by ChainRestore, which decrypts incrementals with the chain root's key")
			}
			if !bytes.Equal(got, rootCEK) {
				t.Fatalf("resolved neither CEK: got %x, want the chain root's %x", got, rootCEK)
			}
		})
	}
}

// TestChainCEKOwner_MatchesWhatChainRestoreWillUse_Bug215 is the
// agreement half: the writer's answer must be BYTE-IDENTICAL to the key
// the read path derives. Asserting only "the writer picked the root"
// would still pass if the read side later moved; this compares the two
// derivations directly, which is the invariant that actually matters.
func TestChainCEKOwner_MatchesWhatChainRestoreWillUse_Bug215(t *testing.T) {
	ctx := context.Background()
	env := cheapTestEnvelope(t)
	store, rootCEK, _ := bug215RotatedChainStore(t, env)

	segFull, err := lineage.ReadManifestAt(ctx, lineage.NewPrefixedStore(store, "seg-1"), lineage.ManifestFileName)
	if err != nil {
		t.Fatalf("read segment full: %v", err)
	}
	writerCEK, err := (&IncrementalBackup{
		Store:      store,
		segStore:   lineage.NewPrefixedStore(store, "seg-1"),
		Encryption: &lineage.BackupEncryption{Envelope: env},
	}).alignEncryption(ctx, segFull)
	if err != nil {
		t.Fatalf("alignEncryption: %v", err)
	}

	// The read side, derived independently the way ChainRestore's
	// preflight does: unwrap the LINEAGE ROOT manifest's wrap.
	root, err := lineage.ReadRootManifest(ctx, store)
	if err != nil {
		t.Fatalf("ReadRootManifest: %v", err)
	}
	readerCEK, err := lineage.UnwrapChainCEK(env, root.ChainEncryption.WrappedCEK, root)
	if err != nil {
		t.Fatalf("UnwrapChainCEK: %v", err)
	}
	if !bytes.Equal(writerCEK, readerCEK) {
		t.Fatalf("writer and reader disagree about the chain CEK: writer %x, reader %x — "+
			"every chunk the writer seals will fail authenticated decryption at restore", writerCEK, readerCEK)
	}
	if !bytes.Equal(readerCEK, rootCEK) {
		t.Fatal("the reader's derivation drifted off the chain root; retarget this pin")
	}
}
