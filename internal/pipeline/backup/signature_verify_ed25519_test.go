// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// buildEd25519SignedChain writes a 2-link chain (full + 1 incremental)
// signed under an Ed25519 keypair, returning the store, the PUBLIC verify
// key, and the walked links. Mirrors buildSignedChain (HMAC) but uses the
// Phase 2 asymmetric scheme.
func buildEd25519SignedChain(t *testing.T) (*memStore, ed25519.PublicKey, []lineage.SegmentRecord) {
	t.Helper()
	ctx := context.Background()
	pub, priv, err := crypto.GenerateEd25519Keypair()
	if err != nil {
		t.Fatal(err)
	}
	signer := lineage.NewEd25519Signer(priv)
	store := newMemStore()

	full := &irbackup.Manifest{
		FormatVersion: irbackup.FormatVersionSignedManifest,
		CreatedAt:     time.Date(2026, 7, 9, 0, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Kind:          irbackup.BackupKindFull,
		BackupID:      "full0001",
		SchemaHash:    "fh",
		Tables: []*irbackup.TableManifest{
			{Name: "t", RowCount: 1, Chunks: []*irbackup.ChunkInfo{{File: "chunks/t/t-0.jsonl.gz", RowCount: 1, SHA256: "s0"}}},
		},
	}
	if err := lineage.WriteManifest(ctx, store, full); err != nil {
		t.Fatal(err)
	}
	lineage.UpdateLineageForManifestBestEffort(ctx, store, full, lineage.ManifestFileName, blobcodec.DefaultCodec)

	incrPath := "manifests/incr-0001.json"
	incr := &irbackup.Manifest{
		FormatVersion:  irbackup.FormatVersionSignedManifest,
		CreatedAt:      time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC),
		SourceEngine:   "postgres",
		Kind:           irbackup.BackupKindIncremental,
		BackupID:       "incr0001",
		ParentBackupID: "full0001",
		SchemaHash:     "fh",
		ChangeChunks: []*irbackup.ChunkInfo{
			{File: "chunks/_changes/c-0.jsonl.gz", RowCount: 2, SHA256: "cs0"},
			{File: "chunks/_changes/c-1.jsonl.gz", RowCount: 2, SHA256: "cs1"},
		},
	}
	if err := lineage.WriteManifestAt(ctx, store, incrPath, incr); err != nil {
		t.Fatal(err)
	}
	lineage.UpdateLineageForManifestBestEffort(ctx, store, incr, incrPath, blobcodec.DefaultCodec)

	if err := lineage.ResignLineage(ctx, store, signer); err != nil {
		t.Fatal(err)
	}
	links, err := lineage.ListAllSegmentManifests(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 2 {
		t.Fatalf("expected 2 links, got %d", len(links))
	}
	return store, pub, links
}

// TestVerifyChainSignatures_Ed25519_Untampered pins the Phase 2 happy
// path: an Ed25519 chain verifies with only the PUBLIC key (no signing
// secret, no envelope).
func TestVerifyChainSignatures_Ed25519_Untampered(t *testing.T) {
	store, pub, links := buildEd25519SignedChain(t)
	if err := verifyChainSignatures(context.Background(), store, links, verifyMaterial{verifyKey: pub}, false); err != nil {
		t.Fatalf("untampered ed25519 chain refused: %v", err)
	}
}

// TestVerifyChainSignatures_Ed25519_TamperMatrix pins every refusal class
// for the Ed25519 scheme.
func TestVerifyChainSignatures_Ed25519_TamperMatrix(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name     string
		tamper   func(store *memStore, links []lineage.SegmentRecord)
		wantCode sluicecode.Code
	}{
		{"invalid (tampered manifest)", func(_ *memStore, l []lineage.SegmentRecord) { l[1].Manifest.SchemaHash = "x" }, sluicecode.CodeBackupSignatureInvalid},
		{"truncated change-list", func(_ *memStore, l []lineage.SegmentRecord) {
			l[1].Manifest.ChangeChunks = l[1].Manifest.ChangeChunks[:1]
		}, sluicecode.CodeBackupSignatureInvalid},
		{"missing manifest sig", func(s *memStore, l []lineage.SegmentRecord) { _ = s.Delete(ctx, lineage.ManifestSigPath(l[1].Path)) }, sluicecode.CodeBackupSignatureMissing},
		{"dropped-newest-link", func(s *memStore, _ []lineage.SegmentRecord) {
			cat, _, _ := lineage.LoadLineageCatalog(ctx, s)
			cat.Segments[0].Incrementals = nil
			_ = lineage.WriteLineageCatalog(ctx, s, cat)
		}, sluicecode.CodeBackupSignatureInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, pub, links := buildEd25519SignedChain(t)
			tc.tamper(store, links)
			err := verifyChainSignatures(ctx, store, links, verifyMaterial{verifyKey: pub}, false)
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != tc.wantCode {
				t.Fatalf("got %v (code ok=%v), want %s", err, ok, tc.wantCode)
			}
		})
	}
}

// TestVerifyChainSignatures_Ed25519_WrongKey pins that a DIFFERENT public
// key refuses.
func TestVerifyChainSignatures_Ed25519_WrongKey(t *testing.T) {
	store, _, links := buildEd25519SignedChain(t)
	otherPub, _, _ := crypto.GenerateEd25519Keypair()
	err := verifyChainSignatures(context.Background(), store, links, verifyMaterial{verifyKey: otherPub}, false)
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupSignatureInvalid {
		t.Fatalf("wrong verify key: got %v, want SIGNATURE-INVALID", err)
	}
}

// TestVerifyChainSignatures_Ed25519_Policy pins the DR-safe policy for an
// Ed25519 chain with NO verify key: warn-and-proceed by default, refuse
// under --require-signature.
func TestVerifyChainSignatures_Ed25519_Policy(t *testing.T) {
	store, _, links := buildEd25519SignedChain(t)
	ctx := context.Background()
	if err := verifyChainSignatures(ctx, store, links, verifyMaterial{}, false); err != nil {
		t.Fatalf("no verify key, default: want warn-and-proceed (nil), got %v", err)
	}
	err := verifyChainSignatures(ctx, store, links, verifyMaterial{}, true)
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupSignatureMissing {
		t.Fatalf("no verify key, strict: got %v, want SIGNATURE-MISSING", err)
	}
}

// TestVerifyChainSignatures_Ed25519_KEKOnlyNoDowngrade is the "no
// cross-scheme downgrade" pin: an Ed25519-signed chain presented with
// ONLY a KEK-holding envelope (no --verify-key) must NOT silently pass by
// verifying as HMAC, nor refuse as invalid — it takes the unverifiable
// warn/refuse path (the operator lacks the Ed25519 material).
func TestVerifyChainSignatures_Ed25519_KEKOnlyNoDowngrade(t *testing.T) {
	store, _, links := buildEd25519SignedChain(t)
	ctx := context.Background()
	kek := make([]byte, crypto.KEKLen)
	for i := range kek {
		kek[i] = 0x5a
	}
	env := sigFakeEnv{kek: kek} // HMAC-capable, but the chain is Ed25519
	// Default: warn-and-proceed (cannot verify this scheme with a KEK).
	if err := verifyChainSignatures(ctx, store, links, verifyMaterial{env: env}, false); err != nil {
		t.Fatalf("ed25519 chain + KEK only, default: want warn-and-proceed, got %v", err)
	}
	// Strict: refuse MISSING (unverifiable), NOT a spurious HMAC verify.
	err := verifyChainSignatures(ctx, store, links, verifyMaterial{env: env}, true)
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupSignatureMissing {
		t.Fatalf("ed25519 chain + KEK only, strict: got %v, want SIGNATURE-MISSING", err)
	}
}

// TestVerifyChainSignatures_SchemeConfusion_Chain is the chain-level
// scheme-confusion pin: relabeling a chain's scheme in both directions,
// with the operator holding the material matching the CLAIMED scheme,
// must REFUSE (the recomputed bytes + scheme-specific primitive fail).
func TestVerifyChainSignatures_SchemeConfusion_Chain(t *testing.T) {
	ctx := context.Background()

	// HMAC chain relabeled ed25519, verified with the ed25519 public key.
	t.Run("hmac_relabeled_ed25519", func(t *testing.T) {
		store, _, links := buildSignedChain(t)
		pub, _, _ := crypto.GenerateEd25519Keypair()
		relabelChainScheme(t, ctx, store, links, irbackup.SignatureSchemeEd25519)
		err := verifyChainSignatures(ctx, store, links, verifyMaterial{verifyKey: pub}, false)
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupSignatureInvalid {
			t.Fatalf("hmac chain relabeled ed25519: got %v, want SIGNATURE-INVALID", err)
		}
	})

	// Ed25519 chain relabeled hmac-kek, verified with the KEK.
	t.Run("ed25519_relabeled_hmac", func(t *testing.T) {
		store, _, links := buildEd25519SignedChain(t)
		kek := make([]byte, crypto.KEKLen)
		for i := range kek {
			kek[i] = 0x5a
		}
		relabelChainScheme(t, ctx, store, links, irbackup.SignatureSchemeHMACKEK)
		err := verifyChainSignatures(ctx, store, links, verifyMaterial{env: sigFakeEnv{kek: kek}}, false)
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupSignatureInvalid {
			t.Fatalf("ed25519 chain relabeled hmac: got %v, want SIGNATURE-INVALID", err)
		}
	})
}

// relabelChainScheme rewrites the `scheme` field of every per-manifest
// `.sig` and the lineage `.sig` in the chain — the adversary's chain-wide
// relabel primitive.
func relabelChainScheme(t *testing.T, ctx context.Context, store *memStore, links []lineage.SegmentRecord, newScheme string) {
	t.Helper()
	paths := []string{lineage.LineageSigFileName}
	for i := range links {
		segStore := links[i].Segment.Store(store)
		relabelOneSig(t, ctx, segStore, lineage.ManifestSigPath(links[i].Path), newScheme)
	}
	for _, p := range paths {
		relabelOneSig(t, ctx, store, p, newScheme)
	}
}

func relabelOneSig(t *testing.T, ctx context.Context, store irbackup.Store, path, newScheme string) {
	t.Helper()
	rc, err := store.Get(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	var sig irbackup.ManifestSignature
	if err := json.Unmarshal(body, &sig); err != nil {
		t.Fatal(err)
	}
	sig.Scheme = newScheme
	nb, _ := json.Marshal(&sig)
	if err := store.Put(ctx, path, bytes.NewReader(nb)); err != nil {
		t.Fatal(err)
	}
}
