// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	stdcrypto "crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// fakeKMSSigner is an in-process ECDSA-P256 [crypto.KMSSigner] (DER
// signatures, as AWS returns) for the chain-level kms tests.
type fakeKMSSigner struct {
	priv  *ecdsa.PrivateKey
	keyID string
}

func newFakeKMSSigner(t *testing.T) *fakeKMSSigner {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := crypto.KMSManifestKeyID(&priv.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	return &fakeKMSSigner{priv: priv, keyID: id}
}

func (f *fakeKMSSigner) Sign(_ context.Context, payload []byte) ([]byte, error) {
	return ecdsa.SignASN1(rand.Reader, f.priv, crypto.DigestForKMSAlgorithm(f.Algorithm(), payload))
}
func (f *fakeKMSSigner) Algorithm() string              { return crypto.KMSAlgorithmECDSAP256 }
func (f *fakeKMSSigner) KeyID() string                  { return f.keyID }
func (f *fakeKMSSigner) KeyRef() string                 { return "arn:aws:kms:us-east-1:1:key/fake" }
func (f *fakeKMSSigner) PublicKey() stdcrypto.PublicKey { return &f.priv.PublicKey }

// buildKMSSignedChain writes a 2-link chain signed under a KMS (ECDSA)
// signer, returning the store, the signing public key, and the links. The
// ResignLineage call here also PINS the §7-Q4 keyless-cron property: the
// whole chain is (re)signed through the KMS Sign seam without any raw
// private-key material in process.
func buildKMSSignedChain(t *testing.T) (*memStore, stdcrypto.PublicKey, []lineage.SegmentRecord) {
	t.Helper()
	ctx := context.Background()
	fake := newFakeKMSSigner(t)
	signer := lineage.NewKMSSigner(fake)
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
		t.Fatalf("ResignLineage via KMS (keyless-cron path): %v", err)
	}
	links, err := lineage.ListAllSegmentManifests(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 2 {
		t.Fatalf("expected 2 links, got %d", len(links))
	}
	return store, fake.PublicKey(), links
}

// TestVerifyChainSignatures_KMS_Untampered pins the Phase 3 happy path: a
// KMS-signed chain verifies with ONLY the public key (offline DR — no KMS
// access, no envelope).
func TestVerifyChainSignatures_KMS_Untampered(t *testing.T) {
	store, pub, links := buildKMSSignedChain(t)
	if err := verifyChainSignatures(context.Background(), store, links, verifyMaterial{verifyPub: pub}, false); err != nil {
		t.Fatalf("untampered kms chain refused: %v", err)
	}
}

// TestVerifyChainSignatures_KMS_TamperMatrix pins every refusal class for
// the kms scheme (chain level).
func TestVerifyChainSignatures_KMS_TamperMatrix(t *testing.T) {
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
			store, pub, links := buildKMSSignedChain(t)
			tc.tamper(store, links)
			err := verifyChainSignatures(ctx, store, links, verifyMaterial{verifyPub: pub}, false)
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != tc.wantCode {
				t.Fatalf("got %v (code ok=%v), want %s", err, ok, tc.wantCode)
			}
		})
	}
}

// TestVerifyChainSignatures_KMS_WrongKey pins that a DIFFERENT trusted key
// refuses.
func TestVerifyChainSignatures_KMS_WrongKey(t *testing.T) {
	store, _, links := buildKMSSignedChain(t)
	other, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	err := verifyChainSignatures(context.Background(), store, links, verifyMaterial{verifyPub: &other.PublicKey}, false)
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupSignatureInvalid {
		t.Fatalf("wrong trusted key: got %v, want SIGNATURE-INVALID", err)
	}
}

// TestVerifyChainSignatures_KMS_KEKOnlyNoDowngrade is the "no cross-scheme
// downgrade" pin for kms: a KMS-signed chain presented with ONLY a
// KEK-holding envelope (no --verify-key) must take the unverifiable
// warn/refuse path — never silently verify as HMAC.
func TestVerifyChainSignatures_KMS_KEKOnlyNoDowngrade(t *testing.T) {
	store, _, links := buildKMSSignedChain(t)
	ctx := context.Background()
	kek := make([]byte, crypto.KEKLen)
	for i := range kek {
		kek[i] = 0x5a
	}
	env := sigFakeEnv{kek: kek}
	if err := verifyChainSignatures(ctx, store, links, verifyMaterial{env: env}, false); err != nil {
		t.Fatalf("kms chain + KEK only, default: want warn-and-proceed, got %v", err)
	}
	err := verifyChainSignatures(ctx, store, links, verifyMaterial{env: env}, true)
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupSignatureMissing {
		t.Fatalf("kms chain + KEK only, strict: got %v, want SIGNATURE-MISSING", err)
	}
}

// TestVerifyChainSignatures_SchemeConfusion_KMSChain pins the chain-level
// 3-way relabel refusals involving kms.
func TestVerifyChainSignatures_SchemeConfusion_KMSChain(t *testing.T) {
	ctx := context.Background()
	kmsToken := irbackup.SignatureSchemeKMS + "/" + crypto.KMSAlgorithmECDSAP256

	// KMS chain relabeled ed25519, verified with an ed25519 public key.
	t.Run("kms_relabeled_ed25519", func(t *testing.T) {
		store, _, links := buildKMSSignedChain(t)
		pub, _, _ := crypto.GenerateEd25519Keypair()
		relabelChainScheme(t, ctx, store, links, irbackup.SignatureSchemeEd25519)
		err := verifyChainSignatures(ctx, store, links, verifyMaterial{verifyPub: pub}, false)
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupSignatureInvalid {
			t.Fatalf("kms chain relabeled ed25519: got %v, want SIGNATURE-INVALID", err)
		}
	})

	// Ed25519 chain relabeled kms, verified with the kms public key.
	t.Run("ed25519_relabeled_kms", func(t *testing.T) {
		store, _, links := buildEd25519SignedChain(t)
		fake := newFakeKMSSigner(t)
		relabelChainScheme(t, ctx, store, links, kmsToken)
		err := verifyChainSignatures(ctx, store, links, verifyMaterial{verifyPub: fake.PublicKey()}, false)
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupSignatureInvalid {
			t.Fatalf("ed25519 chain relabeled kms: got %v, want SIGNATURE-INVALID", err)
		}
	})
}
