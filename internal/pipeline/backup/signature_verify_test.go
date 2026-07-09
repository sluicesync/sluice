// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// sigFakeEnv is a ManifestSigner envelope over a fixed KEK — the verify
// side derives the same HMAC key the writer used.
type sigFakeEnv struct{ kek []byte }

func (sigFakeEnv) WrapCEK(c []byte) ([]byte, error)   { return c, nil }
func (sigFakeEnv) UnwrapCEK(c []byte) ([]byte, error) { return c, nil }
func (sigFakeEnv) Mode() string                       { return "passphrase-argon2id" }
func (e sigFakeEnv) ManifestSigningKey() ([]byte, error) {
	return crypto.DeriveManifestHMACKey(e.kek)
}

var _ crypto.ManifestSigner = sigFakeEnv{}

// buildSignedChain writes a 2-link signed chain (full + 1 incremental)
// with all detached signatures + a signed lineage catalog into a fresh
// memStore, and returns the store, the verify envelope, and the walked
// links (in flat lineage order).
func buildSignedChain(t *testing.T) (*memStore, crypto.EnvelopeEncryption, []lineage.SegmentRecord) {
	t.Helper()
	ctx := context.Background()
	kek := make([]byte, crypto.KEKLen)
	for i := range kek {
		kek[i] = 0x5a
	}
	env := sigFakeEnv{kek: kek}
	signer, ok, err := lineage.NewSigner(env)
	if err != nil || !ok {
		t.Fatalf("new signer: ok=%v err=%v", ok, err)
	}
	store := newMemStore()

	full := &irbackup.Manifest{
		FormatVersion: irbackup.FormatVersionSignedManifest,
		CreatedAt:     time.Date(2026, 7, 9, 0, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Kind:          irbackup.BackupKindFull,
		BackupID:      "full0001",
		SchemaHash:    "fh",
		ChainEncryption: &irbackup.ChainEncryption{
			Algorithm: "AES-256-GCM", Mode: "per-chain", KEKMode: "passphrase-argon2id",
		},
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

	// Sign everything at its flat position + the lineage catalog.
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
	return store, env, links
}

// TestVerifyChainSignatures_Untampered pins the happy path.
func TestVerifyChainSignatures_Untampered(t *testing.T) {
	store, env, links := buildSignedChain(t)
	if err := verifyChainSignatures(context.Background(), store, links, verifyMaterial{env: env}, false); err != nil {
		t.Fatalf("untampered signed chain refused: %v", err)
	}
}

// TestVerifyChainSignatures_TamperMatrix pins every refusal class on a v6
// encrypted chain, with the coded-error class asserted.
func TestVerifyChainSignatures_TamperMatrix(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name     string
		tamper   func(store *memStore, links []lineage.SegmentRecord)
		wantCode sluicecode.Code
	}{
		{
			name:     "invalid signature (tampered manifest)",
			tamper:   func(_ *memStore, links []lineage.SegmentRecord) { links[1].Manifest.SchemaHash = "tampered" },
			wantCode: sluicecode.CodeBackupSignatureInvalid,
		},
		{
			name: "truncated change-list tail",
			tamper: func(_ *memStore, links []lineage.SegmentRecord) {
				links[1].Manifest.ChangeChunks = links[1].Manifest.ChangeChunks[:1]
			},
			wantCode: sluicecode.CodeBackupSignatureInvalid,
		},
		{
			name: "missing manifest signature",
			tamper: func(store *memStore, links []lineage.SegmentRecord) {
				_ = store.Delete(ctx, lineage.ManifestSigPath(links[1].Path))
			},
			wantCode: sluicecode.CodeBackupSignatureMissing,
		},
		{
			name: "dropped-newest-link (lineage enumeration)",
			tamper: func(store *memStore, _ []lineage.SegmentRecord) {
				// Drop the incremental from the catalog but leave the old
				// lineage.sig — the signed link count no longer matches.
				cat, _, _ := lineage.LoadLineageCatalog(ctx, store)
				cat.Segments[0].Incrementals = nil
				_ = lineage.WriteLineageCatalog(ctx, store, cat)
			},
			wantCode: sluicecode.CodeBackupSignatureInvalid,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, env, links := buildSignedChain(t)
			tc.tamper(store, links)
			err := verifyChainSignatures(ctx, store, links, verifyMaterial{env: env}, false)
			if err == nil {
				t.Fatal("tamper was not refused")
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != tc.wantCode {
				t.Fatalf("got %v (code ok=%v), want code %s", err, ok, tc.wantCode)
			}
		})
	}
}

// TestVerifyChainSignatures_FormatVersionDowngrade is the CRITICAL pin:
// signedness must NOT be decided from the MAC-covered FormatVersion. An
// adversary flips every manifest's v6->v5 (the chunk-binding gate is
// >=5, so v5 and v6 decrypt identically) to make a naive verifier skip
// all checks. With the .sig files intact the verifier must still verify
// — and refuse, because format_version is inside the signed bytes.
func TestVerifyChainSignatures_FormatVersionDowngrade(t *testing.T) {
	store, env, links := buildSignedChain(t)
	for i := range links {
		links[i].Manifest.FormatVersion = irbackup.FormatVersionEncryptedChunkBinding // v6 -> v5
	}
	err := verifyChainSignatures(context.Background(), store, links, verifyMaterial{env: env}, false)
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupSignatureInvalid {
		t.Fatalf("v6->v5 downgrade with sigs present: got %v, want SIGNATURE-INVALID (verification must not trust FormatVersion)", err)
	}
}

// TestVerifyChainSignatures_DowngradeAndStripRequiresStrict pins the
// residual boundary: stripping BOTH the version stamp AND every signature
// object evades verification by default (the honestly-documented external
// -anchor residual, ADR-0154 option b) — but --require-signature closes
// it (the operator asserts the chain should be signed).
func TestVerifyChainSignatures_DowngradeAndStripRequiresStrict(t *testing.T) {
	ctx := context.Background()
	store, env, links := buildSignedChain(t)
	// Downgrade + strip every signature object.
	for i := range links {
		links[i].Manifest.FormatVersion = irbackup.FormatVersionEncryptedChunkBinding
		_ = store.Delete(ctx, lineage.ManifestSigPath(links[i].Path))
	}
	_ = store.Delete(ctx, lineage.LineageSigFileName)

	// Default policy: no artifacts, not strict → documented residual (no-op).
	if err := verifyChainSignatures(ctx, store, links, verifyMaterial{env: env}, false); err != nil {
		t.Fatalf("fully-stripped chain, default policy: got %v, want nil (documented option-b residual)", err)
	}
	// Strict: the operator asserts it should be signed → refuse.
	err := verifyChainSignatures(ctx, store, links, verifyMaterial{env: env}, true)
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupSignatureMissing {
		t.Fatalf("fully-stripped chain, --require-signature: got %v, want SIGNATURE-MISSING", err)
	}
}

// TestVerifyChainSignatures_WrongKey pins that a re-keyed chain refuses.
func TestVerifyChainSignatures_WrongKey(t *testing.T) {
	store, _, links := buildSignedChain(t)
	wrongKEK := make([]byte, crypto.KEKLen) // all zeros != 0x5a chain
	err := verifyChainSignatures(context.Background(), store, links, verifyMaterial{env: sigFakeEnv{kek: wrongKEK}}, false)
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupSignatureInvalid {
		t.Fatalf("wrong key: got %v, want SIGNATURE-INVALID", err)
	}
}

// TestVerifyChainSignatures_PreV6NoOp pins that an unsigned (pre-v6) chain
// is a no-op — legitimately-unsigned older backups restore untouched.
func TestVerifyChainSignatures_PreV6NoOp(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	full := &irbackup.Manifest{
		FormatVersion: irbackup.FormatVersionEncryptedChunkBinding, // v5, unsigned
		CreatedAt:     time.Now().UTC(),
		SourceEngine:  "postgres",
		Kind:          irbackup.BackupKindFull,
		BackupID:      "full0001",
	}
	if err := lineage.WriteManifest(ctx, store, full); err != nil {
		t.Fatal(err)
	}
	lineage.UpdateLineageForManifestBestEffort(ctx, store, full, lineage.ManifestFileName, blobcodec.DefaultCodec)
	links, err := lineage.ListAllSegmentManifests(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	// No signer supplied and no sigs present — must be a clean no-op.
	if err := verifyChainSignatures(ctx, store, links, verifyMaterial{}, false); err != nil {
		t.Fatalf("pre-v6 chain refused: %v", err)
	}
}

// TestVerifyChainSignatures_UnverifiablePolicy pins the WARN-vs-strict
// branch: a signed chain with NO verify key warns-and-proceeds by
// default, refuses under RequireSignature.
func TestVerifyChainSignatures_UnverifiablePolicy(t *testing.T) {
	store, _, links := buildSignedChain(t)
	ctx := context.Background()
	// No envelope → cannot verify. Default: proceed.
	if err := verifyChainSignatures(ctx, store, links, verifyMaterial{}, false); err != nil {
		t.Fatalf("default policy should warn-and-proceed, got %v", err)
	}
	// Strict: refuse with the coded SIGNATURE-MISSING class.
	err := verifyChainSignatures(ctx, store, links, verifyMaterial{}, true)
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupSignatureMissing {
		t.Fatalf("strict policy: got %v, want SIGNATURE-MISSING", err)
	}
}
