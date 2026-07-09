// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// overwriteManifestSigV2 replaces a manifest's `.sig` with a Phase-1 v2
// signature (as v0.99.208 wrote it): v2 canon, no scheme token, HMAC.
func overwriteManifestSigV2(t *testing.T, ctx context.Context, segStore irbackup.Store, path string, m *irbackup.Manifest, seq int, key []byte) {
	t.Helper()
	payload, err := irbackup.CanonicalManifestBytesForVersion(m, seq, irbackup.ManifestCanonVersionV2, "")
	if err != nil {
		t.Fatal(err)
	}
	sig := &irbackup.ManifestSignature{
		CanonVersion: irbackup.ManifestCanonVersionV2,
		Scheme:       irbackup.SignatureSchemeHMACKEK,
		KeyID:        crypto.ManifestSigKeyID(key),
		Sequence:     seq,
		ChunkCount:   irbackup.ManifestChunkCount(m),
		MAC:          hex.EncodeToString(crypto.SignManifestHMAC(key, payload)),
	}
	body, _ := irbackup.MarshalManifestSignature(sig)
	if err := segStore.Put(ctx, lineage.ManifestSigPath(path), bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
}

// TestVerifyChainSignatures_MixedV2V3 pins the real-world back-compat
// shape: a Phase-1 (v0.99.208) chain — full signed v2 — that a Phase-2
// binary EXTENDED with a v3 incremental (the full's v2 sig is NOT
// rewritten; only the new incremental + the lineage catalog are v3). The
// dual-version verifier checks each artifact at its own recorded version,
// so the mixed chain verifies GREEN.
func TestVerifyChainSignatures_MixedV2V3(t *testing.T) {
	ctx := context.Background()
	store, env, links := buildSignedChain(t) // all-v3 HMAC chain
	key, err := env.(crypto.ManifestSigner).ManifestSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	// Downgrade ONLY the full (link 0) to a v2 signature — as if it was
	// written by the shipped Phase-1 binary and never re-signed.
	full := &links[0]
	overwriteManifestSigV2(t, ctx, full.Segment.Store(store), full.Path, full.Manifest, 0, key)

	if err := verifyChainSignatures(ctx, store, links, verifyMaterial{env: env}, false); err != nil {
		t.Fatalf("mixed v2-full + v3-incremental chain refused: %v", err)
	}
	// Strict policy also passes (a signed chain we CAN verify).
	if err := verifyChainSignatures(ctx, store, links, verifyMaterial{env: env}, true); err != nil {
		t.Fatalf("mixed chain under --require-signature refused: %v", err)
	}
	// Tampering the v2 full is still caught.
	full.Manifest.SchemaHash = "tampered"
	err = verifyChainSignatures(ctx, store, links, verifyMaterial{env: env}, false)
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBackupSignatureInvalid {
		t.Fatalf("tampered v2 full in mixed chain: got %v, want SIGNATURE-INVALID", err)
	}
}
