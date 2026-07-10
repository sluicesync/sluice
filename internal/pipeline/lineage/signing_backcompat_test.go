// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// writeV2ManifestSig writes a signature EXACTLY as the shipped Phase-1
// (v0.99.208) binary did: v2 canonicalization (no scheme token), scheme
// tag hmac-kek, HMAC MAC. It reconstructs the on-disk bytes rather than
// calling the current signer (which emits v3) so the pin proves the
// dual-version verifier accepts REAL Phase-1 signatures, not v3 in
// disguise.
func writeV2ManifestSig(t *testing.T, ctx context.Context, store irbackup.Store, path string, m *irbackup.Manifest, seq int, key []byte) {
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
	body, err := irbackup.MarshalManifestSignature(sig)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, ManifestSigPath(path), bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
}

// TestVerifyManifest_V2BackCompat is the BACK-COMPAT pin: a Phase-1
// (v0.99.208) v2 signature verifies GREEN under the Phase-2 dual-version
// verifier, and tampering it still refuses.
func TestVerifyManifest_V2BackCompat(t *testing.T) {
	ctx := context.Background()
	s := testSigner(t)
	store := newMemStore()
	m := testManifest()
	writeV2ManifestSig(t, ctx, store, ManifestFileName, m, 0, s.Key)

	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, s); err != nil {
		t.Fatalf("Phase-1 v2 signature did not verify on a Phase-2 binary: %v", err)
	}
	// Tampering a v2-signed manifest still refuses (the v2 MAC covers it).
	m.SchemaHash = "tampered"
	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, s); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("tampered v2 manifest: got %v, want ErrSignatureInvalid", err)
	}
}

// TestVerifyManifest_V2RelabelStillRefused pins that relabeling a v2
// signature's scheme field to ed25519 does NOT downgrade it — v2's scheme
// is definitionally hmac-kek, so the effective-scheme check refuses the
// relabel (matching Phase 1's refuse-on-relabel).
func TestVerifyManifest_V2RelabelStillRefused(t *testing.T) {
	ctx := context.Background()
	s := testSigner(t)
	store := newMemStore()
	m := testManifest()
	writeV2ManifestSig(t, ctx, store, ManifestFileName, m, 0, s.Key)
	relabelSigScheme(t, ctx, store, ManifestSigPath(ManifestFileName), irbackup.SignatureSchemeEd25519)
	// Verified with the HMAC signer: effective scheme of a v2 sig is
	// hmac-kek, matches the verifier; the relabel is inert AND the MAC
	// still verifies (the relabeled field is not in the v2 signed bytes) —
	// so a relabel of a v2 sig cannot force a weaker path. Verify GREEN.
	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, s); err != nil {
		t.Fatalf("relabeled v2 sig verified with HMAC key should still pass (v2 scheme is definitionally hmac): %v", err)
	}
	// And an Ed25519 verifier cannot be steered onto it by the relabel: the
	// effective scheme (hmac-kek) != ed25519 verifier scheme → refuse.
	_, edVerifier := testEd25519Signer(t)
	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, edVerifier); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("relabeled v2 sig via ed25519 verifier: got %v, want ErrSignatureInvalid", err)
	}
}

// TestVerifyManifest_FutureVersionUpgrade pins that a signature recording
// a canon version this build doesn't know (a future v5) refuses with the
// UPGRADE message, NOT SIGNATURE-INVALID (it's a version gap, not tamper).
func TestVerifyManifest_FutureVersionUpgrade(t *testing.T) {
	ctx := context.Background()
	s := testSigner(t)
	store := newMemStore()
	m := testManifest()
	// A well-formed sig envelope claiming a future canon version.
	sig := &irbackup.ManifestSignature{
		CanonVersion: "sluice-manifest-canon/v5",
		Scheme:       irbackup.SignatureSchemeHMACKEK,
		KeyID:        s.KeyID,
		Sequence:     0,
		ChunkCount:   irbackup.ManifestChunkCount(m),
		MAC:          hex.EncodeToString(bytes.Repeat([]byte{0x01}, 32)),
	}
	body, _ := irbackup.MarshalManifestSignature(sig)
	if err := store.Put(ctx, ManifestSigPath(ManifestFileName), bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	err := VerifyManifest(ctx, store, ManifestFileName, m, 0, s)
	if !errors.Is(err, ErrSignatureUnsupportedVersion) {
		t.Fatalf("future canon version: got %v, want ErrSignatureUnsupportedVersion (upgrade, not tamper)", err)
	}
	if errors.Is(err, ErrSignatureInvalid) {
		t.Fatal("future canon version must NOT surface as SIGNATURE-INVALID (alarming tamper signal)")
	}
}

// relabelSigCanonVersion rewrites the canon_version field of an on-disk
// `.sig` (the downgrade-relabel a store adversary would attempt) without
// touching the MAC — mirrors relabelSigScheme.
func relabelSigCanonVersion(t *testing.T, ctx context.Context, store irbackup.Store, path, newVersion string) {
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
	sig, err := irbackup.UnmarshalManifestSignature(body)
	if err != nil {
		t.Fatal(err)
	}
	sig.CanonVersion = newVersion
	nb, err := irbackup.MarshalManifestSignature(sig)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, path, bytes.NewReader(nb)); err != nil {
		t.Fatal(err)
	}
}

// TestVerifyManifest_V3RelabelToV2Refused pins the downgrade-relabel
// direction the dual-version verifier must reject: a v3 signature (whose
// MAC covers bytes that INCLUDE the scheme token) relabeled to canon v2
// (which recomputes WITHOUT the scheme token) can never verify. This is
// the twin of TestVerifyManifest_V2RelabelStillRefused — it proves the
// dual-version verifier does not become a downgrade oracle.
func TestVerifyManifest_V3RelabelToV2Refused(t *testing.T) {
	ctx := context.Background()
	m := testManifest()

	// (a) HMAC-off-KEK: a real v3 hmac signature relabeled to v2. The v2
	// recompute drops the scheme token, so the bytes differ and the MAC
	// fails — SIGNATURE-INVALID (the relabel IS a tamper of the sig).
	t.Run("hmac", func(t *testing.T) {
		s := testSigner(t)
		store := newMemStore()
		if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, s); err != nil {
			t.Fatal(err)
		}
		relabelSigCanonVersion(t, ctx, store, ManifestSigPath(ManifestFileName), irbackup.ManifestCanonVersionV2)
		if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, s); !errors.Is(err, ErrSignatureInvalid) {
			t.Fatalf("v3->v2 relabel (hmac): got %v, want ErrSignatureInvalid", err)
		}
	})

	// (b) Ed25519: relabeling a v3 ed25519 signature to v2 forces the
	// effective scheme to hmac-kek (v2 predates the scheme token), which no
	// longer matches the ed25519 verifier — refused at the scheme check.
	t.Run("ed25519", func(t *testing.T) {
		signer, verifier := testEd25519Signer(t)
		store := newMemStore()
		if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, signer); err != nil {
			t.Fatal(err)
		}
		relabelSigCanonVersion(t, ctx, store, ManifestSigPath(ManifestFileName), irbackup.ManifestCanonVersionV2)
		if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, verifier); !errors.Is(err, ErrSignatureInvalid) {
			t.Fatalf("v3->v2 relabel (ed25519): got %v, want ErrSignatureInvalid", err)
		}
	})
}

// TestVerifyManifest_V4RelabelToV3Refused is the SEC-F1 downgrade-oracle
// pin: an adversary who reassigns row chunks between two same-column-set
// tables AND relabels the v4 signature to canon v3 (which recomputes
// WITHOUT the parent-table tokens, where the swap is invisible) still
// fails. The MAC was computed over the v4 bytes (with the table tokens);
// recomputing at v3 drops those tokens, so the bytes differ and the MAC
// fails — SIGNATURE-INVALID. The dual-version verifier does not become a
// downgrade oracle that strips the new binding.
func TestVerifyManifest_V4RelabelToV3Refused(t *testing.T) {
	ctx := context.Background()
	s := testSigner(t)
	store := newMemStore()
	m := twoTableManifest()
	if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, s); err != nil {
		t.Fatal(err)
	}
	// Reassign the chunks between the two same-schema tables and relabel the
	// signature down to v3 to try to hide the swap.
	m.Tables[0].Chunks, m.Tables[1].Chunks = m.Tables[1].Chunks, m.Tables[0].Chunks
	relabelSigCanonVersion(t, ctx, store, ManifestSigPath(ManifestFileName), irbackup.ManifestCanonVersionV3)
	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, s); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("v4->v3 relabel + chunk swap: got %v, want ErrSignatureInvalid (no downgrade oracle)", err)
	}
}

// TestVerifyLineage_V2BackCompat pins the dual-version lineage catalog
// verify: a Phase-1 v2 lineage signature verifies on a Phase-2 binary.
func TestVerifyLineage_V2BackCompat(t *testing.T) {
	ctx := context.Background()
	s := testSigner(t)
	store := newMemStore()
	cat := &Catalog{
		FormatVersion: 1,
		SourceEngine:  "postgres",
		Segments: []Segment{{
			SegmentID:        "seg0",
			FullManifestPath: ManifestFileName,
			Incrementals:     []string{"manifests/incr-1.json"},
		}},
	}
	// Reconstruct the Phase-1 v2 lineage signature.
	payload, err := CanonicalCatalogBytesForVersion(cat, LineageCatalogCanonVersionV2, "")
	if err != nil {
		t.Fatal(err)
	}
	sig := &irbackup.ManifestSignature{
		CanonVersion: LineageCatalogCanonVersionV2,
		Scheme:       irbackup.SignatureSchemeHMACKEK,
		KeyID:        s.KeyID,
		Sequence:     totalManifestCount(cat),
		MAC:          hex.EncodeToString(crypto.SignManifestHMAC(s.Key, payload)),
	}
	body, _ := irbackup.MarshalManifestSignature(sig)
	if err := store.Put(ctx, LineageSigFileName, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	if err := VerifyLineage(ctx, store, cat, s); err != nil {
		t.Fatalf("Phase-1 v2 lineage signature did not verify on a Phase-2 binary: %v", err)
	}
	// Dropping the newest link still refuses.
	cat.Segments[0].Incrementals = nil
	if err := VerifyLineage(ctx, store, cat, s); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("dropped link on v2 lineage: got %v, want ErrSignatureInvalid", err)
	}
}
