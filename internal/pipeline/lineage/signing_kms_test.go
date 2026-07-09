// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"bytes"
	"context"
	stdcrypto "crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// fakeKMSSigner is an in-process [crypto.KMSSigner] backed by a real
// ECDSA-P256 key — the faithful DER-signing shape AWS KMS produces. It
// lets the lineage-level tests exercise the kms scheme (sign seam + PURE
// verify) without a KMS client.
type fakeKMSSigner struct {
	priv   *ecdsa.PrivateKey
	keyID  string
	keyRef string
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
	return &fakeKMSSigner{priv: priv, keyID: id, keyRef: "arn:aws:kms:us-east-1:1:key/fake-sign"}
}

func (f *fakeKMSSigner) Sign(_ context.Context, payload []byte) ([]byte, error) {
	digest := crypto.DigestForKMSAlgorithm(f.Algorithm(), payload)
	return ecdsa.SignASN1(rand.Reader, f.priv, digest)
}
func (f *fakeKMSSigner) Algorithm() string              { return crypto.KMSAlgorithmECDSAP256 }
func (f *fakeKMSSigner) KeyID() string                  { return f.keyID }
func (f *fakeKMSSigner) KeyRef() string                 { return f.keyRef }
func (f *fakeKMSSigner) PublicKey() stdcrypto.PublicKey { return &f.priv.PublicKey }

func testKMSSigner(t *testing.T) (signer, verifier *Signer, pub stdcrypto.PublicKey) {
	t.Helper()
	f := newFakeKMSSigner(t)
	return NewKMSSigner(f), NewKMSVerifier(f.PublicKey(), crypto.KMSAlgorithmECDSAP256), f.PublicKey()
}

// TestVerifyManifest_KMS_RoundTrip pins that a KMS (ECDSA) signature
// written via the KMS Sign seam verifies with the PURE local verifier
// built from the public key — no KMS access needed to verify.
func TestVerifyManifest_KMS_RoundTrip(t *testing.T) {
	ctx := context.Background()
	signer, verifier, _ := testKMSSigner(t)
	store := newMemStore()
	m := testManifest()
	if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, signer); err != nil {
		t.Fatal(err)
	}
	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, verifier); err != nil {
		t.Fatalf("valid kms signature did not verify locally: %v", err)
	}
	// The recorded `.sig` carries the composite scheme token + advisory
	// algorithm/key_ref.
	sig := readSig(t, ctx, store, ManifestSigPath(ManifestFileName))
	if sig.Scheme != irbackup.SignatureSchemeKMS+"/"+crypto.KMSAlgorithmECDSAP256 {
		t.Fatalf("scheme token: got %q want kms/ecdsa-p256", sig.Scheme)
	}
	if sig.Algorithm != crypto.KMSAlgorithmECDSAP256 {
		t.Fatalf("advisory algorithm: got %q", sig.Algorithm)
	}
	if sig.KeyRef == "" {
		t.Fatal("advisory key_ref not recorded")
	}
}

// TestVerifyManifest_KMS_TamperMatrix mirrors the HMAC/Ed25519 tamper
// matrix for the kms scheme.
func TestVerifyManifest_KMS_TamperMatrix(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		mutate  func(store irbackup.Store, m *irbackup.Manifest)
		seq     int
		wantErr error
	}{
		{"missing", func(s irbackup.Store, _ *irbackup.Manifest) { _ = s.Delete(ctx, ManifestSigPath(ManifestFileName)) }, 0, ErrSignatureMissing},
		{"tampered manifest", func(_ irbackup.Store, m *irbackup.Manifest) { m.SchemaHash = "x" }, 0, ErrSignatureInvalid},
		{"rolled-back sequence", func(_ irbackup.Store, _ *irbackup.Manifest) {}, 1, ErrSignatureInvalid},
		{"truncated change-list", func(_ irbackup.Store, m *irbackup.Manifest) { m.Tables[0].Chunks = nil }, 0, ErrSignatureInvalid},
		{"corrupt sig", func(s irbackup.Store, _ *irbackup.Manifest) {
			corruptSigMAC(t, ctx, s, ManifestSigPath(ManifestFileName))
		}, 0, ErrSignatureInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			signer, verifier, _ := testKMSSigner(t)
			store := newMemStore()
			m := testManifest()
			if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, signer); err != nil {
				t.Fatal(err)
			}
			tc.mutate(store, m)
			if err := VerifyManifest(ctx, store, ManifestFileName, m, tc.seq, verifier); !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want errors.Is %v", err, tc.wantErr)
			}
		})
	}
}

// TestVerifyManifest_KMS_WrongKey pins that a DIFFERENT public key refuses.
func TestVerifyManifest_KMS_WrongKey(t *testing.T) {
	ctx := context.Background()
	signer, _, _ := testKMSSigner(t)
	_, wrongVerifier, _ := testKMSSigner(t)
	store := newMemStore()
	m := testManifest()
	if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, signer); err != nil {
		t.Fatal(err)
	}
	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, wrongVerifier); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("wrong kms key: got %v, want ErrSignatureInvalid", err)
	}
}

// TestSigner_SchemeConfusion_KMS is the 3-WAY security core for Phase 3:
// the kms scheme cannot be relabeled to/from ed25519 or hmac-kek to force a
// different verification path, and a genuine kms signature never verifies
// under another scheme's verifier. Every arm must REFUSE.
func TestSigner_SchemeConfusion_KMS(t *testing.T) {
	ctx := context.Background()
	hmac := testSigner(t)
	edSigner, edVerifier := testEd25519Signer(t)
	kmsSigner, kmsVerifier, _ := testKMSSigner(t)
	kmsToken := irbackup.SignatureSchemeKMS + "/" + crypto.KMSAlgorithmECDSAP256

	arms := []struct {
		name        string
		writeSigner *Signer
		relabelTo   string // "" = no relabel
		verifier    *Signer
	}{
		{"kms_relabeled_ed25519", kmsSigner, irbackup.SignatureSchemeEd25519, edVerifier},
		{"kms_relabeled_hmac", kmsSigner, irbackup.SignatureSchemeHMACKEK, hmac},
		{"ed25519_relabeled_kms", edSigner, kmsToken, kmsVerifier},
		{"hmac_relabeled_kms", hmac, kmsToken, kmsVerifier},
		{"genuine_kms_via_ed25519", kmsSigner, "", edVerifier},
		{"genuine_kms_via_hmac", kmsSigner, "", hmac},
		{"genuine_ed25519_via_kms", edSigner, "", kmsVerifier},
		{"genuine_hmac_via_kms", hmac, "", kmsVerifier},
	}
	for _, a := range arms {
		t.Run(a.name, func(t *testing.T) {
			store := newMemStore()
			m := testManifest()
			if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, a.writeSigner); err != nil {
				t.Fatal(err)
			}
			if a.relabelTo != "" {
				relabelSigScheme(t, ctx, store, ManifestSigPath(ManifestFileName), a.relabelTo)
			}
			if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, a.verifier); !errors.Is(err, ErrSignatureInvalid) {
				t.Fatalf("%s verified: got %v, want ErrSignatureInvalid", a.name, err)
			}
		})
	}
}

// TestVerifyManifest_KMS_AlgorithmDowngrade pins the algorithm binding: an
// adversary relabeling the composite scheme token's ALGORITHM (kms/ecdsa-p256
// → kms/ecdsa-p384) — with the verifier selecting its primitive from the
// relabeled token, as the read side does — is refused, because the algorithm
// is inside the signed canonical bytes.
func TestVerifyManifest_KMS_AlgorithmDowngrade(t *testing.T) {
	ctx := context.Background()
	signer, _, pub := testKMSSigner(t)
	store := newMemStore()
	m := testManifest()
	if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, signer); err != nil {
		t.Fatal(err)
	}
	// Relabel the scheme token to a different algorithm and verify with the
	// verifier the read side would build for the RELABELED token.
	downgraded := irbackup.SignatureSchemeKMS + "/" + crypto.KMSAlgorithmECDSAP384
	relabelSigScheme(t, ctx, store, ManifestSigPath(ManifestFileName), downgraded)
	verifier := NewKMSVerifier(pub, crypto.KMSAlgorithmECDSAP384)
	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, verifier); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("algorithm downgrade verified: got %v, want ErrSignatureInvalid", err)
	}
}

// TestVerifyManifest_KMS_Offline pins the DR path: a KMS-signed manifest
// verifies with a verifier built from a public key parsed OFFLINE from an
// exported PEM — no KMS client, no network.
func TestVerifyManifest_KMS_Offline(t *testing.T) {
	ctx := context.Background()
	signer, _, pub := testKMSSigner(t)
	store := newMemStore()
	m := testManifest()
	if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, signer); err != nil {
		t.Fatal(err)
	}
	// Export → PEM → parse (what `--verify-key pub.pem` does on a DR host).
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := crypto.ParseManifestPublicKeyPEM(pemPublic(der))
	if err != nil {
		t.Fatal(err)
	}
	offline := NewKMSVerifier(parsed, crypto.KMSAlgorithmECDSAP256)
	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, offline); err != nil {
		t.Fatalf("offline verify with exported public key failed: %v", err)
	}
}

// TestVerifyManifest_KMS_KeyRefIsAdvisory pins the trust-anchor property: a
// store adversary rewriting the manifest's advisory KeyRef to a key they
// control does NOT redirect trust — verification uses the operator's
// supplied public key, not the recorded ref — so the signature still
// verifies green (the ref is never consulted for trust). Conversely a
// verifier holding a DIFFERENT key refuses even when the KeyRef is intact.
func TestVerifyManifest_KMS_KeyRefIsAdvisory(t *testing.T) {
	ctx := context.Background()
	signer, verifier, _ := testKMSSigner(t)
	store := newMemStore()
	m := testManifest()
	if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, signer); err != nil {
		t.Fatal(err)
	}
	rewriteSigKeyRef(t, ctx, store, ManifestSigPath(ManifestFileName), "arn:aws:kms:us-east-1:999:key/attacker-controlled")
	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, verifier); err != nil {
		t.Fatalf("rewriting the advisory KeyRef broke verification against the trusted key: %v", err)
	}
	_, wrong, _ := testKMSSigner(t)
	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, wrong); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("a different trusted key must refuse regardless of KeyRef: got %v", err)
	}
}

// TestKMSVerifier_CannotSign pins that a verify-only KMS signer (built from
// a public key alone) cannot produce a signature and refuses to re-sign a
// chain — the key-separated verification guarantee, and the guard on
// ResignLineage (compact/prune needs sign material).
func TestKMSVerifier_CannotSign(t *testing.T) {
	_, verifier, _ := testKMSSigner(t)
	if verifier.canSign() {
		t.Fatal("verify-only kms signer reports canSign()==true")
	}
	if _, err := verifier.SignManifest(context.Background(), testManifest(), 0); err == nil {
		t.Fatal("verify-only kms signer produced a signature")
	}
	if err := ResignLineage(context.Background(), newMemStore(), verifier); err == nil {
		t.Fatal("ResignLineage with a verify-only kms signer did not refuse")
	}
}

func readSig(t *testing.T, ctx context.Context, store irbackup.Store, path string) *irbackup.ManifestSignature {
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
	return &sig
}

func rewriteSigKeyRef(t *testing.T, ctx context.Context, store irbackup.Store, path, newRef string) {
	t.Helper()
	sig := readSig(t, ctx, store, path)
	sig.KeyRef = newRef
	nb, _ := json.Marshal(sig)
	if err := store.Put(ctx, path, bytes.NewReader(nb)); err != nil {
		t.Fatal(err)
	}
}

func pemPublic(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}
