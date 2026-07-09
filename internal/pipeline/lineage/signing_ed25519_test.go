// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

func testEd25519Signer(t *testing.T) (signer, verifier *Signer) {
	t.Helper()
	pub, priv, err := crypto.GenerateEd25519Keypair()
	if err != nil {
		t.Fatal(err)
	}
	return NewEd25519Signer(priv), NewEd25519Verifier(pub)
}

// TestVerifyManifest_Ed25519_RoundTrip pins that an Ed25519 signature
// written with the private key verifies with the public key (Phase 2
// key-separated verification).
func TestVerifyManifest_Ed25519_RoundTrip(t *testing.T) {
	ctx := context.Background()
	signer, verifier := testEd25519Signer(t)
	store := newMemStore()
	m := testManifest()
	if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, signer); err != nil {
		t.Fatal(err)
	}
	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, verifier); err != nil {
		t.Fatalf("valid ed25519 signature did not verify with the public key: %v", err)
	}
}

// TestVerifyManifest_Ed25519_TamperMatrix mirrors the HMAC tamper matrix
// for the Ed25519 scheme.
func TestVerifyManifest_Ed25519_TamperMatrix(t *testing.T) {
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
			signer, verifier := testEd25519Signer(t)
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

// TestVerifyManifest_Ed25519_WrongKey pins that a DIFFERENT public key
// refuses.
func TestVerifyManifest_Ed25519_WrongKey(t *testing.T) {
	ctx := context.Background()
	signer, _ := testEd25519Signer(t)
	_, wrongVerifier := testEd25519Signer(t)
	store := newMemStore()
	m := testManifest()
	if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, signer); err != nil {
		t.Fatal(err)
	}
	if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, wrongVerifier); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("wrong ed25519 key: got %v, want ErrSignatureInvalid", err)
	}
}

// TestSigner_SchemeConfusion is the Phase 2 SECURITY CORE: an adversary
// cannot relabel one scheme's `.sig` as the other to force a
// different/weaker verification path, and a genuine signature of one
// scheme never verifies under the other scheme's verifier. Every arm must
// REFUSE.
func TestSigner_SchemeConfusion(t *testing.T) {
	ctx := context.Background()
	hmac := testSigner(t) // hmac-kek scheme
	edSigner, edVerifier := testEd25519Signer(t)

	// Arm 1 — HMAC `.sig` RELABELED ed25519, verified with the ed25519
	// public key (the material matching the CLAIMED scheme). The recorded
	// MAC is an HMAC; ed25519.Verify over the ed25519-scheme canonical
	// bytes fails.
	t.Run("hmac_relabeled_ed25519", func(t *testing.T) {
		store := newMemStore()
		m := testManifest()
		if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, hmac); err != nil {
			t.Fatal(err)
		}
		relabelSigScheme(t, ctx, store, ManifestSigPath(ManifestFileName), irbackup.SignatureSchemeEd25519)
		if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, edVerifier); !errors.Is(err, ErrSignatureInvalid) {
			t.Fatalf("relabeled hmac->ed25519 verified: got %v, want ErrSignatureInvalid", err)
		}
	})

	// Arm 2 — Ed25519 `.sig` RELABELED hmac-kek, verified with the HMAC
	// key. The recorded MAC is a 64-byte ed25519 signature; the HMAC
	// comparison over the hmac-scheme canonical bytes fails.
	t.Run("ed25519_relabeled_hmac", func(t *testing.T) {
		store := newMemStore()
		m := testManifest()
		if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, edSigner); err != nil {
			t.Fatal(err)
		}
		relabelSigScheme(t, ctx, store, ManifestSigPath(ManifestFileName), irbackup.SignatureSchemeHMACKEK)
		if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, hmac); !errors.Is(err, ErrSignatureInvalid) {
			t.Fatalf("relabeled ed25519->hmac verified: got %v, want ErrSignatureInvalid", err)
		}
	})

	// Arm 3 — a GENUINE HMAC signature verified by an ed25519 verifier
	// (no relabel) is refused by the scheme-mismatch check.
	t.Run("genuine_hmac_via_ed25519_verifier", func(t *testing.T) {
		store := newMemStore()
		m := testManifest()
		if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, hmac); err != nil {
			t.Fatal(err)
		}
		if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, edVerifier); !errors.Is(err, ErrSignatureInvalid) {
			t.Fatalf("genuine hmac via ed25519 verifier: got %v, want ErrSignatureInvalid", err)
		}
	})

	// Arm 4 — a GENUINE ed25519 signature verified by the HMAC signer is
	// refused by the scheme-mismatch check.
	t.Run("genuine_ed25519_via_hmac_verifier", func(t *testing.T) {
		store := newMemStore()
		m := testManifest()
		if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, edSigner); err != nil {
			t.Fatal(err)
		}
		if err := VerifyManifest(ctx, store, ManifestFileName, m, 0, hmac); !errors.Is(err, ErrSignatureInvalid) {
			t.Fatalf("genuine ed25519 via hmac verifier: got %v, want ErrSignatureInvalid", err)
		}
	})
}

// TestEd25519Verifier_CannotSign pins that a verify-only signer (built
// from a public key) refuses to produce a signature — the key-separation
// guarantee (a verify host holds no signing capability).
func TestEd25519Verifier_CannotSign(t *testing.T) {
	_, verifier := testEd25519Signer(t)
	if verifier.canSign() {
		t.Fatal("verify-only ed25519 signer reports canSign()==true")
	}
	if _, err := verifier.SignManifest(context.Background(), testManifest(), 0); err == nil {
		t.Fatal("verify-only ed25519 signer produced a signature")
	}
	if err := ResignLineage(context.Background(), newMemStore(), verifier); err == nil {
		t.Fatal("ResignLineage with a verify-only signer did not refuse")
	}
}

// relabelSigScheme rewrites the `scheme` field of a detached `.sig`
// object in place — the adversary's relabel primitive.
func relabelSigScheme(t *testing.T, ctx context.Context, store irbackup.Store, path, newScheme string) {
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
