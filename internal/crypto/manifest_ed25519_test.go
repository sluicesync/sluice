// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

import (
	"bytes"
	"crypto/ed25519"
	"strings"
	"testing"
)

// TestEd25519Keypair_RoundTrip pins the full keygen contract: generate →
// marshal (PEM) → parse → sign → verify, plus that the parsed keys equal
// the generated ones (the on-disk format is lossless).
func TestEd25519Keypair_RoundTrip(t *testing.T) {
	pub, priv, err := GenerateEd25519Keypair()
	if err != nil {
		t.Fatal(err)
	}
	privPEM, err := MarshalEd25519PrivateKeyPEM(priv)
	if err != nil {
		t.Fatal(err)
	}
	pubPEM, err := MarshalEd25519PublicKeyPEM(pub)
	if err != nil {
		t.Fatal(err)
	}
	gotPriv, err := ParseEd25519PrivateKeyPEM(privPEM)
	if err != nil {
		t.Fatal(err)
	}
	gotPub, err := ParseEd25519PublicKeyPEM(pubPEM)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotPriv, priv) {
		t.Fatal("parsed private key != generated")
	}
	if !bytes.Equal(gotPub, pub) {
		t.Fatal("parsed public key != generated")
	}
	// Sign with the parsed private, verify with the parsed public.
	payload := []byte("canonical-manifest-bytes")
	sig := SignManifestEd25519(gotPriv, payload)
	if !VerifyManifestEd25519(gotPub, payload, sig) {
		t.Fatal("valid signature did not verify after round-trip")
	}
}

// TestEd25519PEM_FormatGolden pins the on-disk PEM block types (the
// on-disk contract): PKCS#8 "PRIVATE KEY" + SPKI "PUBLIC KEY", so a
// format drift that breaks interoperability with openssl / ssh-keygen is
// caught.
func TestEd25519PEM_FormatGolden(t *testing.T) {
	pub, priv, err := GenerateEd25519Keypair()
	if err != nil {
		t.Fatal(err)
	}
	privPEM, _ := MarshalEd25519PrivateKeyPEM(priv)
	pubPEM, _ := MarshalEd25519PublicKeyPEM(pub)
	if !strings.HasPrefix(string(privPEM), "-----BEGIN PRIVATE KEY-----") {
		t.Fatalf("private PEM header drift: %q", firstLine(privPEM))
	}
	if !strings.HasPrefix(string(pubPEM), "-----BEGIN PUBLIC KEY-----") {
		t.Fatalf("public PEM header drift: %q", firstLine(pubPEM))
	}
}

func firstLine(b []byte) string {
	if i := bytes.IndexByte(b, '\n'); i >= 0 {
		return string(b[:i])
	}
	return string(b)
}

// TestVerifyManifestEd25519_WrongKeyAndTamper pins the refusal classes:
// wrong key, tampered payload, corrupted signature, and a wrong-size
// public key (which must NOT panic).
func TestVerifyManifestEd25519_WrongKeyAndTamper(t *testing.T) {
	pub, priv, _ := GenerateEd25519Keypair()
	other, _, _ := GenerateEd25519Keypair()
	payload := []byte("payload")
	sig := SignManifestEd25519(priv, payload)

	if !VerifyManifestEd25519(pub, payload, sig) {
		t.Fatal("valid signature did not verify")
	}
	if VerifyManifestEd25519(other, payload, sig) {
		t.Fatal("wrong public key verified")
	}
	if VerifyManifestEd25519(pub, append(payload, '!'), sig) {
		t.Fatal("tampered payload verified")
	}
	corrupt := bytes.Clone(sig)
	corrupt[0] ^= 0xff
	if VerifyManifestEd25519(pub, payload, corrupt) {
		t.Fatal("corrupted signature verified")
	}
	// An HMAC-sized (32-byte) blob relabeled as an ed25519 sig must not
	// verify (this is the scheme-confusion primitive).
	if VerifyManifestEd25519(pub, payload, make([]byte, 32)) {
		t.Fatal("32-byte (HMAC-sized) blob verified as ed25519")
	}
	// A wrong-size public key must return false, not panic.
	if VerifyManifestEd25519(ed25519.PublicKey(make([]byte, 5)), payload, sig) {
		t.Fatal("wrong-size public key verified")
	}
}

// TestEd25519KeyID_MatchesFromPrivate pins that the key-id derived from
// the public key equals the one derived from the private key's public
// half — the signer (private) and verifier (public) MUST agree on the id.
func TestEd25519KeyID_MatchesFromPrivate(t *testing.T) {
	pub, priv, _ := GenerateEd25519Keypair()
	fromPub := Ed25519KeyID(pub)
	fromPriv := Ed25519KeyIDFromPrivate(priv)
	if fromPub != fromPriv {
		t.Fatalf("key-id mismatch: pub %s != priv %s", fromPub, fromPriv)
	}
	if fromPub == "" {
		t.Fatal("empty key id")
	}
	// A different keypair yields a different id.
	pub2, _, _ := GenerateEd25519Keypair()
	if Ed25519KeyID(pub2) == fromPub {
		t.Fatal("distinct keys produced the same id")
	}
}

// TestParseEd25519_RejectsNonEd25519 pins that the parsers refuse
// malformed / wrong-algorithm PEM loudly rather than returning a zero key.
func TestParseEd25519_RejectsNonEd25519(t *testing.T) {
	if _, err := ParseEd25519PrivateKeyPEM([]byte("not pem")); err == nil {
		t.Fatal("non-PEM private key parsed without error")
	}
	if _, err := ParseEd25519PublicKeyPEM([]byte("not pem")); err == nil {
		t.Fatal("non-PEM public key parsed without error")
	}
}
