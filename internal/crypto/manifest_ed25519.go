// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

// Ed25519 manifest-signing primitives + on-disk key format (ADR-0154
// Phase 2, Option B "asymmetric keypair").
//
// Phase 1 (Option A) keyed an HMAC off the chain KEK — zero new key
// management, but symmetric (verifier holds the signing secret) and
// unavailable for plaintext chains (no KEK to key off). Phase 2 adds a
// SECOND, orthogonal scheme: an Ed25519 keypair the operator manages.
// The PRIVATE key signs at backup time; the PUBLIC key — distributable
// freely — verifies. This unlocks (1) signing PLAINTEXT backups (the
// keypair is independent of encryption) and (2) key-separated
// verification (a CI/restore host holds only the public key, never a
// signing secret).
//
// The scheme is selected EXPLICITLY by the operator supplying a signing
// key (`--sign-key`), never inferred. The scheme identifier is folded
// into the signed canonical bytes ([backup.CanonicalManifestBytes]), so
// an adversary cannot relabel an HMAC signature as Ed25519 (or vice
// versa) to force a weaker verification path — a relabel changes the
// signed bytes AND is checked against the operator-supplied verify
// material's scheme.
//
// On-disk key format (ON-DISK CONTRACT — chosen for interoperability):
//
//   - Private key: PKCS#8, PEM-armored ("PRIVATE KEY" block). This is
//     the format `openssl genpkey -algorithm ed25519`,
//     `ssh-keygen -m PKCS8`, and Go's [x509.MarshalPKCS8PrivateKey]
//     all speak, so an operator can generate/inspect keys with standard
//     tooling. Written 0600 by the keygen command.
//   - Public key: SPKI (SubjectPublicKeyInfo), PEM-armored
//     ("PUBLIC KEY" block) — the `openssl pkey -pubout` /
//     [x509.MarshalPKIXPublicKey] format. Freely distributable.
//
// Both marshal/parse through crypto/x509 (stdlib), no third-party deps.

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
)

const (
	// pemBlockEd25519Private is the PEM block type for the PKCS#8 private
	// key. "PRIVATE KEY" (not "ED25519 PRIVATE KEY") is the PKCS#8
	// convention — the algorithm is carried inside the DER.
	pemBlockEd25519Private = "PRIVATE KEY"

	// pemBlockEd25519Public is the PEM block type for the SPKI public key.
	pemBlockEd25519Public = "PUBLIC KEY"

	// ed25519KeyIDLabel domain-separates the public-key fingerprint from
	// any other hash of the key bytes so the id can be recorded in the
	// clear (in the detached signature's `key_id`) and reported without
	// implying anything secret. The public key is not secret; the label
	// just keeps the id namespace distinct from the HMAC key-id.
	ed25519KeyIDLabel = "sluice-manifest-sig-ed25519-keyid/v1\n"
)

// GenerateEd25519Keypair returns a fresh Ed25519 keypair from
// [crypto/rand]. The private key signs backups; the public key verifies.
func GenerateEd25519Keypair() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("crypto: generate ed25519 keypair: %w", err)
	}
	return pub, priv, nil
}

// MarshalEd25519PrivateKeyPEM encodes priv as PKCS#8 PEM (the on-disk
// private-key contract). The caller writes the result 0600.
func MarshalEd25519PrivateKeyPEM(priv ed25519.PrivateKey) ([]byte, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("crypto: ed25519 private key length %d != %d", len(priv), ed25519.PrivateKeySize)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("crypto: marshal ed25519 private key (pkcs8): %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: pemBlockEd25519Private, Bytes: der}), nil
}

// ParseEd25519PrivateKeyPEM decodes a PKCS#8 PEM private key written by
// [MarshalEd25519PrivateKeyPEM]. Refuses a non-Ed25519 key loudly.
func ParseEd25519PrivateKeyPEM(pemBytes []byte) (ed25519.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("crypto: ed25519 private key: no PEM block found")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("crypto: parse ed25519 private key (pkcs8): %w", err)
	}
	priv, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("crypto: private key is %T, not ed25519", key)
	}
	return priv, nil
}

// MarshalEd25519PublicKeyPEM encodes pub as SPKI PEM (the on-disk
// public-key contract). Freely distributable.
func MarshalEd25519PublicKeyPEM(pub ed25519.PublicKey) ([]byte, error) {
	if len(pub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("crypto: ed25519 public key length %d != %d", len(pub), ed25519.PublicKeySize)
	}
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, fmt.Errorf("crypto: marshal ed25519 public key (spki): %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: pemBlockEd25519Public, Bytes: der}), nil
}

// ParseEd25519PublicKeyPEM decodes an SPKI PEM public key written by
// [MarshalEd25519PublicKeyPEM]. Refuses a non-Ed25519 key loudly.
func ParseEd25519PublicKeyPEM(pemBytes []byte) (ed25519.PublicKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("crypto: ed25519 public key: no PEM block found")
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("crypto: parse ed25519 public key (spki): %w", err)
	}
	pub, ok := key.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("crypto: public key is %T, not ed25519", key)
	}
	return pub, nil
}

// SignManifestEd25519 returns the Ed25519 signature of payload under
// priv. payload is the canonical manifest serialization
// ([backup.CanonicalManifestBytes], with the "ed25519" scheme folded in).
func SignManifestEd25519(priv ed25519.PrivateKey, payload []byte) []byte {
	return ed25519.Sign(priv, payload)
}

// VerifyManifestEd25519 reports whether sig is a valid Ed25519 signature
// of payload under pub. A wrong key, a tampered payload, or a signature
// of the wrong length / scheme (e.g. an HMAC relabeled as Ed25519) all
// return false. ed25519.Verify panics on a wrong-size public key, so the
// length is guarded first.
func VerifyManifestEd25519(pub ed25519.PublicKey, payload, sig []byte) bool {
	if len(pub) != ed25519.PublicKeySize {
		return false
	}
	return ed25519.Verify(pub, payload, sig)
}

// Ed25519KeyID returns a stable, non-secret fingerprint of the PUBLIC
// key for the detached signature's `key_id` field. It is derived from
// the public key (not the private) so the signer (which holds the
// private key) and a verifier (which holds only the public key) compute
// the IDENTICAL id — rotation is expressible and a verifier can report
// which key a signature claims. Domain-separated by [ed25519KeyIDLabel].
func Ed25519KeyID(pub ed25519.PublicKey) string {
	h := sha256.Sum256(append([]byte(ed25519KeyIDLabel), pub...))
	return hex.EncodeToString(h[:8])
}

// Ed25519KeyIDFromPrivate returns the key-id of priv's public half, so
// the signing side (which holds only the private key) records the same
// id a verifier derives from the distributed public key.
func Ed25519KeyIDFromPrivate(priv ed25519.PrivateKey) string {
	return Ed25519KeyID(priv.Public().(ed25519.PublicKey))
}
