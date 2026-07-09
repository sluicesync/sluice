// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

// Manifest-signature key derivation + HMAC primitives (ADR-0154 Phase 1,
// Option A "HMAC-off-KEK").
//
// Phase 1 signs the manifest with an HMAC-SHA-256 keyed off a key
// DERIVED from the chain KEK — never the KEK (or CEK) material directly.
// The derivation is HKDF-SHA-256 with a DISTINCT info label
// ([ManifestSigHKDFLabel]), so the signing key is cryptographically
// separated from every encryption use of the same KEK. The label is
// on-disk contract exactly like ADR-0152's AAD strings: it keys every
// signature ever written, so changing it strands them and requires a new
// signature-scheme version.
//
// Symmetric HMAC authenticates "written by someone who holds the chain's
// passphrase-derived KEK" — sufficient for the ADR-0152 store-adversary
// model (the adversary has neither the passphrase nor the derived key)
// and available with zero new key management on an already-encrypted
// chain. Asymmetric signing (Ed25519 / KMS) — key-separated verification
// and plaintext coverage — is ADR-0154 Phases 2-3.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/hkdf"
)

const (
	// ManifestSigHKDFLabel is the HKDF info label that derives the
	// manifest HMAC key from the chain KEK. On-disk contract (it keys
	// every signature); a change needs a new signature-scheme version.
	ManifestSigHKDFLabel = "sluice-manifest-sig/v1"

	// ManifestSigKeyLen is the derived HMAC-SHA-256 key length. 32 bytes
	// matches the HMAC block/key sizing for SHA-256.
	ManifestSigKeyLen = 32

	// manifestSigKeyIDLabel domain-separates the public key-id
	// fingerprint from the signing key itself so the id can be recorded
	// in the clear (in the detached signature) without leaking key bits.
	manifestSigKeyIDLabel = "sluice-manifest-sig-keyid/v1\n"
)

// DeriveManifestHMACKey derives the manifest-signing HMAC key from kek
// via HKDF-SHA-256 with [ManifestSigHKDFLabel]. kek is the chain's KEK
// (passphrase-Argon2id-derived in Phase 1); the result is used only to
// HMAC the canonical manifest serialization, never to encrypt.
func DeriveManifestHMACKey(kek []byte) ([]byte, error) {
	if len(kek) == 0 {
		return nil, errors.New("crypto: manifest sig: empty kek")
	}
	r := hkdf.New(sha256.New, kek, nil, []byte(ManifestSigHKDFLabel))
	out := make([]byte, ManifestSigKeyLen)
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, fmt.Errorf("crypto: manifest sig: derive key: %w", err)
	}
	return out, nil
}

// SignManifestHMAC returns the HMAC-SHA-256 of payload under key. payload
// is the canonical manifest serialization ([backup.CanonicalManifestBytes]).
func SignManifestHMAC(key, payload []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(payload)
	return m.Sum(nil)
}

// VerifyManifestHMAC reports whether mac is the HMAC-SHA-256 of payload
// under key, in constant time.
func VerifyManifestHMAC(key, payload, mac []byte) bool {
	want := SignManifestHMAC(key, payload)
	return hmac.Equal(want, mac)
}

// ManifestSigKeyID returns a stable, non-secret fingerprint of the
// signing key for recording in the detached signature's `key_id` field
// (so a verifier can tell which key a signature claims, and rotation is
// expressible). Domain-separated from the key so publishing it leaks
// nothing about the key.
func ManifestSigKeyID(key []byte) string {
	h := sha256.Sum256(append([]byte(manifestSigKeyIDLabel), key...))
	return hex.EncodeToString(h[:8])
}
