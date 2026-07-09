// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

// KMS manifest-signing seam + the PURE, provider-neutral verification
// primitive (ADR-0154 Phase 3, Option C "KMS-backed signing").
//
// Phases 1-2 kept a signature's SIGN and VERIFY on the same in-process
// key material (HMAC off the KEK; Ed25519 keypair). Phase 3 splits them:
// the SIGN call reaches a cloud KMS Sign API (the private key never
// leaves the HSM — an IAM-audited, CloudTrail-logged operation), but the
// VERIFY stays a PURE local computation against a public key the operator
// supplies (an exported PEM for offline DR, or a `kms://` reference whose
// public half is fetched once). This file holds the two halves of that
// split that are provider-INDEPENDENT:
//
//   - [KMSSigner]: the narrow seam the provider adapters implement (AWS in
//     Phase 3a; GCP/Azure in 3b). Only Sign does ctx/IO.
//   - [VerifyManifestKMS]: the pure verifier. It selects the crypto
//     primitive (ecdsa / rsa-pss / ed25519) from the recorded-AND-signed
//     algorithm and runs stdlib crypto against the operator's trusted
//     public key. No KMS access, no network — a DR host verifies a
//     KMS-signed chain offline.
//
// The signing ALGORITHM is bound by living in the composite scheme token
// `kms/<algorithm>` (see [backup.SignatureSchemeKMS]); this file just maps
// each algorithm string to its concrete primitive + hash. Getting the
// signature ENCODING right is load-bearing and adversarially reviewed: AWS
// KMS (and GCP) return ECDSA signatures as ASN.1 DER (RFC 3279), so the
// verifier uses [ecdsa.VerifyASN1] — NOT the raw r‖s form. A round-trip
// pin guards against a DER-vs-raw confusion.

import (
	"context"
	stdcrypto "crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"hash"
)

// KMS signing-algorithm identifiers — the sluice-canonical, provider-
// NEUTRAL names that appear after the `/` in a `kms/<algorithm>` scheme
// token and in the advisory [backup.ManifestSignature.Algorithm] field.
// Each provider adapter maps these to/from its own naming (AWS
// ECDSA_SHA_256, GCP EC_SIGN_P256_SHA256, Azure ES256, …). These strings
// are ON-DISK CONTRACT — they are inside the signed scheme token, so a
// rename strands every signature that recorded the old name.
const (
	// ECDSA over the NIST P-curves, DER-encoded signatures, digest =
	// SHA-256/384/512 matching the curve (the pairing AWS/GCP enforce).
	KMSAlgorithmECDSAP256 = "ecdsa-p256"
	KMSAlgorithmECDSAP384 = "ecdsa-p384"
	KMSAlgorithmECDSAP521 = "ecdsa-p521"

	// RSASSA-PSS with an MGF1/SHA-N mask and salt length = hash length
	// (the AWS/GCP PSS convention; Go's PSSSaltLengthAuto verifies it).
	KMSAlgorithmRSAPSS256 = "rsa-pss-256"
	KMSAlgorithmRSAPSS384 = "rsa-pss-384"
	KMSAlgorithmRSAPSS512 = "rsa-pss-512"

	// Ed25519 via KMS (GCP EC_SIGN_ED25519 — Phase 3b). The whole payload
	// is signed (no external pre-digest). Present here so the pure verifier
	// covers it; no Phase-3a signer emits it (AWS/Azure lack Ed25519).
	KMSAlgorithmEd25519 = "ed25519"

	// kmsKeyIDLabel domain-separates the KMS public-key fingerprint from
	// the HMAC / Ed25519 key-id namespaces so ids never collide across
	// schemes. The public key is not secret; the label just keeps the id
	// namespace distinct.
	kmsKeyIDLabel = "sluice-manifest-sig-kms-keyid/v1\n"
)

// KMSSigner is the narrow seam a cloud-KMS provider adapter implements to
// sign a manifest under ADR-0154 Phase 3. It is the ONLY part of the
// signing path that performs ctx/IO — [lineage.Signer] calls Sign for the
// `kms` scheme and keeps every other operation (canonical serialization,
// verification) pure. An adapter holds a KMS client + a resolved public
// key (fetched once at construction via the provider's GetPublicKey), so
// [KMSSigner.PublicKey] and [KMSSigner.KeyID] are available without
// further IO and match what an offline verifier computes from the same
// exported key.
type KMSSigner interface {
	// Sign returns the provider signature over the canonical manifest
	// payload. The adapter hashes payload as its algorithm requires
	// (digest-signing for ECDSA/RSA; whole-message for Ed25519) and calls
	// the KMS Sign API. The private key never leaves the HSM.
	Sign(ctx context.Context, payload []byte) ([]byte, error)

	// Algorithm is the sluice-canonical algorithm identifier (e.g.
	// [KMSAlgorithmECDSAP256]) — the suffix of the composite `kms/<algo>`
	// scheme token and the authoritative algorithm binding.
	Algorithm() string

	// KeyID is the stable, non-secret fingerprint of the signing key's
	// PUBLIC half ([KMSManifestKeyID]) — identical to what a verifier
	// derives from the exported public key, so rotation is expressible.
	KeyID() string

	// KeyRef is the concrete VERSIONED KMS key reference the signature was
	// produced under (advisory; recorded in the `.sig` for rotation/audit,
	// never trusted for verification).
	KeyRef() string

	// PublicKey is the signing key's public half, resolved at construction.
	// Recorded nowhere secret; used to derive [KMSSigner.KeyID] and to let
	// a sign+verify signer self-verify locally.
	PublicKey() stdcrypto.PublicKey
}

// KMSManifestKeyID returns a stable, non-secret fingerprint of an
// asymmetric PUBLIC key for the detached signature's `key_id` field,
// domain-separated by [kmsKeyIDLabel]. It fingerprints the SPKI (PKIX)
// DER encoding, so the signer (which resolves the public key via
// GetPublicKey) and an offline verifier (which parses the same key from a
// PEM file) compute the IDENTICAL id. Returns an error if the key cannot
// be marshalled (an unsupported key type).
func KMSManifestKeyID(pub stdcrypto.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("crypto: kms key id: marshal public key: %w", err)
	}
	h := sha256.Sum256(append([]byte(kmsKeyIDLabel), der...))
	return hex.EncodeToString(h[:8]), nil
}

// ParseManifestPublicKeyPEM decodes an SPKI (PKIX) PEM public key of ANY
// supported asymmetric type — Ed25519, ECDSA (P-256/384/521), or RSA —
// the general offline-verify parser for `--verify-key <file>`. It is the
// `openssl pkey -pubout` / `x509.MarshalPKIXPublicKey` format every
// provider's GetPublicKey / keygen produces. Returns the parsed key as a
// stdlib [crypto.PublicKey]; the caller pairs it with the recorded
// algorithm to pick the verification primitive.
func ParseManifestPublicKeyPEM(pemBytes []byte) (stdcrypto.PublicKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("crypto: manifest verify key: no PEM block found")
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("crypto: parse manifest verify key (spki): %w", err)
	}
	switch key.(type) {
	case ed25519.PublicKey, *ecdsa.PublicKey, *rsa.PublicKey:
		return key, nil
	default:
		return nil, fmt.Errorf("crypto: manifest verify key is %T, not a supported signing key (ed25519 / ecdsa / rsa)", key)
	}
}

// parseSPKIDER parses a raw SPKI (PKIX) DER public key — the form AWS/GCP
// GetPublicKey returns (unarmored) — into a stdlib public key, refusing an
// unsupported key type. The PEM path ([ParseManifestPublicKeyPEM]) shares
// the same type gate.
func parseSPKIDER(der []byte) (stdcrypto.PublicKey, error) {
	key, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		return nil, fmt.Errorf("parse public key (spki der): %w", err)
	}
	switch key.(type) {
	case ed25519.PublicKey, *ecdsa.PublicKey, *rsa.PublicKey:
		return key, nil
	default:
		return nil, fmt.Errorf("public key is %T, not a supported signing key (ed25519 / ecdsa / rsa)", key)
	}
}

// VerifyManifestKMS reports whether sig is a valid signature of payload
// under pub for the given sluice-canonical algorithm. It is PURE (no KMS,
// no network) — the DR-offline verifier. The primitive is selected from
// algorithm (which is authoritatively bound in the signed scheme token),
// and a key of the WRONG type for the algorithm (e.g. an RSA key presented
// for an ecdsa-* algorithm), a corrupted signature, a tampered payload, or
// an unknown algorithm all return false — fail-closed.
//
// ECDSA signatures are ASN.1 DER (the AWS/GCP KMS encoding, RFC 3279), so
// verification uses [ecdsa.VerifyASN1]. Ed25519 signs the whole message
// (no external pre-digest); ECDSA/RSA-PSS verify a digest sized to match
// the algorithm.
func VerifyManifestKMS(pub stdcrypto.PublicKey, algorithm string, payload, sig []byte) bool {
	switch algorithm {
	case KMSAlgorithmEd25519:
		edPub, ok := pub.(ed25519.PublicKey)
		if !ok || len(edPub) != ed25519.PublicKeySize {
			return false
		}
		return ed25519.Verify(edPub, payload, sig)

	case KMSAlgorithmECDSAP256, KMSAlgorithmECDSAP384, KMSAlgorithmECDSAP521:
		ecPub, ok := pub.(*ecdsa.PublicKey)
		if !ok {
			return false
		}
		digest := hashPayload(algorithm, payload)
		if digest == nil {
			return false
		}
		return ecdsa.VerifyASN1(ecPub, digest, sig)

	case KMSAlgorithmRSAPSS256, KMSAlgorithmRSAPSS384, KMSAlgorithmRSAPSS512:
		rsaPub, ok := pub.(*rsa.PublicKey)
		if !ok {
			return false
		}
		h, digest := hashForAlgorithm(algorithm), hashPayload(algorithm, payload)
		if digest == nil {
			return false
		}
		// SaltLengthAuto verifies the salt-length = hash-length convention
		// AWS/GCP use without hard-coding it.
		return rsa.VerifyPSS(rsaPub, h, digest, sig, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthAuto}) == nil

	default:
		return false
	}
}

// hashForAlgorithm maps a sluice-canonical KMS algorithm to the stdlib
// hash function the digest-signing primitives (ECDSA / RSA-PSS) use.
// Returns 0 for Ed25519 (no external pre-digest) or an unknown algorithm.
func hashForAlgorithm(algorithm string) stdcrypto.Hash {
	switch algorithm {
	case KMSAlgorithmECDSAP256, KMSAlgorithmRSAPSS256:
		return stdcrypto.SHA256
	case KMSAlgorithmECDSAP384, KMSAlgorithmRSAPSS384:
		return stdcrypto.SHA384
	case KMSAlgorithmECDSAP521, KMSAlgorithmRSAPSS512:
		return stdcrypto.SHA512
	default:
		return 0
	}
}

// hashPayload returns the digest of payload for a digest-signing
// algorithm, or nil for Ed25519 / an unknown algorithm. The provider
// adapters MUST hash identically before calling the KMS Sign API (with
// MessageType=DIGEST), so this is the single source of truth for the
// digest a `kms` signature covers.
func hashPayload(algorithm string, payload []byte) []byte {
	var h hash.Hash
	switch hashForAlgorithm(algorithm) {
	case stdcrypto.SHA256:
		h = sha256.New()
	case stdcrypto.SHA384:
		h = sha512.New384()
	case stdcrypto.SHA512:
		h = sha512.New()
	default:
		return nil
	}
	h.Write(payload)
	return h.Sum(nil)
}

// DigestForKMSAlgorithm exposes [hashPayload] to the provider adapters so
// they hash a manifest payload to exactly the digest [VerifyManifestKMS]
// expects, then hand it to their KMS Sign API as MessageType=DIGEST.
// Returns nil for Ed25519 (the adapter signs the whole message) or an
// unknown algorithm.
func DigestForKMSAlgorithm(algorithm string, payload []byte) []byte {
	return hashPayload(algorithm, payload)
}
