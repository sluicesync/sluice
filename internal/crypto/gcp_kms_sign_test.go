// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

import (
	"bytes"
	"context"
	stdcrypto "crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// fakeGCPKMSSign is a FAITHFUL in-process stand-in for Cloud KMS asymmetric
// signing: it holds a real private key, GetPublicKey returns the SPKI PEM
// (exactly GCP's export) with a correct PemCrc32C, and AsymmetricSign
// returns the SAME signature encoding GCP returns — ASN.1 DER for ECDSA
// (RFC 5480), PSS for RSA, raw for Ed25519 — with a correct SignatureCrc32C
// and the VerifiedDigest/DataCrc32C acknowledgement. This is what makes the
// round-trip test meaningful: a DER-vs-raw confusion in sluice's verifier
// would fail against this faithful DER signer.
//
// The knobs (corruptSignatureCRC / corruptPemCRC / unverifyRequestCRC)
// simulate the GCP wire-integrity failure shapes the adapter must refuse.
type fakeGCPKMSSign struct {
	alg     kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm
	ecPriv  *ecdsa.PrivateKey
	rsaPriv *rsa.PrivateKey
	edPriv  ed25519.PrivateKey
	edPub   ed25519.PublicKey

	signCalls int

	corruptSignatureCRC bool // return a wrong SignatureCrc32C
	corruptPemCRC       bool // return a wrong PemCrc32C from GetPublicKey
	unverifyRequestCRC  bool // report VerifiedDigest/DataCrc32C == false
}

func newFakeGCPKMSSign(t *testing.T, alg kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm) *fakeGCPKMSSign {
	t.Helper()
	f := &fakeGCPKMSSign{alg: alg}
	var err error
	switch alg {
	case kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256,
		kmspb.CryptoKeyVersion_EC_SIGN_SECP256K1_SHA256:
		// secp256k1 is a refusal case (non-NIST); a P-256 key is fine as a
		// stand-in because the algorithm enum is refused before the key is used.
		f.ecPriv, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case kmspb.CryptoKeyVersion_EC_SIGN_P384_SHA384:
		f.ecPriv, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	case kmspb.CryptoKeyVersion_RSA_SIGN_PSS_2048_SHA256,
		kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_2048_SHA256:
		// RSA_DECRYPT_OAEP is a refusal case (encryption purpose); an RSA key
		// gives GetPublicKey a valid PEM, and the algorithm is refused.
		f.rsaPriv, err = rsa.GenerateKey(rand.Reader, 2048)
	case kmspb.CryptoKeyVersion_EC_SIGN_ED25519:
		f.edPub, f.edPriv, err = ed25519.GenerateKey(rand.Reader)
	default:
		t.Fatalf("unsupported fake key algorithm %v", alg)
	}
	if err != nil {
		t.Fatalf("generate fake key (%v): %v", alg, err)
	}
	return f
}

func (f *fakeGCPKMSSign) publicKey() stdcrypto.PublicKey {
	switch {
	case f.ecPriv != nil:
		return &f.ecPriv.PublicKey
	case f.rsaPriv != nil:
		return &f.rsaPriv.PublicKey
	default:
		return f.edPub
	}
}

func (f *fakeGCPKMSSign) GetPublicKey(_ context.Context, _ *kmspb.GetPublicKeyRequest, _ ...gax) (*kmspb.PublicKey, error) {
	der, err := x509.MarshalPKIXPublicKey(f.publicKey())
	if err != nil {
		return nil, err
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	pemCRC := crc32c(pemBytes)
	if f.corruptPemCRC {
		pemCRC ^= 0xff
	}
	return &kmspb.PublicKey{
		Pem:       string(pemBytes),
		Algorithm: f.alg,
		PemCrc32C: wrapperspb.Int64(pemCRC),
	}, nil
}

func (f *fakeGCPKMSSign) AsymmetricSign(_ context.Context, in *kmspb.AsymmetricSignRequest, _ ...gax) (*kmspb.AsymmetricSignResponse, error) {
	f.signCalls++
	var sig []byte
	var err error
	switch f.alg {
	case kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256:
		// GCP returns ASN.1 DER for ECDSA — SignASN1 produces exactly that.
		sig, err = ecdsa.SignASN1(rand.Reader, f.ecPriv, in.GetDigest().GetSha256())
	case kmspb.CryptoKeyVersion_EC_SIGN_P384_SHA384:
		sig, err = ecdsa.SignASN1(rand.Reader, f.ecPriv, in.GetDigest().GetSha384())
	case kmspb.CryptoKeyVersion_RSA_SIGN_PSS_2048_SHA256:
		sig, err = rsa.SignPSS(rand.Reader, f.rsaPriv, stdcrypto.SHA256, in.GetDigest().GetSha256(),
			&rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
	case kmspb.CryptoKeyVersion_EC_SIGN_ED25519:
		sig = ed25519.Sign(f.edPriv, in.GetData())
	default:
		return nil, status.Error(codes.InvalidArgument, "fake: unsupported signing algorithm "+f.alg.String())
	}
	if err != nil {
		return nil, err
	}
	sigCRC := crc32c(sig)
	if f.corruptSignatureCRC {
		sigCRC ^= 0xff
	}
	return &kmspb.AsymmetricSignResponse{
		Signature:            sig,
		Name:                 in.GetName(),
		SignatureCrc32C:      wrapperspb.Int64(sigCRC),
		VerifiedDigestCrc32C: in.GetDigest() != nil && !f.unverifyRequestCRC,
		VerifiedDataCrc32C:   in.GetData() != nil && !f.unverifyRequestCRC,
	}, nil
}

func (f *fakeGCPKMSSign) Close() error { return nil }

// versionedRef is a well-formed CryptoKeyVersion resource for the fakes.
const versionedRef = "projects/p/locations/us/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1"

// gcpSignCase pins one CryptoKeyVersion algorithm → expected sluice algorithm.
type gcpSignCase struct {
	alg  kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm
	algo string
}

var gcpSignCases = []gcpSignCase{
	{kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256, KMSAlgorithmECDSAP256},
	{kmspb.CryptoKeyVersion_EC_SIGN_P384_SHA384, KMSAlgorithmECDSAP384},
	{kmspb.CryptoKeyVersion_RSA_SIGN_PSS_2048_SHA256, KMSAlgorithmRSAPSS256},
	{kmspb.CryptoKeyVersion_EC_SIGN_ED25519, KMSAlgorithmEd25519},
}

// TestGCPKMSSigner_RoundTrip is the PIN-THE-CLASS matrix: EVERY supported
// family GCP KMS offers for manifest signing (ECDSA P-256/384, RSA-PSS-256,
// Ed25519) signs via the faithful fake and verifies through the PURE local
// verifier. A green on one family does not cover the others (different
// curve/OID/digest-vs-data paths), so all four are exercised.
func TestGCPKMSSigner_RoundTrip(t *testing.T) {
	ctx := context.Background()
	payload := []byte("canonical-manifest-bytes-under-test")
	for _, tc := range gcpSignCases {
		tc := tc
		t.Run(tc.algo, func(t *testing.T) {
			fake := newFakeGCPKMSSign(t, tc.alg)
			signer, err := NewGCPKMSSigner(ctx, versionedRef, WithGCPKMSSignClient(fake))
			if err != nil {
				t.Fatalf("NewGCPKMSSigner: %v", err)
			}
			if signer.Algorithm() != tc.algo {
				t.Fatalf("algorithm: got %q want %q", signer.Algorithm(), tc.algo)
			}
			if signer.KeyRef() != versionedRef {
				t.Fatalf("KeyRef: got %q want %q", signer.KeyRef(), versionedRef)
			}
			sig, err := signer.Sign(ctx, payload)
			if err != nil {
				t.Fatalf("Sign: %v", err)
			}
			if !VerifyManifestKMS(signer.PublicKey(), signer.Algorithm(), payload, sig) {
				t.Fatalf("VerifyManifestKMS returned false for a valid %s signature", tc.algo)
			}
			// Tampered payload → refuse.
			if VerifyManifestKMS(signer.PublicKey(), signer.Algorithm(), []byte("tampered"), sig) {
				t.Fatalf("%s: tampered payload verified", tc.algo)
			}
			// Corrupted signature → refuse.
			bad := bytes.Clone(sig)
			bad[len(bad)-1] ^= 0xff
			if VerifyManifestKMS(signer.PublicKey(), signer.Algorithm(), payload, bad) {
				t.Fatalf("%s: corrupted signature verified", tc.algo)
			}
			// KeyID is stable + matches the id derived from the exported key.
			wantID, err := KMSManifestKeyID(signer.PublicKey())
			if err != nil {
				t.Fatalf("KMSManifestKeyID: %v", err)
			}
			if signer.KeyID() != wantID {
				t.Fatalf("KeyID mismatch: signer %q vs pub-derived %q", signer.KeyID(), wantID)
			}
			if err := signer.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

// TestGCPKMSSigner_ECDSA_IsDER_NotRawConcat is the load-bearing encoding
// pin: GCP returns ECDSA signatures as ASN.1 DER, and the verifier MUST use
// ecdsa.VerifyASN1. It asserts the signature decodes as DER {R,S}, and that
// the equivalent RAW r‖s (IEEE-P1363) form of the SAME signature does NOT
// verify — so a DER-vs-raw confusion is caught, not silently accepted.
func TestGCPKMSSigner_ECDSA_IsDER_NotRawConcat(t *testing.T) {
	ctx := context.Background()
	payload := []byte("der-encoding-pin")
	fake := newFakeGCPKMSSign(t, kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256)
	signer, err := NewGCPKMSSigner(ctx, versionedRef, WithGCPKMSSignClient(fake))
	if err != nil {
		t.Fatalf("NewGCPKMSSigner: %v", err)
	}
	sig, err := signer.Sign(ctx, payload)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	// (1) The signature is valid ASN.1 DER {R, S}.
	var parsed struct{ R, S *big.Int }
	rest, err := asn1.Unmarshal(sig, &parsed)
	if err != nil || len(rest) != 0 {
		t.Fatalf("GCP ECDSA signature is not clean ASN.1 DER (err=%v, trailing=%d) — encoding contract broken", err, len(rest))
	}

	// (2) The RAW r‖s form (fixed 32-byte big-endian each for P-256) of the
	// same (r,s) must NOT verify — the verifier requires DER.
	raw := make([]byte, 64)
	parsed.R.FillBytes(raw[:32])
	parsed.S.FillBytes(raw[32:])
	if VerifyManifestKMS(signer.PublicKey(), signer.Algorithm(), payload, raw) {
		t.Fatal("raw r‖s signature verified — verifier is not DER-strict (DER-vs-raw confusion)")
	}

	// Sanity: the DER form does verify (guards against a false-negative in (2)).
	if !VerifyManifestKMS(signer.PublicKey(), signer.Algorithm(), payload, sig) {
		t.Fatal("DER signature failed to verify")
	}
}

// TestGCPKMSSigner_WrongKeyAndType pins fail-closed behavior for a wrong key
// and a wrong key TYPE (an RSA key presented for an ecdsa-* algorithm, and
// vice versa) — the type assertion must refuse, never panic.
func TestGCPKMSSigner_WrongKeyAndType(t *testing.T) {
	ctx := context.Background()
	payload := []byte("wrong-key")

	ecFake := newFakeGCPKMSSign(t, kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256)
	ecSigner, _ := NewGCPKMSSigner(ctx, versionedRef, WithGCPKMSSignClient(ecFake))
	ecSig, _ := ecSigner.Sign(ctx, payload)

	// Wrong key of the right type.
	other := newFakeGCPKMSSign(t, kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256)
	if VerifyManifestKMS(other.publicKey(), ecSigner.Algorithm(), payload, ecSig) {
		t.Fatal("verified an ECDSA signature under a different ECDSA key")
	}
	// Wrong key TYPE: ECDSA sig under an rsa-pss algorithm with an RSA key.
	rsaFake := newFakeGCPKMSSign(t, kmspb.CryptoKeyVersion_RSA_SIGN_PSS_2048_SHA256)
	if VerifyManifestKMS(rsaFake.publicKey(), KMSAlgorithmRSAPSS256, payload, ecSig) {
		t.Fatal("verified an ECDSA signature under an RSA key/algorithm")
	}
	// RSA key presented for an ecdsa-* algorithm → refuse.
	if VerifyManifestKMS(rsaFake.publicKey(), KMSAlgorithmECDSAP256, payload, ecSig) {
		t.Fatal("verified with an RSA key under an ecdsa-* algorithm")
	}
	// Ed25519 sig under an ecdsa algorithm → refuse (no panic).
	edFake := newFakeGCPKMSSign(t, kmspb.CryptoKeyVersion_EC_SIGN_ED25519)
	edSigner, _ := NewGCPKMSSigner(ctx, versionedRef, WithGCPKMSSignClient(edFake))
	edSig, _ := edSigner.Sign(ctx, payload)
	if VerifyManifestKMS(ecSigner.PublicKey(), KMSAlgorithmEd25519, payload, edSig) {
		t.Fatal("verified an Ed25519 signature under an ECDSA key")
	}
}

// TestNewGCPKMSSigner_RefusesEncryptionKey pins that a non-signing key
// (an encryption-purpose RSA_DECRYPT_OAEP key mistakenly passed as a signing
// key) is refused loudly at construction. GetPublicKey carries no purpose
// field, so the algorithm enum is the discriminator: an encryption algorithm
// maps to no sluice signing algorithm and is refused.
func TestNewGCPKMSSigner_RefusesEncryptionKey(t *testing.T) {
	fake := newFakeGCPKMSSign(t, kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_2048_SHA256)
	_, err := NewGCPKMSSigner(context.Background(), versionedRef, WithGCPKMSSignClient(fake))
	if err == nil {
		t.Fatal("NewGCPKMSSigner accepted an RSA_DECRYPT_OAEP (encryption) key")
	}
	if !strings.Contains(err.Error(), "not a supported manifest-signing algorithm") {
		t.Fatalf("refusal should name the unsupported algorithm; got %v", err)
	}
}

// TestNewGCPKMSSigner_RefusesSecp256k1 pins that the non-NIST secp256k1
// signing curve is refused (sluice's verifier only covers the NIST P-curves).
func TestNewGCPKMSSigner_RefusesSecp256k1(t *testing.T) {
	fake := newFakeGCPKMSSign(t, kmspb.CryptoKeyVersion_EC_SIGN_SECP256K1_SHA256)
	_, err := NewGCPKMSSigner(context.Background(), versionedRef, WithGCPKMSSignClient(fake))
	if err == nil {
		t.Fatal("NewGCPKMSSigner accepted a SECP256K1 key")
	}
	if !strings.Contains(err.Error(), "not a supported manifest-signing algorithm") {
		t.Fatalf("refusal should name the unsupported algorithm; got %v", err)
	}
}

// TestNewGCPKMSSigner_RequiresVersion pins the loud refusal of a bare
// crypto-key resource — GCP signs with a specific CryptoKeyVersion, so a
// resource without `/cryptoKeyVersions/` is rejected before any RPC.
func TestNewGCPKMSSigner_RequiresVersion(t *testing.T) {
	fake := newFakeGCPKMSSign(t, kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256)
	_, err := NewGCPKMSSigner(context.Background(),
		"projects/p/locations/us/keyRings/r/cryptoKeys/k", WithGCPKMSSignClient(fake))
	if err == nil {
		t.Fatal("NewGCPKMSSigner accepted a bare crypto-key (no version)")
	}
	if !strings.Contains(err.Error(), "cryptoKeyVersion") {
		t.Fatalf("refusal should name the required CryptoKeyVersion; got %v", err)
	}
}

// TestGCPKMSSigner_SignatureCRCMismatch pins that a signature whose
// SignatureCrc32C does not match (a corrupted-in-transit signature) is
// refused loudly — the GCP-specific wire-integrity check, no panic.
func TestGCPKMSSigner_SignatureCRCMismatch(t *testing.T) {
	ctx := context.Background()
	fake := newFakeGCPKMSSign(t, kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256)
	fake.corruptSignatureCRC = true
	signer, err := NewGCPKMSSigner(ctx, versionedRef, WithGCPKMSSignClient(fake))
	if err != nil {
		t.Fatalf("NewGCPKMSSigner: %v", err)
	}
	if _, err := signer.Sign(ctx, []byte("payload")); err == nil {
		t.Fatal("Sign accepted a signature with a mismatched CRC32C")
	} else if !strings.Contains(err.Error(), "CRC32C mismatch") {
		t.Fatalf("refusal should name the CRC32C mismatch; got %v", err)
	}
}

// TestGCPKMSSigner_UnverifiedRequestCRC pins that a response reporting the
// request digest/data CRC was NOT verified (in-transit corruption of the
// signing request) is refused loudly.
func TestGCPKMSSigner_UnverifiedRequestCRC(t *testing.T) {
	ctx := context.Background()
	// Digest path (ECDSA).
	ecFake := newFakeGCPKMSSign(t, kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256)
	ecFake.unverifyRequestCRC = true
	ecSigner, _ := NewGCPKMSSigner(ctx, versionedRef, WithGCPKMSSignClient(ecFake))
	if _, err := ecSigner.Sign(ctx, []byte("p")); err == nil {
		t.Fatal("Sign accepted an unverified-digest-CRC response")
	} else if !strings.Contains(err.Error(), "digest CRC32C") {
		t.Fatalf("refusal should name the digest CRC32C; got %v", err)
	}
	// Data path (Ed25519).
	edFake := newFakeGCPKMSSign(t, kmspb.CryptoKeyVersion_EC_SIGN_ED25519)
	edFake.unverifyRequestCRC = true
	edSigner, _ := NewGCPKMSSigner(ctx, versionedRef, WithGCPKMSSignClient(edFake))
	if _, err := edSigner.Sign(ctx, []byte("p")); err == nil {
		t.Fatal("Sign accepted an unverified-data-CRC response")
	} else if !strings.Contains(err.Error(), "data CRC32C") {
		t.Fatalf("refusal should name the data CRC32C; got %v", err)
	}
}

// TestGCPKMSSigner_PublicKeyCRCMismatch pins that a GetPublicKey PEM whose
// PemCrc32C does not match (a corrupted-in-transit public key) is refused at
// construction.
func TestGCPKMSSigner_PublicKeyCRCMismatch(t *testing.T) {
	fake := newFakeGCPKMSSign(t, kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256)
	fake.corruptPemCRC = true
	_, err := NewGCPKMSSigner(context.Background(), versionedRef, WithGCPKMSSignClient(fake))
	if err == nil {
		t.Fatal("NewGCPKMSSigner accepted a PEM with a mismatched CRC32C")
	}
	if !strings.Contains(err.Error(), "PEM CRC32C mismatch") {
		t.Fatalf("refusal should name the PEM CRC32C mismatch; got %v", err)
	}
}

// TestGCPKMSSigner_EmptyResource pins the empty-resource guard.
func TestGCPKMSSigner_EmptyResource(t *testing.T) {
	if _, err := NewGCPKMSSigner(context.Background(), "  "); err == nil {
		t.Fatal("NewGCPKMSSigner accepted an empty resource")
	} else if !strings.Contains(err.Error(), "empty") {
		t.Fatalf("err should mention 'empty'; got %v", err)
	}
	if _, err := FetchGCPKMSPublicKey(context.Background(), ""); err == nil {
		t.Fatal("FetchGCPKMSPublicKey accepted an empty resource")
	}
}

// TestFetchGCPKMSPublicKey pins the online verify-key resolution: it returns
// the public half of the operator-named key (a real GetPublicKey), which
// matches the signer's public key across every family.
func TestFetchGCPKMSPublicKey(t *testing.T) {
	ctx := context.Background()
	for _, tc := range gcpSignCases {
		tc := tc
		t.Run(tc.algo, func(t *testing.T) {
			fake := newFakeGCPKMSSign(t, tc.alg)
			signer, err := NewGCPKMSSigner(ctx, versionedRef, WithGCPKMSSignClient(fake))
			if err != nil {
				t.Fatalf("NewGCPKMSSigner: %v", err)
			}
			pub, err := FetchGCPKMSPublicKey(ctx, versionedRef, WithGCPKMSSignClient(fake))
			if err != nil {
				t.Fatalf("FetchGCPKMSPublicKey: %v", err)
			}
			got, _ := x509.MarshalPKIXPublicKey(pub)
			want, _ := x509.MarshalPKIXPublicKey(signer.PublicKey())
			if !bytes.Equal(got, want) {
				t.Fatal("fetched public key differs from the signer's public key")
			}
		})
	}
}

// TestGCPKMSSigner_PreflightNotFound pins that a GetPublicKey NotFound
// surfaces at construction with an operator-actionable message.
func TestGCPKMSSigner_PreflightNotFound(t *testing.T) {
	fake := &notFoundGCPKMSSign{}
	_, err := NewGCPKMSSigner(context.Background(), versionedRef, WithGCPKMSSignClient(fake))
	if err == nil {
		t.Fatal("expected not-found to surface at preflight")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("error should mention 'not found'; got %v", err)
	}
}

// notFoundGCPKMSSign is a minimal stub whose GetPublicKey returns a gRPC
// NotFound — the preflight failure path.
type notFoundGCPKMSSign struct{}

func (notFoundGCPKMSSign) AsymmetricSign(context.Context, *kmspb.AsymmetricSignRequest, ...gax) (*kmspb.AsymmetricSignResponse, error) {
	return nil, status.Error(codes.NotFound, "unreached")
}

func (notFoundGCPKMSSign) GetPublicKey(context.Context, *kmspb.GetPublicKeyRequest, ...gax) (*kmspb.PublicKey, error) {
	return nil, status.Error(codes.NotFound, "key version not found")
}

func (notFoundGCPKMSSign) Close() error { return nil }

// TestTranslateGCPKMSSignError_PermissionHints pins the signing-specific IAM
// hints (signerVerifier / publicKeyViewer) and that the key + credentials
// hints appear — the parallel of the envelope translator's role test.
func TestTranslateGCPKMSSignError_PermissionHints(t *testing.T) {
	const ref = versionedRef
	perm := translateGCPKMSSignError(status.Error(codes.PermissionDenied, "no"), ref, "sign")
	if !strings.Contains(perm.Error(), "signerVerifier") || !strings.Contains(perm.Error(), ref) {
		t.Fatalf("permission-denied hint missing role/key; got %v", perm)
	}
	auth := translateGCPKMSSignError(status.Error(codes.Unauthenticated, "no"), ref, "get-public-key")
	if !strings.Contains(auth.Error(), "GOOGLE_APPLICATION_CREDENTIALS") {
		t.Fatalf("unauthenticated hint missing credentials guidance; got %v", auth)
	}
}
