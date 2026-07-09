// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

import (
	"bytes"
	"context"
	stdcrypto "crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"math/big"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
)

// fakeAWSKMS is a FAITHFUL in-process stand-in for AWS KMS asymmetric
// signing: it holds a real private key, GetPublicKey returns the SPKI DER
// (exactly AWS's encoding), and Sign returns the SAME signature encoding
// AWS returns — ASN.1 DER for ECDSA (RFC 3279), PSS for RSA. This is what
// makes the round-trip test meaningful: if sluice's verifier assumed the
// raw r‖s form, verification against this faithful DER signer would fail.
type fakeAWSKMS struct {
	keySpec   kmstypes.KeySpec
	usage     kmstypes.KeyUsageType
	ecPriv    *ecdsa.PrivateKey
	rsaPriv   *rsa.PrivateKey
	signCalls int
}

func newFakeAWSKMS(t *testing.T, spec kmstypes.KeySpec) *fakeAWSKMS {
	t.Helper()
	f := &fakeAWSKMS{keySpec: spec, usage: kmstypes.KeyUsageTypeSignVerify}
	var err error
	switch spec {
	case kmstypes.KeySpecEccNistP256:
		f.ecPriv, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case kmstypes.KeySpecEccNistP384:
		f.ecPriv, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	case kmstypes.KeySpecEccNistP521:
		f.ecPriv, err = ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	case kmstypes.KeySpecRsa2048:
		f.rsaPriv, err = rsa.GenerateKey(rand.Reader, 2048)
	default:
		t.Fatalf("unsupported fake key spec %q", spec)
	}
	if err != nil {
		t.Fatalf("generate fake key (%s): %v", spec, err)
	}
	return f
}

func (f *fakeAWSKMS) publicKey() stdcrypto.PublicKey {
	if f.ecPriv != nil {
		return &f.ecPriv.PublicKey
	}
	return &f.rsaPriv.PublicKey
}

func (f *fakeAWSKMS) GetPublicKey(_ context.Context, _ *kms.GetPublicKeyInput, _ ...func(*kms.Options)) (*kms.GetPublicKeyOutput, error) {
	der, err := x509.MarshalPKIXPublicKey(f.publicKey())
	if err != nil {
		return nil, err
	}
	return &kms.GetPublicKeyOutput{
		PublicKey: der,
		KeySpec:   f.keySpec,
		KeyUsage:  f.usage,
	}, nil
}

func (f *fakeAWSKMS) Sign(_ context.Context, in *kms.SignInput, _ ...func(*kms.Options)) (*kms.SignOutput, error) {
	f.signCalls++
	if in.MessageType != kmstypes.MessageTypeDigest {
		return nil, &kmstypes.KMSInvalidStateException{Message: aws.String("fake: expected MessageType=DIGEST")}
	}
	var sig []byte
	var err error
	switch in.SigningAlgorithm {
	case kmstypes.SigningAlgorithmSpecEcdsaSha256,
		kmstypes.SigningAlgorithmSpecEcdsaSha384,
		kmstypes.SigningAlgorithmSpecEcdsaSha512:
		// AWS returns ASN.1 DER for ECDSA — SignASN1 produces exactly that.
		sig, err = ecdsa.SignASN1(rand.Reader, f.ecPriv, in.Message)
	case kmstypes.SigningAlgorithmSpecRsassaPssSha256:
		sig, err = rsa.SignPSS(rand.Reader, f.rsaPriv, stdcrypto.SHA256, in.Message,
			&rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
	default:
		return nil, &kmstypes.KMSInvalidStateException{Message: aws.String("fake: unsupported SigningAlgorithm " + string(in.SigningAlgorithm))}
	}
	if err != nil {
		return nil, err
	}
	return &kms.SignOutput{Signature: sig, SigningAlgorithm: in.SigningAlgorithm}, nil
}

// kmsSignCase pins one KeySpec → expected sluice algorithm.
type kmsSignCase struct {
	spec kmstypes.KeySpec
	algo string
}

var kmsSignCases = []kmsSignCase{
	{kmstypes.KeySpecEccNistP256, KMSAlgorithmECDSAP256},
	{kmstypes.KeySpecEccNistP384, KMSAlgorithmECDSAP384},
	{kmstypes.KeySpecEccNistP521, KMSAlgorithmECDSAP521},
	{kmstypes.KeySpecRsa2048, KMSAlgorithmRSAPSS256},
}

// TestAWSKMSSigner_RoundTrip is the PIN-THE-CLASS matrix: EVERY supported
// KeySpec/algorithm family (ECDSA P-256/384/521 + RSA-PSS-256) signs via
// the faithful fake and verifies through the PURE local verifier. A green
// on one family does not cover the others (different curve/OID paths), so
// all four are exercised.
func TestAWSKMSSigner_RoundTrip(t *testing.T) {
	ctx := context.Background()
	payload := []byte("canonical-manifest-bytes-under-test")
	for _, tc := range kmsSignCases {
		t.Run(tc.algo, func(t *testing.T) {
			fake := newFakeAWSKMS(t, tc.spec)
			signer, err := NewAWSKMSSigner(ctx, "arn:aws:kms:us-east-1:1:key/test", WithAWSKMSSignClient(fake))
			if err != nil {
				t.Fatalf("NewAWSKMSSigner: %v", err)
			}
			if signer.Algorithm() != tc.algo {
				t.Fatalf("algorithm: got %q want %q", signer.Algorithm(), tc.algo)
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
		})
	}
}

// TestAWSKMSSigner_ECDSA_IsDER_NotRawConcat is the load-bearing
// encoding pin (the adversarial-review target): AWS returns ECDSA
// signatures as ASN.1 DER, and the verifier MUST use ecdsa.VerifyASN1. It
// asserts the signature decodes as DER {R,S}, and that the equivalent RAW
// r‖s (IEEE-P1363) form of the SAME signature does NOT verify — so a
// DER-vs-raw confusion in the verifier is caught, not silently accepted.
func TestAWSKMSSigner_ECDSA_IsDER_NotRawConcat(t *testing.T) {
	ctx := context.Background()
	payload := []byte("der-encoding-pin")
	fake := newFakeAWSKMS(t, kmstypes.KeySpecEccNistP256)
	signer, err := NewAWSKMSSigner(ctx, "arn", WithAWSKMSSignClient(fake))
	if err != nil {
		t.Fatalf("NewAWSKMSSigner: %v", err)
	}
	sig, err := signer.Sign(ctx, payload)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	// (1) The signature is valid ASN.1 DER {R, S}.
	var parsed struct{ R, S *big.Int }
	rest, err := asn1.Unmarshal(sig, &parsed)
	if err != nil || len(rest) != 0 {
		t.Fatalf("AWS ECDSA signature is not clean ASN.1 DER (err=%v, trailing=%d) — encoding contract broken", err, len(rest))
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

// TestVerifyManifestKMS_WrongKeyAndType pins fail-closed behavior for a
// wrong key and a wrong key TYPE (an RSA key presented for an ecdsa-*
// algorithm, and vice versa) — the type assertion must refuse, never panic.
func TestVerifyManifestKMS_WrongKeyAndType(t *testing.T) {
	ctx := context.Background()
	payload := []byte("wrong-key")

	ecFake := newFakeAWSKMS(t, kmstypes.KeySpecEccNistP256)
	ecSigner, _ := NewAWSKMSSigner(ctx, "arn", WithAWSKMSSignClient(ecFake))
	ecSig, _ := ecSigner.Sign(ctx, payload)

	// Wrong key of the right type.
	other := newFakeAWSKMS(t, kmstypes.KeySpecEccNistP256)
	if VerifyManifestKMS(other.publicKey(), ecSigner.Algorithm(), payload, ecSig) {
		t.Fatal("verified an ECDSA signature under a different ECDSA key")
	}

	// Wrong key TYPE: verify the ECDSA sig under an rsa-pss algorithm with an
	// RSA key → refuse (no panic).
	rsaFake := newFakeAWSKMS(t, kmstypes.KeySpecRsa2048)
	if VerifyManifestKMS(rsaFake.publicKey(), KMSAlgorithmRSAPSS256, payload, ecSig) {
		t.Fatal("verified an ECDSA signature under an RSA key/algorithm")
	}
	// And an RSA key presented for an ecdsa-* algorithm → refuse.
	if VerifyManifestKMS(rsaFake.publicKey(), KMSAlgorithmECDSAP256, payload, ecSig) {
		t.Fatal("verified with an RSA key under an ecdsa-* algorithm")
	}
	// Unknown algorithm → refuse.
	if VerifyManifestKMS(ecSigner.PublicKey(), "ecdsa-p999", payload, ecSig) {
		t.Fatal("verified under an unknown algorithm")
	}
}

// TestNewAWSKMSSigner_RefusesEncryptionKey pins that a non-SIGN_VERIFY key
// (an ENCRYPT_DECRYPT key mistakenly passed as a signing key) is refused
// loudly at construction, not silently used.
func TestNewAWSKMSSigner_RefusesEncryptionKey(t *testing.T) {
	fake := newFakeAWSKMS(t, kmstypes.KeySpecEccNistP256)
	fake.usage = kmstypes.KeyUsageTypeEncryptDecrypt
	_, err := NewAWSKMSSigner(context.Background(), "arn", WithAWSKMSSignClient(fake))
	if err == nil {
		t.Fatal("NewAWSKMSSigner accepted an ENCRYPT_DECRYPT key")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("SIGN_VERIFY")) {
		t.Fatalf("refusal should name SIGN_VERIFY; got %v", err)
	}
}

// TestParseManifestPublicKeyPEM_AllTypes pins the general offline-verify
// parser across every supported key family (Ed25519 / ECDSA / RSA) — the
// `--verify-key <pem>` path. Pin-the-class: one family green does not
// cover the others.
func TestParseManifestPublicKeyPEM_AllTypes(t *testing.T) {
	edPub, _, err := GenerateEd25519Keypair()
	if err != nil {
		t.Fatalf("GenerateEd25519Keypair: %v", err)
	}
	ecPriv, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	rsaPriv, _ := rsa.GenerateKey(rand.Reader, 2048)

	cases := map[string]stdcrypto.PublicKey{
		"ed25519": edPub,
		"ecdsa":   &ecPriv.PublicKey,
		"rsa":     &rsaPriv.PublicKey,
	}
	for name, pub := range cases {
		t.Run(name, func(t *testing.T) {
			der, err := x509.MarshalPKIXPublicKey(pub)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			pemBytes := pemEncode(t, der)
			got, err := ParseManifestPublicKeyPEM(pemBytes)
			if err != nil {
				t.Fatalf("ParseManifestPublicKeyPEM: %v", err)
			}
			// Round-trip the parsed key back to DER and compare — proves the
			// exact key came through.
			gotDER, err := x509.MarshalPKIXPublicKey(got)
			if err != nil {
				t.Fatalf("re-marshal: %v", err)
			}
			if !bytes.Equal(der, gotDER) {
				t.Fatalf("%s: parsed key differs from original", name)
			}
		})
	}

	// A non-key PEM refuses.
	if _, err := ParseManifestPublicKeyPEM([]byte("not a pem")); err == nil {
		t.Fatal("ParseManifestPublicKeyPEM accepted junk")
	}
}

// TestFetchAWSKMSPublicKey pins the online verify-key resolution: it
// returns the public half of the operator-named key (a real GetPublicKey),
// which matches the signer's public key.
func TestFetchAWSKMSPublicKey(t *testing.T) {
	ctx := context.Background()
	fake := newFakeAWSKMS(t, kmstypes.KeySpecEccNistP256)
	signer, err := NewAWSKMSSigner(ctx, "arn", WithAWSKMSSignClient(fake))
	if err != nil {
		t.Fatalf("NewAWSKMSSigner: %v", err)
	}
	pub, err := FetchAWSKMSPublicKey(ctx, "arn", WithAWSKMSSignClient(fake))
	if err != nil {
		t.Fatalf("FetchAWSKMSPublicKey: %v", err)
	}
	got, _ := x509.MarshalPKIXPublicKey(pub)
	want, _ := x509.MarshalPKIXPublicKey(signer.PublicKey())
	if !bytes.Equal(got, want) {
		t.Fatal("fetched public key differs from the signer's public key")
	}
}

func pemEncode(t *testing.T, der []byte) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}
