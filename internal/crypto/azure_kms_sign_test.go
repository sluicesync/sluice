// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

import (
	"context"
	stdcrypto "crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"math/big"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
)

// fakeAzureSign is a FAITHFUL in-process Key Vault signing stub backed by
// a REAL ecdsa/rsa private key. Sign returns signatures in Azure's NATIVE
// wire format — raw r‖s (IEEE P-1363) for ECDSA, standard PKCS#1-PSS for
// RSA — so the adapter's r‖s→DER conversion (Divergence 1) is actually
// exercised, not stubbed around. GetKey returns the matching JWK
// (Divergence 2) so the adapter's JWK→stdlib conversion is exercised too.
// Mirrors newFakeAWSKMS's role for the AWS suite.
type fakeAzureSign struct {
	keyName string
	version string
	ecKey   *ecdsa.PrivateKey      // set for EC keys
	rsaKey  *rsa.PrivateKey        // set for RSA keys
	octKey  bool                   // set → GetKey returns an unsupported oct JWK
	keyOps  []*azkeys.KeyOperation // key_ops the JWK advertises (default: sign)
	errOnOp map[string]error

	signs int64
	gets  int64
}

func signOps() []*azkeys.KeyOperation {
	sign := azkeys.KeyOperationSign
	return []*azkeys.KeyOperation{&sign}
}

func newFakeAzureSignEC(t *testing.T, name string, curve elliptic.Curve) *fakeAzureSign {
	t.Helper()
	key, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey(%v): %v", curve.Params().Name, err)
	}
	return &fakeAzureSign{keyName: name, version: "v1", ecKey: key, keyOps: signOps(), errOnOp: map[string]error{}}
}

func newFakeAzureSignRSA(t *testing.T, name string, bits int) *fakeAzureSign {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		t.Fatalf("rsa.GenerateKey(%d): %v", bits, err)
	}
	return &fakeAzureSign{keyName: name, version: "v1", rsaKey: key, keyOps: signOps(), errOnOp: map[string]error{}}
}

func (f *fakeAzureSign) kid() azkeys.ID {
	return azkeys.ID("https://stub.vault.azure.net/keys/" + f.keyName + "/" + f.version)
}

func (f *fakeAzureSign) Sign(_ context.Context, name, _ string, p azkeys.SignParameters, _ *azkeys.SignOptions) (azkeys.SignResponse, error) {
	atomic.AddInt64(&f.signs, 1)
	if err := f.popErr("sign"); err != nil {
		return azkeys.SignResponse{}, err
	}
	if name != f.keyName {
		return azkeys.SignResponse{}, fakeAzureAPIError("KeyNotFound", 404, "key not found")
	}
	var result []byte
	switch {
	case f.ecKey != nil:
		r, s, err := ecdsa.Sign(rand.Reader, f.ecKey, p.Value)
		if err != nil {
			return azkeys.SignResponse{}, err
		}
		// Azure's native ECDSA form: raw r‖s, each fixed-width to the
		// curve's coordinate size (P-521 → 66 bytes, the odd-size trap).
		byteLen := (f.ecKey.Curve.Params().BitSize + 7) / 8
		result = append(bigIntToFixedBytes(r, byteLen), bigIntToFixedBytes(s, byteLen)...)
	case f.rsaKey != nil:
		sig, err := rsa.SignPSS(rand.Reader, f.rsaKey, stdcrypto.SHA256, p.Value,
			&rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: stdcrypto.SHA256})
		if err != nil {
			return azkeys.SignResponse{}, err
		}
		result = sig
	default:
		return azkeys.SignResponse{}, fakeAzureAPIError("BadParameter", 400, "key cannot sign")
	}
	kid := f.kid()
	return azkeys.SignResponse{
		KeyOperationResult: azkeys.KeyOperationResult{Result: result, KID: &kid},
	}, nil
}

func (f *fakeAzureSign) GetKey(_ context.Context, name, _ string, _ *azkeys.GetKeyOptions) (azkeys.GetKeyResponse, error) {
	atomic.AddInt64(&f.gets, 1)
	if err := f.popErr("get"); err != nil {
		return azkeys.GetKeyResponse{}, err
	}
	if name != f.keyName {
		return azkeys.GetKeyResponse{}, fakeAzureAPIError("KeyNotFound", 404, "key not found")
	}
	kid := f.kid()
	jwk := &azkeys.JSONWebKey{KID: &kid, KeyOps: f.keyOps}
	switch {
	case f.octKey:
		kty := azkeys.KeyTypeOct
		jwk.Kty = &kty
		jwk.K = []byte("0123456789abcdef0123456789abcdef")
	case f.ecKey != nil:
		kty := azkeys.KeyTypeEC
		crv := azureCurveName(f.ecKey.Curve)
		byteLen := (f.ecKey.Curve.Params().BitSize + 7) / 8
		jwk.Kty = &kty
		jwk.Crv = &crv
		jwk.X = bigIntToFixedBytes(f.ecKey.X, byteLen)
		jwk.Y = bigIntToFixedBytes(f.ecKey.Y, byteLen)
	case f.rsaKey != nil:
		kty := azkeys.KeyTypeRSA
		jwk.Kty = &kty
		jwk.N = f.rsaKey.N.Bytes()
		jwk.E = big.NewInt(int64(f.rsaKey.E)).Bytes()
	}
	enabled := true
	return azkeys.GetKeyResponse{
		KeyBundle: azkeys.KeyBundle{
			Key:        jwk,
			Attributes: &azkeys.KeyAttributes{Enabled: &enabled},
		},
	}, nil
}

func (f *fakeAzureSign) popErr(op string) error {
	err, ok := f.errOnOp[op]
	if !ok {
		return nil
	}
	delete(f.errOnOp, op)
	return err
}

// bigIntToFixedBytes left-pads v's big-endian bytes to exactly size bytes
// (the fixed-width form JWK coordinates + IEEE-P1363 signature halves use).
func bigIntToFixedBytes(v *big.Int, size int) []byte {
	b := v.Bytes()
	if len(b) >= size {
		return b
	}
	out := make([]byte, size)
	copy(out[size-len(b):], b)
	return out
}

func azureCurveName(c elliptic.Curve) azkeys.CurveName {
	switch c {
	case elliptic.P256():
		return azkeys.CurveNameP256
	case elliptic.P384():
		return azkeys.CurveNameP384
	case elliptic.P521():
		return azkeys.CurveNameP521
	default:
		return ""
	}
}

// ecCurveCases is the per-curve matrix — every ECDSA family × curve, NOT
// one representative (the Bug-74 discipline). P-521's 66-byte odd-size
// half is the specific trap the r‖s→DER conversion must get right.
var ecCurveCases = []struct {
	name     string
	curve    elliptic.Curve
	wantAlg  string
	azureAlg azkeys.SignatureAlgorithm
}{
	{"P-256", elliptic.P256(), KMSAlgorithmECDSAP256, azkeys.SignatureAlgorithmES256},
	{"P-384", elliptic.P384(), KMSAlgorithmECDSAP384, azkeys.SignatureAlgorithmES384},
	{"P-521", elliptic.P521(), KMSAlgorithmECDSAP521, azkeys.SignatureAlgorithmES512},
}

func TestAzureKMSSigner_RoundTrip_AllCurvesAndRSA(t *testing.T) {
	const keyID = "https://test-vault.vault.azure.net/keys/sign-key"

	run := func(t *testing.T, stub *fakeAzureSign, wantAlg string, pub stdcrypto.PublicKey) {
		t.Helper()
		signer, err := NewAzureKMSSigner(context.Background(), keyID, WithAzureKMSSignClient(stub))
		if err != nil {
			t.Fatalf("NewAzureKMSSigner: %v", err)
		}
		if atomic.LoadInt64(&stub.gets) != 1 {
			t.Errorf("Gets = %d; want 1 (preflight)", stub.gets)
		}
		if signer.Algorithm() != wantAlg {
			t.Errorf("Algorithm() = %q; want %q", signer.Algorithm(), wantAlg)
		}
		if signer.KeyRef() != "https://stub.vault.azure.net/keys/sign-key/v1" {
			t.Errorf("KeyRef() = %q; want the resolved versioned KID", signer.KeyRef())
		}
		// KeyID is derived from the PUBLIC key — it must match what an
		// offline verifier computes from the same exported key.
		wantID, err := KMSManifestKeyID(pub)
		if err != nil {
			t.Fatalf("KMSManifestKeyID(orig): %v", err)
		}
		if signer.KeyID() != wantID {
			t.Errorf("KeyID() = %q; want %q (public-key fingerprint mismatch)", signer.KeyID(), wantID)
		}

		payload := []byte("canonical manifest payload — " + wantAlg)
		sig, err := signer.Sign(context.Background(), payload)
		if err != nil {
			t.Fatalf("Sign: %v", err)
		}
		if !VerifyManifestKMS(signer.PublicKey(), signer.Algorithm(), payload, sig) {
			t.Fatalf("VerifyManifestKMS rejected a freshly produced %s signature", wantAlg)
		}
		// Tamper → fail-closed.
		if VerifyManifestKMS(signer.PublicKey(), signer.Algorithm(), append(payload, '!'), sig) {
			t.Fatalf("VerifyManifestKMS accepted a %s signature over a TAMPERED payload", wantAlg)
		}
	}

	for _, c := range ecCurveCases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			stub := newFakeAzureSignEC(t, "sign-key", c.curve)
			run(t, stub, c.wantAlg, &stub.ecKey.PublicKey)
		})
	}
	t.Run("RSA-PSS-2048", func(t *testing.T) {
		stub := newFakeAzureSignRSA(t, "sign-key", 2048)
		run(t, stub, KMSAlgorithmRSAPSS256, &stub.rsaKey.PublicKey)
	})
}

// TestAzureKMSSigner_ECDSA_RawConcat_ConvertedToDER is the Azure twin of
// TestAWSKMSSigner_ECDSA_IsDER_NotRawConcat. It pins, across ALL THREE
// curves (incl. P-521's odd 66-byte half), that:
//   - Azure's native raw r‖s form of a signature does NOT verify via
//     VerifyManifestKMS (which is DER-only), and
//   - the r‖s→DER-converted form of the SAME signature DOES verify.
//
// A DER-vs-raw confusion is thus caught, not silently accepted.
func TestAzureKMSSigner_ECDSA_RawConcat_ConvertedToDER(t *testing.T) {
	const keyID = "https://test-vault.vault.azure.net/keys/sign-key"
	payload := []byte("payload for the raw-vs-DER pin")

	for _, c := range ecCurveCases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			stub := newFakeAzureSignEC(t, "sign-key", c.curve)
			pub := &stub.ecKey.PublicKey
			bitSize := c.curve.Params().BitSize
			byteLen := (bitSize + 7) / 8

			// Produce the signature directly through the fake to get the
			// RAW r‖s bytes (the same signature we then convert to DER).
			digest := DigestForKMSAlgorithm(c.wantAlg, payload)
			if digest == nil {
				t.Fatalf("DigestForKMSAlgorithm(%s) = nil", c.wantAlg)
			}
			alg := c.azureAlg
			resp, err := stub.Sign(context.Background(), "sign-key", "v1",
				azkeys.SignParameters{Algorithm: &alg, Value: digest}, nil)
			if err != nil {
				t.Fatalf("fake Sign: %v", err)
			}
			raw := resp.Result
			if len(raw) != 2*byteLen {
				t.Fatalf("%s: raw signature is %d bytes; want %d (2×%d)", c.name, len(raw), 2*byteLen, byteLen)
			}
			// P-521 specifically: the half must be the ODD 66 bytes.
			if c.name == "P-521" && byteLen != 66 {
				t.Fatalf("P-521 half width = %d; want 66 (the odd-size trap)", byteLen)
			}

			// The raw form must NOT verify (verifier is DER-only).
			if VerifyManifestKMS(pub, c.wantAlg, payload, raw) {
				t.Fatalf("%s: raw r‖s signature verified via VerifyManifestKMS — DER-vs-raw confusion NOT caught", c.name)
			}
			// The converted DER form MUST verify.
			der, err := azureECDSARawToDER(raw, bitSize)
			if err != nil {
				t.Fatalf("%s: azureECDSARawToDER: %v", c.name, err)
			}
			if !VerifyManifestKMS(pub, c.wantAlg, payload, der) {
				t.Fatalf("%s: r‖s→DER-converted signature failed to verify", c.name)
			}

			// End-to-end: the adapter's Sign() must already return the DER
			// form (so the whole path is exercised, not just the helper).
			signer, err := NewAzureKMSSigner(context.Background(), keyID, WithAzureKMSSignClient(stub))
			if err != nil {
				t.Fatalf("NewAzureKMSSigner: %v", err)
			}
			out, err := signer.Sign(context.Background(), payload)
			if err != nil {
				t.Fatalf("signer.Sign: %v", err)
			}
			if len(out) == 2*byteLen {
				t.Fatalf("%s: signer.Sign returned a %d-byte value — looks like un-converted raw r‖s", c.name, len(out))
			}
			if !VerifyManifestKMS(signer.PublicKey(), signer.Algorithm(), payload, out) {
				t.Fatalf("%s: signer.Sign output failed to verify", c.name)
			}
		})
	}
}

// TestAzureECDSARawToDER_RejectsWrongLength pins the loud-failure edge: a
// raw signature that is not exactly two curve-halves is refused, never
// mis-split. Covers P-521's 66-byte half explicitly.
func TestAzureECDSARawToDER_RejectsWrongLength(t *testing.T) {
	// A correctly sized P-521 raw signature (132 bytes) converts cleanly.
	if _, err := azureECDSARawToDER(make([]byte, 132), 521); err != nil {
		t.Errorf("P-521 132-byte raw sig: unexpected error %v", err)
	}
	for _, n := range []int{0, 63, 64, 65, 131, 133} {
		if _, err := azureECDSARawToDER(make([]byte, n), 521); err == nil {
			t.Errorf("P-521 raw sig of %d bytes: want length-refusal error, got nil", n)
		}
	}
	// P-256 wants 64 bytes; 66 (a P-521-sized sig) must be refused.
	if _, err := azureECDSARawToDER(make([]byte, 66), 256); err == nil {
		t.Error("P-256 with a 66-byte raw sig: want refusal, got nil")
	}
}

// TestAzureKMSSigner_JWKPublicKeyRoundTrip pins Divergence 2: the JWK the
// adapter parses from GetKey reconstructs the ORIGINAL public key exactly
// (same fingerprint) for every EC curve + RSA.
func TestAzureKMSSigner_JWKPublicKeyRoundTrip(t *testing.T) {
	const keyID = "https://test-vault.vault.azure.net/keys/sign-key"

	check := func(t *testing.T, stub *fakeAzureSign, orig stdcrypto.PublicKey) {
		t.Helper()
		signer, err := NewAzureKMSSigner(context.Background(), keyID, WithAzureKMSSignClient(stub))
		if err != nil {
			t.Fatalf("NewAzureKMSSigner: %v", err)
		}
		wantID, err := KMSManifestKeyID(orig)
		if err != nil {
			t.Fatalf("KMSManifestKeyID(orig): %v", err)
		}
		gotID, err := KMSManifestKeyID(signer.PublicKey())
		if err != nil {
			t.Fatalf("KMSManifestKeyID(parsed): %v", err)
		}
		if gotID != wantID {
			t.Errorf("JWK-parsed public key fingerprint = %q; want %q", gotID, wantID)
		}
	}

	for _, c := range ecCurveCases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			stub := newFakeAzureSignEC(t, "sign-key", c.curve)
			check(t, stub, &stub.ecKey.PublicKey)
		})
	}
	t.Run("RSA-2048", func(t *testing.T) {
		stub := newFakeAzureSignRSA(t, "sign-key", 2048)
		check(t, stub, &stub.rsaKey.PublicKey)
	})
}

func TestNewAzureKMSSigner_EmptyKeyID(t *testing.T) {
	_, err := NewAzureKMSSigner(context.Background(), "")
	if err == nil {
		t.Fatal("err = nil; want error for empty key ID")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("err = %v; want \"empty\" hint", err)
	}
}

func TestNewAzureKMSSigner_RefusesUnsupportedKeyType(t *testing.T) {
	stub := &fakeAzureSign{keyName: "oct-key", version: "v1", octKey: true, keyOps: signOps(), errOnOp: map[string]error{}}
	_, err := NewAzureKMSSigner(context.Background(),
		"https://v.vault.azure.net/keys/oct-key", WithAzureKMSSignClient(stub))
	if err == nil {
		t.Fatal("err = nil; want refusal of an oct (symmetric) key")
	}
	if !strings.Contains(err.Error(), "not a supported manifest-signing key") {
		t.Errorf("err = %v; want unsupported-key-type refusal", err)
	}
}

func TestNewAzureKMSSigner_RefusesNonSignKeyOp(t *testing.T) {
	stub := newFakeAzureSignEC(t, "enc-key", elliptic.P256())
	// A key advertising only encrypt/wrapKey ops — not sign.
	enc, wrap := azkeys.KeyOperationEncrypt, azkeys.KeyOperationWrapKey
	stub.keyOps = []*azkeys.KeyOperation{&enc, &wrap}
	_, err := NewAzureKMSSigner(context.Background(),
		"https://v.vault.azure.net/keys/enc-key", WithAzureKMSSignClient(stub))
	if err == nil {
		t.Fatal("err = nil; want refusal of a key whose key_ops omits sign")
	}
	if !strings.Contains(err.Error(), "sign") {
		t.Errorf("err = %v; want a sign-key-op refusal", err)
	}
}

func TestNewAzureKMSSigner_PreflightNotFound(t *testing.T) {
	stub := newFakeAzureSignEC(t, "sign-key", elliptic.P256())
	stub.errOnOp["get"] = fakeAzureAPIError("KeyNotFound", 404, "missing")
	_, err := NewAzureKMSSigner(context.Background(),
		"https://v.vault.azure.net/keys/sign-key", WithAzureKMSSignClient(stub))
	if err == nil {
		t.Fatal("err = nil; want preflight failure")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("err = %v; want \"not found\" hint", err)
	}
}

// TestVerifyAzureManifestKMS_WrongKeyAndType exercises the fail-closed
// paths of the pure verifier against Azure-produced signatures: a
// different key of the same curve, and a key of the wrong TYPE for the
// algorithm — neither must verify, and neither must panic.
func TestVerifyAzureManifestKMS_WrongKeyAndType(t *testing.T) {
	const keyID = "https://test-vault.vault.azure.net/keys/sign-key"
	payload := []byte("payload under test")

	stubA := newFakeAzureSignEC(t, "sign-key", elliptic.P256())
	signerA, err := NewAzureKMSSigner(context.Background(), keyID, WithAzureKMSSignClient(stubA))
	if err != nil {
		t.Fatalf("NewAzureKMSSigner(A): %v", err)
	}
	sigA, err := signerA.Sign(context.Background(), payload)
	if err != nil {
		t.Fatalf("Sign(A): %v", err)
	}

	// Wrong key, same curve/algorithm → false.
	stubB := newFakeAzureSignEC(t, "sign-key", elliptic.P256())
	if VerifyManifestKMS(&stubB.ecKey.PublicKey, KMSAlgorithmECDSAP256, payload, sigA) {
		t.Error("VerifyManifestKMS accepted signature A under a DIFFERENT key")
	}

	// Wrong key TYPE for the algorithm: an RSA key presented for an
	// ecdsa-* algorithm → false (not a panic).
	rsaStub := newFakeAzureSignRSA(t, "sign-key", 2048)
	if VerifyManifestKMS(&rsaStub.rsaKey.PublicKey, KMSAlgorithmECDSAP256, payload, sigA) {
		t.Error("VerifyManifestKMS accepted an ecdsa signature under an RSA key")
	}
	// And the mirror: an ECDSA key for an rsa-pss-* algorithm → false.
	if VerifyManifestKMS(&stubA.ecKey.PublicKey, KMSAlgorithmRSAPSS256, payload, sigA) {
		t.Error("VerifyManifestKMS accepted an rsa-pss algorithm under an ECDSA key")
	}
}

func TestFetchAzureKMSPublicKey(t *testing.T) {
	const keyID = "https://test-vault.vault.azure.net/keys/sign-key"

	t.Run("EC", func(t *testing.T) {
		stub := newFakeAzureSignEC(t, "sign-key", elliptic.P384())
		pub, err := FetchAzureKMSPublicKey(context.Background(), keyID, WithAzureKMSSignClient(stub))
		if err != nil {
			t.Fatalf("FetchAzureKMSPublicKey: %v", err)
		}
		if _, ok := pub.(*ecdsa.PublicKey); !ok {
			t.Fatalf("pub is %T; want *ecdsa.PublicKey", pub)
		}
		wantID, _ := KMSManifestKeyID(&stub.ecKey.PublicKey)
		gotID, _ := KMSManifestKeyID(pub)
		if gotID != wantID {
			t.Errorf("fetched key fingerprint = %q; want %q", gotID, wantID)
		}
	})

	t.Run("empty key ID", func(t *testing.T) {
		if _, err := FetchAzureKMSPublicKey(context.Background(), ""); err == nil {
			t.Fatal("err = nil; want empty-key-ID error")
		}
	})

	t.Run("refuses non-sign key", func(t *testing.T) {
		stub := newFakeAzureSignEC(t, "sign-key", elliptic.P256())
		verify := azkeys.KeyOperationVerify
		stub.keyOps = []*azkeys.KeyOperation{&verify}
		if _, err := FetchAzureKMSPublicKey(context.Background(), keyID, WithAzureKMSSignClient(stub)); err == nil {
			t.Fatal("err = nil; want refusal of a verify-only key on the fetch path")
		}
	})
}
