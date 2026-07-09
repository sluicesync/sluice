// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

// Azure Key Vault asymmetric-signing adapter (ADR-0154 Phase 3b, Azure
// half). Implements the provider-neutral [KMSSigner] seam against a Key
// Vault signing key: the private key never leaves the vault; sluice hands
// Key Vault the manifest digest and gets back a signature. Verification is
// PURE and local (the exported public key + [VerifyManifestKMS]) — this
// adapter is only the SIGN side plus the one-time public-key fetch.
//
// Mirrors the AWS Phase-3a adapter (`aws_kms_sign.go`) intentionally, and
// reuses the encryption-side Azure conventions (`azure_kms.go`): the
// `azkeys.Client` + `azcore.TokenCredential` auth flow, [parseAzureKeyID],
// and [translateAzureKMSError]'s rich error-shape mapping. Key Vault's
// Sign/Verify live directly on `azkeys.Client` in the vendored SDK
// (v1.5.0) — there is no separate CryptographyClient — so the sign client
// is the same client type the envelope adapter uses; [AzureKMSSignAPI] is
// just the narrow Sign+GetKey surface this adapter needs.
//
// Azure carries TWO real format divergences from AWS/GCP that this adapter
// normalizes so the single-form pure verifier stays untouched:
//
//   - ★ Divergence 1 — signature encoding. Azure returns ECDSA signatures
//     as raw r‖s (IEEE P-1363: two fixed-width big-endian integers
//     concatenated), NOT the ASN.1 DER that [VerifyManifestKMS]'s
//     [ecdsa.VerifyASN1] expects. Sign converts r‖s → DER before
//     returning. The half-width is per-curve — P-521 is the trap (521
//     bits → 66-byte halves, an ODD size), so the round-trip pin covers
//     P-256/P-384/P-521, not one representative. RSA-PSS signatures come
//     back in the standard form the verifier already accepts (no convert).
//   - ★ Divergence 2 — public-key export. Azure GetKey returns a JWK
//     (`azkeys.JSONWebKey`: Kty/Crv/X/Y for EC, N/E for RSA — all
//     base64url `[]byte` the SDK has already decoded), not SPKI DER.
//     [azureSignerParamsFromJWK] rebuilds a stdlib `*ecdsa.PublicKey` /
//     `*rsa.PublicKey` from it, which [KMSManifestKeyID] then fingerprints
//     identically to an offline verifier's exported PEM.
//
// Azure Key Vault has NO Ed25519 signing key type — an unsupported key
// type is a loud refusal.
//
// Real-cloud validation is DEFERRED (the N-9 pattern): there is no
// first-party local Key Vault emulator, so this adapter is pinned against
// a faithful in-process fake (see azure_kms_sign_test.go). A real-Azure
// pin (a live Key Vault signing key across P-256/384/521 + RSA) is a
// follow-up.

import (
	"context"
	stdcrypto "crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/asn1"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
)

// AzureKMSSignAPI is the narrow surface [AzureKMSSigner] needs from
// [azkeys.Client] for asymmetric signing — Sign + GetKey (the latter both
// for the construction preflight and the public-key / key-id derivation).
// Declared as an interface so tests can stub it with a faithful in-process
// fake. `*azkeys.Client` satisfies it in production.
type AzureKMSSignAPI interface {
	Sign(ctx context.Context, name, version string, parameters azkeys.SignParameters, options *azkeys.SignOptions) (azkeys.SignResponse, error)
	GetKey(ctx context.Context, name, version string, options *azkeys.GetKeyOptions) (azkeys.GetKeyResponse, error)
}

// AzureKMSSignerOption configures [NewAzureKMSSigner] /
// [FetchAzureKMSPublicKey].
type AzureKMSSignerOption func(*azureKMSSignerOptions)

type azureKMSSignerOptions struct {
	client        AzureKMSSignAPI
	cred          azcore.TokenCredential
	clientOptions *azkeys.ClientOptions
}

// WithAzureKMSSignClient injects a pre-built client (or a stub satisfying
// [AzureKMSSignAPI]) — the test seam for a faithful fake Key Vault Sign.
func WithAzureKMSSignClient(client AzureKMSSignAPI) AzureKMSSignerOption {
	return func(o *azureKMSSignerOptions) { o.client = client }
}

// WithAzureKMSSignCredential overrides the token credential used to build
// the production client. Defaults to
// [azidentity.NewDefaultAzureCredential] (env vars, managed identity,
// Azure CLI cached login, etc.). Mirrors [WithAzureKMSCredential].
func WithAzureKMSSignCredential(cred azcore.TokenCredential) AzureKMSSignerOption {
	return func(o *azureKMSSignerOptions) { o.cred = cred }
}

// WithAzureKMSSignClientOptions sets the SDK's client options (custom
// retry policy, transport, etc.). Ignored when [WithAzureKMSSignClient] is
// also supplied.
func WithAzureKMSSignClientOptions(opts *azkeys.ClientOptions) AzureKMSSignerOption {
	return func(o *azureKMSSignerOptions) { o.clientOptions = opts }
}

// AzureKMSSigner is the Azure implementation of [KMSSigner]. It resolves
// the public key + algorithm once at construction (GetKey → JWK), so Sign
// is the only per-call Key Vault roundtrip and PublicKey/KeyID/Algorithm
// are IO-free.
type AzureKMSSigner struct {
	client     AzureKMSSignAPI
	keyRef     string // versioned key URL resolved from GetKey's KID (advisory)
	keyName    string // parsed from the operator's key URL
	keyVersion string // pinned concrete version (from GetKey's KID) — Sign targets it
	algorithm  string // sluice-canonical, e.g. "ecdsa-p256"
	azureAlg   azkeys.SignatureAlgorithm
	pub        stdcrypto.PublicKey
	keyID      string
}

// NewAzureKMSSigner constructs a signer against an Azure Key Vault signing
// key. keyID is a Key Vault key identifier URL
// (`https://VAULT.vault.azure.net/keys/KEY[/VERSION]`, or the
// `managedhsm.azure.net` host for Managed HSM). It preflights GetKey to
// (a) confirm the key exists + is usable, (b) fetch the JWK public key for
// local verification / key-id derivation, and (c) fix the signing
// algorithm from the key's Kty+Crv — so a mid-backup Sign never fails for
// a key that was wrong all along. Refuses a key whose key_ops omits "sign"
// (an encryption/wrap-only key) and an unsupported key type (Azure has no
// Ed25519) loudly.
//
// The concrete key VERSION is pinned from GetKey's returned KID (as the
// envelope adapter does for audit N-9), so every Sign this run performs
// lands on ONE version even if the vault key auto-rotates mid-run.
func NewAzureKMSSigner(ctx context.Context, keyID string, opts ...AzureKMSSignerOption) (*AzureKMSSigner, error) {
	if strings.TrimSpace(keyID) == "" {
		return nil, errors.New("crypto: Azure Key Vault signing key ID is empty")
	}
	o := &azureKMSSignerOptions{}
	for _, opt := range opts {
		opt(o)
	}
	vaultURL, name, version, err := parseAzureKeyID(keyID)
	if err != nil {
		return nil, err
	}
	client, err := o.buildSignClient(vaultURL)
	if err != nil {
		return nil, err
	}

	jwk, err := getAzureKMSSignKey(ctx, client, name, version, keyID)
	if err != nil {
		return nil, err
	}
	if err := requireAzureSignKeyOp(jwk, keyID); err != nil {
		return nil, err
	}
	pub, algorithm, azureAlg, err := azureSignerParamsFromJWK(jwk)
	if err != nil {
		return nil, fmt.Errorf("crypto: azure kms signing key %q: %w", keyID, err)
	}
	kid, err := KMSManifestKeyID(pub)
	if err != nil {
		return nil, err
	}

	// Pin the concrete key version + advisory versioned ref from the
	// resolved KID. An unversioned operator URL resolves "latest" to an
	// exact version here; a versioned URL echoes back unchanged.
	keyVersion, keyRef := version, keyID
	if jwk.KID != nil {
		if v := jwk.KID.Version(); v != "" {
			keyVersion = v
		}
		if s := string(*jwk.KID); s != "" {
			keyRef = s
		}
	}

	return &AzureKMSSigner{
		client:     client,
		keyRef:     keyRef,
		keyName:    name,
		keyVersion: keyVersion,
		algorithm:  algorithm,
		azureAlg:   azureAlg,
		pub:        pub,
		keyID:      kid,
	}, nil
}

func (o *azureKMSSignerOptions) buildSignClient(vaultURL string) (AzureKMSSignAPI, error) {
	if o.client != nil {
		return o.client, nil
	}
	cred := o.cred
	if cred == nil {
		built, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("crypto: build Azure default credential: %w", err)
		}
		cred = built
	}
	clientOpts := o.clientOptions
	if clientOpts == nil {
		clientOpts = &azkeys.ClientOptions{}
	}
	if clientOpts.Retry.MaxRetries == 0 {
		clientOpts.Retry = policy.RetryOptions{MaxRetries: 3}
	}
	client, err := azkeys.NewClient(vaultURL, cred, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("crypto: build Azure Key Vault client: %w", err)
	}
	return client, nil
}

// Sign digests payload (SHA-256/384/512 per the algorithm) and signs the
// digest via Key Vault's Sign RPC. For ECDSA it converts Azure's raw r‖s
// signature to the ASN.1 DER form [VerifyManifestKMS] expects (Divergence
// 1); RSA-PSS is returned unchanged.
func (s *AzureKMSSigner) Sign(ctx context.Context, payload []byte) ([]byte, error) {
	digest := DigestForKMSAlgorithm(s.algorithm, payload)
	if digest == nil {
		return nil, fmt.Errorf("crypto: azure kms sign: algorithm %q is not a digest-signing algorithm", s.algorithm)
	}
	alg := s.azureAlg
	out, err := s.client.Sign(ctx, s.keyName, s.keyVersion, azkeys.SignParameters{
		Algorithm: &alg,
		Value:     digest,
	}, nil)
	if err != nil {
		return nil, translateAzureKMSError(err, s.keyRef, "sign")
	}
	if len(out.Result) == 0 {
		return nil, errors.New("crypto: azure kms sign returned an empty signature")
	}
	// Divergence 1: Azure ECDSA signatures are raw r‖s (IEEE P-1363), not
	// DER. The pure verifier uses ecdsa.VerifyASN1, so convert before
	// returning — teaching the verifier to accept raw r‖s would give it a
	// second signature form and weaken the single-form invariant.
	if ecPub, ok := s.pub.(*ecdsa.PublicKey); ok {
		return azureECDSARawToDER(out.Result, ecPub.Curve.Params().BitSize)
	}
	return out.Result, nil
}

// Algorithm returns the sluice-canonical algorithm identifier.
func (s *AzureKMSSigner) Algorithm() string { return s.algorithm }

// KeyID returns the public-key fingerprint recorded in the `.sig`.
func (s *AzureKMSSigner) KeyID() string { return s.keyID }

// KeyRef returns the versioned Key Vault key URL the signature was
// produced under (advisory; recorded for rotation/audit, never trusted
// for verification).
func (s *AzureKMSSigner) KeyRef() string { return s.keyRef }

// PublicKey returns the resolved signing public key.
func (s *AzureKMSSigner) PublicKey() stdcrypto.PublicKey { return s.pub }

// FetchAzureKMSPublicKey resolves the PUBLIC key of an operator-named
// trusted Azure Key Vault signing key — the ONLINE `--verify-key
// kms://azure/<key-url>` path. The verifier fetches the key the OPERATOR
// names, never the one a manifest's advisory KeyRef records, so a
// rewritten ref cannot redirect trust. The algorithm used to verify comes
// from the chain's signed scheme token, not from here.
func FetchAzureKMSPublicKey(ctx context.Context, keyID string, opts ...AzureKMSSignerOption) (stdcrypto.PublicKey, error) {
	if strings.TrimSpace(keyID) == "" {
		return nil, errors.New("crypto: Azure Key Vault verify key ID is empty")
	}
	o := &azureKMSSignerOptions{}
	for _, opt := range opts {
		opt(o)
	}
	vaultURL, name, version, err := parseAzureKeyID(keyID)
	if err != nil {
		return nil, err
	}
	client, err := o.buildSignClient(vaultURL)
	if err != nil {
		return nil, err
	}
	jwk, err := getAzureKMSSignKey(ctx, client, name, version, keyID)
	if err != nil {
		return nil, err
	}
	if err := requireAzureSignKeyOp(jwk, keyID); err != nil {
		return nil, err
	}
	pub, _, _, err := azureSignerParamsFromJWK(jwk)
	if err != nil {
		return nil, fmt.Errorf("crypto: azure kms verify key %q: %w", keyID, err)
	}
	return pub, nil
}

// getAzureKMSSignKey calls GetKey and returns the JWK, mapping error
// shapes via the shared translator. It does NOT interpret the key — the
// caller pairs it with [requireAzureSignKeyOp] + [azureSignerParamsFromJWK].
func getAzureKMSSignKey(ctx context.Context, client AzureKMSSignAPI, name, version, keyID string) (*azkeys.JSONWebKey, error) {
	resp, err := client.GetKey(ctx, name, version, nil)
	if err != nil {
		return nil, translateAzureKMSError(err, keyID, "describe")
	}
	if resp.Key == nil {
		return nil, fmt.Errorf("crypto: azure kms GetKey returned no key material for %q", keyID)
	}
	return resp.Key, nil
}

// requireAzureSignKeyOp refuses a key whose key_ops does not include
// "sign" — an encryption / wrap-only key mistakenly passed as a signing
// key. The refusal is loud and names the key.
func requireAzureSignKeyOp(jwk *azkeys.JSONWebKey, keyID string) error {
	for _, op := range jwk.KeyOps {
		if op != nil && *op == azkeys.KeyOperationSign {
			return nil
		}
	}
	return fmt.Errorf("crypto: azure kms key %q key_ops does not include %q (an encryption/wrap-only key cannot sign manifests); create the key with the sign operation enabled", keyID, azkeys.KeyOperationSign)
}

// azureSignerParamsFromJWK converts an Azure JWK public key (Divergence 2)
// into a stdlib public key + the sluice-canonical algorithm + the concrete
// Azure SignatureAlgorithm, deriving the algorithm from the key's Kty+Crv
// (EC) or Kty (RSA). Refuses an unsupported key type loudly — Azure Key
// Vault has no Ed25519.
func azureSignerParamsFromJWK(jwk *azkeys.JSONWebKey) (stdcrypto.PublicKey, string, azkeys.SignatureAlgorithm, error) {
	if jwk.Kty == nil {
		return nil, "", "", errors.New("JWK has no key type (kty)")
	}
	switch *jwk.Kty {
	case azkeys.KeyTypeEC, azkeys.KeyTypeECHSM:
		return azureECFromJWK(jwk)
	case azkeys.KeyTypeRSA, azkeys.KeyTypeRSAHSM:
		return azureRSAFromJWK(jwk)
	default:
		return nil, "", "", fmt.Errorf("key type %q is not a supported manifest-signing key (need EC/EC-HSM or RSA/RSA-HSM; Azure Key Vault has no Ed25519)", *jwk.Kty)
	}
}

// azureECFromJWK rebuilds an ECDSA public key from an EC JWK, mapping the
// curve to the sluice-canonical ecdsa-* algorithm + the Azure ES* enum
// (ES512 pairs with P-521 + SHA-512). P-256K/secp256k1 is not a
// manifest-signing curve and is refused.
func azureECFromJWK(jwk *azkeys.JSONWebKey) (stdcrypto.PublicKey, string, azkeys.SignatureAlgorithm, error) {
	if jwk.Crv == nil {
		return nil, "", "", errors.New("EC JWK has no curve (crv)")
	}
	var (
		curve     elliptic.Curve
		sluiceAlg string
		azureAlg  azkeys.SignatureAlgorithm
	)
	switch *jwk.Crv {
	case azkeys.CurveNameP256:
		curve, sluiceAlg, azureAlg = elliptic.P256(), KMSAlgorithmECDSAP256, azkeys.SignatureAlgorithmES256
	case azkeys.CurveNameP384:
		curve, sluiceAlg, azureAlg = elliptic.P384(), KMSAlgorithmECDSAP384, azkeys.SignatureAlgorithmES384
	case azkeys.CurveNameP521:
		curve, sluiceAlg, azureAlg = elliptic.P521(), KMSAlgorithmECDSAP521, azkeys.SignatureAlgorithmES512
	default:
		return nil, "", "", fmt.Errorf("EC curve %q is not a supported manifest-signing curve (need P-256/P-384/P-521)", *jwk.Crv)
	}
	if len(jwk.X) == 0 || len(jwk.Y) == 0 {
		return nil, "", "", errors.New("EC JWK is missing the X/Y public coordinates")
	}
	pub := &ecdsa.PublicKey{
		Curve: curve,
		X:     new(big.Int).SetBytes(jwk.X),
		Y:     new(big.Int).SetBytes(jwk.Y),
	}
	return pub, sluiceAlg, azureAlg, nil
}

// azureRSAFromJWK rebuilds an RSA public key from an RSA JWK. sluice signs
// RSA keys with RSASSA-PSS/SHA-256 (PS256), matching the AWS adapter's
// PSS-preferred default.
func azureRSAFromJWK(jwk *azkeys.JSONWebKey) (stdcrypto.PublicKey, string, azkeys.SignatureAlgorithm, error) {
	if len(jwk.N) == 0 || len(jwk.E) == 0 {
		return nil, "", "", errors.New("RSA JWK is missing the modulus (n) or exponent (e)")
	}
	e := new(big.Int).SetBytes(jwk.E)
	if !e.IsInt64() || e.Int64() < 2 || e.Int64() > (1<<31-1) {
		return nil, "", "", fmt.Errorf("RSA JWK public exponent %s is out of the supported range", e)
	}
	pub := &rsa.PublicKey{
		N: new(big.Int).SetBytes(jwk.N),
		E: int(e.Int64()),
	}
	return pub, KMSAlgorithmRSAPSS256, azkeys.SignatureAlgorithmPS256, nil
}

// azureECDSARawToDER converts an Azure ECDSA signature — raw r‖s / IEEE
// P-1363, two fixed-width big-endian integers concatenated — into the
// ASN.1 DER SEQUENCE{ R, S } that [ecdsa.VerifyASN1] (and thus
// [VerifyManifestKMS]) expects. The half-width is (bitSize+7)/8 per curve;
// for P-521 that is 66 bytes (an ODD size, the trap). A raw signature that
// is not exactly two halves is refused loudly rather than mis-split.
func azureECDSARawToDER(raw []byte, bitSize int) ([]byte, error) {
	byteLen := (bitSize + 7) / 8
	if len(raw) != 2*byteLen {
		return nil, fmt.Errorf("crypto: azure kms ecdsa signature is %d bytes, want %d (r‖s of 2×%d for the curve)", len(raw), 2*byteLen, byteLen)
	}
	sig := struct{ R, S *big.Int }{
		R: new(big.Int).SetBytes(raw[:byteLen]),
		S: new(big.Int).SetBytes(raw[byteLen:]),
	}
	der, err := asn1.Marshal(sig)
	if err != nil {
		return nil, fmt.Errorf("crypto: azure kms ecdsa signature r‖s→DER: %w", err)
	}
	return der, nil
}
