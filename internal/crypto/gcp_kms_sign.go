// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

// GCP Cloud KMS asymmetric-signing adapter (ADR-0154 Phase 3b, GCP half).
// Implements the provider-neutral [KMSSigner] seam against an
// ASYMMETRIC_SIGN Cloud KMS key: the private key never leaves the HSM;
// sluice hands KMS the manifest digest (or, for Ed25519, the whole
// payload) and gets back a signature. Verification stays PURE and local
// ([VerifyManifestKMS] against the exported public key) — this adapter is
// only the SIGN side plus the one-time public-key fetch.
//
// Mirrors the AWS Phase-3a shape (`aws_kms_sign.go`) intentionally, and
// reuses the GCP envelope's client/option/error patterns (`gcp_kms.go`) so
// operators moving between clouds see the same flag / error / log shapes.
//
// GCP specifics the adapter pins down (the real work over AWS):
//   - Sign targets a specific CryptoKeyVersion (`.../cryptoKeyVersions/N`),
//     not the bare crypto-key — GCP signs with an explicit version, so the
//     resource MUST be versioned (refused loudly otherwise).
//   - ECDSA / RSA-PSS set Digest (a pre-computed [DigestForKMSAlgorithm]);
//     Ed25519 sets Data (the whole payload, no external pre-digest).
//   - CRC32C integrity (Castagnoli): every request carries the CRC32C of
//     its digest/data, and after signing we VERIFY the response reported
//     VerifiedDigest/DataCrc32C == true AND crc32c(signature) matches
//     SignatureCrc32C — refusing loudly on any mismatch (a corrupted-in-
//     transit signing request or signature). AWS had no such wire-integrity
//     handshake; this is the GCP correctness detail, pinned by test.
//   - GetPublicKey returns an SPKI PEM (parsed via
//     [ParseManifestPublicKeyPEM]) + a CryptoKeyVersion algorithm enum that
//     fixes the sluice-canonical algorithm; the PEM's own CRC32C is checked.
//   - The ECDSA signature GCP returns is ASN.1 DER (RFC 5480) — identical to
//     AWS, so the pure verifier (ecdsa.VerifyASN1) handles it unchanged; NO
//     r‖s conversion (that is Azure's divergence, not GCP's).
//
// Real-GCP-KMS validation is DEFERRED (the N-9 pattern): this adapter is
// pinned against a faithful in-process fake; a real-cloud pin (a live
// ASYMMETRIC_SIGN key + GetPublicKey/AsymmetricSign round-trip) is a
// follow-up, tracked alongside the AWS `kmsverify` localstack leg.

import (
	"context"
	stdcrypto "crypto"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"

	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// crc32cTable is the Castagnoli CRC32C table Cloud KMS uses for its
// request/response wire-integrity checksums. Package-level so the checksum
// helper allocates it once.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// crc32c returns the Castagnoli CRC32C of b — the checksum Cloud KMS
// authenticates on the digest/data it signs and on the signature it
// returns.
func crc32c(b []byte) int64 { return int64(crc32.Checksum(b, crc32cTable)) }

// GCPKMSSignAPI is the narrow surface [GCPKMSSigner] needs from
// [kms.KeyManagementClient] for asymmetric signing — AsymmetricSign +
// GetPublicKey, plus Close for the owned-client path. Declared as an
// interface so tests can stub it with a faithful in-process signer.
// `*kms.KeyManagementClient` satisfies it in production (via
// [gcpRealSignClient]).
type GCPKMSSignAPI interface {
	AsymmetricSign(ctx context.Context, req *kmspb.AsymmetricSignRequest, opts ...gax) (*kmspb.AsymmetricSignResponse, error)
	GetPublicKey(ctx context.Context, req *kmspb.GetPublicKeyRequest, opts ...gax) (*kmspb.PublicKey, error)
	Close() error
}

// GCPKMSSignerOption configures [NewGCPKMSSigner] / [FetchGCPKMSPublicKey].
type GCPKMSSignerOption func(*gcpKMSSignerOptions)

type gcpKMSSignerOptions struct {
	client        GCPKMSSignAPI
	clientOptions []option.ClientOption
}

// WithGCPKMSSignClient injects a pre-built client (or a stub satisfying
// [GCPKMSSignAPI]) — the test seam for a faithful fake KMS signer.
func WithGCPKMSSignClient(client GCPKMSSignAPI) GCPKMSSignerOption {
	return func(o *gcpKMSSignerOptions) { o.client = client }
}

// WithGCPKMSSignClientOptions appends `option.ClientOption` values used to
// build the production KMS client (e.g. `option.WithCredentialsFile`,
// `option.WithEndpoint` for an emulator). Ignored when
// [WithGCPKMSSignClient] is also supplied.
func WithGCPKMSSignClientOptions(opts ...option.ClientOption) GCPKMSSignerOption {
	return func(o *gcpKMSSignerOptions) { o.clientOptions = append(o.clientOptions, opts...) }
}

// GCPKMSSigner is the GCP Cloud KMS implementation of [KMSSigner]. It
// resolves the public key + algorithm once at construction (GetPublicKey),
// so Sign is the only per-call KMS roundtrip and PublicKey/KeyID/Algorithm
// are IO-free.
type GCPKMSSigner struct {
	client     GCPKMSSignAPI
	keyRef     string // versioned CryptoKeyVersion resource
	algorithm  string // sluice-canonical, e.g. "ecdsa-p256"
	pub        stdcrypto.PublicKey
	keyID      string
	ownsClient bool // true → Close() the client; false → caller owns it
}

// NewGCPKMSSigner constructs a signer against a GCP Cloud KMS
// ASYMMETRIC_SIGN key VERSION. keyResource must be a versioned
// CryptoKeyVersion (`projects/.../cryptoKeys/KEY/cryptoKeyVersions/N`) —
// GCP signs with a specific version, so a bare crypto-key is refused
// loudly. It preflights GetPublicKey to (a) confirm the version exists + is
// usable, (b) fetch the public key for local verification / key-id
// derivation, and (c) fix the signing algorithm from the version's
// algorithm enum — so a mid-backup Sign never fails for a key that was
// wrong all along. A non-signing key surfaces a non-signing algorithm from
// GetPublicKey and is refused (there is no separate purpose field on the
// public-key response; the algorithm is the discriminator).
func NewGCPKMSSigner(ctx context.Context, keyResource string, opts ...GCPKMSSignerOption) (*GCPKMSSigner, error) {
	if strings.TrimSpace(keyResource) == "" {
		return nil, errors.New("crypto: GCP KMS signing key resource is empty")
	}
	o := &gcpKMSSignerOptions{}
	for _, opt := range opts {
		opt(o)
	}
	client, ownsClient, err := o.buildSignClient(ctx)
	if err != nil {
		return nil, err
	}

	pub, gcpAlg, err := getGCPKMSPublicKey(ctx, client, keyResource)
	if err != nil {
		if ownsClient {
			_ = client.Close()
		}
		return nil, err
	}
	algorithm, err := gcpAlgorithmForKeyVersionAlgorithm(gcpAlg)
	if err != nil {
		if ownsClient {
			_ = client.Close()
		}
		return nil, fmt.Errorf("crypto: GCP KMS signing key %q: %w", keyResource, err)
	}
	keyID, err := KMSManifestKeyID(pub)
	if err != nil {
		if ownsClient {
			_ = client.Close()
		}
		return nil, err
	}
	return &GCPKMSSigner{
		client:     client,
		keyRef:     keyResource,
		algorithm:  algorithm,
		pub:        pub,
		keyID:      keyID,
		ownsClient: ownsClient,
	}, nil
}

func (o *gcpKMSSignerOptions) buildSignClient(ctx context.Context) (GCPKMSSignAPI, bool, error) {
	if o.client != nil {
		return o.client, false, nil
	}
	client, err := kms.NewKeyManagementClient(ctx, o.clientOptions...)
	if err != nil {
		return nil, false, fmt.Errorf("crypto: build GCP KMS client for signing: %w", err)
	}
	return gcpRealSignClient{client}, true, nil
}

// gcpRealSignClient adapts the production *kms.KeyManagementClient to the
// [GCPKMSSignAPI] interface. The SDK's method signatures match modulo the
// gax.CallOption variadic (aliased locally as [gax]); Close is inherited
// from the embedded client.
type gcpRealSignClient struct {
	*kms.KeyManagementClient
}

func (c gcpRealSignClient) AsymmetricSign(ctx context.Context, req *kmspb.AsymmetricSignRequest, _ ...gax) (*kmspb.AsymmetricSignResponse, error) {
	return c.KeyManagementClient.AsymmetricSign(ctx, req)
}

func (c gcpRealSignClient) GetPublicKey(ctx context.Context, req *kmspb.GetPublicKeyRequest, _ ...gax) (*kmspb.PublicKey, error) {
	return c.KeyManagementClient.GetPublicKey(ctx, req)
}

// Sign hashes payload as its algorithm requires (digest-signing for
// ECDSA/RSA; whole-message for Ed25519), signs it via AsymmetricSign, and
// VERIFIES the CRC32C wire-integrity handshake before returning. Returns
// the raw provider signature (ASN.1 DER for ECDSA; PKCS#1/PSS for RSA; raw
// for Ed25519) that [VerifyManifestKMS] verifies.
func (s *GCPKMSSigner) Sign(ctx context.Context, payload []byte) ([]byte, error) {
	req := &kmspb.AsymmetricSignRequest{Name: s.keyRef}
	digested := s.algorithm != KMSAlgorithmEd25519
	if digested {
		digest := DigestForKMSAlgorithm(s.algorithm, payload)
		if digest == nil {
			return nil, fmt.Errorf("crypto: gcp kms sign: algorithm %q is not a digest-signing algorithm", s.algorithm)
		}
		d, err := gcpDigestForAlgorithm(s.algorithm, digest)
		if err != nil {
			return nil, err
		}
		req.Digest = d
		req.DigestCrc32C = wrapperspb.Int64(crc32c(digest))
	} else {
		// Ed25519 signs the whole payload (no external pre-digest).
		req.Data = payload
		req.DataCrc32C = wrapperspb.Int64(crc32c(payload))
	}

	out, err := s.client.AsymmetricSign(ctx, req)
	if err != nil {
		return nil, translateGCPKMSSignError(err, s.keyRef, "sign")
	}
	if len(out.GetSignature()) == 0 {
		return nil, errors.New("crypto: gcp kms sign returned an empty signature")
	}

	// CRC32C wire-integrity: refuse loudly rather than emit a signature that
	// may have been corrupted in transit (either direction).
	if digested && !out.GetVerifiedDigestCrc32C() {
		return nil, fmt.Errorf("crypto: gcp kms sign: server did not verify the request digest CRC32C for key %q (signing request corrupted in transit — refusing)", s.keyRef)
	}
	if !digested && !out.GetVerifiedDataCrc32C() {
		return nil, fmt.Errorf("crypto: gcp kms sign: server did not verify the request data CRC32C for key %q (signing request corrupted in transit — refusing)", s.keyRef)
	}
	if out.GetSignatureCrc32C() == nil || out.GetSignatureCrc32C().GetValue() != crc32c(out.GetSignature()) {
		return nil, fmt.Errorf("crypto: gcp kms sign: signature CRC32C mismatch for key %q (signature corrupted in transit — refusing)", s.keyRef)
	}
	return out.GetSignature(), nil
}

// Algorithm returns the sluice-canonical algorithm identifier.
func (s *GCPKMSSigner) Algorithm() string { return s.algorithm }

// KeyID returns the public-key fingerprint recorded in the `.sig`.
func (s *GCPKMSSigner) KeyID() string { return s.keyID }

// KeyRef returns the versioned CryptoKeyVersion resource the signature was
// produced under (advisory; a GCP signature is always bound to one version).
func (s *GCPKMSSigner) KeyRef() string { return s.keyRef }

// PublicKey returns the resolved signing public key.
func (s *GCPKMSSigner) PublicKey() stdcrypto.PublicKey { return s.pub }

// Close releases the underlying gRPC connection when the signer owns the
// client (built via the default path, not [WithGCPKMSSignClient]). Safe to
// call multiple times.
func (s *GCPKMSSigner) Close() error {
	if s.ownsClient && s.client != nil {
		return s.client.Close()
	}
	return nil
}

// FetchGCPKMSPublicKey resolves the PUBLIC key of an operator-named trusted
// GCP KMS signing key VERSION — the ONLINE `--verify-key kms://gcp/<ref>`
// path. The verifier fetches the key the OPERATOR names, never the one a
// manifest's advisory KeyRef records, so a rewritten manifest ref cannot
// redirect trust. The algorithm used to verify comes from the chain's
// signed scheme token, not from here.
func FetchGCPKMSPublicKey(ctx context.Context, keyResource string, opts ...GCPKMSSignerOption) (stdcrypto.PublicKey, error) {
	if strings.TrimSpace(keyResource) == "" {
		return nil, errors.New("crypto: GCP KMS verify key resource is empty")
	}
	o := &gcpKMSSignerOptions{}
	for _, opt := range opts {
		opt(o)
	}
	client, ownsClient, err := o.buildSignClient(ctx)
	if err != nil {
		return nil, err
	}
	if ownsClient {
		defer func() { _ = client.Close() }()
	}
	pub, _, err := getGCPKMSPublicKey(ctx, client, keyResource)
	return pub, err
}

// getGCPKMSPublicKey calls GetPublicKey, verifies the PEM's CRC32C,
// requires a versioned resource, and parses the SPKI PEM into a stdlib
// public key alongside the version's algorithm enum. The algorithm is the
// only signal that a key is a signing key (the PublicKey response carries
// no purpose field), so a non-signing key is refused by the caller's
// [gcpAlgorithmForKeyVersionAlgorithm] mapping.
func getGCPKMSPublicKey(ctx context.Context, client GCPKMSSignAPI, keyResource string) (stdcrypto.PublicKey, kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm, error) {
	if !strings.Contains(keyResource, "/cryptoKeyVersions/") {
		return nil, 0, fmt.Errorf("crypto: GCP KMS signing key %q must be a versioned CryptoKeyVersion resource (.../cryptoKeys/KEY/cryptoKeyVersions/N) — GCP signs with a specific key version", keyResource)
	}
	out, err := client.GetPublicKey(ctx, &kmspb.GetPublicKeyRequest{Name: keyResource})
	if err != nil {
		return nil, 0, translateGCPKMSSignError(err, keyResource, "get-public-key")
	}
	pemStr := out.GetPem()
	if pemStr == "" {
		return nil, 0, fmt.Errorf("crypto: gcp kms GetPublicKey returned no PEM for %q", keyResource)
	}
	if out.GetPemCrc32C() == nil || out.GetPemCrc32C().GetValue() != crc32c([]byte(pemStr)) {
		return nil, 0, fmt.Errorf("crypto: gcp kms GetPublicKey PEM CRC32C mismatch for %q (public key corrupted in transit — refusing)", keyResource)
	}
	pub, err := ParseManifestPublicKeyPEM([]byte(pemStr))
	if err != nil {
		return nil, 0, fmt.Errorf("crypto: gcp kms key %q: %w", keyResource, err)
	}
	return pub, out.GetAlgorithm(), nil
}

// gcpDigestForAlgorithm wraps the pre-computed digest in the Cloud KMS
// [kmspb.Digest] oneof matching the algorithm's hash. Only reached for
// digest-signing algorithms (ECDSA / RSA-PSS); Ed25519 signs Data.
func gcpDigestForAlgorithm(algorithm string, digest []byte) (*kmspb.Digest, error) {
	switch algorithm {
	case KMSAlgorithmECDSAP256, KMSAlgorithmRSAPSS256:
		return &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: digest}}, nil
	case KMSAlgorithmECDSAP384, KMSAlgorithmRSAPSS384:
		return &kmspb.Digest{Digest: &kmspb.Digest_Sha384{Sha384: digest}}, nil
	case KMSAlgorithmECDSAP521, KMSAlgorithmRSAPSS512:
		return &kmspb.Digest{Digest: &kmspb.Digest_Sha512{Sha512: digest}}, nil
	default:
		return nil, fmt.Errorf("crypto: gcp kms: algorithm %q has no digest mapping", algorithm)
	}
}

// gcpAlgorithmForKeyVersionAlgorithm maps a Cloud KMS CryptoKeyVersion
// algorithm enum to the sluice-canonical signing algorithm. RSA-PSS keys
// map to rsa-pss-256 (SHA-256 variants) or rsa-pss-512 (the SHA-512
// variant). GCP KMS does NOT offer P-521 signing (so ecdsa-p521 is never
// emitted here), and EC_SIGN_SECP256K1_SHA256 is a non-NIST curve sluice
// does not support — both, like any encryption-purpose or HMAC algorithm,
// hit the loud refusal (the key's algorithm is the only purpose signal on
// the public-key response).
func gcpAlgorithmForKeyVersionAlgorithm(alg kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm) (string, error) {
	switch alg {
	case kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256:
		return KMSAlgorithmECDSAP256, nil
	case kmspb.CryptoKeyVersion_EC_SIGN_P384_SHA384:
		return KMSAlgorithmECDSAP384, nil
	case kmspb.CryptoKeyVersion_RSA_SIGN_PSS_2048_SHA256,
		kmspb.CryptoKeyVersion_RSA_SIGN_PSS_3072_SHA256,
		kmspb.CryptoKeyVersion_RSA_SIGN_PSS_4096_SHA256:
		return KMSAlgorithmRSAPSS256, nil
	case kmspb.CryptoKeyVersion_RSA_SIGN_PSS_4096_SHA512:
		return KMSAlgorithmRSAPSS512, nil
	case kmspb.CryptoKeyVersion_EC_SIGN_ED25519:
		return KMSAlgorithmEd25519, nil
	default:
		return "", fmt.Errorf("key uses algorithm %s, not a supported manifest-signing algorithm (need EC_SIGN_P256_SHA256 / EC_SIGN_P384_SHA384 / RSA_SIGN_PSS_{2048,3072,4096}_SHA256 / RSA_SIGN_PSS_4096_SHA512 / EC_SIGN_ED25519)", alg)
	}
}

// translateGCPKMSSignError maps a raw gRPC status error from a Cloud KMS
// AsymmetricSign / GetPublicKey call to an operator-actionable message.
// Parallels [translateGCPKMSError] (the envelope translator) but with
// SIGNING-appropriate IAM hints — the signing/verify roles are
// `roles/cloudkms.signerVerifier` (Sign) and `roles/cloudkms.publicKeyViewer`
// (GetPublicKey), which don't fit the envelope translator's cryptoKey* role
// template.
func translateGCPKMSSignError(err error, keyResource, op string) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("crypto: gcp kms %s failed (key=%q): %w", op, keyResource, err)
	}
	switch st.Code() {
	case codes.NotFound:
		return fmt.Errorf("crypto: gcp kms %s failed: key version %q not found (verify the resource is a valid CryptoKeyVersion and the service account has access). underlying: %w",
			op, keyResource, err)
	case codes.PermissionDenied:
		return fmt.Errorf("crypto: gcp kms %s denied: service account lacks the required IAM role on key %q (grant roles/cloudkms.signerVerifier for Sign or roles/cloudkms.publicKeyViewer for GetPublicKey — see https://cloud.google.com/kms/docs/iam). underlying: %w",
			op, keyResource, err)
	case codes.FailedPrecondition:
		return fmt.Errorf("crypto: gcp kms %s rejected: key %q is in an invalid state (verify the key version is enabled). underlying: %w",
			op, keyResource, err)
	case codes.InvalidArgument:
		return fmt.Errorf("crypto: gcp kms %s rejected: invalid argument for key %q (verify the resource name format and that the key purpose is ASYMMETRIC_SIGN). underlying: %w",
			op, keyResource, err)
	case codes.Unauthenticated:
		return fmt.Errorf("crypto: gcp kms %s denied: no valid credentials available (ensure GOOGLE_APPLICATION_CREDENTIALS is set or run `gcloud auth application-default login`). underlying: %w",
			op, err)
	}
	return fmt.Errorf("crypto: gcp kms %s failed (key=%q, code=%s): %w", op, keyResource, st.Code(), err)
}
