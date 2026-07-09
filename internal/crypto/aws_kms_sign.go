// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

// AWS KMS asymmetric-signing adapter (ADR-0154 Phase 3a). Implements the
// provider-neutral [KMSSigner] seam against a SIGN_VERIFY KMS key: the
// private key never leaves KMS; sluice hands KMS the manifest digest and
// gets back a signature. Verification is PURE and local (the exported
// public key + [VerifyManifestKMS]) — this adapter is only the SIGN side
// plus the one-time public-key fetch.
//
// This reuses the encryption-side ARN/config conventions (`aws_kms.go`)
// but is a DISTINCT type: a signing key is an asymmetric SIGN_VERIFY key,
// separate from the symmetric ENCRYPT_DECRYPT key `--kms-key-arn` names —
// which is exactly the §7-Q4 property (a maintenance cron re-signs via an
// IAM-granted KMS Sign key without holding — or being — the encryption
// key).
//
// AWS specifics the adapter pins down:
//   - Sign takes MessageType=DIGEST + the pre-computed digest (sluice's
//     canonical payload can exceed KMS's 4 KiB raw-message limit, so we
//     always digest-then-sign).
//   - The ECDSA signature AWS returns is ASN.1 DER (RFC 3279) — the pure
//     verifier uses [ecdsa.VerifyASN1] to match. A round-trip test pins it.
//   - GetPublicKey returns SPKI DER; the KeySpec fixes the algorithm
//     (P-256→ecdsa-p256, RSA→rsa-pss-256 by default — PSS preferred).

import (
	"context"
	stdcrypto "crypto"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
)

// AWSKMSSignAPI is the narrow surface [AWSKMSSigner] needs from
// [kms.Client] for asymmetric signing — Sign + GetPublicKey, plus
// DescribeKey for the preflight. Declared as an interface so tests can
// stub it (or a localstack container satisfies it). `*kms.Client`
// satisfies it in production.
type AWSKMSSignAPI interface {
	Sign(ctx context.Context, params *kms.SignInput, optFns ...func(*kms.Options)) (*kms.SignOutput, error)
	GetPublicKey(ctx context.Context, params *kms.GetPublicKeyInput, optFns ...func(*kms.Options)) (*kms.GetPublicKeyOutput, error)
}

// AWSKMSSignerOption configures [NewAWSKMSSigner] / [FetchAWSKMSPublicKey].
type AWSKMSSignerOption func(*awsKMSSignerOptions)

type awsKMSSignerOptions struct {
	cfg    *aws.Config
	client AWSKMSSignAPI
	region string
}

// WithAWSKMSSignConfig overrides the AWS config the sign client is built
// from (tests point it at localstack; production passes a pre-configured
// cross-account config).
func WithAWSKMSSignConfig(cfg aws.Config) AWSKMSSignerOption {
	return func(o *awsKMSSignerOptions) { o.cfg = &cfg }
}

// WithAWSKMSSignClient injects a pre-built client (or a stub satisfying
// [AWSKMSSignAPI]) — the test seam for a faithful fake KMS Sign.
func WithAWSKMSSignClient(client AWSKMSSignAPI) AWSKMSSignerOption {
	return func(o *awsKMSSignerOptions) { o.client = client }
}

// WithAWSKMSSignRegion overrides the region the default config resolves
// to. Ignored when a config or client is also supplied.
func WithAWSKMSSignRegion(region string) AWSKMSSignerOption {
	return func(o *awsKMSSignerOptions) { o.region = region }
}

// AWSKMSSigner is the AWS implementation of [KMSSigner]. It resolves the
// public key + algorithm once at construction (GetPublicKey), so Sign is
// the only per-call KMS roundtrip and PublicKey/KeyID/Algorithm are IO-free.
type AWSKMSSigner struct {
	client    AWSKMSSignAPI
	keyRef    string // resolved key ARN (from GetPublicKey)
	algorithm string // sluice-canonical, e.g. "ecdsa-p256"
	awsAlg    kmstypes.SigningAlgorithmSpec
	pub       stdcrypto.PublicKey
	keyID     string
}

// NewAWSKMSSigner constructs a signer against an AWS KMS SIGN_VERIFY key.
// keyRef is a key ID / ARN / alias. It preflights GetPublicKey to (a)
// confirm the key exists + is usable, (b) fetch the public key for local
// verification / key-id derivation, and (c) fix the signing algorithm from
// the key's KeySpec — so a mid-backup Sign never fails for a key that was
// wrong all along. Refuses a non-SIGN_VERIFY key (an encryption key
// mistakenly passed as a signing key) loudly.
func NewAWSKMSSigner(ctx context.Context, keyRef string, opts ...AWSKMSSignerOption) (*AWSKMSSigner, error) {
	if strings.TrimSpace(keyRef) == "" {
		return nil, errors.New("crypto: AWS KMS signing key reference is empty")
	}
	o := &awsKMSSignerOptions{}
	for _, opt := range opts {
		opt(o)
	}
	client, err := o.buildSignClient(ctx)
	if err != nil {
		return nil, err
	}

	pub, keySpec, err := getAWSKMSPublicKey(ctx, client, keyRef)
	if err != nil {
		return nil, err
	}
	algorithm, awsAlg, err := awsAlgorithmForKeySpec(keySpec)
	if err != nil {
		return nil, fmt.Errorf("crypto: AWS KMS signing key %q: %w", keyRef, err)
	}
	keyID, err := KMSManifestKeyID(pub)
	if err != nil {
		return nil, err
	}
	return &AWSKMSSigner{
		client:    client,
		keyRef:    keyRef,
		algorithm: algorithm,
		awsAlg:    awsAlg,
		pub:       pub,
		keyID:     keyID,
	}, nil
}

func (o *awsKMSSignerOptions) buildSignClient(ctx context.Context) (AWSKMSSignAPI, error) {
	if o.client != nil {
		return o.client, nil
	}
	var cfg aws.Config
	if o.cfg != nil {
		cfg = *o.cfg
	} else {
		loadOpts := []func(*config.LoadOptions) error{}
		if o.region != "" {
			loadOpts = append(loadOpts, config.WithRegion(o.region))
		}
		loaded, err := config.LoadDefaultConfig(ctx, loadOpts...)
		if err != nil {
			return nil, fmt.Errorf("crypto: load AWS config for KMS signing: %w", err)
		}
		cfg = loaded
	}
	return kms.NewFromConfig(cfg), nil
}

// Sign digests payload (SHA-256/384/512 per the algorithm) and signs the
// digest via KMS Sign with MessageType=DIGEST. Returns the raw provider
// signature (ASN.1 DER for ECDSA; PKCS#1/PSS for RSA) that
// [VerifyManifestKMS] verifies.
func (s *AWSKMSSigner) Sign(ctx context.Context, payload []byte) ([]byte, error) {
	digest := DigestForKMSAlgorithm(s.algorithm, payload)
	if digest == nil {
		return nil, fmt.Errorf("crypto: AWS KMS sign: algorithm %q is not a digest-signing algorithm", s.algorithm)
	}
	out, err := s.client.Sign(ctx, &kms.SignInput{
		KeyId:            aws.String(s.keyRef),
		Message:          digest,
		MessageType:      kmstypes.MessageTypeDigest,
		SigningAlgorithm: s.awsAlg,
	})
	if err != nil {
		return nil, translateKMSError(err, s.keyRef, "sign")
	}
	if len(out.Signature) == 0 {
		return nil, errors.New("crypto: AWS KMS sign returned an empty signature")
	}
	return out.Signature, nil
}

// Algorithm returns the sluice-canonical algorithm identifier.
func (s *AWSKMSSigner) Algorithm() string { return s.algorithm }

// KeyID returns the public-key fingerprint recorded in the `.sig`.
func (s *AWSKMSSigner) KeyID() string { return s.keyID }

// KeyRef returns the key ARN the signature was produced under (advisory).
// For an AWS asymmetric key the key material is fixed per key (rotation =
// a new key), so the ARN is itself the version anchor.
func (s *AWSKMSSigner) KeyRef() string { return s.keyRef }

// PublicKey returns the resolved signing public key.
func (s *AWSKMSSigner) PublicKey() stdcrypto.PublicKey { return s.pub }

// FetchAWSKMSPublicKey resolves the PUBLIC key of an operator-named
// trusted AWS KMS signing key — the ONLINE `--verify-key kms://aws/<ref>`
// path. The verifier fetches the key the OPERATOR names, never the one a
// manifest's advisory KeyRef records, so a rewritten manifest ref cannot
// redirect trust. The algorithm used to verify comes from the chain's
// signed scheme token, not from here.
func FetchAWSKMSPublicKey(ctx context.Context, keyRef string, opts ...AWSKMSSignerOption) (stdcrypto.PublicKey, error) {
	if strings.TrimSpace(keyRef) == "" {
		return nil, errors.New("crypto: AWS KMS verify key reference is empty")
	}
	o := &awsKMSSignerOptions{}
	for _, opt := range opts {
		opt(o)
	}
	client, err := o.buildSignClient(ctx)
	if err != nil {
		return nil, err
	}
	pub, _, err := getAWSKMSPublicKey(ctx, client, keyRef)
	return pub, err
}

// getAWSKMSPublicKey calls GetPublicKey, validates the key is a signing
// key, and parses the SPKI DER into a stdlib public key.
func getAWSKMSPublicKey(ctx context.Context, client AWSKMSSignAPI, keyRef string) (stdcrypto.PublicKey, kmstypes.KeySpec, error) {
	out, err := client.GetPublicKey(ctx, &kms.GetPublicKeyInput{KeyId: aws.String(keyRef)})
	if err != nil {
		return nil, "", translateKMSError(err, keyRef, "get-public-key")
	}
	if out.KeyUsage != kmstypes.KeyUsageTypeSignVerify {
		return nil, "", fmt.Errorf("crypto: AWS KMS key %q has KeyUsage %q, not SIGN_VERIFY (an encryption key cannot sign manifests)", keyRef, out.KeyUsage)
	}
	if len(out.PublicKey) == 0 {
		return nil, "", fmt.Errorf("crypto: AWS KMS GetPublicKey returned no public key for %q", keyRef)
	}
	pub, err := parseSPKIDER(out.PublicKey)
	if err != nil {
		return nil, "", fmt.Errorf("crypto: AWS KMS key %q: %w", keyRef, err)
	}
	return pub, out.KeySpec, nil
}

// awsAlgorithmForKeySpec maps an AWS KMS KeySpec to the sluice-canonical
// signing algorithm + the concrete AWS SigningAlgorithmSpec. RSA keys
// default to RSASSA-PSS with a SHA-256 digest (PSS preferred over
// PKCS#1-v1.5, per AWS's own guidance). Refuses a symmetric / unsupported
// key spec loudly.
func awsAlgorithmForKeySpec(spec kmstypes.KeySpec) (string, kmstypes.SigningAlgorithmSpec, error) {
	switch spec {
	case kmstypes.KeySpecEccNistP256:
		return KMSAlgorithmECDSAP256, kmstypes.SigningAlgorithmSpecEcdsaSha256, nil
	case kmstypes.KeySpecEccNistP384:
		return KMSAlgorithmECDSAP384, kmstypes.SigningAlgorithmSpecEcdsaSha384, nil
	case kmstypes.KeySpecEccNistP521:
		return KMSAlgorithmECDSAP521, kmstypes.SigningAlgorithmSpecEcdsaSha512, nil
	case kmstypes.KeySpecRsa2048, kmstypes.KeySpecRsa3072, kmstypes.KeySpecRsa4096:
		return KMSAlgorithmRSAPSS256, kmstypes.SigningAlgorithmSpecRsassaPssSha256, nil
	default:
		return "", "", fmt.Errorf("key spec %q is not a supported manifest-signing key (need ECC_NIST_P256/384/521 or RSA_2048/3072/4096)", spec)
	}
}
