// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

// AWS KMS-backed envelope-encryption implementation. Phase 6.2 of the
// logical-backup feature (`docs/dev/design/logical-backups-phase-6.md`):
// the operator hands sluice a KMS key ARN (or alias); WrapCEK / UnwrapCEK
// route through `kms.Encrypt` / `kms.Decrypt` instead of an Argon2id-
// derived KEK.
//
// Same `EnvelopeEncryption` seam Phase 6.1 introduced — the chunk
// writer/reader paths don't change. The only Phase-6.2-specific bits
// are recorded in the manifest's [backup.ChainEncryption]: KEKMode is
// "aws-kms", KEKRef is the operator's key ARN, and Argon2id is left
// nil (KMS handles its own key state via cloud-provider mechanisms).

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	smithy "github.com/aws/smithy-go"
)

// KEKModeAWSKMS is the KEKMode tag recorded in
// [backup.ChainEncryption.KEKMode] when the chain was encrypted under an
// AWS KMS key. Restore-side validation matches it against the
// supplied envelope's Mode().
//
// String literal is part of the on-disk format; renaming requires a
// manifest format-version bump.
const KEKModeAWSKMS = "aws-kms"

// KMSAPI is the narrow surface KMSEnvelope needs from
// [kms.Client]. Declared as an interface so tests can stub the SDK
// without spinning a real client (or a localstack container). The
// production implementation is satisfied by `*kms.Client`.
type KMSAPI interface {
	Encrypt(ctx context.Context, params *kms.EncryptInput, optFns ...func(*kms.Options)) (*kms.EncryptOutput, error)
	Decrypt(ctx context.Context, params *kms.DecryptInput, optFns ...func(*kms.Options)) (*kms.DecryptOutput, error)
	DescribeKey(ctx context.Context, params *kms.DescribeKeyInput, optFns ...func(*kms.Options)) (*kms.DescribeKeyOutput, error)
}

// KMSOption configures [NewKMSEnvelope]. Forwards mainly used by
// tests (custom AWS config with a localstack endpoint, stubbed client)
// rather than production callers — production opens with the default
// AWS config chain.
type KMSOption func(*kmsOptions)

type kmsOptions struct {
	cfg           *aws.Config
	client        KMSAPI
	region        string
	skipPreflight bool
}

// WithKMSConfig overrides the AWS config the envelope's KMS client is
// built from. Used by tests pointing at a localstack endpoint or
// production callers passing a pre-configured client (cross-account
// role assumption etc.).
func WithKMSConfig(cfg aws.Config) KMSOption {
	return func(o *kmsOptions) { o.cfg = &cfg }
}

// WithKMSClient injects a pre-built KMS client (or stub satisfying
// [KMSAPI]). Tests use this to record call counts + simulate AWS error
// shapes without the SDK roundtrip.
func WithKMSClient(client KMSAPI) KMSOption {
	return func(o *kmsOptions) { o.client = client }
}

// WithKMSRegion overrides the region the default AWS config resolves
// to. Equivalent to setting AWS_REGION before the call. Ignored when
// [WithKMSConfig] or [WithKMSClient] is also supplied (those carry
// their own region).
func WithKMSRegion(region string) KMSOption {
	return func(o *kmsOptions) { o.region = region }
}

// withSkipPreflight skips the DescribeKey preflight; used by tests
// that stub the client and don't want to assert a Describe call. Not
// exported — operators shouldn't bypass preflight in production.
func withSkipPreflight() KMSOption {
	return func(o *kmsOptions) { o.skipPreflight = true }
}

// KMSEnvelope is the AWS KMS implementation of [EnvelopeEncryption].
// The KEK is the KMS key referenced by keyARN; WrapCEK / UnwrapCEK
// route through `kms.Encrypt` / `kms.Decrypt`. The wrapped CEK
// recorded in the manifest is the KMS CiphertextBlob, an opaque
// (typically ~200-byte) byte slice that KMS round-trips back to the
// original plaintext on Decrypt.
//
// Lifecycle: NewKMSEnvelope (validates ARN, loads AWS config,
// pre-flights DescribeKey) → Wrap/Unwrap as needed. The DescribeKey
// preflight surfaces auth/region/key-not-found errors at construction
// time rather than mid-backup or mid-restore.
type KMSEnvelope struct {
	keyARN string
	client KMSAPI
}

// NewKMSEnvelope constructs a KMSEnvelope against the supplied AWS
// KMS key reference. Acceptable shapes:
//
//   - Full ARN: arn:aws:kms:<region>:<account>:key/<uuid>
//   - Alias ARN: arn:aws:kms:<region>:<account>:alias/<name>
//   - Bare key ID: <uuid> (resolved via the SDK's configured region)
//   - Alias name: alias/<name> (same)
//
// Returns an error when the ARN is empty, the AWS config can't be
// loaded (no creds in the chain, missing region for non-ARN refs), or
// the DescribeKey preflight fails (auth denied, key not found).
//
// Pre-flighting at construction is load-bearing: a backup that's
// already streamed half its rows shouldn't fail at the first chunk
// flush because the operator's IAM role lacks kms:Encrypt. The
// DescribeKey call costs one KMS API request (~$0.0000002) on every
// backup / restore startup; cheap relative to the cost of a failed
// half-backup.
func NewKMSEnvelope(ctx context.Context, keyARN string, opts ...KMSOption) (*KMSEnvelope, error) {
	if strings.TrimSpace(keyARN) == "" {
		return nil, errors.New("crypto: KMS key ARN is empty")
	}

	o := &kmsOptions{}
	for _, opt := range opts {
		opt(o)
	}

	client := o.client
	if client == nil {
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
				return nil, fmt.Errorf("crypto: load AWS config for KMS: %w", err)
			}
			cfg = loaded
		}
		client = kms.NewFromConfig(cfg)
	}

	env := &KMSEnvelope{keyARN: keyARN, client: client}

	if !o.skipPreflight {
		if _, err := client.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String(keyARN)}); err != nil {
			return nil, translateKMSError(err, keyARN, "describe")
		}
	}
	return env, nil
}

// Mode returns [KEKModeAWSKMS] — the tag recorded in
// [backup.ChainEncryption.KEKMode] for KMS-encrypted chains.
func (e *KMSEnvelope) Mode() string { return KEKModeAWSKMS }

// KeyARN returns the ARN the envelope was constructed with. Exposed so
// callers (orchestrator's [backup.ChainEncryption.KEKRef] populater) can
// record it on the manifest without re-asking the operator.
func (e *KMSEnvelope) KeyARN() string { return e.keyARN }

// WrapCEK encrypts cek by routing through `kms.Encrypt` against the
// envelope's key ARN. The returned bytes are the KMS CiphertextBlob —
// an opaque byte slice that KMS Decrypt round-trips back to the
// original plaintext. Recorded in the manifest's
// [backup.ChainEncryption.WrappedCEK] (per-chain mode) or
// [backup.ChunkEncryption.WrappedCEK] (per-chunk mode).
//
// Uses a background context internally because the
// [EnvelopeEncryption] interface is context-free. KMS calls in
// production sluice flows are short (~50-200ms typical) and the
// orchestrator's outer context cancellation propagates to the chunk
// writer's loop; the KMS Encrypt for the chain CEK happens once at
// chain start, well before any cancel-on-failure cleanup runs.
func (e *KMSEnvelope) WrapCEK(cek []byte) ([]byte, error) {
	return e.WrapCEKBound(cek, "")
}

// WrapCEKBound implements [BoundEnvelope]: the binding string becomes
// the KMS EncryptionContext, which KMS authenticates (the Decrypt
// fails with InvalidCiphertextException unless the identical context
// is supplied), records in CloudTrail, and lets key policies enforce.
// Empty binding is the legacy context-free wrap.
func (e *KMSEnvelope) WrapCEKBound(cek []byte, binding string) ([]byte, error) {
	if len(cek) != CEKLen {
		return nil, fmt.Errorf("crypto: kms wrap cek length %d != %d", len(cek), CEKLen)
	}
	out, err := e.client.Encrypt(context.Background(), &kms.EncryptInput{
		KeyId:             aws.String(e.keyARN),
		Plaintext:         cek,
		EncryptionContext: kmsEncryptionContext(binding),
	})
	if err != nil {
		return nil, translateKMSError(err, e.keyARN, "encrypt")
	}
	if len(out.CiphertextBlob) == 0 {
		return nil, errors.New("crypto: kms encrypt returned empty CiphertextBlob")
	}
	return out.CiphertextBlob, nil
}

// UnwrapCEK is the inverse of WrapCEK: hands the wrapped bytes to
// `kms.Decrypt`, returns the plaintext CEK. KMS infers the key from
// the wrapped blob's metadata so the decrypt succeeds even when the
// key was rotated since wrap (KMS's transparent key-version chain).
//
// Note: KMS Decrypt does not require KeyId for symmetric keys (the
// blob carries its own key reference). We pass it anyway as a
// belt-and-braces guard against accidental cross-key decryption when
// the operator misconfigures aliases.
func (e *KMSEnvelope) UnwrapCEK(wrapped []byte) ([]byte, error) {
	return e.UnwrapCEKBound(wrapped, "")
}

// UnwrapCEKBound implements [BoundEnvelope]. The binding must equal
// the wrap-time EncryptionContext exactly; KMS refuses otherwise
// (InvalidCiphertextException), which is the loud "this CEK was
// wrapped for a different backup" signal.
func (e *KMSEnvelope) UnwrapCEKBound(wrapped []byte, binding string) ([]byte, error) {
	if len(wrapped) == 0 {
		return nil, errors.New("crypto: kms unwrap wrapped bytes are empty")
	}
	out, err := e.client.Decrypt(context.Background(), &kms.DecryptInput{
		KeyId:             aws.String(e.keyARN),
		CiphertextBlob:    wrapped,
		EncryptionContext: kmsEncryptionContext(binding),
	})
	if err != nil {
		return nil, translateKMSError(err, e.keyARN, "decrypt")
	}
	if len(out.Plaintext) != CEKLen {
		return nil, fmt.Errorf("crypto: kms unwrap returned plaintext of length %d (want %d)", len(out.Plaintext), CEKLen)
	}
	return out.Plaintext, nil
}

// kmsEncryptionContextKey is the EncryptionContext map key sluice's
// CEK binding rides under. Part of the wrapped-CEK contract for
// FormatVersion-5+ manifests (ADR-0152); renaming would strand every
// chain wrapped since.
const kmsEncryptionContextKey = "sluice_cek_binding"

// kmsEncryptionContext maps a binding string to the KMS
// EncryptionContext argument: "" → nil (the legacy context-free wrap,
// required for pre-FormatVersion-5 chains), anything else → the
// one-entry sluice context.
func kmsEncryptionContext(binding string) map[string]string {
	if binding == "" {
		return nil
	}
	return map[string]string{kmsEncryptionContextKey: binding}
}

// translateKMSError maps a raw aws-sdk-go-v2 KMS error to an
// operator-actionable message. The SDK surfaces typed errors
// ([*kmstypes.NotFoundException], etc.) we unwrap and re-wrap with
// the key ARN inline + a hint about what to fix. AccessDenied is not
// a KMS-typed error in the v2 SDK — it surfaces as a generic
// [smithy.APIError] with ErrorCode() == "AccessDeniedException";
// we branch on ErrorCode for that case. The op argument names which
// call failed (encrypt / decrypt / describe) so the resulting error
// pinpoints the failing phase.
//
// Generic / unrecognised errors fall through with the key ARN and op
// preserved so support can correlate against KMS request logs.
func translateKMSError(err error, keyARN, op string) error {
	if err == nil {
		return nil
	}
	var (
		invalid   *kmstypes.KMSInvalidStateException
		notFound  *kmstypes.NotFoundException
		disabled  *kmstypes.DisabledException
		incorrect *kmstypes.IncorrectKeyException
		apiErr    smithy.APIError
	)
	switch {
	case errors.As(err, &invalid):
		return fmt.Errorf("crypto: kms %s rejected: key %q is in an invalid state for the operation (verify the key is enabled and not pending deletion). underlying: %w",
			op, keyARN, err)
	case errors.As(err, &notFound):
		return fmt.Errorf("crypto: kms %s failed: key %q not found (verify the ARN/alias is correct + the role/credentials have access). underlying: %w",
			op, keyARN, err)
	case errors.As(err, &disabled):
		return fmt.Errorf("crypto: kms %s rejected: key %q is disabled (re-enable in the KMS console + retry). underlying: %w",
			op, keyARN, err)
	case errors.As(err, &incorrect):
		return fmt.Errorf("crypto: kms %s rejected: ciphertext was wrapped under a different key (chain manifest's KEKRef does not match the supplied --kms-key-arn). underlying: %w",
			op, err)
	case errors.As(err, &apiErr):
		switch apiErr.ErrorCode() {
		case "AccessDeniedException":
			return fmt.Errorf("crypto: kms %s denied: AWS IAM principal lacks kms:%s on key %q (verify key policy + role policy grants the action). underlying: %w",
				op, kmsActionForOp(op), keyARN, err)
		case "InvalidKeyUsageException":
			return fmt.Errorf("crypto: kms %s rejected: key %q is not configured for symmetric encrypt/decrypt (KeyUsage must be ENCRYPT_DECRYPT). underlying: %w",
				op, keyARN, err)
		}
	}
	return fmt.Errorf("crypto: kms %s failed (key=%q): %w", op, keyARN, err)
}

// kmsActionForOp returns the IAM action name corresponding to op for
// inclusion in the access-denied error message — the operator's
// recovery hint is "grant kms:<Action>", which only makes sense if
// the message names the right action.
func kmsActionForOp(op string) string {
	switch op {
	case "encrypt":
		return "Encrypt"
	case "decrypt":
		return "Decrypt"
	case "describe":
		return "DescribeKey"
	default:
		return op
	}
}
