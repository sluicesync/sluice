// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

// GCP Cloud KMS-backed envelope-encryption implementation. Phase 6.3
// of the logical-backup feature (`docs/dev/design-logical-backups-
// phase-6.md`): the operator hands sluice a Cloud KMS crypto-key
// resource name; WrapCEK / UnwrapCEK route through the KMS service's
// Encrypt / Decrypt RPCs instead of an Argon2id-derived KEK.
//
// Same [EnvelopeEncryption] seam Phase 6.1 introduced — the chunk
// writer/reader paths don't change. The only Phase-6.3-GCP-specific
// bits are recorded in the manifest's [ir.ChainEncryption]: KEKMode
// is "gcp-kms", KEKRef is the operator's crypto-key resource name,
// and Argon2id is left nil (the cloud KMS service handles its own
// key state).
//
// Mirrors the AWS Phase 6.2 shape (`aws_kms.go`) intentionally —
// operators moving between clouds see the same flag / error / log
// patterns.

import (
	"context"
	"errors"
	"fmt"
	"strings"

	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KEKModeGCPKMS is the KEKMode tag recorded in
// [ir.ChainEncryption.KEKMode] when the chain was encrypted under a
// GCP Cloud KMS key. Restore-side validation matches it against the
// supplied envelope's Mode().
//
// String literal is part of the on-disk format; renaming requires a
// manifest format-version bump.
const KEKModeGCPKMS = "gcp-kms"

// GCPKMSAPI is the narrow surface [GCPKMSEnvelope] needs from
// [kms.KeyManagementClient]. Declared as an interface so tests can
// stub the SDK without spinning a real client. The production
// implementation is satisfied by `*kms.KeyManagementClient`.
type GCPKMSAPI interface {
	Encrypt(ctx context.Context, req *kmspb.EncryptRequest, opts ...gax) (*kmspb.EncryptResponse, error)
	Decrypt(ctx context.Context, req *kmspb.DecryptRequest, opts ...gax) (*kmspb.DecryptResponse, error)
	GetCryptoKey(ctx context.Context, req *kmspb.GetCryptoKeyRequest, opts ...gax) (*kmspb.CryptoKey, error)
	Close() error
}

// gax is a type alias matching `gax.CallOption` from
// `github.com/googleapis/gax-go/v2` — the variadic option type every
// Google client method accepts. Keeping it as a type alias avoids a
// direct gax import in this file (the production *kms.KeyManagementClient
// already pulls gax in transitively).
type gax = interface{}

// GCPKMSOption configures [NewGCPKMSEnvelope]. Mostly used by tests
// (stubbed client) rather than production callers — production opens
// with the default GCP client and Application Default Credentials.
type GCPKMSOption func(*gcpKMSOptions)

type gcpKMSOptions struct {
	client        GCPKMSAPI
	clientOptions []option.ClientOption
	skipPreflight bool
}

// WithGCPKMSClient injects a pre-built KMS client (or stub satisfying
// [GCPKMSAPI]). Tests use this to record call counts + simulate GCP
// error shapes without the SDK roundtrip.
func WithGCPKMSClient(client GCPKMSAPI) GCPKMSOption {
	return func(o *gcpKMSOptions) { o.client = client }
}

// WithGCPKMSClientOptions appends `option.ClientOption` values used to
// build the production KMS client (e.g. `option.WithCredentialsFile`,
// `option.WithEndpoint` for an emulator). Ignored when
// [WithGCPKMSClient] is also supplied.
func WithGCPKMSClientOptions(opts ...option.ClientOption) GCPKMSOption {
	return func(o *gcpKMSOptions) { o.clientOptions = append(o.clientOptions, opts...) }
}

// withSkipGCPPreflight skips the GetCryptoKey preflight; used by
// tests that stub the client and don't want to assert a Get call.
// Not exported — operators shouldn't bypass preflight in production.
func withSkipGCPPreflight() GCPKMSOption {
	return func(o *gcpKMSOptions) { o.skipPreflight = true }
}

// GCPKMSEnvelope is the GCP Cloud KMS implementation of
// [EnvelopeEncryption]. The KEK is the Cloud KMS crypto-key
// identified by keyResource; WrapCEK / UnwrapCEK route through the
// KMS service's Encrypt / Decrypt RPCs. The wrapped CEK recorded in
// the manifest is the KMS Ciphertext field — an opaque byte slice
// that the service round-trips back to the original plaintext on
// Decrypt.
//
// Lifecycle: NewGCPKMSEnvelope (validates resource name, loads
// default client unless overridden, pre-flights GetCryptoKey) →
// Wrap/Unwrap as needed → Close (releases the gRPC connection on the
// underlying client). The GetCryptoKey preflight surfaces auth /
// not-found errors at construction time rather than mid-backup or
// mid-restore.
type GCPKMSEnvelope struct {
	keyResource string
	client      GCPKMSAPI
	ownsClient  bool // true → Close() the client; false → caller owns it
}

// NewGCPKMSEnvelope constructs a GCPKMSEnvelope against the supplied
// Cloud KMS crypto-key resource name. Acceptable shapes:
//
//   - Crypto-key resource: `projects/PROJECT/locations/LOCATION/keyRings/RING/cryptoKeys/KEY`
//   - Versioned crypto-key: `projects/.../cryptoKeys/KEY/cryptoKeyVersions/VERSION`
//
// Returns an error when keyResource is empty, the default KMS client
// can't be loaded (no Application Default Credentials available), or
// the GetCryptoKey preflight fails (auth denied, key not found).
//
// Pre-flighting at construction is load-bearing for the same reason
// it is in [NewKMSEnvelope] (AWS): a backup that's already streamed
// half its rows shouldn't fail at the first chunk flush because the
// service account lacks `cloudkms.cryptoKeyVersions.useToEncrypt`.
// The GetCryptoKey call costs one KMS request (~free at GCP's
// pricing tier) and is the right place to fail.
func NewGCPKMSEnvelope(ctx context.Context, keyResource string, opts ...GCPKMSOption) (*GCPKMSEnvelope, error) {
	if strings.TrimSpace(keyResource) == "" {
		return nil, errors.New("crypto: GCP KMS key resource is empty")
	}

	o := &gcpKMSOptions{}
	for _, opt := range opts {
		opt(o)
	}

	env := &GCPKMSEnvelope{keyResource: keyResource}
	if o.client != nil {
		env.client = o.client
		env.ownsClient = false
	} else {
		client, err := kms.NewKeyManagementClient(ctx, o.clientOptions...)
		if err != nil {
			return nil, fmt.Errorf("crypto: build GCP KMS client: %w", err)
		}
		env.client = gcpRealClient{client}
		env.ownsClient = true
	}

	if !o.skipPreflight {
		if _, err := env.client.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{
			Name: cryptoKeyForResource(keyResource),
		}); err != nil {
			if env.ownsClient {
				_ = env.client.Close()
			}
			return nil, translateGCPKMSError(err, keyResource, "describe")
		}
	}
	return env, nil
}

// gcpRealClient adapts the production *kms.KeyManagementClient to the
// [GCPKMSAPI] interface. The SDK's method signatures match GCPKMSAPI's
// modulo the gax.CallOption type — which is itself just a variadic
// `...gax.CallOption`, so the adapter is a straight forward.
type gcpRealClient struct {
	*kms.KeyManagementClient
}

func (c gcpRealClient) Encrypt(ctx context.Context, req *kmspb.EncryptRequest, _ ...gax) (*kmspb.EncryptResponse, error) {
	return c.KeyManagementClient.Encrypt(ctx, req)
}

func (c gcpRealClient) Decrypt(ctx context.Context, req *kmspb.DecryptRequest, _ ...gax) (*kmspb.DecryptResponse, error) {
	return c.KeyManagementClient.Decrypt(ctx, req)
}

func (c gcpRealClient) GetCryptoKey(ctx context.Context, req *kmspb.GetCryptoKeyRequest, _ ...gax) (*kmspb.CryptoKey, error) {
	return c.KeyManagementClient.GetCryptoKey(ctx, req)
}

// Mode returns [KEKModeGCPKMS] — the tag recorded in
// [ir.ChainEncryption.KEKMode] for GCP-KMS-encrypted chains.
func (e *GCPKMSEnvelope) Mode() string { return KEKModeGCPKMS }

// KeyResource returns the resource name the envelope was constructed
// with. Exposed so callers (orchestrator's [ir.ChainEncryption.KEKRef]
// populater) can record it on the manifest without re-asking the
// operator.
func (e *GCPKMSEnvelope) KeyResource() string { return e.keyResource }

// WrapCEK encrypts cek by routing through the KMS service's Encrypt
// RPC against the envelope's crypto-key. The returned bytes are the
// service's Ciphertext field — an opaque byte slice that Decrypt
// round-trips back to the original plaintext. Recorded in the
// manifest's [ir.ChainEncryption.WrappedCEK] (per-chain mode) or
// [ir.ChunkEncryption.WrappedCEK] (per-chunk mode).
//
// Uses a background context internally because the
// [EnvelopeEncryption] interface is context-free. KMS calls in
// production sluice flows are short (~50-200ms typical) and the
// orchestrator's outer context cancellation propagates to the chunk
// writer's loop; the KMS Encrypt for the chain CEK happens once at
// chain start, well before any cancel-on-failure cleanup runs.
func (e *GCPKMSEnvelope) WrapCEK(cek []byte) ([]byte, error) {
	if len(cek) != CEKLen {
		return nil, fmt.Errorf("crypto: gcp kms wrap cek length %d != %d", len(cek), CEKLen)
	}
	out, err := e.client.Encrypt(context.Background(), &kmspb.EncryptRequest{
		Name:      e.keyResource,
		Plaintext: cek,
	})
	if err != nil {
		return nil, translateGCPKMSError(err, e.keyResource, "encrypt")
	}
	if len(out.Ciphertext) == 0 {
		return nil, errors.New("crypto: gcp kms encrypt returned empty Ciphertext")
	}
	return out.Ciphertext, nil
}

// UnwrapCEK is the inverse of WrapCEK: hands the wrapped bytes to
// the KMS service's Decrypt RPC, returns the plaintext CEK. Cloud
// KMS infers the key version from the ciphertext's metadata so the
// decrypt succeeds even when the key was rotated since wrap.
//
// Cloud KMS's Decrypt accepts the crypto-key resource (without
// version) and figures out the right version from the ciphertext.
// Passing a versioned resource also works as long as it matches the
// version the ciphertext was wrapped under.
func (e *GCPKMSEnvelope) UnwrapCEK(wrapped []byte) ([]byte, error) {
	if len(wrapped) == 0 {
		return nil, errors.New("crypto: gcp kms unwrap wrapped bytes are empty")
	}
	out, err := e.client.Decrypt(context.Background(), &kmspb.DecryptRequest{
		Name:       cryptoKeyForResource(e.keyResource),
		Ciphertext: wrapped,
	})
	if err != nil {
		return nil, translateGCPKMSError(err, e.keyResource, "decrypt")
	}
	if len(out.Plaintext) != CEKLen {
		return nil, fmt.Errorf("crypto: gcp kms unwrap returned plaintext of length %d (want %d)", len(out.Plaintext), CEKLen)
	}
	return out.Plaintext, nil
}

// Close releases the underlying gRPC connection when the envelope
// owns the client (i.e. it was built via the default client path,
// not [WithGCPKMSClient]). Safe to call multiple times.
func (e *GCPKMSEnvelope) Close() error {
	if e.ownsClient && e.client != nil {
		return e.client.Close()
	}
	return nil
}

// cryptoKeyForResource strips the `/cryptoKeyVersions/N` suffix from
// `name` if present. GCP's Decrypt accepts the crypto-key resource
// without a version, while GetCryptoKey requires it; this helper
// makes both work from a single operator-supplied resource string.
func cryptoKeyForResource(name string) string {
	if idx := strings.Index(name, "/cryptoKeyVersions/"); idx >= 0 {
		return name[:idx]
	}
	return name
}

// translateGCPKMSError maps a raw gRPC status error from a Cloud KMS
// call to an operator-actionable message. The grpc/status package
// surfaces canonical codes (NotFound, PermissionDenied, etc.) that
// we branch on; the op argument names which call failed (encrypt /
// decrypt / describe) so the resulting error pinpoints the failing
// phase.
//
// Errors that don't carry a gRPC status (network failures, context
// cancellation) fall through with the key resource + op preserved so
// support can correlate against Cloud KMS request logs.
func translateGCPKMSError(err error, keyResource, op string) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("crypto: gcp kms %s failed (key=%q): %w", op, keyResource, err)
	}
	switch st.Code() {
	case codes.NotFound:
		return fmt.Errorf("crypto: gcp kms %s failed: key %q not found (verify the resource name is correct + the service account has access). underlying: %w",
			op, keyResource, err)
	case codes.PermissionDenied:
		return fmt.Errorf("crypto: gcp kms %s denied: service account lacks the required IAM role on key %q (grant roles/cloudkms.cryptoKey%s — see https://cloud.google.com/kms/docs/iam). underlying: %w",
			op, keyResource, gcpRoleForOp(op), err)
	case codes.FailedPrecondition:
		return fmt.Errorf("crypto: gcp kms %s rejected: key %q is in an invalid state (verify the key is enabled and the primary version is not disabled). underlying: %w",
			op, keyResource, err)
	case codes.InvalidArgument:
		return fmt.Errorf("crypto: gcp kms %s rejected: invalid argument for key %q (verify the resource name format and that the key purpose is ENCRYPT_DECRYPT). underlying: %w",
			op, keyResource, err)
	case codes.Unauthenticated:
		return fmt.Errorf("crypto: gcp kms %s denied: no valid credentials available (ensure GOOGLE_APPLICATION_CREDENTIALS is set or run `gcloud auth application-default login`). underlying: %w",
			op, err)
	}
	return fmt.Errorf("crypto: gcp kms %s failed (key=%q, code=%s): %w", op, keyResource, st.Code(), err)
}

// gcpRoleForOp returns the IAM role suffix that matches op for the
// operator's recovery hint. Cloud KMS's IAM roles use
// `roles/cloudkms.cryptoKeyEncrypter` / `Decrypter` / `Viewer`.
func gcpRoleForOp(op string) string {
	switch op {
	case "encrypt":
		return "Encrypter"
	case "decrypt":
		return "Decrypter"
	case "describe":
		return "Viewer"
	default:
		return op
	}
}
