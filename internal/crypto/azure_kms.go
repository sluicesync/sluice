// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

// Azure Key Vault-backed envelope-encryption implementation. Phase
// 6.3 of the logical-backup feature (`docs/dev/design-logical-
// backups-phase-6.md`): the operator hands sluice an Azure Key Vault
// key identifier; WrapCEK / UnwrapCEK route through the Key Vault
// service's WrapKey / UnwrapKey RPCs instead of an Argon2id-derived
// KEK.
//
// Same [EnvelopeEncryption] seam Phase 6.1 introduced — the chunk
// writer/reader paths don't change. The only Phase-6.3-Azure-specific
// bits are recorded in the manifest's [backup.ChainEncryption]: KEKMode
// is "azure-kms", KEKRef is the operator's Key Vault key identifier
// URL, and Argon2id is left nil.
//
// Mirrors the AWS Phase 6.2 + GCP Phase 6.3 shape intentionally —
// operators moving between clouds see the same flag / error / log
// patterns.
//
// Note on Azure's "wrap" vs AWS/GCP's "encrypt": Key Vault exposes
// two distinct surfaces: Encrypt/Decrypt for arbitrary byte
// plaintexts (limited to ~245 bytes for asymmetric keys) and
// WrapKey/UnwrapKey specifically for encrypting symmetric keys.
// Semantically both round-trip bytes; we use WrapKey/UnwrapKey to
// match Key Vault's recommended pattern for the AES-256 CEK wrap
// case (CEKLen = 32 bytes — well within wrap's payload limit for
// any supported key type).

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
)

// KEKModeAzureKMS is the KEKMode tag recorded in
// [backup.ChainEncryption.KEKMode] when the chain was encrypted under an
// Azure Key Vault key. Restore-side validation matches it against
// the supplied envelope's Mode().
//
// String literal is part of the on-disk format; renaming requires a
// manifest format-version bump.
const KEKModeAzureKMS = "azure-kms"

// DefaultAzureWrapAlgorithm is the wrap algorithm sluice asks Key
// Vault to use when wrapping the CEK. RSA-OAEP-256 is the Azure-
// recommended default for software-protected keys and is supported
// by both Vault and Managed HSM tiers. Operators with HSM-only AES
// keys can override via [WithAzureWrapAlgorithm].
//
// AES-256-CBC-HSM (`A256CBC`) is the HSM-only AES-wrap algorithm; we
// don't make it the default because it requires an AES-typed key on
// a Managed HSM, which has different provisioning costs than the
// vault-tier RSA keys most operators start with.
var DefaultAzureWrapAlgorithm = azkeys.EncryptionAlgorithmRSAOAEP256

// AzureKMSAPI is the narrow surface [AzureKMSEnvelope] needs from
// [azkeys.Client]. Declared as an interface so tests can stub the
// SDK without spinning a real Key Vault. The production
// implementation is satisfied by `*azkeys.Client`.
type AzureKMSAPI interface {
	WrapKey(ctx context.Context, name, version string, params azkeys.KeyOperationParameters, opts *azkeys.WrapKeyOptions) (azkeys.WrapKeyResponse, error)
	UnwrapKey(ctx context.Context, name, version string, params azkeys.KeyOperationParameters, opts *azkeys.UnwrapKeyOptions) (azkeys.UnwrapKeyResponse, error)
	GetKey(ctx context.Context, name, version string, opts *azkeys.GetKeyOptions) (azkeys.GetKeyResponse, error)
}

// AzureKMSOption configures [NewAzureKMSEnvelope]. Mostly used by
// tests rather than production callers.
type AzureKMSOption func(*azureKMSOptions)

type azureKMSOptions struct {
	client        AzureKMSAPI
	cred          azcore.TokenCredential
	clientOptions *azkeys.ClientOptions
	wrapAlgorithm azkeys.EncryptionAlgorithm
	skipPreflight bool
}

// WithAzureKMSClient injects a pre-built Key Vault keys client (or
// stub satisfying [AzureKMSAPI]). Tests use this to record call
// counts + simulate Azure error shapes without the SDK roundtrip.
func WithAzureKMSClient(client AzureKMSAPI) AzureKMSOption {
	return func(o *azureKMSOptions) { o.client = client }
}

// WithAzureKMSCredential overrides the token credential used to
// build the production client. Defaults to
// [azidentity.NewDefaultAzureCredential] (env vars, managed identity,
// Azure CLI cached login, etc.). Useful for service-principal flows
// where the operator wants explicit credential control.
func WithAzureKMSCredential(cred azcore.TokenCredential) AzureKMSOption {
	return func(o *azureKMSOptions) { o.cred = cred }
}

// WithAzureKMSClientOptions sets the SDK's client options (custom
// retry policy, transport, etc.). Ignored when [WithAzureKMSClient]
// is also supplied.
func WithAzureKMSClientOptions(opts *azkeys.ClientOptions) AzureKMSOption {
	return func(o *azureKMSOptions) { o.clientOptions = opts }
}

// WithAzureWrapAlgorithm overrides the default wrap algorithm
// [DefaultAzureWrapAlgorithm]. Operators using HSM-backed AES keys
// would pass `azkeys.EncryptionAlgorithmA256KW` (AES Key Wrap).
func WithAzureWrapAlgorithm(alg azkeys.EncryptionAlgorithm) AzureKMSOption {
	return func(o *azureKMSOptions) { o.wrapAlgorithm = alg }
}

// WithAzureWrapAlgorithmString is the string-typed convenience
// wrapper for callers (the CLI) that take the algorithm name as a
// string flag. Pass a Key Vault algorithm name verbatim (e.g.
// "RSA-OAEP-256", "A256KW"); an invalid value surfaces as a
// BadParameter error from the service on first wrap. Empty string
// is treated as "use the default."
func WithAzureWrapAlgorithmString(alg string) AzureKMSOption {
	return func(o *azureKMSOptions) {
		if alg != "" {
			o.wrapAlgorithm = azkeys.EncryptionAlgorithm(alg)
		}
	}
}

// AzureKMSEnvelope is the Azure Key Vault implementation of
// [EnvelopeEncryption]. The KEK is the Key Vault key identified by
// keyID; WrapCEK / UnwrapCEK route through the service's WrapKey /
// UnwrapKey RPCs. The wrapped CEK recorded in the manifest is the
// service's Result field — an opaque byte slice that the service
// round-trips back to the original plaintext on UnwrapKey.
//
// Lifecycle: NewAzureKMSEnvelope (validates key ID URL, loads
// default credential unless overridden, pre-flights GetKey) →
// Wrap/Unwrap as needed. The GetKey preflight surfaces auth /
// not-found / permission errors at construction time rather than
// mid-backup or mid-restore.
type AzureKMSEnvelope struct {
	keyID         string // operator-supplied (full URL)
	vaultURL      string // parsed: e.g. https://my-vault.vault.azure.net
	keyName       string // parsed: e.g. my-key
	wrapAlgorithm azkeys.EncryptionAlgorithm
	client        AzureKMSAPI

	// mu guards keyVersion + warnedUnversioned. keyVersion starts as
	// the version parsed from the operator's key URL (empty → latest)
	// and is PINNED to a concrete version at the first opportunity —
	// the GetKey preflight, a WrapKey response's KID, or a manifest's
	// recorded KEKRef via [AzureKMSEnvelope.RebindChainKEKRef]. The pin
	// exists because Azure key-wrap ciphertext carries NO key-version
	// metadata (unlike AWS/GCP): an unpinned unwrap always targets the
	// vault's LATEST version, which breaks restores of older chains
	// once key auto-rotation is enabled (audit N-9). Pinning at wrap
	// time also keeps a per-chunk-mode run on ONE version even if the
	// key rotates mid-run.
	//
	// versionExplicit records that the OPERATOR's key URL named a
	// version; an explicit version is never overridden by a manifest's
	// recorded ref (the operator may be deliberately retargeting).
	mu                sync.Mutex
	keyVersion        string
	versionExplicit   bool
	warnedUnversioned bool
}

// NewAzureKMSEnvelope constructs an AzureKMSEnvelope against the
// supplied Key Vault key identifier URL. Acceptable shapes:
//
//   - Versioned key: `https://VAULT.vault.azure.net/keys/KEY/VERSION`
//   - Latest version: `https://VAULT.vault.azure.net/keys/KEY` (empty
//     version → resolved to the key's CURRENT version by the GetKey
//     preflight and pinned; Azure key-wrap ciphertext carries no
//     version metadata, so unwrap must be told the wrap-time version —
//     see [AzureKMSEnvelope.RebindChainKEKRef] / audit N-9)
//   - Managed HSM: `https://VAULT.managedhsm.azure.net/keys/KEY[/VERSION]`
//
// Returns an error when keyID is empty or unparseable, the default
// credential can't be loaded (no auth available), or the GetKey
// preflight fails (auth denied, key not found, key disabled).
//
// Pre-flighting at construction is load-bearing for the same reason
// it is in [NewKMSEnvelope] / [NewGCPKMSEnvelope].
func NewAzureKMSEnvelope(ctx context.Context, keyID string, opts ...AzureKMSOption) (*AzureKMSEnvelope, error) {
	if strings.TrimSpace(keyID) == "" {
		return nil, errors.New("crypto: Azure Key Vault key ID is empty")
	}

	o := &azureKMSOptions{wrapAlgorithm: DefaultAzureWrapAlgorithm}
	for _, opt := range opts {
		opt(o)
	}
	if string(o.wrapAlgorithm) == "" {
		o.wrapAlgorithm = DefaultAzureWrapAlgorithm
	}

	vaultURL, name, version, err := parseAzureKeyID(keyID)
	if err != nil {
		return nil, err
	}

	env := &AzureKMSEnvelope{
		keyID:           keyID,
		vaultURL:        vaultURL,
		keyName:         name,
		keyVersion:      version,
		versionExplicit: version != "",
		wrapAlgorithm:   o.wrapAlgorithm,
	}

	if o.client != nil {
		env.client = o.client
	} else {
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
		// Mirror the AWS preflight DescribeKey policy timing — give
		// the client a short retry on transient 5xx but don't burn
		// minutes on flaky networks.
		if clientOpts.Retry.MaxRetries == 0 {
			clientOpts.Retry = policy.RetryOptions{MaxRetries: 3}
		}
		client, err := azkeys.NewClient(vaultURL, cred, clientOpts)
		if err != nil {
			return nil, fmt.Errorf("crypto: build Azure Key Vault client: %w", err)
		}
		env.client = client
	}

	if !o.skipPreflight {
		resp, err := env.client.GetKey(ctx, name, version, nil)
		if err != nil {
			return nil, translateAzureKMSError(err, keyID, "describe")
		}
		// Pin the concrete key version now (audit N-9): with an
		// unversioned operator URL, GetKey resolved "latest" to an
		// exact version — adopt it so every wrap this run performs
		// lands on ONE version and the manifest can record it
		// (ResolvedKEKRef). Versioned operator URLs pass through
		// unchanged (GetKey echoes the same version back).
		if resp.Key != nil && resp.Key.KID != nil {
			env.pinKeyVersion(resp.Key.KID.Version())
		}
	}
	return env, nil
}

// pinKeyVersion records version as the envelope's concrete key-version
// target, once: the first non-empty pin wins and later calls are
// no-ops (wraps that already happened used the first pin). Empty
// version is ignored.
func (e *AzureKMSEnvelope) pinKeyVersion(version string) {
	if version == "" {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.keyVersion == "" {
		e.keyVersion = version
	}
}

// currentKeyVersion returns the pinned (or operator-supplied) key
// version, or "" when unresolved ("latest").
func (e *AzureKMSEnvelope) currentKeyVersion() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.keyVersion
}

// Mode returns [KEKModeAzureKMS] — the tag recorded in
// [backup.ChainEncryption.KEKMode] for Azure-KMS-encrypted chains.
func (e *AzureKMSEnvelope) Mode() string { return KEKModeAzureKMS }

// KeyID returns the full Key Vault key identifier URL the envelope
// was constructed with. Exposed so callers (orchestrator's
// [backup.ChainEncryption.KEKRef] populater) can record it on the
// manifest without re-asking the operator.
func (e *AzureKMSEnvelope) KeyID() string { return e.keyID }

// WrapCEK encrypts cek by routing through the service's WrapKey RPC
// against the envelope's key. The returned bytes are the service's
// Result field — an opaque byte slice that UnwrapKey round-trips
// back to the original plaintext.
//
// The response's KID names the exact key VERSION the service wrapped
// under; it is pinned on the envelope so (a) every later wrap this run
// performs stays on the same version even if the key rotates mid-run,
// and (b) [AzureKMSEnvelope.ResolvedKEKRef] can hand the orchestrator
// a versioned reference to record in the manifest (audit N-9).
func (e *AzureKMSEnvelope) WrapCEK(cek []byte) ([]byte, error) {
	if len(cek) != CEKLen {
		return nil, fmt.Errorf("crypto: azure kms wrap cek length %d != %d", len(cek), CEKLen)
	}
	alg := e.wrapAlgorithm
	out, err := e.client.WrapKey(context.Background(), e.keyName, e.currentKeyVersion(),
		azkeys.KeyOperationParameters{
			Algorithm: &alg,
			Value:     cek,
		}, nil)
	if err != nil {
		return nil, translateAzureKMSError(err, e.keyID, "encrypt")
	}
	if len(out.Result) == 0 {
		return nil, errors.New("crypto: azure kms wrap returned empty Result")
	}
	if out.KID != nil {
		e.pinKeyVersion(out.KID.Version())
	}
	return out.Result, nil
}

// UnwrapCEK is the inverse of WrapCEK: hands the wrapped bytes to
// the service's UnwrapKey RPC, returns the plaintext CEK.
//
// Version targeting (audit N-9): Azure key-wrap ciphertext carries NO
// key-version metadata — unlike AWS KMS / GCP KMS, whose ciphertexts
// self-identify their wrapping key version — so the unwrap targets
// whatever version this envelope is pinned at. FormatVersion-5+
// manifests record the wrap-time versioned key URL in
// [backup.ChainEncryption.KEKRef] and the orchestrator retargets the
// envelope at it via [AzureKMSEnvelope.RebindChainKEKRef] before
// unwrapping. When no version is known (a pre-FormatVersion-5
// manifest + an unversioned operator URL + no preflight resolution),
// the unwrap falls to the vault's LATEST version — correct until the
// key auto-rotates, after which restores of older chains fail with an
// auth error; a WARN names that hazard up front.
func (e *AzureKMSEnvelope) UnwrapCEK(wrapped []byte) ([]byte, error) {
	if len(wrapped) == 0 {
		return nil, errors.New("crypto: azure kms unwrap wrapped bytes are empty")
	}
	e.warnIfUnversionedUnwrap()
	alg := e.wrapAlgorithm
	out, err := e.client.UnwrapKey(context.Background(), e.keyName, e.currentKeyVersion(),
		azkeys.KeyOperationParameters{
			Algorithm: &alg,
			Value:     wrapped,
		}, nil)
	if err != nil {
		return nil, translateAzureKMSError(err, e.keyID, "decrypt")
	}
	if len(out.Result) != CEKLen {
		return nil, fmt.Errorf("crypto: azure kms unwrap returned plaintext of length %d (want %d)", len(out.Result), CEKLen)
	}
	return out.Result, nil
}

// ResolvedKEKRef implements [ResolvedKEKReferencer]: the versioned Key
// Vault key URL the envelope's wraps actually target, or "" when no
// concrete version is known yet. The orchestrator records this in
// [backup.ChainEncryption.KEKRef] so restore-side unwraps can be
// retargeted at the wrap-time version after key rotation.
func (e *AzureKMSEnvelope) ResolvedKEKRef() string {
	v := e.currentKeyVersion()
	if v == "" {
		return ""
	}
	return e.vaultURL + "/keys/" + e.keyName + "/" + v
}

// RebindChainKEKRef implements [ChainKEKRebinder]: retarget this
// envelope's wrap/unwrap at the key VERSION recordedRef names — the
// version the chain's CEK was actually wrapped under. Advisory by
// contract (the ref comes from an unauthenticated manifest): a
// malformed / different-key ref, or a conflict with an
// operator-explicit version, logs a WARN and keeps the current target
// — the unwrap then fails loudly if that target is wrong. A recorded
// ref without a version (pre-FormatVersion-5 manifests) keeps the
// current behavior and lets the unwrap-time rotation-hazard WARN fire.
func (e *AzureKMSEnvelope) RebindChainKEKRef(recordedRef string) {
	if recordedRef == "" {
		return
	}
	vault, name, version, err := parseAzureKeyID(recordedRef)
	if err != nil {
		slog.Warn("crypto: azure kms: manifest's recorded kek_ref is not a parseable Key Vault key URL; keeping the operator-supplied key target",
			slog.String("recorded_kek_ref", recordedRef), slog.String("err", err.Error()))
		return
	}
	if !strings.EqualFold(vault, e.vaultURL) || name != e.keyName {
		slog.Warn("crypto: azure kms: manifest's recorded kek_ref names a different vault/key than the operator-supplied one; keeping the operator's (the unwrap fails loudly if it is the wrong key)",
			slog.String("recorded_kek_ref", recordedRef), slog.String("operator_key_id", e.keyID))
		return
	}
	if version == "" {
		// Old manifest (recorded before the versioned-KEKRef stamp):
		// nothing to retarget at; the unwrap-time WARN covers the
		// rotation hazard.
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.versionExplicit {
		if e.keyVersion != version {
			slog.Warn("crypto: azure kms: operator-supplied key version differs from the chain's recorded wrap-time version; honoring the operator's",
				slog.String("operator_version", e.keyVersion), slog.String("recorded_version", version))
		}
		return
	}
	e.keyVersion = version
}

// warnIfUnversionedUnwrap emits the once-per-envelope rotation-hazard
// WARN when an unwrap is about to target the vault's latest key
// version because no wrap-time version is known (audit N-9's failure
// mode: enable key auto-rotation and restores of older chains fail
// with a misleading auth error).
func (e *AzureKMSEnvelope) warnIfUnversionedUnwrap() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.keyVersion != "" || e.warnedUnversioned {
		return
	}
	e.warnedUnversioned = true
	slog.Warn("crypto: azure kms: unwrapping against the key's LATEST version — this chain's manifest records no wrap-time key version (written before sluice recorded versioned Azure kek_refs) and the supplied --azure-key-vault-id names none. If the key has auto-rotated since the backup, the unwrap will fail with an auth error; pass the wrap-time version explicitly (https://VAULT.vault.azure.net/keys/KEY/VERSION) to recover",
		slog.String("key_id", e.keyID))
}

// parseAzureKeyID validates and decomposes an Azure Key Vault key
// identifier URL into its vault base URL, key name, and key version.
// Acceptable shapes per the Key Vault REST API spec:
//
//	https://VAULT.vault.azure.net/keys/KEY
//	https://VAULT.vault.azure.net/keys/KEY/VERSION
//	https://VAULT.managedhsm.azure.net/keys/KEY[/VERSION]
//
// Returns a clear error for malformed input — operators see this at
// flag-validation time, before any network call.
func parseAzureKeyID(keyID string) (vaultURL, name, version string, err error) {
	parsed, perr := url.Parse(keyID)
	if perr != nil {
		return "", "", "", fmt.Errorf("crypto: parse Azure Key Vault key ID %q: %w", keyID, perr)
	}
	if parsed.Scheme != "https" {
		return "", "", "", fmt.Errorf("crypto: Azure Key Vault key ID %q must use https scheme", keyID)
	}
	if parsed.Host == "" {
		return "", "", "", fmt.Errorf("crypto: Azure Key Vault key ID %q has no host", keyID)
	}
	// Path shape: /keys/KEY[/VERSION].
	trimmed := strings.Trim(parsed.Path, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 2 || parts[0] != "keys" {
		return "", "", "", fmt.Errorf("crypto: Azure Key Vault key ID %q path must start with /keys/", keyID)
	}
	name = parts[1]
	if name == "" {
		return "", "", "", fmt.Errorf("crypto: Azure Key Vault key ID %q has empty key name", keyID)
	}
	if len(parts) >= 3 {
		version = parts[2]
	}
	vaultURL = parsed.Scheme + "://" + parsed.Host
	return vaultURL, name, version, nil
}

// translateAzureKMSError maps a raw Azure SDK error from a Key Vault
// call to an operator-actionable message. The SDK surfaces errors
// as *azcore.ResponseError carrying an ErrorCode the service-side
// returned ("KeyNotFound", "Forbidden", "BadParameter", etc.) which
// we branch on. The op argument names which call failed (encrypt /
// decrypt / describe) so the resulting error pinpoints the failing
// phase.
//
// Errors that don't carry an azcore.ResponseError shape (transport,
// auth issues from azidentity) fall through with the key ID + op
// preserved so support can correlate against Key Vault request logs.
func translateAzureKMSError(err error, keyID, op string) error {
	if err == nil {
		return nil
	}
	var resp *azcore.ResponseError
	if errors.As(err, &resp) {
		switch resp.ErrorCode {
		case "KeyNotFound":
			return fmt.Errorf("crypto: azure kms %s failed: key %q not found (verify the key identifier URL + the role assignment grants 'Key Vault Crypto User' or equivalent). underlying: %w",
				op, keyID, err)
		case "Forbidden", "AccessDenied":
			return fmt.Errorf("crypto: azure kms %s denied: principal lacks the required role on key %q (grant 'Key Vault Crypto %s' — see https://learn.microsoft.com/azure/key-vault/general/rbac-guide). underlying: %w",
				op, keyID, azureRoleForOp(op), err)
		case "BadParameter":
			return fmt.Errorf("crypto: azure kms %s rejected: bad parameter for key %q (verify the wrap algorithm matches the key type — RSA keys default to RSA-OAEP-256; AES keys need --azure-wrap-algorithm=A256KW). underlying: %w",
				op, keyID, err)
		case "KeyDisabled":
			return fmt.Errorf("crypto: azure kms %s rejected: key %q is disabled (re-enable via az keyvault key set-attributes --enabled true). underlying: %w",
				op, keyID, err)
		}
		// Status-code fallback for cases where the ErrorCode field is
		// empty (some Key Vault errors don't populate it).
		switch resp.StatusCode {
		case 401:
			return fmt.Errorf("crypto: azure kms %s denied: no valid credentials (run `az login` or set AZURE_CLIENT_ID/AZURE_TENANT_ID/AZURE_CLIENT_SECRET). underlying: %w", op, err)
		case 403:
			return fmt.Errorf("crypto: azure kms %s denied: principal lacks access on key %q (grant Key Vault Crypto User role). underlying: %w", op, keyID, err)
		case 404:
			return fmt.Errorf("crypto: azure kms %s failed: key %q not found. underlying: %w", op, keyID, err)
		}
	}
	return fmt.Errorf("crypto: azure kms %s failed (key=%q): %w", op, keyID, err)
}

// azureRoleForOp returns the Azure RBAC role suffix that matches op
// for the operator's recovery hint. Azure Key Vault's modern RBAC
// uses "Key Vault Crypto User", "Key Vault Crypto Officer", etc.;
// the actual minimum-privilege role for these ops is "Key Vault
// Crypto User" (which grants wrap/unwrap + sign/verify + get).
func azureRoleForOp(op string) string {
	switch op {
	case "encrypt", "decrypt":
		return "User" // → "Key Vault Crypto User"
	case "describe":
		return "Reader" // → "Key Vault Reader" (less privilege than User; explicit get-only role)
	default:
		return "User"
	}
}
