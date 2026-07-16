// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	stdcrypto "crypto"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/progress"
	"sluicesync.dev/sluice/internal/redact"
)

// EncryptionFlags is the shared kong flag set for `--encrypt*` and
// `--encryption-passphrase*` flags. Embedded into the backup / restore
// / sync subcommand structs so every command surface that touches a
// chain accepts the same shape. Phase 6.1 covers passphrase mode;
// Phase 6.2/6.3 will add `--kms-key-arn` etc. via additional flags
// here.
type EncryptionFlags struct {
	Encrypt bool `name:"encrypt" help:"Enable client-side envelope encryption. Backup paths require a passphrase source (--encryption-passphrase, --encryption-passphrase-env, or --encryption-passphrase-file) OR a KMS key (--kms-key-arn). Restore / broker paths read the same flag and supply the operator's key material to unwrap the chain's CEK."`

	EncryptionPassphrase     string `name:"encryption-passphrase" help:"Encryption passphrase (DEPRECATED for production — passphrase shows up in shell history; prefer --encryption-passphrase-env or --encryption-passphrase-file)." placeholder:"PASS"`
	EncryptionPassphraseEnv  string `name:"encryption-passphrase-env" help:"Read encryption passphrase from this environment variable. Recommended over --encryption-passphrase for production." placeholder:"VAR"`
	EncryptionPassphraseFile string `name:"encryption-passphrase-file" help:"Read encryption passphrase from this file path. Recommended for secrets-management integrations (1Password CLI, AWS Secrets Manager, etc.)." placeholder:"PATH"`

	KMSKeyARN string `name:"kms-key-arn" help:"AWS KMS key ARN, alias ARN, or alias/name for envelope encryption (Phase 6.2). Mutually exclusive with the other --*-key flags. Sluice routes CEK wrap/unwrap through KMS Encrypt/Decrypt; the KMS root key never leaves AWS." placeholder:"ARN"`
	KMSRegion string `name:"kms-region" help:"AWS region override for KMS calls. Defaults to AWS_REGION env var or the SDK's default region resolution." placeholder:"REGION"`

	GCPKMSKeyResource string `name:"gcp-kms-key-resource" help:"GCP Cloud KMS crypto-key resource name for envelope encryption (Phase 6.3). Format: projects/PROJECT/locations/LOCATION/keyRings/RING/cryptoKeys/KEY (optionally with /cryptoKeyVersions/VERSION). Mutually exclusive with other --*-key flags. Auth via Application Default Credentials (gcloud auth application-default login or GOOGLE_APPLICATION_CREDENTIALS)." placeholder:"RESOURCE"`

	AzureKeyVaultID    string `name:"azure-key-vault-id" help:"Azure Key Vault key identifier URL for envelope encryption (Phase 6.3). Format: https://VAULT.vault.azure.net/keys/KEY[/VERSION] (or managedhsm.azure.net for HSM-backed vaults). Mutually exclusive with other --*-key flags. Auth via DefaultAzureCredential (az login, managed identity, or AZURE_* env vars)." placeholder:"URL"`
	AzureWrapAlgorithm string `name:"azure-wrap-algorithm" help:"Override the Azure Key Vault wrap algorithm. Defaults to RSA-OAEP-256 (works for software-protected RSA keys). HSM-backed AES keys need 'A256KW'." placeholder:"ALG"`

	// No kong default: an OMITTED --encrypt-mode must stay empty so the
	// backup orchestrator can distinguish "operator didn't choose" (inherit
	// the chain's mode when extending an encrypted chain; default to
	// per-chain for a fresh full) from an explicit choice (which is enforced
	// against the chain's mode — Bug 179/180). A kong default of "per-chain"
	// made omission indistinguishable from an explicit per-chain and left
	// the inherit path unreachable. The trailing comma admits the empty value.
	EncryptMode string `name:"encrypt-mode" enum:"per-chain,per-chunk," default:"" help:"Encryption mode. Omit to inherit an existing chain's mode (a fresh full defaults to per-chain): 'per-chain' (single CEK per chain; one KEK derive / KMS Decrypt per restore) or 'per-chunk' (one CEK per chunk; defense-in-depth at the cost of per-chunk wrap). Must match the chain's mode when extending an encrypted chain."`

	// ADR-0154 Phase 1 signing. Sign gates the WRITE side (backup full /
	// incremental); RequireSignature gates the READ side (restore /
	// verify) strict-always policy.
	Sign             bool `name:"sign" help:"Sign the backup manifest + lineage catalog with a detached HMAC-SHA-256 keyed off the chain KEK (ADR-0154 Phase 1). Requires --encrypt (HMAC-off-KEK signs only encrypted chains) with a passphrase (to sign a KMS-encrypted chain, or for plaintext / key-separated verification, use --sign-key with an Ed25519 key or 'kms://...' instead). Extending an already-signed chain signs automatically."`
	RequireSignature bool `name:"require-signature" help:"Strict-always signature policy on restore/verify: a signed (v6) backup that cannot be verified (no matching key supplied) is refused rather than warned. An INVALID signature is always refused regardless of this flag. Leave off for the DR-safe default (never fail a restore for a signature it cannot check)."`

	// ADR-0154 Phase 2/3 (asymmetric signing). SignKey gates the WRITE side
	// (backup full / incremental) and EXPLICITLY selects the Ed25519 (Phase
	// 2) or KMS (Phase 3) scheme; VerifyKey gates the READ side (restore /
	// verify) and is REQUIRED to verify an asymmetrically-signed chain (the
	// KEK does not verify an asymmetric signature). Both accept `env:VAR`, a
	// file path, or a `kms://<provider>/<key-ref>` reference.
	SignKey   string `name:"sign-key" help:"Sign the backup with an Ed25519 PRIVATE key (ADR-0154 Phase 2, PKCS#8 PEM), OR via a cloud KMS key given as 'kms://<provider>/<key-ref>' (Phase 3 — the private key stays in the HSM; needs the provider's Sign permission on a signing key, which may differ from the --kms-key-arn encryption key). Providers: 'aws' (key ARN/id; ECDSA P-256/384/521 or RSA-PSS), 'gcp' (a versioned .../cryptoKeyVersions/N resource; ECDSA P-256/384, RSA-PSS, or Ed25519), 'azure' (a Key Vault key-identifier URL; ECDSA P-256/384/521 or RSA-PSS). Selects the asymmetric scheme over the --sign HMAC-off-KEK default; works on BOTH plaintext AND encrypted backups. Accepts a file path, 'env:VAR', or 'kms://...'. Generate an Ed25519 pair with 'sluice backup keygen'. Never logged. Mutually exclusive with --sign." placeholder:"PATH|env:VAR|kms://..."`
	VerifyKey string `name:"verify-key" help:"PUBLIC key that verifies an asymmetrically-signed chain (ADR-0154 Phase 2/3) on restore / verify — an SPKI PEM file (Ed25519 / ECDSA / RSA; the offline DR path, verified locally per the recorded algorithm) OR 'kms://<provider>/<key-ref>' ('aws' / 'gcp' / 'azure') to fetch the trusted key's public half online. REQUIRED for such a chain — the KEK does NOT verify an asymmetric signature, even on an encrypted chain. The recorded manifest key reference is NEVER trusted; verification anchors on the key YOU name here. Absent it, the chain WARNs present-but-unverified and proceeds (DR-safe) unless --require-signature. Accepts a file path, 'env:VAR', or 'kms://...'." placeholder:"PATH|env:VAR|kms://..."`
}

// readKeyMaterial resolves a `--sign-key` / `--verify-key` spec: `env:VAR`
// reads the named environment variable (the recommended form — the key
// bytes never land in shell history or a bundle), anything else is a file
// path. The RESOLVED bytes are a signing secret for a private key; the
// SPEC itself (a path or a variable NAME) is not, so it is safe to log.
func readKeyMaterial(spec string) ([]byte, error) {
	if v, ok := strings.CutPrefix(spec, "env:"); ok {
		val := os.Getenv(v)
		if val == "" {
			return nil, fmt.Errorf("key source env:%s: environment variable is empty", v)
		}
		return []byte(val), nil
	}
	raw, err := os.ReadFile(spec)
	if err != nil {
		return nil, fmt.Errorf("key source %q: %w", spec, err)
	}
	return raw, nil
}

// resolveWriteSigner is the ONLY write-side (`backup full` / `backup
// incremental`) entry to --sign-key resolution: it refuses the
// --sign + --sign-key combo BEFORE building anything, then delegates to
// [EncryptionFlags.buildSignKeySigner]. The ordering is load-bearing
// (audit TEST-F3 T-2): a `--sign-key kms://...` builds a cloud KMS
// client, so an invalid flag combo must refuse before any KMS
// credential resolution / network attempt — previously the exclusivity
// check ran on the BUILT signer, after that attempt.
func (e *EncryptionFlags) resolveWriteSigner() (*lineage.Signer, error) {
	if e.Sign && e.SignKey != "" {
		return nil, errors.New("--sign (HMAC-off-KEK) and --sign-key (Ed25519 / kms://) are mutually exclusive — choose one signing scheme")
	}
	return e.buildSignKeySigner()
}

// buildSignKeySigner builds the write-side signer from --sign-key, or nil
// when the flag is unset. A `kms://<provider>/<keyref>` value selects the
// KMS scheme (ADR-0154 Phase 3 — the private key stays in the HSM); any
// other value is an Ed25519 PKCS#8 PEM private key (Phase 2). The private
// key / KMS reference is never logged.
func (e *EncryptionFlags) buildSignKeySigner() (*lineage.Signer, error) {
	if e.SignKey == "" {
		return nil, nil
	}
	if isKMSURI(e.SignKey) {
		provider, keyRef, err := parseKMSURI(e.SignKey)
		if err != nil {
			return nil, fmt.Errorf("--sign-key: %w", err)
		}
		signer, err := buildKMSSigner(provider, keyRef, e.KMSRegion)
		if err != nil {
			return nil, fmt.Errorf("--sign-key: %w", err)
		}
		return signer, nil
	}
	raw, err := readKeyMaterial(e.SignKey)
	if err != nil {
		return nil, fmt.Errorf("--sign-key: %w", err)
	}
	priv, err := crypto.ParseEd25519PrivateKeyPEM(raw)
	if err != nil {
		return nil, fmt.Errorf("--sign-key: %w", err)
	}
	return lineage.NewEd25519Signer(priv), nil
}

// resolveVerifyKey resolves the asymmetric PUBLIC verify key from
// --verify-key, or nil when unset. A `kms://<provider>/<keyref>` value
// fetches the public half of the OPERATOR-NAMED trusted key via the
// provider's GetPublicKey (online); any other value is an SPKI PEM public
// key file (offline DR path) parsed for Ed25519 / ECDSA / RSA. The
// verification primitive is chosen from the chain's SIGNED algorithm, not
// from this key — so a rewritten manifest key reference cannot redirect
// trust to a different key.
func (e *EncryptionFlags) resolveVerifyKey() (stdcrypto.PublicKey, error) {
	if e.VerifyKey == "" {
		return nil, nil
	}
	if isKMSURI(e.VerifyKey) {
		provider, keyRef, err := parseKMSURI(e.VerifyKey)
		if err != nil {
			return nil, fmt.Errorf("--verify-key: %w", err)
		}
		pub, err := fetchKMSPublicKey(provider, keyRef, e.KMSRegion)
		if err != nil {
			return nil, fmt.Errorf("--verify-key: %w", err)
		}
		return pub, nil
	}
	raw, err := readKeyMaterial(e.VerifyKey)
	if err != nil {
		return nil, fmt.Errorf("--verify-key: %w", err)
	}
	pub, err := crypto.ParseManifestPublicKeyPEM(raw)
	if err != nil {
		return nil, fmt.Errorf("--verify-key: %w", err)
	}
	return pub, nil
}

// kmsSignURIScheme is the prefix that selects a KMS keystore for
// --sign-key / --verify-key: `kms://<provider>/<keyref>`. The provider is
// the segment before the first `/`; the key reference is everything after
// (ARNs contain their own `:` and `/`, so only the FIRST separator splits
// provider from ref). Phase 3a supports only `aws`.
const kmsSignURIScheme = "kms://"

// isKMSURI reports whether spec selects a KMS keystore.
func isKMSURI(spec string) bool {
	return strings.HasPrefix(spec, kmsSignURIScheme)
}

// parseKMSURI splits a `kms://<provider>/<keyref>` value into its provider
// and key reference. The key reference is passed VERBATIM to the provider
// (an AWS key ID / ARN / alias), so only the first `/` after the provider
// is consumed.
func parseKMSURI(spec string) (provider, keyRef string, err error) {
	rest := strings.TrimPrefix(spec, kmsSignURIScheme)
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 || slash == len(rest)-1 {
		return "", "", fmt.Errorf("malformed kms:// reference %q (expected kms://<provider>/<key-ref>, e.g. kms://aws/arn:aws:kms:...)", spec)
	}
	return rest[:slash], rest[slash+1:], nil
}

// buildKMSSigner constructs the write-side KMS signer for provider. AWS
// (Phase 3a), GCP + Azure (Phase 3b). The key reference is the provider's
// native form: an AWS key ARN/id, a GCP versioned CryptoKeyVersion
// resource, or an Azure Key Vault key-identifier URL. `region` is
// AWS-only (GCP bakes the location into the resource, Azure into the vault
// URL).
func buildKMSSigner(provider, keyRef, region string) (*lineage.Signer, error) {
	switch provider {
	case "aws":
		adapter, err := crypto.NewAWSKMSSigner(kongContext(), keyRef, awsKMSSignOpts(region)...)
		if err != nil {
			return nil, err
		}
		return lineage.NewKMSSigner(adapter), nil
	case "gcp":
		adapter, err := crypto.NewGCPKMSSigner(kongContext(), keyRef)
		if err != nil {
			return nil, err
		}
		return lineage.NewKMSSigner(adapter), nil
	case "azure":
		adapter, err := crypto.NewAzureKMSSigner(kongContext(), keyRef)
		if err != nil {
			return nil, err
		}
		return lineage.NewKMSSigner(adapter), nil
	default:
		return nil, fmt.Errorf("unknown kms:// signing provider %q (expected 'aws', 'gcp', or 'azure')", provider)
	}
}

// fetchKMSPublicKey resolves the public half of the operator-named trusted
// KMS key for the online `--verify-key kms://...` path.
func fetchKMSPublicKey(provider, keyRef, region string) (stdcrypto.PublicKey, error) {
	switch provider {
	case "aws":
		return crypto.FetchAWSKMSPublicKey(kongContext(), keyRef, awsKMSSignOpts(region)...)
	case "gcp":
		return crypto.FetchGCPKMSPublicKey(kongContext(), keyRef)
	case "azure":
		return crypto.FetchAzureKMSPublicKey(kongContext(), keyRef)
	default:
		return nil, fmt.Errorf("unknown kms:// verify provider %q (expected 'aws', 'gcp', or 'azure')", provider)
	}
}

// awsKMSSignOpts builds the AWS-signer option slice, forwarding the shared
// --kms-region flag.
func awsKMSSignOpts(region string) []crypto.AWSKMSSignerOption {
	if region == "" {
		return nil
	}
	return []crypto.AWSKMSSignerOption{crypto.WithAWSKMSSignRegion(region)}
}

// resolvePassphrase returns the operator's passphrase from whichever
// source they chose, or an error if zero or more-than-one source was
// supplied. Empty passphrases (env var / file is empty) are also
// treated as an operator error.
func (e *EncryptionFlags) resolvePassphrase() (string, error) {
	count := 0
	if e.EncryptionPassphrase != "" {
		count++
	}
	if e.EncryptionPassphraseEnv != "" {
		count++
	}
	if e.EncryptionPassphraseFile != "" {
		count++
	}
	if count == 0 {
		return "", errors.New("--encrypt requires one of --encryption-passphrase, --encryption-passphrase-env, or --encryption-passphrase-file")
	}
	if count > 1 {
		return "", errors.New("--encryption-passphrase, --encryption-passphrase-env, and --encryption-passphrase-file are mutually exclusive")
	}
	switch {
	case e.EncryptionPassphrase != "":
		return e.EncryptionPassphrase, nil
	case e.EncryptionPassphraseEnv != "":
		v := os.Getenv(e.EncryptionPassphraseEnv)
		if v == "" {
			return "", fmt.Errorf("--encryption-passphrase-env=%s: environment variable is empty", e.EncryptionPassphraseEnv)
		}
		return v, nil
	case e.EncryptionPassphraseFile != "":
		raw, err := os.ReadFile(e.EncryptionPassphraseFile)
		if err != nil {
			return "", fmt.Errorf("--encryption-passphrase-file=%s: %w", e.EncryptionPassphraseFile, err)
		}
		// Trim trailing whitespace (operators commonly pipe `echo
		// passphrase > file`, leaving a trailing newline).
		v := strings.TrimRight(string(raw), "\r\n\t ")
		if v == "" {
			return "", fmt.Errorf("--encryption-passphrase-file=%s: file is empty", e.EncryptionPassphraseFile)
		}
		return v, nil
	}
	return "", errors.New("internal: passphrase source resolution fell through")
}

// buildBackupEncryption constructs a [lineage.BackupEncryption] for
// the write side (backup full / incremental / stream) using whichever
// key source the operator supplied (passphrase or AWS KMS). Returns
// nil when e.Encrypt is false (plaintext backup).
//
// Bug 43 (v0.22.1): for passphrase mode, the returned struct's
// RebuildForChain field captures the operator's passphrase in a
// closure so the orchestrator can rebuild the envelope against the
// chain root's recorded Argon2id salt when extending an existing
// encrypted chain. KMS mode (Phase 6.2) leaves RebuildForChain nil —
// KMS unwrap doesn't depend on a chain-recorded salt; the orchestrator's
// `rebindForChain` is a no-op for it.
func (e *EncryptionFlags) buildBackupEncryption() (*lineage.BackupEncryption, error) {
	if !e.Encrypt {
		// Sanity: passphrase / KMS-key supplied without --encrypt is suspicious.
		if e.EncryptionPassphrase != "" || e.EncryptionPassphraseEnv != "" || e.EncryptionPassphraseFile != "" {
			return nil, errors.New("--encryption-passphrase* is set but --encrypt is not; pass --encrypt to enable encryption")
		}
		if e.KMSKeyARN != "" {
			return nil, errors.New("--kms-key-arn is set but --encrypt is not; pass --encrypt to enable encryption")
		}
		if e.GCPKMSKeyResource != "" {
			return nil, errors.New("--gcp-kms-key-resource is set but --encrypt is not; pass --encrypt to enable encryption")
		}
		if e.AzureKeyVaultID != "" {
			return nil, errors.New("--azure-key-vault-id is set but --encrypt is not; pass --encrypt to enable encryption")
		}
		return nil, nil
	}
	if err := e.validateKeySources(); err != nil {
		return nil, err
	}
	// Pass an omitted mode through as "" — the orchestrator resolves it
	// (inherit the chain's mode / default a fresh full to per-chain). Do NOT
	// collapse it to per-chain here; that is the Bug 180 defect that made the
	// inherit branch unreachable from the CLI.
	mode := e.EncryptMode
	switch {
	case e.KMSKeyARN != "":
		env, err := crypto.NewKMSEnvelope(kongContext(), e.KMSKeyARN, kmsOpts(e.KMSRegion)...)
		if err != nil {
			return nil, fmt.Errorf("encryption: build aws kms envelope: %w", err)
		}
		return &lineage.BackupEncryption{
			Envelope: env,
			Mode:     mode,
			KEKRef:   e.KMSKeyARN,
		}, nil
	case e.GCPKMSKeyResource != "":
		env, err := crypto.NewGCPKMSEnvelope(kongContext(), e.GCPKMSKeyResource)
		if err != nil {
			return nil, fmt.Errorf("encryption: build gcp kms envelope: %w", err)
		}
		return &lineage.BackupEncryption{
			Envelope: env,
			Mode:     mode,
			KEKRef:   e.GCPKMSKeyResource,
		}, nil
	case e.AzureKeyVaultID != "":
		env, err := crypto.NewAzureKMSEnvelope(kongContext(), e.AzureKeyVaultID, azureKMSOpts(e.AzureWrapAlgorithm)...)
		if err != nil {
			return nil, fmt.Errorf("encryption: build azure kms envelope: %w", err)
		}
		return &lineage.BackupEncryption{
			Envelope: env,
			Mode:     mode,
			KEKRef:   e.AzureKeyVaultID,
		}, nil
	}
	passphrase, err := e.resolvePassphrase()
	if err != nil {
		return nil, err
	}
	params, err := crypto.DefaultArgon2idParams()
	if err != nil {
		return nil, fmt.Errorf("encryption: argon2id params: %w", err)
	}
	env, err := crypto.NewPassphraseEnvelope(passphrase, params)
	if err != nil {
		return nil, fmt.Errorf("encryption: build envelope: %w", err)
	}
	return &lineage.BackupEncryption{
		Envelope:        env,
		RebuildForChain: passphraseRebuildForChain(passphrase),
		Mode:            mode,
	}, nil
}

// validateKeySources enforces mutual exclusion between the
// passphrase-mode flag family and the KMS-mode flag(s). Operators who
// pass both get a clear error before any envelope-building work
// happens.
func (e *EncryptionFlags) validateKeySources() error {
	hasPassphrase := e.EncryptionPassphrase != "" || e.EncryptionPassphraseEnv != "" || e.EncryptionPassphraseFile != ""
	hasAWSKMS := e.KMSKeyARN != ""
	hasGCPKMS := e.GCPKMSKeyResource != ""
	hasAzureKMS := e.AzureKeyVaultID != ""
	count := 0
	for _, v := range []bool{hasPassphrase, hasAWSKMS, hasGCPKMS, hasAzureKMS} {
		if v {
			count++
		}
	}
	if count > 1 {
		return errors.New("--encryption-passphrase{,-env,-file}, --kms-key-arn, --gcp-kms-key-resource, and --azure-key-vault-id are mutually exclusive")
	}
	if count == 0 {
		return errors.New("--encrypt requires one of --encryption-passphrase{,-env,-file}, --kms-key-arn, --gcp-kms-key-resource, or --azure-key-vault-id")
	}
	return nil
}

// kmsOpts builds the [crypto.KMSOption] slice for the AWS path. Only
// the region override is operator-facing; tests construct envelopes
// via [crypto.WithKMSClient] directly without going through the CLI
// builder.
func kmsOpts(region string) []crypto.KMSOption {
	if region == "" {
		return nil
	}
	return []crypto.KMSOption{crypto.WithKMSRegion(region)}
}

// azureKMSOpts builds the [crypto.AzureKMSOption] slice for the
// Azure path. Today only the wrap-algorithm override is
// operator-facing; tests construct envelopes via
// [crypto.WithAzureKMSClient] directly.
//
// The wrap-algorithm string is a verbatim Key Vault algorithm name
// (RSA-OAEP, RSA-OAEP-256, A256KW, etc.) — the Azure SDK accepts
// the type as `azkeys.EncryptionAlgorithm`, which is a typed string
// alias. We pass through whatever the operator typed; an invalid
// algorithm name surfaces as a BadParameter from the service.
func azureKMSOpts(wrapAlgorithm string) []crypto.AzureKMSOption {
	if wrapAlgorithm == "" {
		return nil
	}
	return []crypto.AzureKMSOption{crypto.WithAzureWrapAlgorithmString(wrapAlgorithm)}
}

// passphraseRebuildForChain returns a builder closure that the
// pipeline orchestrator calls when extending an existing encrypted
// chain. The closure derives a fresh KEK from the operator's
// passphrase + the chain's recorded Argon2id params (salt + cost),
// returning a [crypto.PassphraseEnvelope] whose UnwrapCEK call will
// succeed against the chain's WrappedCEK.
//
// Mirrors the read-side pattern in [EncryptionFlags.buildReadEnvelope]:
// load the recorded params from the chain root, hand the operator's
// passphrase + those params to [crypto.NewPassphraseEnvelope].
func passphraseRebuildForChain(passphrase string) func(*irbackup.Argon2idParams) (crypto.EnvelopeEncryption, error) {
	return func(p *irbackup.Argon2idParams) (crypto.EnvelopeEncryption, error) {
		if p == nil {
			return nil, errors.New("rebuild envelope: chain has no recorded Argon2id params")
		}
		params := crypto.Argon2idParams{
			Salt:        p.Salt,
			Memory:      p.Memory,
			Iterations:  p.Iterations,
			Parallelism: p.Parallelism,
			KeyLen:      p.KeyLen,
		}
		env, err := crypto.NewPassphraseEnvelope(passphrase, params)
		if err != nil {
			return nil, fmt.Errorf("rebuild envelope: %w", err)
		}
		return env, nil
	}
}

// buildReadEnvelope constructs a [crypto.EnvelopeEncryption] for the
// read side (restore / chain restore / broker). For passphrase mode,
// the chain root manifest's recorded Argon2id params are needed to
// re-derive the KEK; the CLI loads them from rootManifest before
// constructing the envelope.
//
// For KMS mode (Phase 6.2), the chain root's KEKRef is the
// operator-recorded ARN; the operator must supply a matching
// --kms-key-arn (and KMS Decrypt does the rest — no params to load
// from the manifest).
//
// Returns nil when --encrypt is false (plaintext chain expected).
func (e *EncryptionFlags) buildReadEnvelope(rootManifest *irbackup.Manifest) (crypto.EnvelopeEncryption, error) {
	if !e.Encrypt {
		// Sanity: chain is encrypted but operator didn't pass
		// --encrypt? The pipeline's preflight returns a clearer error
		// in that case (it knows the kek_mode); leave that path alone.
		return nil, nil
	}
	if err := e.validateKeySources(); err != nil {
		return nil, err
	}
	switch {
	case e.KMSKeyARN != "":
		env, err := crypto.NewKMSEnvelope(kongContext(), e.KMSKeyARN, kmsOpts(e.KMSRegion)...)
		if err != nil {
			return nil, fmt.Errorf("encryption: build aws kms read envelope: %w", err)
		}
		return env, nil
	case e.GCPKMSKeyResource != "":
		env, err := crypto.NewGCPKMSEnvelope(kongContext(), e.GCPKMSKeyResource)
		if err != nil {
			return nil, fmt.Errorf("encryption: build gcp kms read envelope: %w", err)
		}
		return env, nil
	case e.AzureKeyVaultID != "":
		env, err := crypto.NewAzureKMSEnvelope(kongContext(), e.AzureKeyVaultID, azureKMSOpts(e.AzureWrapAlgorithm)...)
		if err != nil {
			return nil, fmt.Errorf("encryption: build azure kms read envelope: %w", err)
		}
		return env, nil
	}
	passphrase, err := e.resolvePassphrase()
	if err != nil {
		return nil, err
	}
	// For passphrase mode, the read-side envelope needs the SAME
	// Argon2id params the writer used (recorded in rootManifest's
	// ChainEncryption.Argon2id). Load them; fall back to
	// DefaultArgon2idParams when the manifest is unencrypted (the
	// envelope still holds — the pipeline preflight is a no-op then).
	var params crypto.Argon2idParams
	if rootManifest != nil && rootManifest.ChainEncryption != nil && rootManifest.ChainEncryption.Argon2id != nil {
		p := rootManifest.ChainEncryption.Argon2id
		params = crypto.Argon2idParams{
			Salt:        p.Salt,
			Memory:      p.Memory,
			Iterations:  p.Iterations,
			Parallelism: p.Parallelism,
			KeyLen:      p.KeyLen,
		}
	} else {
		// Non-encrypted root or no Argon2id params recorded — generate
		// fresh defaults; the pipeline will refuse the chain with a
		// clearer error if the chain is actually encrypted under
		// different params, since the unwrap will fail.
		dp, derr := crypto.DefaultArgon2idParams()
		if derr != nil {
			return nil, fmt.Errorf("encryption: default argon2id params: %w", derr)
		}
		params = dp
	}
	env, err := crypto.NewPassphraseEnvelope(passphrase, params)
	if err != nil {
		return nil, fmt.Errorf("encryption: build read envelope: %w", err)
	}
	return env, nil
}

// buildSigner derives the ADR-0154 signing key for a maintenance
// (compact / prune) run that must re-sign a signed chain. Returns nil
// when --encrypt was not supplied (the pipeline then refuses a signed
// chain loudly) or when the envelope cannot key an HMAC off its KEK
// (KMS — Phase 3). Reads the chain-root manifest for the Argon2id
// params, mirroring buildReadEnvelope.
func (e *EncryptionFlags) buildSigner(ctx context.Context, store irbackup.Store) (*lineage.Signer, error) {
	signer, err := e.buildMaintenanceSigner(ctx, store)
	if err != nil || signer == nil {
		return nil, err
	}
	// A signed chain must be re-signed under its EXISTING scheme — a
	// compact/prune must never convert an Ed25519 chain to HMAC (or vice
	// versa). Refuse loudly on a scheme mismatch before any mutation.
	scheme, ok, err := lineage.ChainSignatureScheme(ctx, store)
	if err != nil {
		return nil, err
	}
	if ok && irbackup.SchemeFamily(scheme) != signer.Scheme {
		return nil, fmt.Errorf("this chain is %q-signed (ADR-0154); re-signing it during compact/prune needs the matching key material (%q supplied) — pass the chain's --encrypt passphrase for hmac-kek, --sign-key <pem> for ed25519, or --sign-key kms://... for kms", scheme, signer.Scheme)
	}
	return signer, nil
}

// buildMaintenanceSigner resolves the re-sign signer from the operator's
// flags: Ed25519 (--sign-key) takes precedence and is independent of
// --encrypt; otherwise HMAC-off-KEK from the chain envelope. nil when
// neither is supplied (a signed chain then refuses to compact/prune).
func (e *EncryptionFlags) buildMaintenanceSigner(ctx context.Context, store irbackup.Store) (*lineage.Signer, error) {
	if e.SignKey != "" {
		return e.buildSignKeySigner()
	}
	if !e.Encrypt {
		return nil, nil
	}
	rootManifest, err := lineage.ReadRootManifest(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("read root manifest for signer: %w", err)
	}
	env, err := e.buildReadEnvelope(rootManifest)
	if err != nil {
		return nil, err
	}
	signer, ok, err := lineage.NewSigner(env)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return signer, nil
}

// BackupCmd groups the backup verbs. Phase 1 shipped `full` and
// `verify`; Phase 3 (v0.17.0) adds `incremental` for chained backups
// taken on top of a previous full or incremental; Phase 4 (v0.19.0)
// adds `stream` for continuous-incremental long-running streams. See
// `docs/dev/design/logical-backups.md`,
// `docs/dev/design/logical-backups-phase-3.md`, and
// `docs/dev/design/logical-backups-phase-4.md` for the staged plan.
type BackupCmd struct {
	Full            BackupFullCmd            `cmd:"" help:"Take a full logical backup of a source database to a local directory."`
	Incremental     BackupIncrementalCmd     `cmd:"" help:"Take an incremental backup chained off a previous full or incremental (Phase 3)."`
	Stream          BackupStreamCmdGroup     `cmd:"" help:"Long-running stream that produces rolling incrementals (Phase 4)."`
	Verify          BackupVerifyCmd          `cmd:"" help:"Re-checksum every chunk in an existing backup chain and report any mismatches."`
	ExportAsParquet BackupExportAsParquetCmd `cmd:"" name:"export-as-parquet" help:"Transcode an existing backup's row chunks into one Parquet file per table (analytics exit surface — ADR-0164). Read-only against the backup store."`
	Prune           BackupPruneCmd           `cmd:"" help:"Drop the oldest incrementals from an existing chain to bound disk + restore time (GitHub #20 chunk 14c)."`
	Compact         BackupCompactCmd         `cmd:"" help:"Concatenate consecutive segments whose CreatedAt gaps fall within --merge-window into a single segment (GitHub #20 chunk 14d, Task #15)."`
	Keygen          BackupKeygenCmd          `cmd:"" help:"Generate an Ed25519 signing keypair for --sign-key / --verify-key (ADR-0154 Phase 2)."`
}

// BackupKeygenCmd runs `sluice backup keygen`. Generates an Ed25519
// keypair and writes the private key (PKCS#8 PEM, 0600) + public key
// (SPKI PEM). The private key signs backups (`--sign-key`); the public
// key — distributable freely — verifies them (`--verify-key`).
type BackupKeygenCmd struct {
	OutDir string `name:"out-dir" help:"Directory to write the keypair into as sluice-sign-key.pem (private, 0600) + sluice-verify-key.pem (public). Created if absent. Mutually exclusive with --priv/--pub." placeholder:"DIR"`
	Priv   string `name:"priv" help:"Explicit path for the PRIVATE key file (PKCS#8 PEM, written 0600). Use with --pub. Mutually exclusive with --out-dir." placeholder:"PATH"`
	Pub    string `name:"pub" help:"Explicit path for the PUBLIC key file (SPKI PEM). Use with --priv. Mutually exclusive with --out-dir." placeholder:"PATH"`
	Force  bool   `name:"force" help:"Overwrite existing key files. By default keygen refuses to clobber an existing private key (losing/replacing it strands the signing of any chain it already signed)."`
}

// Run implements `sluice backup keygen`.
func (k *BackupKeygenCmd) Run(_ *Globals) error {
	privPath, pubPath, err := k.resolvePaths()
	if err != nil {
		return err
	}
	if !k.Force {
		for _, p := range []string{privPath, pubPath} {
			if _, statErr := os.Stat(p); statErr == nil {
				return fmt.Errorf("keygen: %q already exists; pass --force to overwrite (overwriting a private key strands any chain it already signed)", p)
			}
		}
	}
	pub, priv, err := crypto.GenerateEd25519Keypair()
	if err != nil {
		return err
	}
	privPEM, err := crypto.MarshalEd25519PrivateKeyPEM(priv)
	if err != nil {
		return err
	}
	pubPEM, err := crypto.MarshalEd25519PublicKeyPEM(pub)
	if err != nil {
		return err
	}
	if dir := filepath.Dir(privPath); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("keygen: create dir %q: %w", dir, err)
		}
	}
	// Private key 0600 — the whole point of key-separated signing is that
	// this file is the ONLY signing secret; it must not be world-readable.
	if err := os.WriteFile(privPath, privPEM, 0o600); err != nil {
		return fmt.Errorf("keygen: write private key %q: %w", privPath, err)
	}
	if err := os.WriteFile(pubPath, pubPEM, 0o644); err != nil {
		return fmt.Errorf("keygen: write public key %q: %w", pubPath, err)
	}
	slog.Info(
		"backup keygen: wrote Ed25519 keypair (ADR-0154 Phase 2)",
		slog.String("private_key", privPath),
		slog.String("public_key", pubPath),
		slog.String("key_id", crypto.Ed25519KeyID(pub)),
	)
	return nil
}

// resolvePaths validates the flag combination and returns the private +
// public key file paths.
func (k *BackupKeygenCmd) resolvePaths() (priv, pub string, err error) {
	hasOutDir := k.OutDir != ""
	hasExplicit := k.Priv != "" || k.Pub != ""
	switch {
	case hasOutDir && hasExplicit:
		return "", "", errors.New("keygen: --out-dir is mutually exclusive with --priv/--pub")
	case hasOutDir:
		return filepath.Join(k.OutDir, "sluice-sign-key.pem"), filepath.Join(k.OutDir, "sluice-verify-key.pem"), nil
	case k.Priv != "" && k.Pub != "":
		return k.Priv, k.Pub, nil
	case hasExplicit:
		return "", "", errors.New("keygen: --priv and --pub must both be set together")
	default:
		return "", "", errors.New("keygen: one of --out-dir or (--priv and --pub) is required")
	}
}

// BackupFullCmd runs `sluice backup full`. Reads the source schema,
// streams each table's rows to one or more JSON-Lines + gzip chunk
// files under --output-dir or --target, and writes a manifest.json
// describing the schema + chunks + per-chunk SHA-256.
//
// Storage targets:
//
//   - --output-dir or --target=file:///path  → local filesystem
//   - --target=s3://bucket/prefix             → S3 (or compatible via
//     --backup-endpoint, e.g. MinIO, R2, B2, Wasabi, Tigris, Archil-read)
//   - --target=gs://bucket/prefix             → Google Cloud Storage
//   - --target=azblob://container/prefix      → Azure Blob
//
// Phase-2 caveats:
//
//   - Full snapshot only. Incremental backups are Phase 3.
//   - No client-side encryption. Backups rest on disk unencrypted;
//     operators relying on filesystem-level encryption (LUKS /
//     BitLocker / FileVault) carry that responsibility today.
//     KMS-backed encryption is Phase 6.
//   - Re-running into the same destination resumes a partial backup
//     automatically; refuses to clobber a completed one without
//     --force-overwrite.
type BackupFullCmd struct {
	SourceDriver string `help:"Source engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`

	OutputDir string `help:"Directory the backup is written to (local filesystem). Created if it doesn't exist. Manifest lives at <DIR>/manifest.json; chunks live under <DIR>/chunks/<table>/. Mutually exclusive with --target." placeholder:"DIR"`
	Target    string `help:"Backup destination URL (s3://bucket/prefix, gs://bucket/prefix, azblob://container/prefix, file:///path). Mutually exclusive with --output-dir." placeholder:"URL"`

	BackupEndpoint  string `help:"Override the S3 endpoint (e.g. http://minio.local:9000) for S3-compatible providers — MinIO, Cloudflare R2, Backblaze B2, Wasabi, Tigris, Archil's S3 read API. Only meaningful when --target is an s3:// URL." placeholder:"URL"`
	BackupRegion    string `help:"Override the S3 region. Required by some S3-compatible providers (Archil uses provider-specific codes like 'aws-us-east-1'). Only meaningful when --target is an s3:// URL." placeholder:"REGION"`
	BackupPathStyle bool   `help:"Force path-style addressing (bucket-in-path rather than bucket-in-hostname). Required by Archil and many MinIO setups. Only meaningful when --target is an s3:// URL."`

	IncludeTable []string `help:"Only back up these tables (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --exclude-table." sep:"," placeholder:"TABLE"`
	ExcludeTable []string `help:"Back up every table except these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-table." sep:"," placeholder:"TABLE"`

	ChunkSize int `help:"Maximum rows per chunk file. The writer rolls over to a new file whenever the current chunk hits this row count. Smaller chunks restore faster (per-chunk SHA-256 verification can fail-fast on the smallest possible unit) but inflate the manifest. Default 100000." default:"100000" placeholder:"N"`

	TableParallelism int `help:"READ-side (backup): number of tables read CONCURRENTLY during the backup row sweep (the read-side analog of pg_dump -j / migrate --table-parallelism). Postgres pins every parallel reader to ONE shareable exported snapshot; vanilla MySQL opens N readers whose consistent snapshots COINCIDE under a brief FLUSH TABLES WITH READ LOCK window (ADR-0088) — so cross-table consistency matches the serial sweep on both. MySQL falls back to a serial single reader (a loud INFO names the reason) when the source role lacks RELOAD; PlanetScale/Vitess sources keep the VStream-COPY path. The resolved value is bounded by the source's connection budget, reserving one slot for the snapshot's replication conn. 0 (default) = auto: 4. 1 disables cross-table concurrency. See ADR-0084 / ADR-0088." default:"0" placeholder:"N"`

	BulkParallelism int `help:"READ-side (backup): number of parallel PK-range readers per LARGE table during the row sweep — the within-table axis (ADR-0149), composed with the cross-table --table-parallelism axis exactly as in migrate. Tables above --bulk-parallel-min-rows whose primary key is chunkable (single integer PK → MIN/MAX ranges; other orderable / composite PKs → sampled keyset) are split into disjoint ranges read concurrently, every range reader pinned to the SAME exported snapshot — so the backup's consistency is identical to the single-reader sweep. Requires the shareable-snapshot source path (Postgres); MySQL's coordinated-FTWRL readers and the non-snapshot fallback keep one reader per table (a loud INFO names the reason). Orthogonal to --chunk-size, which stays the rows-per-chunk-FILE roll boundary regardless of how many readers produced the files (a chunked table may carry up to ranges-1 extra partial-size chunk files). The --table-parallelism × --bulk-parallelism product is bounded by the SOURCE's connection budget (cross-table is satisfied first; within-table gets the remainder — a single-huge-table backup gets the full width). 0 (default) = auto: min(8, NumCPU), budget-split. 1 disables within-table chunking." default:"0" placeholder:"N"`

	BulkParallelMinRows int64 `help:"READ-side (backup): estimated-row-count threshold below which a table streams with a single reader regardless of --bulk-parallelism — the same knob as migrate's --bulk-parallel-min-rows. Avoids per-range overhead on small tables. 0 (default) = auto: base 80000, dialled DOWN on many-table schemas (base/table-count, floored at 10000). Explicit values are never auto-lowered." default:"0" placeholder:"N"`

	Compression string `help:"Per-segment chunk compression codec: none | gzip | zstd. Default zstd (klauspost/compress at SpeedDefault — 55-85% faster restore, the DR-critical axis; ~1-5% larger than gzip on representative data). 'none' leaves chunks as human-readable .jsonl on a local-FS target; 'gzip' is the pre-v0.67.0 codec. Recorded in lineage.json and read back from there on restore (never inferred from bytes)." default:"zstd" enum:"none,gzip,zstd" placeholder:"CODEC"`

	SlotName string `help:"Replication-slot name suffix on engines with a slot concept (Postgres). Used to label the EndPosition recorded on the manifest so a Phase 3 incremental chained off this full opens CDC against a slot of the same name. Default 'sluice_slot'. Engines without slots (MySQL: binlog stream is the slot) ignore this flag." placeholder:"NAME"`

	ChainSlot bool `help:"Provision incremental-chain prerequisites at backup time (Postgres): create the PERSISTENT replication slot (named by --slot-name) as the snapshot anchor and ensure the pgoutput publication exists before the anchor. 'backup incremental' then chains with zero gap, no manual slot management. The slot is kept once the run's in-progress manifest records the anchor — including across an interruption, where re-running the same command resumes and ADOPTS the slot (ADR-0085); it is dropped only when the run fails before that point. Costs source-side WAL retention until the next incremental consumes the slot (drop via 'sluice slot drop' to abandon the chain). Refuses if the slot already exists on a fresh (non-resume) run. Loud no-op on engines without slots (MySQL)."`

	ForceOverwrite bool `help:"Discard whatever is at the destination and start fresh. By default 'sluice backup full' refuses to overwrite a successful prior backup and RESUMES a partial (in-progress) one, adopting the interrupted attempt's chain anchor (ADR-0085); pass this to discard either. It is also the escape hatch the resume guards (schema drift, keyless re-stream, chain-slot preflight) name. A discarded --chain-slot attempt's slot must be dropped separately ('sluice slot drop')."`

	StrictFloat bool `name:"strict-float" help:"VStream (PlanetScale/Vitess source) only: demand EXACT single-precision FLOAT or FAIL. vttablet's rowstreamer renders FLOAT at mysqld's 6-significant-digit display precision (8388608 → 8388610). 'backup full' re-reads FLOAT columns exactly from the source by default and patches the archived rows; --strict-float keeps that exact archive for every repairable table under --float-reread-max-rows, but REFUSES loudly (SLUICE-E-VSTREAM-FLOAT-LOSSY, exit 3) for any FLOAT column that cannot be made exact — a keyless / float-PK-only table (refused upfront) or a table larger than the row cap (refused when reached). Without it, those un-exactable tables fall back to a WARN + rounded archive. Inert on non-VStream sources."`

	NoFloatExactReread bool `name:"no-float-exact-reread" help:"VStream (PlanetScale/Vitess source) only: archive single-precision FLOAT columns as the COPY's display-rounded values instead of re-reading them exactly. By DEFAULT 'backup full' re-reads FLOAT columns exactly from the source (the ADR-0153 (col * 1E0) projection) and patches the archived rows, so backups store exact float32 — at the cost of a bounded within-row temporal skew (the FLOAT reflects a read instant slightly after the snapshot VGTID; ZERO on a quiescent source; SELF-HEALS on a chain restore because incrementals replay from the full's position forward; persists only for a standalone-full restore of a source with concurrent FLOAT writes). Set this to keep the rounded-but-PERFECTLY-CONSISTENT snapshot, for operators who value within-row consistency over FLOAT precision. A keyless table (no PK to target the re-read) retains the rounding regardless. Inert on non-VStream sources."`

	FloatRereadMaxRows int `name:"float-reread-max-rows" help:"VStream (PlanetScale/Vitess source) only: cap on the per-table row buffer the exact FLOAT re-read uses, so the repair is BOUNDED-memory. A FLOAT-bearing table with more than this many rows falls back loudly (default: WARN + archive that table rounded; --strict-float: refuse) rather than buffering the whole table's primary keys + float values in memory. 0 (default) uses 2,000,000 rows (~a few hundred MB worst case). Raise it when you have the headroom and want exact FLOAT on a larger table. Inert on non-VStream sources and when there are no single-precision FLOAT columns." default:"0" placeholder:"N"`

	Redact       []string `help:"Redact a PII column in backup chunks (repeatable). Format: '[schema.]table.column=STRATEGY[:options]'. Strategies: null, static:<v>, hash:sha256, hash:hmac-sha256[:<keyname>], truncate:<n>, mask:inner:<m1>,<m2>[,<char>], mask:outer:<m1>,<m2>[,<char>], mask:ssn, mask:pan, mask:pan-relaxed, mask:email, mask:ca-sin, mask:uk-nin, mask:iban, mask:uuid, randomize:int:<min>,<max>, randomize:email, randomize:us-phone, randomize:uuid, randomize:ssn, randomize:pan[:<brand>], randomize:ca-sin, randomize:uk-nin, randomize:iban[:<country-code>], randomize:dict:<name>, tokenize:dict:<name>[:<keyname>] (Phase 3 v0.61.0+, keyset-sourced Phase 4 v0.62.0+; dictionaries declared in YAML) — same set as 'sluice migrate --redact'. PII Phase 1.5 (v0.55.0+): redaction applies during chunk write, so the on-disk backup is PII-clean. Restore from a redacted chain produces the same redacted shape; restore does NOT re-apply redactions (they were applied at backup time). See docs/dev/notes/prep-pii-redaction-phase-1.md." placeholder:"RULE" sep:"none"`
	KeysetSource string   `help:"Operator keyset source (file:PATH | env:VARNAME | db:DSN) for keyset-using strategies (hash:hmac-sha256, tokenize:dict). PII Phase 4 (ADR-0041). Same forms as 'sluice migrate --keyset-source'." placeholder:"SRC"`

	Format string `help:"Output format: 'text' (default) or 'json' (machine-readable: ONE result envelope on stdout at command end — status completed/refused/failed, per-table row counts, next steps; the slog progress stream stays on stderr in both modes)." default:"text" enum:"text,json" placeholder:"FORMAT"`

	sourceTLSCAFlag

	EncryptionFlags
}

// Run implements `sluice backup full`: it wraps the body in the
// `--format json` result-envelope lifecycle (a pass-through in text
// mode) so exactly one JSON object reaches stdout on every exit path.
func (b *BackupFullCmd) Run(g *Globals) error {
	env := newEnvelopeRun("backup full", b.Format)
	env.scrub(b.Source)
	env.setResume(true, "re-run the same backup command to resume a partial backup")
	env.setNextSteps("sluice backup verify --from-dir <BACKUP_DIR>")
	return env.finish(b.run(g, env))
}

func (b *BackupFullCmd) run(g *Globals, env *envelopeRun) error {
	cfg, err := config.Load(g.Config)
	if err != nil {
		return err
	}

	source, err := resolveEngine(b.SourceDriver)
	if err != nil {
		return fmt.Errorf("--source-driver: %w", err)
	}
	// Value-fidelity flags (task 2.5): a backup reads source values, so its reader
	// honors --zero-date / --sqlite-date-encoding / --mysql-sql-mode.
	if source, err = applySourceEngineOptions(source, g); err != nil {
		return err
	}
	// CA-pinned verify-ca TLS (ADR-0158): rewrite the source DSN so a MySQL
	// source (snapshot + binlog/CDC stream) dials verify-ca.
	if b.Source, err = applyEndpointTLSCA(source, b.Source, b.SourceTLSCA, "source"); err != nil {
		return err
	}
	env.setEngines(source.Name(), "")
	codec, err := blobcodec.ParseCompression(b.Compression)
	if err != nil {
		return fmt.Errorf("--compression: %w", err)
	}

	if len(b.IncludeTable) > 0 && len(b.ExcludeTable) > 0 {
		return errors.New("--include-table and --exclude-table are mutually exclusive")
	}
	if b.OutputDir == "" && b.Target == "" {
		return errors.New("one of --output-dir or --target is required")
	}
	if b.OutputDir != "" && b.Target != "" {
		return errors.New("--output-dir and --target are mutually exclusive")
	}
	include, exclude := resolveTableFilterArgs(b.IncludeTable, b.ExcludeTable, cfg)
	filter, err := migcore.NewTableFilter(include, exclude)
	if err != nil {
		return err
	}

	ctx := kongContext()
	store, storeDesc, closer, err := openBackupStore(ctx, b.OutputDir, b.Target, blobcodec.BlobStoreOptions{
		Endpoint:  b.BackupEndpoint,
		Region:    b.BackupRegion,
		PathStyle: b.BackupPathStyle,
	})
	if err != nil {
		return err
	}
	if closer != nil {
		defer func() { _ = closer() }()
	}

	// ADR-0155: compute the pretty gate up-front. The pre-run INFO lines below
	// fire before runWithProgress installs the TTY slog gate, so on the pretty
	// path they would leak above the live view; suppress them there (the
	// summary panel is the interactive output). The non-TTY path is unchanged.
	pretty := wantPrettyProgress(g, env.jsonMode, false, false)

	if !pretty {
		slog.InfoContext(
			ctx, "backup: starting full backup",
			slog.String("source_engine", source.Name()),
			slog.String("destination", storeDesc),
			slog.Int("chunk_size", b.ChunkSize),
		)
	}

	encConfig, err := b.buildBackupEncryption()
	if err != nil {
		return err
	}
	signer, err := b.resolveWriteSigner()
	if err != nil {
		return err
	}
	bk := &backup.Backup{
		Source:              source,
		SourceDSN:           b.Source,
		Store:               store,
		Filter:              filter,
		ChunkRows:           b.ChunkSize,
		SluiceVersion:       version,
		SlotName:            pipeline.ResolveSlotName(b.SlotName),
		ChainSlot:           b.ChainSlot,
		StrictFloat:         b.StrictFloat,
		NoFloatExactReread:  b.NoFloatExactReread,
		FloatRereadMaxRows:  b.FloatRereadMaxRows,
		TableParallelism:    b.TableParallelism,
		BulkParallelism:     b.BulkParallelism,
		BulkParallelMinRows: b.BulkParallelMinRows,
		ForceOverwrite:      b.ForceOverwrite,
		Encryption:          encConfig,
		Sign:                b.Sign,
		Ed25519Signer:       signer,
		Codec:               codec,
		// --format json envelope hookup; nil in text mode (no-ops).
		Summary: env.summary,
	}
	keysetSource := b.KeysetSource
	if keysetSource == "" {
		keysetSource = cfg.KeysetSource
	}
	keyset, err := redact.LoadKeyset(ctx, keysetSource)
	if err != nil {
		return err
	}
	dictionaries, err := redact.LoadDictionaries(cfg.Dictionaries)
	if err != nil {
		return err
	}
	redactor, err := parseRedactFlags(b.Redact, keyset, "", dictionaries)
	if err != nil {
		return err
	}
	redactor, err = mergeYAMLRedactions(redactor, cfg.Redactions, keyset, "", dictionaries)
	if err != nil {
		return fmt.Errorf("redactions (YAML): %w", err)
	}
	bk.Redactor = redactor
	if !pretty {
		logKeysetLoaded(keyset)
		logRedactionConfig(redactor, "backup full")
	}
	// Validation is done; errors past this point classify as "failed"
	// (not "refused") in the --format json envelope.
	env.markEngaged()
	// ADR-0155: pretty TTY view for an interactive, non-envelope run (pretty
	// gate computed above so the pre-run INFO lines don't leak over the view).
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	return runWithProgress(pretty, cancel, backup.BackupFullProgressSpec,
		func(s progress.Sink) { bk.Progress = s },
		func() error { return bk.Run(runCtx) })
}

// openBackupStore opens the right [irbackup.Store] for the operator's
// flag combination. Returns the store, a human-readable destination
// description (for log lines), and an optional closer for backends
// that need cleanup. The S3-only options are validated against the
// URL scheme inside [pipeline.OpenBlobStore].
func openBackupStore(
	ctx context.Context,
	outputDir, target string,
	opts blobcodec.BlobStoreOptions,
) (store irbackup.Store, description string, closer func() error, err error) {
	switch {
	case outputDir != "":
		s, err := blobcodec.NewLocalStore(outputDir)
		if err != nil {
			return nil, "", nil, fmt.Errorf("open output directory: %w", err)
		}
		root := s.Root()
		return s, root, nil, nil
	case target != "":
		s, err := blobcodec.OpenBlobStore(ctx, target, opts)
		if err != nil {
			return nil, "", nil, fmt.Errorf("open backup destination: %w", err)
		}
		desc := s.URL()
		return s, desc, s.Close, nil
	}
	return nil, "", nil, errors.New("no backup destination configured")
}

// BackupIncrementalCmd runs `sluice backup incremental`. Reads the
// parent manifest from --output-dir / --target, opens the source's
// CDC pump at the parent's terminal CDC position, streams events
// for the configured window, and writes a new chain-linked manifest
// + change chunks into the same store.
//
// Phase 3.1 caveats:
//
//   - The store must already contain at least one full backup (the
//     parent). Pass --since=<backup-id> to chain off a specific
//     manifest, or leave it empty to chain off the most recent one.
//   - The window closes on either --window (wall-clock) or
//     --max-changes (event count); first to fire wins. The window is
//     extended to the next TxCommit so the chain doesn't end
//     mid-transaction.
//   - When the source's WAL / binlog has been pruned past the parent's
//     terminal position, the run refuses loudly with "take a fresh
//     full backup" guidance.
//   - Schema deltas (DDL on the source between the parent's snapshot
//     and the incremental's window end) are captured by re-reading
//     the source schema at start and end of the window and diffing.
//     Restore-side replay applies AddTable cleanly; AlterTable falls
//     through to change-stream column reconciliation. Column renames
//     within a single window are flagged as ambiguous and recommend
//     a fresh full.
type BackupIncrementalCmd struct {
	SourceDriver string `help:"Source engine name (e.g. mysql, postgres). Must declare CDC support. See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`

	OutputDir string `help:"Directory the parent backup lives in (and the incremental will be written into). Mutually exclusive with --target." placeholder:"DIR"`
	Target    string `help:"URL of the existing backup directory the incremental chains off (s3://, gs://, azblob://, file:///). Mutually exclusive with --output-dir." placeholder:"URL"`

	BackupEndpoint  string `help:"Override the S3 endpoint for S3-compatible providers. Only meaningful when --target is an s3:// URL." placeholder:"URL"`
	BackupRegion    string `help:"Override the S3 region. Only meaningful when --target is an s3:// URL." placeholder:"REGION"`
	BackupPathStyle bool   `help:"Force path-style addressing. Only meaningful when --target is an s3:// URL."`

	Since string `help:"BackupID of the parent manifest the incremental chains off. Empty selects the most recent manifest in the destination." placeholder:"BACKUP-ID"`

	SlotName string `help:"Replication-slot name suffix on engines with a slot concept (Postgres). Reuses the same slot the original full was taken under so WAL retention covers the chain." placeholder:"NAME"`

	Window     time.Duration `help:"Wall-clock duration the incremental streams CDC events for before closing the window. The window is extended to the next TxCommit so the chain doesn't end mid-transaction." default:"5m" placeholder:"DUR"`
	MaxChanges int           `help:"Stop streaming after this many CDC events (approximate; the window closes at the next TxCommit). Zero means time-bound only." default:"0" placeholder:"N"`

	ChunkSize int `help:"Maximum changes per chunk file. Smaller chunks restore faster (per-chunk SHA-256 fail-fast) but inflate the manifest." default:"100000" placeholder:"N"`

	sourceTLSCAFlag

	EncryptionFlags
}

// Run implements `sluice backup incremental`.
func (b *BackupIncrementalCmd) Run(g *Globals) error {
	source, err := resolveEngine(b.SourceDriver)
	if err != nil {
		return fmt.Errorf("--source-driver: %w", err)
	}
	// Value-fidelity flags (task 2.5): an incremental backup reads source values.
	if source, err = applySourceEngineOptions(source, g); err != nil {
		return err
	}
	// CA-pinned verify-ca TLS (ADR-0158): rewrite the source DSN so a MySQL
	// source (snapshot + binlog/CDC stream) dials verify-ca.
	if b.Source, err = applyEndpointTLSCA(source, b.Source, b.SourceTLSCA, "source"); err != nil {
		return err
	}
	if b.OutputDir == "" && b.Target == "" {
		return errors.New("one of --output-dir or --target is required")
	}
	if b.OutputDir != "" && b.Target != "" {
		return errors.New("--output-dir and --target are mutually exclusive")
	}
	ctx := kongContext()
	store, storeDesc, closer, err := openBackupStore(ctx, b.OutputDir, b.Target, blobcodec.BlobStoreOptions{
		Endpoint:  b.BackupEndpoint,
		Region:    b.BackupRegion,
		PathStyle: b.BackupPathStyle,
	})
	if err != nil {
		return err
	}
	if closer != nil {
		defer func() { _ = closer() }()
	}

	// ADR-0155: pretty gate up-front so the pre-run INFO line doesn't leak
	// above the live view (it fires before runWithProgress installs the TTY
	// slog gate). Non-TTY path unchanged.
	pretty := wantPrettyProgress(g, false, false, false)

	if !pretty {
		slog.InfoContext(
			ctx, "backup: starting incremental",
			slog.String("source_engine", source.Name()),
			slog.String("destination", storeDesc),
			slog.String("since", b.Since),
			slog.Duration("window", b.Window),
		)
	}

	encConfig, err := b.buildBackupEncryption()
	if err != nil {
		return err
	}
	signer, err := b.resolveWriteSigner()
	if err != nil {
		return err
	}
	incr := &pipeline.IncrementalBackup{
		Source:        source,
		SourceDSN:     b.Source,
		Store:         store,
		ParentRef:     b.Since,
		SlotName:      pipeline.ResolveSlotName(b.SlotName),
		Window:        b.Window,
		MaxChanges:    b.MaxChanges,
		ChunkChanges:  b.ChunkSize,
		SluiceVersion: version,
		Encryption:    encConfig,
		Sign:          b.Sign,
		Ed25519Signer: signer,
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	return runWithProgress(pretty, cancel, pipeline.IncrementalProgressSpec,
		func(s progress.Sink) { incr.Progress = s },
		func() error { return incr.Run(runCtx) })
}

// BackupStreamCmdGroup groups `sluice backup stream` (run) and
// `sluice backup stream stop` (companion stop). The "run" verb is
// `sluice backup stream run` for kong to dispatch cleanly with a
// sibling `stop` subcommand.
type BackupStreamCmdGroup struct {
	Run  BackupStreamCmd     `cmd:"" help:"Run the long-running stream (rolling incrementals at configured cadence)."`
	Stop BackupStreamStopCmd `cmd:"" help:"Request a running stream to commit the in-flight rollover and exit cleanly."`
}

// BackupStreamCmd runs `sluice backup stream run`. Drives a continuous-
// incremental long-running stream against the source: each rollover
// captures CDC events for a bounded window (time / change-count /
// byte ceilings, first-fired wins) and commits a new manifest under
// `manifests/incr-<unix-millis>-<seq>.json`. Window extends to next
// TxCommit so the chain doesn't end mid-tx.
//
// Operator stop paths:
//
//   - SIGTERM / SIGINT (Ctrl-C): drain in-flight rollover, exit cleanly.
//   - `sluice backup stream stop --target=<url>`: cross-machine stop
//     via `stream_state.json`. Polled between rollovers; the stream
//     exits within ≤ rollover-window of the request.
type BackupStreamCmd struct {
	SourceDriver string `help:"Source engine name (e.g. mysql, postgres). Must declare CDC support. See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`

	OutputDir string `help:"Directory the parent backup lives in (and stream rollovers will be written into). Mutually exclusive with --target." placeholder:"DIR"`
	Target    string `help:"URL of the existing backup directory the stream chains off (s3://, gs://, azblob://, file:///). Mutually exclusive with --output-dir." placeholder:"URL"`

	BackupEndpoint  string `help:"Override the S3 endpoint for S3-compatible providers. Only meaningful when --target is an s3:// URL." placeholder:"URL"`
	BackupRegion    string `help:"Override the S3 region. Only meaningful when --target is an s3:// URL." placeholder:"REGION"`
	BackupPathStyle bool   `help:"Force path-style addressing. Only meaningful when --target is an s3:// URL."`

	Since string `help:"BackupID of the parent manifest the stream chains off. Empty selects the most recent manifest in the destination." placeholder:"BACKUP-ID"`

	SlotName string `help:"Replication-slot name suffix on engines with a slot concept (Postgres). Reuses the slot the original full was taken under so WAL retention covers the chain." placeholder:"NAME"`

	RolloverWindow     time.Duration `help:"Wall-clock cadence each rollover commits at. Window extends to next TxCommit so the chain doesn't end mid-tx." default:"5m" placeholder:"DUR"`
	RolloverMaxChanges int           `help:"Commit a rollover after this many CDC events queue up (approximate; closes at next TxCommit)." default:"100000" placeholder:"N"`
	RolloverMaxBytes   int64         `help:"Commit a rollover when buffered chunk bytes cross this ceiling. Default 67108864 (64 MiB)." default:"67108864" placeholder:"BYTES"`

	ChunkSize int `help:"Maximum changes per chunk file. Smaller chunks restore faster (per-chunk SHA-256 fail-fast) but inflate the manifest." default:"100000" placeholder:"N"`

	IncludeEmpty bool `help:"Commit a manifest for rollovers that captured zero changes. Default off (skip empty rollovers; stream_state.json covers liveness without polluting the chain)."`

	Force bool `help:"Bypass the concurrent-writer check at startup (refuses to start when an existing stream_state.json shows a recent last_rollover_at from a different pid/host). Operator-confirmed: 'I'm taking over this destination from a previous stream that may still be running.'"`

	RolloverHook string `help:"Shell command to invoke after each rollover commits successfully. Receives env vars SLUICE_ROLLOVER_MANIFEST_PATH, SLUICE_ROLLOVER_PARENT_BACKUP_ID, SLUICE_ROLLOVER_BACKUP_ID, SLUICE_ROLLOVER_CHANGES, SLUICE_ROLLOVER_BYTES, SLUICE_ROLLOVER_ELAPSED_MS. Hook errors are WARN-logged but don't fail the stream. 30s timeout." placeholder:"CMD"`

	// ADR-0118 finding 2: the retry knobs are the same concept the sync
	// stream exposes as --apply-retry-* (identical defaults, 8/100ms/30s).
	// Cross-add each sync spelling as an alias so an operator's muscle
	// memory works on either command; the primary (shown in --help) stays
	// backup's existing --retry-* name. Additive — no existing command line
	// changes behaviour.
	RetryAttempts    int           `aliases:"apply-retry-attempts" help:"Cap on consecutive retriable rollover failures the stream will absorb before giving up. Mirrors the sync-stream's --apply-retry-attempts (accepted here as an alias). GitHub #22: transient source-side errors that v0.46.0 fixed for sync streams now also retry on backup-stream. 1 disables retry." default:"8" placeholder:"N"`
	RetryBackoffBase time.Duration `aliases:"apply-retry-backoff-base" help:"Base interval for exponential backoff between retriable rollover failures. Doubles each attempt, capped at --retry-backoff-cap. Alias: --apply-retry-backoff-base (the sync-stream spelling)." default:"100ms" placeholder:"DUR"`
	RetryBackoffCap  time.Duration `aliases:"apply-retry-backoff-cap" help:"Upper bound on each retriable rollover backoff interval. Alias: --apply-retry-backoff-cap (the sync-stream spelling)." default:"30s" placeholder:"DUR"`

	RetainRotateAt            time.Duration `help:"In-process backup-segment rotation (ADR-0046): once the open segment reaches this age, the stream caps it and opens a fresh segment over the SAME CDC handle (no operator wrapper, no stream exit). Pair with 'sluice backup prune' to bound total disk. 0 disables (unbounded single segment)." placeholder:"DUR"`
	RetainRotateAtChainLength int           `help:"Rotate the open segment after this many incrementals are committed to it. Either rotation threshold firing wins. 0 disables." placeholder:"N"`

	Compression string `help:"Per-segment chunk compression codec: none | gzip | zstd. Default zstd (klauspost/compress at SpeedDefault — 55-85% faster restore, the DR-critical axis; ~1-5% larger than gzip on representative data). 'none' leaves chunks as human-readable .jsonl on a local-FS target; 'gzip' is the pre-v0.67.0 codec. Recorded per segment in lineage.json and read back from there on restore (never inferred from bytes)." default:"zstd" enum:"none,gzip,zstd" placeholder:"CODEC"`

	// Phase-1 rotation flags removed in v0.67.0 (ADR-0046 §6). Kept as
	// hidden no-value sentinels so the operator gets a CLEAR
	// migration error (clean break, not a silent ignore — kong's
	// generic "unknown flag" is less actionable).
	ExitAfterAge         string `name:"exit-after-age" hidden:"" help:"REMOVED in v0.67.0."`
	ExitAfterChainLength string `name:"exit-after-chain-length" hidden:"" help:"REMOVED in v0.67.0."`

	sourceTLSCAFlag

	EncryptionFlags
}

// Run implements `sluice backup stream run`.
func (b *BackupStreamCmd) Run(g *Globals) error {
	if b.ExitAfterAge != "" || b.ExitAfterChainLength != "" {
		return errors.New("--exit-after-age / --exit-after-chain-length were REMOVED in v0.67.0 (ADR-0046): rotation is now always in-process. Use --retain-rotate-at=DUR and/or --retain-rotate-at-chain-length=N instead — the stream caps the open segment and opens a fresh one over the same CDC handle, no operator wrapper needed")
	}
	source, err := resolveEngine(b.SourceDriver)
	if err != nil {
		return fmt.Errorf("--source-driver: %w", err)
	}
	// Value-fidelity flags (task 2.5): a backup stream reads source values.
	if source, err = applySourceEngineOptions(source, g); err != nil {
		return err
	}
	// CA-pinned verify-ca TLS (ADR-0158): rewrite the source DSN so a MySQL
	// source (snapshot + binlog/CDC stream) dials verify-ca.
	if b.Source, err = applyEndpointTLSCA(source, b.Source, b.SourceTLSCA, "source"); err != nil {
		return err
	}
	codec, err := blobcodec.ParseCompression(b.Compression)
	if err != nil {
		return fmt.Errorf("--compression: %w", err)
	}
	if b.OutputDir == "" && b.Target == "" {
		return errors.New("one of --output-dir or --target is required")
	}
	if b.OutputDir != "" && b.Target != "" {
		return errors.New("--output-dir and --target are mutually exclusive")
	}
	ctx := kongContext()
	store, storeDesc, closer, err := openBackupStore(ctx, b.OutputDir, b.Target, blobcodec.BlobStoreOptions{
		Endpoint:  b.BackupEndpoint,
		Region:    b.BackupRegion,
		PathStyle: b.BackupPathStyle,
	})
	if err != nil {
		return err
	}
	if closer != nil {
		defer func() { _ = closer() }()
	}

	// ADR-0156: compute the pretty gate up-front. The pre-run INFO line below
	// fires before runReadoutLivePanel installs the TTY slog gate, so on the
	// pretty path it would leak above the live view; suppress it there (the
	// panel is the interactive output). The non-TTY path is unchanged.
	pretty := wantPrettyProgress(g, false, false, false)
	if !pretty {
		slog.InfoContext(
			ctx, "backup: starting stream",
			slog.String("source_engine", source.Name()),
			slog.String("destination", storeDesc),
			slog.String("since", b.Since),
			slog.Duration("rollover_window", b.RolloverWindow),
		)
	}

	encConfig, err := b.buildBackupEncryption()
	if err != nil {
		return err
	}
	stream := &pipeline.BackupStream{
		Source:                    source,
		SourceDSN:                 b.Source,
		Store:                     store,
		ParentRef:                 b.Since,
		SlotName:                  pipeline.ResolveSlotName(b.SlotName),
		RolloverWindow:            b.RolloverWindow,
		RolloverMaxChanges:        b.RolloverMaxChanges,
		RolloverMaxBytes:          b.RolloverMaxBytes,
		ChunkChanges:              b.ChunkSize,
		IncludeEmptyRollovers:     b.IncludeEmpty,
		Force:                     b.Force,
		RolloverHook:              b.RolloverHook,
		SluiceVersion:             version,
		Encryption:                encConfig,
		RetryAttempts:             b.RetryAttempts,
		RetryBackoffBase:          b.RetryBackoffBase,
		RetryBackoffCap:           b.RetryBackoffCap,
		RetainRotateAt:            b.RetainRotateAt,
		RetainRotateAtChainLength: b.RetainRotateAtChainLength,
		Codec:                     codec,
	}

	// ADR-0156 phase 2: the TTY-aware live panel for the rolling-incremental
	// cadence loop. Same [wantPrettyProgress] gate as the one-shot commands
	// (this command has no --format json envelope / dry-run / multi-namespace
	// shape). q/ctrl+c cancels the run context, which is the stream's graceful
	// drain (it commits the in-flight rollover before exiting); every other
	// invocation keeps today's byte-identical log stream.
	if pretty {
		header := progress.LiveHeader{
			Mode:   "backup stream",
			Source: source.Name(),
			Target: storeDesc,
		}
		return runReadoutLivePanel(ctx, header, func(sink *progress.LiveTTYSink) func(context.Context) error {
			stream.Readout = sink.Readout
			return stream.Run
		})
	}
	return stream.Run(ctx)
}

// BackupStreamStopCmd runs `sluice backup stream stop`. Writes
// `stop_requested_at` to the destination's `stream_state.json` so the
// running stream observes the request on its next rollover-tick poll
// and exits cleanly. Cross-machine: the operator can stop a stream
// from a different host without process access — both sides agree on
// the destination, not on the host.
type BackupStreamStopCmd struct {
	OutputDir string `help:"Directory the running stream is writing to (local filesystem). Mutually exclusive with --target." placeholder:"DIR"`
	Target    string `help:"URL of the destination the running stream is writing to (s3://, gs://, azblob://, file:///). Mutually exclusive with --output-dir." placeholder:"URL"`

	BackupEndpoint  string `help:"Override the S3 endpoint for S3-compatible providers. Only meaningful when --target is an s3:// URL." placeholder:"URL"`
	BackupRegion    string `help:"Override the S3 region. Only meaningful when --target is an s3:// URL." placeholder:"REGION"`
	BackupPathStyle bool   `help:"Force path-style addressing. Only meaningful when --target is an s3:// URL."`
}

// Run implements `sluice backup stream stop`.
func (b *BackupStreamStopCmd) Run(_ *Globals) error {
	if b.OutputDir == "" && b.Target == "" {
		return errors.New("one of --output-dir or --target is required")
	}
	if b.OutputDir != "" && b.Target != "" {
		return errors.New("--output-dir and --target are mutually exclusive")
	}
	ctx := kongContext()
	store, storeDesc, closer, err := openBackupStore(ctx, b.OutputDir, b.Target, blobcodec.BlobStoreOptions{
		Endpoint:  b.BackupEndpoint,
		Region:    b.BackupRegion,
		PathStyle: b.BackupPathStyle,
	})
	if err != nil {
		return err
	}
	if closer != nil {
		defer func() { _ = closer() }()
	}

	prior, err := pipeline.RequestStreamStop(ctx, store, time.Now())
	if err != nil {
		return err
	}
	slog.InfoContext(
		ctx, "backup stream stop: signal written; running stream will exit on next rollover-tick",
		slog.String("destination", storeDesc),
		slog.Int("running_pid", prior.PID),
		slog.String("running_host", prior.Host),
		slog.Time("running_last_rollover_at", prior.LastRolloverAt),
	)
	return nil
}

// BackupVerifyCmd runs `sluice backup verify`. Walks an existing
// backup directory, recomputes every chunk's SHA-256, and reports
// any that don't match the manifest. Useful for cron probes against
// archived backups — confirms the bits are still good without needing
// a target database to restore into.
//
// When the chain is encrypted and the operator supplies `--encrypt`
// + a passphrase / KMS reference, verify additionally performs a
// decrypt probe on every per-chunk WrappedCEK — the Bug 117 closure.
// A passphrase rotation mid-chain (per-chunk mode) surfaces here as
// a "wrong passphrase for chunk X" verify failure instead of a
// partial-fail at restore-time.
type BackupVerifyCmd struct {
	FromDir string `help:"Directory containing the backup to verify (the same directory --output-dir wrote to). Mutually exclusive with --from." placeholder:"DIR"`
	From    string `help:"URL of the backup to verify (s3://, gs://, azblob://, file:///). Mutually exclusive with --from-dir." placeholder:"URL"`

	BackupEndpoint  string `help:"Override the S3 endpoint for S3-compatible providers. Only meaningful when --from is an s3:// URL." placeholder:"URL"`
	BackupRegion    string `help:"Override the S3 region. Only meaningful when --from is an s3:// URL." placeholder:"REGION"`
	BackupPathStyle bool   `help:"Force path-style addressing. Only meaningful when --from is an s3:// URL."`

	RebuildCatalog bool `help:"Rebuild lineage.json from scratch by walking the conventional one-segment layout (manifest.json + manifests/incr-*.json), then exit. Use after manual mutation of a single-segment backup. The segment's compression codec is sniffed from chunk magic bytes; for an ENCRYPTED chain also pass --encrypt + the chain's passphrase / KMS reference (the codec is sealed inside the encryption envelope). NOTE: a multi-segment (rotated) lineage's sub-dir structure is NOT reconstructable from a bare walk by design — lineage.json IS the structural record for a rotated backup."`

	EncryptionFlags
}

// Run implements `sluice backup verify`.
func (v *BackupVerifyCmd) Run(g *Globals) error {
	if v.FromDir == "" && v.From == "" {
		return errors.New("one of --from-dir or --from is required")
	}
	if v.FromDir != "" && v.From != "" {
		return errors.New("--from-dir and --from are mutually exclusive")
	}
	ctx := kongContext()
	store, _, closer, err := openBackupStore(ctx, v.FromDir, v.From, blobcodec.BlobStoreOptions{
		Endpoint:  v.BackupEndpoint,
		Region:    v.BackupRegion,
		PathStyle: v.BackupPathStyle,
	})
	if err != nil {
		return err
	}
	if closer != nil {
		defer func() { _ = closer() }()
	}
	if v.RebuildCatalog {
		// The rebuilt record's codec is sniffed from chunk bytes (audit
		// N-14). For an ENCRYPTED chain the codec sits inside the
		// encryption envelope, so build the read envelope from the
		// operator's --encrypt flags first (nil when not passed — fine
		// for plaintext chains; the rebuild refuses loudly if it turns
		// out to need one).
		rootManifest, err := lineage.ReadRootManifest(ctx, store)
		if err != nil {
			return fmt.Errorf("rebuild lineage catalog: read root manifest: %w", err)
		}
		envelope, err := v.buildReadEnvelope(rootManifest)
		if err != nil {
			return err
		}
		segments, manifests, err := lineage.RebuildLineageCatalogAt(ctx, store, envelope)
		if err != nil {
			return fmt.Errorf("rebuild lineage catalog: %w", err)
		}
		slog.InfoContext(
			ctx, "lineage catalog rebuilt",
			slog.Int("segments", segments),
			slog.Int("manifests", manifests),
		)
		return nil
	}
	// ADR-0155: pretty TTY view for an interactive run. `backup verify` is
	// CLI-orchestrated (no pipeline Run with a Progress field), so the sink
	// is captured here and driven inline; on the non-TTY path it is the
	// [progress.Nop] and the existing slog lines are byte-identical.
	pretty := wantPrettyProgress(g, false, false, false)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var sink progress.Sink = progress.Nop{}
	return runWithProgress(pretty, cancel, backup.VerifyChainProgressSpec,
		func(s progress.Sink) { sink = s },
		func() error {
			sink.PhaseStarted(backup.VerifyPhaseLoad)
			// Bug 117 closure (v0.94.1): when --encrypt is on, load the
			// chain-root manifest so the read envelope re-derives the same
			// Argon2id KEK the writer used, then thread the envelope into
			// VerifyBackupWith. SHA-only verify silently accepted per-chunk
			// passphrase rotation; the decrypt probe refuses it.
			rootManifest, err := lineage.ReadRootManifest(runCtx, store)
			if err != nil {
				return fmt.Errorf("verify: read root manifest: %w", err)
			}
			envelope, err := v.buildReadEnvelope(rootManifest)
			if err != nil {
				return err
			}
			if envelope == nil && rootManifest != nil && rootManifest.ChainEncryption != nil {
				// Encrypted chain + no envelope = SHA-only verify (legacy
				// behavior). Bug 117's silent passphrase-rotation acceptance
				// is invisible without a decrypt probe — warn the operator
				// loudly so they know to re-run with `--encrypt` + their
				// passphrase for full coverage.
				slog.WarnContext(
					runCtx, "backup verify: chain is encrypted but no envelope supplied — running SHA-only verify; passphrase rotation (Bug 117) is undetectable in this mode. Re-run with --encrypt + the chain's passphrase / KMS reference to enable the per-chunk decrypt probe.",
					slog.String("kek_mode", rootManifest.ChainEncryption.KEKMode),
					slog.String("kek_ref", rootManifest.ChainEncryption.KEKRef),
				)
			}
			verifyKey, err := v.resolveVerifyKey()
			if err != nil {
				return err
			}
			sink.PhaseCompleted(backup.VerifyPhaseLoad)
			sink.PhaseStarted(backup.VerifyPhaseCheck)
			// Bug 185: VerifyBackupCoded returns a coded Refusal (exit 3)
			// when chunks fail — SLUICE-E-BACKUP-CHUNK-CORRUPT (SHA-256
			// mismatch) / -CHUNK-AUTH-FAILED (decrypt/splice) — so operators
			// can script `backup verify` against the code, matching restore.
			total, _, err := backup.VerifyBackupCoded(runCtx, store, backup.VerifyOptions{
				Envelope:         envelope,
				VerifyKey:        verifyKey,
				RequireSignature: v.RequireSignature,
			})
			if err != nil {
				return err
			}
			slog.InfoContext(
				runCtx, "backup verify: all chunks OK",
				slog.Int("chunks", total),
				slog.Bool("decrypt_probe", envelope != nil),
			)
			sink.PhaseCompleted(backup.VerifyPhaseCheck)
			sink.Summary(progress.Result{Fields: []progress.Field{
				{Label: "Chunks", Value: progress.HumanCount(int64(total))},
				{Label: "Mismatched", Value: "0"},
				{Label: "Decrypt probe", Value: boolYesNoCLI(envelope != nil)},
			}})
			return nil
		})
}

// boolYesNoCLI renders a bool as the CLI summary panel's "yes"/"no".
func boolYesNoCLI(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// BackupPruneCmd runs `sluice backup prune`. Drops the oldest
// incrementals from an existing chain to bound disk usage and
// restore time. Closes GitHub #20 roadmap chunk 14c.
//
// Semantics (see internal/pipeline/chain_prune.go):
//
//   - Operator chooses retention via --keep-incrementals N (keep the
//     N most-recent) OR --keep-duration DUR (keep anything younger
//     than DUR). Mutually exclusive; exactly one required.
//   - The full backup at the chain root is always preserved.
//   - The first surviving incremental gets re-stitched to point at
//     the full directly (advances the chain's "earliest restorable
//     position" forward — the dropped incrementals' event windows
//     are LOST from the chain's restore range; operator opts into
//     this).
//   - --dry-run reports what WOULD be pruned without deleting
//     anything or rewriting the catalog.
//
// Prune requires chain.json to be present (the v0.47.0 catalog).
// Run `sluice backup verify --rebuild-catalog` first on pre-v0.47.0
// chains.
type BackupPruneCmd struct {
	FromDir string `help:"Directory containing the chain to prune (the same directory --output-dir wrote to). Mutually exclusive with --from." placeholder:"DIR"`
	From    string `help:"URL of the chain to prune (s3://, gs://, azblob://, file:///). Mutually exclusive with --from-dir." placeholder:"URL"`

	BackupEndpoint  string `help:"Override the S3 endpoint for S3-compatible providers. Only meaningful when --from is an s3:// URL." placeholder:"URL"`
	BackupRegion    string `help:"Override the S3 region. Only meaningful when --from is an s3:// URL." placeholder:"REGION"`
	BackupPathStyle bool   `help:"Force path-style addressing. Only meaningful when --from is an s3:// URL."`

	KeepIncrementals int           `help:"Retain the N most-recent incrementals. Mutually exclusive with --keep-duration." placeholder:"N"`
	KeepDuration     time.Duration `help:"Retain incrementals younger than this duration. Mutually exclusive with --keep-incrementals. Examples: 168h (7d), 720h (30d)." placeholder:"DUR"`

	DryRun bool `help:"Report what would be pruned without deleting or rewriting the catalog."`

	// Pruning a SIGNED (ADR-0154) chain renumbers link positions and must
	// re-sign the survivors; pass --encrypt + the chain's key to enable
	// that (a signed chain refuses to prune without it).
	EncryptionFlags
}

// Run implements `sluice backup prune`.
func (p *BackupPruneCmd) Run(_ *Globals) error {
	if p.FromDir == "" && p.From == "" {
		return errors.New("one of --from-dir or --from is required")
	}
	if p.FromDir != "" && p.From != "" {
		return errors.New("--from-dir and --from are mutually exclusive")
	}
	if (p.KeepIncrementals > 0) == (p.KeepDuration > 0) {
		return errors.New("exactly one of --keep-incrementals or --keep-duration is required")
	}
	ctx := kongContext()
	store, _, closer, err := openBackupStore(ctx, p.FromDir, p.From, blobcodec.BlobStoreOptions{
		Endpoint:  p.BackupEndpoint,
		Region:    p.BackupRegion,
		PathStyle: p.BackupPathStyle,
	})
	if err != nil {
		return err
	}
	if closer != nil {
		defer func() { _ = closer() }()
	}

	signer, err := p.buildSigner(ctx, store)
	if err != nil {
		return err
	}
	res, err := backup.PruneChain(ctx, store, backup.PruneOpts{
		KeepIncrementals: p.KeepIncrementals,
		KeepDuration:     p.KeepDuration,
		DryRun:           p.DryRun,
		Signer:           signer,
	})
	if err != nil {
		return err
	}
	mode := "pruned"
	if p.DryRun {
		mode = "would-prune (dry-run)"
	}
	slog.InfoContext(
		ctx, "backup prune: "+mode,
		slog.Int("manifests_dropped", len(res.Pruned)),
		slog.Int("manifests_kept", len(res.Kept)),
		slog.Int("chunks_deleted", res.ChunksDeleted),
		slog.String("earliest_restorable_backup_id", res.EarliestRestorableBackupID),
	)
	for _, p := range res.Pruned {
		slog.InfoContext(
			ctx, "  dropped",
			slog.String("manifest_path", p),
		)
	}
	return nil
}

// BackupCompactCmd runs `sluice backup compact`. Concatenates
// consecutive lineage segments whose CreatedAt gaps fall within
// --merge-window into a single merged segment, in place. Closes
// GitHub #20 roadmap chunk 14d (Task #15).
//
// Semantics (see internal/pipeline/chain_compact.go):
//
//   - Walk the lineage's retained segments oldest-first; group
//     consecutive segments where each pairwise CreatedAt gap is <=
//     --merge-window. Groups of size >= 2 merge into one segment;
//     size-1 groups are no-ops.
//   - "Naive" = byte-level chunk concat. Each merged source's chunk
//     files are moved verbatim; bytes are NEVER decompressed,
//     recompressed, or re-encrypted (that's event-level dedup,
//     deferred to #16). The merged segment's full = the OLDEST
//     source's full; its incrementals = the union of every source's
//     incrementals in lineage order.
//   - Loud-failure refusals: mixed codecs within a group, divergent
//     encryption keysets within a group, OR position gaps between
//     consecutive sources REFUSE LOUDLY before any mutation. The
//     operator's recovery is to split the merge window so each group
//     is uniform / contiguous.
//   - Atomic safety: staging-dir → final-dir move → ATOMIC catalog
//     swap (lineage.json rewrite). The catalog swap is the
//     linearization commit; a crash before it leaves "compact never
//     happened", a crash after it leaves "compact happened" with
//     orphan source files the next run sweeps.
//   - --dry-run reports the would-merge plan without touching
//     storage or the catalog.
//
// Compact never runs automatically; it is an explicit operator
// action. The chain root (oldest retained segment) is preserved
// (compact operates on the retained segments oldest-first; the
// oldest's full BECOMES the merged segment's full, never moved or
// rewritten in identity).
type BackupCompactCmd struct {
	FromDir string `help:"Directory containing the chain to compact (the same directory --output-dir wrote to). Mutually exclusive with --from." placeholder:"DIR"`
	From    string `help:"URL of the chain to compact (s3://, gs://, azblob://, file:///). Mutually exclusive with --from-dir." placeholder:"URL"`

	BackupEndpoint  string `help:"Override the S3 endpoint for S3-compatible providers. Only meaningful when --from is an s3:// URL." placeholder:"URL"`
	BackupRegion    string `help:"Override the S3 region. Only meaningful when --from is an s3:// URL." placeholder:"REGION"`
	BackupPathStyle bool   `help:"Force path-style addressing. Only meaningful when --from is an s3:// URL."`

	MergeWindow time.Duration `help:"Maximum CreatedAt gap between consecutive segments to be considered part of the same merge group. Required. Examples: 1h, 24h, 168h (7d)." placeholder:"DUR"`

	DryRun bool `help:"Report the would-merge plan without touching storage or rewriting the catalog."`

	SmartCompaction      bool   `name:"smart-compaction" help:"Enable ADR-0064 event-level collapse (INSERT+UPDATE → INSERT, UPDATE+UPDATE → UPDATE, INSERT+DELETE → nothing, UPDATE+DELETE → DELETE) within each merge group's change-chunks. Default off in v1; opt in once update-heavy workload makes the CPU tax worthwhile. Mutually exclusive with --smart-compaction-off."`
	SmartCompactionOff   bool   `name:"smart-compaction-off" help:"Explicitly disable smart compaction (the v1 default). Useful as an audit trail or as the recovery flag after a corrupt-PK refuse-loudly fail. Mutually exclusive with --smart-compaction."`
	CompactionPKStrategy string `name:"compaction-pk-strategy" enum:"pk,replica-identity,none" default:"pk" help:"Row-identity strategy for smart compaction. 'pk' (default) uses the table's declared primary key; 'replica-identity' is a PG-targeted alias for 'pk' (v1); 'none' disables per-row collapse (debugging escape hatch). Has no effect without --smart-compaction." placeholder:"STRATEGY"`

	// Compacting a SIGNED (ADR-0154) chain rewrites + renumbers manifests
	// and must re-sign them; pass --encrypt + the chain's key to enable
	// that (a signed chain refuses to compact without it).
	EncryptionFlags
}

// Run implements `sluice backup compact`.
func (c *BackupCompactCmd) Run(_ *Globals) error {
	if c.FromDir == "" && c.From == "" {
		return errors.New("one of --from-dir or --from is required")
	}
	if c.FromDir != "" && c.From != "" {
		return errors.New("--from-dir and --from are mutually exclusive")
	}
	if c.MergeWindow <= 0 {
		return errors.New("--merge-window is required (positive duration)")
	}
	if c.SmartCompaction && c.SmartCompactionOff {
		return errors.New("--smart-compaction and --smart-compaction-off are mutually exclusive")
	}
	ctx := kongContext()
	store, _, closer, err := openBackupStore(ctx, c.FromDir, c.From, blobcodec.BlobStoreOptions{
		Endpoint:  c.BackupEndpoint,
		Region:    c.BackupRegion,
		PathStyle: c.BackupPathStyle,
	})
	if err != nil {
		return err
	}
	if closer != nil {
		defer func() { _ = closer() }()
	}

	signer, err := c.buildSigner(ctx, store)
	if err != nil {
		return err
	}
	res, err := backup.CompactChain(ctx, store, backup.CompactOpts{
		MergeWindow:     c.MergeWindow,
		DryRun:          c.DryRun,
		SmartCompaction: c.SmartCompaction,
		PKStrategy:      backup.PKStrategy(c.CompactionPKStrategy),
		Signer:          signer,
	})
	if err != nil {
		return err
	}
	mode := "compacted"
	if c.DryRun {
		mode = "would-compact (dry-run)"
	}
	topArgs := []any{
		slog.Int("groups_considered", res.GroupsConsidered),
		slog.Int("groups_merged", res.GroupsMerged),
		slog.Int("segments_removed", res.SegmentsRemoved),
		slog.Int64("bytes_before", res.BytesBefore),
		slog.Int64("bytes_after", res.BytesAfter),
	}
	if c.SmartCompaction {
		topArgs = append(
			topArgs,
			slog.Bool("smart_compaction", true),
			slog.Int64("events_before", res.EventsBefore),
			slog.Int64("events_after", res.EventsAfter),
			slog.Int64("events_collapsed", res.EventsCollapsed),
			slog.Int64("rows_collapsed", res.RowsCollapsed),
			slog.Any("tables_without_pk", res.TablesWithoutPK),
		)
	}
	slog.InfoContext(ctx, "backup compact: "+mode, topArgs...)
	for _, g := range res.Plan {
		if g.MergedSegmentID == "" {
			slog.InfoContext(
				ctx, "  group (size-1, skipped)",
				slog.Any("source_segment_ids", g.SourceSegmentIDs),
			)
			continue
		}
		groupArgs := []any{
			slog.String("merged_segment_id", g.MergedSegmentID),
			slog.String("merged_segment_dir", g.MergedSegmentDir),
			slog.Any("source_segment_ids", g.SourceSegmentIDs),
			slog.Duration("window_span", g.WindowSpan),
			slog.Int64("bytes_moved", g.BytesEstimate),
		}
		if c.SmartCompaction {
			groupArgs = append(
				groupArgs,
				slog.Int64("events_before", g.EventsBefore),
				slog.Int64("events_after", g.EventsAfter),
				slog.Int64("events_collapsed", g.EventsCollapsed),
				slog.Int64("rows_collapsed", g.RowsCollapsed),
			)
		}
		slog.InfoContext(ctx, "  group merged", groupArgs...)
	}
	return nil
}

// RestoreCmd implements `sluice restore`. Reads a manifest from
// --from-dir or --from, applies the schema (with cross-engine
// retargeting if the target differs from the backup's source engine),
// bulk-copies every chunk's rows back, and creates indexes /
// constraints / views.
//
// Cross-engine restore (PG backup → MySQL target, etc.) is supported
// via `translate.RetargetForEngine` — the same machinery `sluice
// schema diff` uses to bridge type differences.
//
// Phase 3 (v0.17.0+): when the supplied --from contains incremental
// manifests in addition to the full, the restore walks the chain in
// order. Same-engine chains apply schema deltas + replay change
// chunks; cross-engine chains with incrementals are refused (Phase
// 5+ topic).
type RestoreCmd struct {
	FromDir string `help:"Directory containing the backup to restore from (local filesystem). Mutually exclusive with --from." placeholder:"DIR"`
	From    string `help:"URL of the backup to restore (s3://, gs://, azblob://, file:///). Mutually exclusive with --from-dir." placeholder:"URL"`

	BackupEndpoint  string `help:"Override the S3 endpoint for S3-compatible providers. Only meaningful when --from is an s3:// URL." placeholder:"URL"`
	BackupRegion    string `help:"Override the S3 region. Only meaningful when --from is an s3:// URL." placeholder:"REGION"`
	BackupPathStyle bool   `help:"Force path-style addressing. Only meaningful when --from is an s3:// URL."`

	TargetDriver string `help:"Target engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"target"`
	Target       string `help:"Target database DSN." required:"" env:"SLUICE_TARGET" placeholder:"DSN" group:"target"`

	IncludeTable []string `help:"Only restore these tables (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --exclude-table." sep:"," placeholder:"TABLE"`
	ExcludeTable []string `help:"Restore every table except these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-table." sep:"," placeholder:"TABLE"`

	MaxBufferBytes int64 `help:"Soft cap on per-batch buffered memory in the bulk-copy writer. Same semantics as 'sluice migrate --max-buffer-bytes'. Default 67108864 (64 MiB)." default:"67108864" placeholder:"N"`

	TableParallelism int `help:"WRITE-side (restore): number of tables bulk-applied CONCURRENTLY during the restore (the write-side analog of pg_restore -j / migrate --table-parallelism). Engine-generic: each concurrent table writes through its own dedicated connection — no snapshot sharing is involved on the write side, so it engages for EVERY target (Postgres, MySQL). The resolved value is bounded by the TARGET's connection budget and clamped to the table count. Applies to chain restores too (each segment full's bulk-apply; incremental change replay stays strictly ordered). 0 (default) = auto: 4. 1 disables cross-table concurrency. See ADR-0084." default:"0" placeholder:"N"`

	BulkParallelism int `help:"WRITE-side (restore): number of a single table's chunks applied CONCURRENTLY (within-table axis), composed with --table-parallelism; bounded by the target connection budget. Each chunk-group worker writes through its own dedicated connection; snapshot chunks are a disjoint partition of the table's rows, so parallel apply cannot collide on a PK on a cold target. Engages only for tables with >= 2 chunks; the two axes multiply (table × bulk) and their product never exceeds the measured budget. Applies to chain restores too (each segment full's bulk-apply). 0 (default) = auto: min(8, NumCPU); 1 = serial (single-stream per table). See ADR-0112." default:"0" placeholder:"N"`

	ApplyConcurrency int `help:"Key-hash concurrent-apply LANE count for the INCREMENTAL-replay leg of a chain restore (ADR-0104/0105). Only matters when restoring a chain that carries incrementals: the full-restore row load is the bulk COPY governed by --table-parallelism × --bulk-parallelism, while the incremental change-replay would otherwise run through a single serial stream — RTT-bound on a high-latency / cross-region target, so a chain with a large incremental stalls (the chain-restore analog of the from-backup broker's concurrent-replay path). Fans each incremental's changes across W in-order PK-hash lanes; exactly-once is preserved (every change carries the segment's chain position, so lanes persist the identical resume position the serial path did). ADR-0106 fast-by-default: 0 (default, unset) = auto:4 (no connection-budget probe; per-lane backpressure handles a tight target); 1 = explicit serial opt-out; W>1 honored. No effect on a single-full restore." default:"0" placeholder:"W"`

	TargetSchema string `help:"Per-source target schema namespace (Postgres-only). When set, restored tables land in the named schema rather than the DSN's default. Mirrors 'sluice migrate --target-schema' / 'sync start --target-schema' (ADR-0031). PG-only: flat-namespace engines (MySQL) refuse at validate time — operators use a different --target DSN database instead. The schema is auto-created on the target if it doesn't exist. v0.56.0+ closure of the v0.55.0 cycle's UX-gap finding." placeholder:"NAME"`

	// A CHAIN restore (full + incrementals) replays CDC change chunks, and its
	// incremental leg writes the three control tables ChainRestore's
	// EnsureControlTable creates (sluice_cdc_state, sluice_cdc_schema_history,
	// sluice_shard_consolidation_lease). On a SHARDED PlanetScale/Vitess target
	// those vindex-less tables are rejected ("table ... does not have a primary
	// vindex"), so the restore needs the same sidecar-keyspace escape hatch
	// `sync start` grew — resolved + recorded via applyControlKeyspace so the
	// control-keyspace-configured target engine is the one that reaches
	// ChainRestore's OpenChangeApplier. A single FULL restore writes no control
	// tables, so this flag is inert there.
	ControlKeyspace string `name:"control-keyspace" help:"MySQL/PlanetScale/Vitess target only: the unsharded sidecar keyspace a CHAIN restore's CDC control tables live in (see 'sync start --control-keyspace'). A chain restore's incremental replay writes sluice_cdc_state / sluice_cdc_schema_history / sluice_shard_consolidation_lease; a SHARDED target rejects those vindex-less tables, so point this at a separate unsharded keyspace to unblock that case. Omit to auto-detect the sole unsharded sidecar on a sharded target (loud refusal if zero or several candidates). Empty + unsharded/non-Vitess target = the default keyspace (unchanged). Inert on non-MySQL targets and on a single-full restore." placeholder:"KEYSPACE"`

	// PlanetScale target-health telemetry (ADR-0107) — OPTIONAL. When set, the
	// restore clamps the AUTO --table-parallelism × --bulk-parallelism product
	// by the target's LIVE CPU/memory headroom (ADR-0115 / item 40) — the
	// PlanetScale-correct bound, since connections are abundant there but CPU
	// is the scarce resource on small tiers and the connection-budget split
	// only bounds prober-equipped engines (Postgres). Same opt-in /
	// all-or-nothing semantics as 'sync start'; off (no clamp) when unset.
	PlanetScaleOrg            string `name:"planetscale-org" help:"PlanetScale org slug, consumed by BOTH optional PlanetScale integrations: target-health telemetry (CPU/mem/storage) clamping the AUTO restore parallelism product by live headroom (ADR-0107/0115; requires --planetscale-metrics-token-id + --planetscale-metrics-token, all-or-nothing) AND the automatic deploy-request index-build fallback on a planetscale target (ADR-0148; requires the service token, see --planetscale-service-token-id). Each integration arms on its own token pair. Control-plane only — distinct from the data-plane --target DSN. Off when unset." placeholder:"ORG"`
	PlanetScaleMetricsTokenID string `name:"planetscale-metrics-token-id" help:"PlanetScale service-token ID (granted read_metrics_endpoints) for --planetscale-org telemetry. Prefer the env var so the id never lands in shell history." env:"PLANETSCALE_METRICS_TOKEN_ID" placeholder:"ID"`
	PlanetScaleMetricsToken   string `name:"planetscale-metrics-token" help:"PlanetScale service-token secret for --planetscale-org telemetry. Set via the env var (never on the command line); masked in all logging." env:"PLANETSCALE_METRICS_TOKEN" placeholder:"SECRET"`
	PlanetScaleMetricsBranch  string `name:"planetscale-metrics-branch" help:"Target branch to filter telemetry series to (defaults to 'main'). Only consulted when --planetscale-org is set." placeholder:"BRANCH"`
	PlanetScaleMetricsDB      string `name:"planetscale-metrics-db" help:"Target database name to filter PlanetScale telemetry SD to. Defaults to the --target DSN's database. Only consulted when --planetscale-org is set." placeholder:"DATABASE"`

	// ADR-0148 / audit MED-A1: restore's Phase-4 CreateIndexes is the same
	// walled deferred index build migrate's index phase runs, so the same
	// deploy-request fallback arms here — a planetscale target +
	// --planetscale-org + the service token (distinct from the metrics token
	// above; the org flag is shared between the two consumers). Unarmed (any
	// piece missing, or a non-planetscale target), the index phase behaves
	// exactly as before, ending at the SLUICE-E-INDEX-* refusal + hints.
	PlanetScaleDatabase       string        `name:"planetscale-database" help:"PlanetScale database name for the ADR-0148 index-build fallback. Defaults to the --target DSN's database name. Only consulted when --planetscale-org is set." placeholder:"DB"`
	PlanetScaleBranch         string        `name:"planetscale-branch" help:"PlanetScale production branch the --target DSN points at, for the ADR-0148 index-build fallback (deploy requests merge into it). Only consulted when --planetscale-org is set." default:"main" placeholder:"BRANCH"`
	PlanetScaleServiceTokenID string        `name:"planetscale-service-token-id" help:"PlanetScale service-token ID (branch + deploy-request scopes) for the ADR-0148 index-build fallback. Prefer the env var so it never lands in shell history." env:"PLANETSCALE_SERVICE_TOKEN_ID" placeholder:"ID"`
	PlanetScaleServiceToken   string        `name:"planetscale-service-token" help:"PlanetScale service-token secret for the ADR-0148 index-build fallback. Set via the env var (never on the command line); never logged." env:"PLANETSCALE_SERVICE_TOKEN" placeholder:"SECRET"`
	PlanetScaleDeployTimeout  time.Duration `name:"planetscale-deploy-timeout" help:"Per-deploy-request deadline for the ADR-0148 index-build fallback (a large table's index deploys via VReplication — real wall-clock, but async and unbounded by errno 3024). On timeout the deploy keeps running in PlanetScale and re-running the restore picks up: the index phase re-probes and rebuilds only what is still missing." default:"1h" placeholder:"DUR"`

	Format string `help:"Output format: 'text' (default) or 'json' (machine-readable: ONE result envelope on stdout at command end — status completed/refused/failed, per-table row counts; the slog progress stream stays on stderr in both modes)." default:"text" enum:"text,json" placeholder:"FORMAT"`

	targetTLSCAFlag

	EncryptionFlags
}

// Run implements `sluice restore`: it wraps the body in the
// `--format json` result-envelope lifecycle (a pass-through in text
// mode) so exactly one JSON object reaches stdout on every exit path.
func (r *RestoreCmd) Run(g *Globals) error {
	env := newEnvelopeRun("restore", r.Format)
	env.scrub(r.Target)
	env.setNextSteps(fmt.Sprintf(
		"sluice verify --source-driver <SOURCE_DRIVER> --source <SOURCE_DSN> --target-driver %s --target <TARGET_DSN>",
		r.TargetDriver,
	))
	return env.finish(r.run(g, env))
}

func (r *RestoreCmd) run(g *Globals, env *envelopeRun) error {
	cfg, err := config.Load(g.Config)
	if err != nil {
		return err
	}

	target, err := resolveEngine(r.TargetDriver)
	if err != nil {
		return fmt.Errorf("--target-driver: %w", err)
	}
	// Value-fidelity flags (task 2.5): restore WRITES values into the target, so
	// the target connection's --mysql-sql-mode and the sql_mode-emit policy apply.
	if target, err = applyEngineOptions(target, g); err != nil {
		return err
	}
	// CA-pinned verify-ca TLS (ADR-0158): rewrite the target DSN so a MySQL
	// target dials verify-ca.
	if r.Target, err = applyEndpointTLSCA(target, r.Target, r.TargetTLSCA, "target"); err != nil {
		return err
	}
	env.setEngines("", target.Name())

	if len(r.IncludeTable) > 0 && len(r.ExcludeTable) > 0 {
		return errors.New("--include-table and --exclude-table are mutually exclusive")
	}
	if r.FromDir == "" && r.From == "" {
		return errors.New("one of --from-dir or --from is required")
	}
	if r.FromDir != "" && r.From != "" {
		return errors.New("--from-dir and --from are mutually exclusive")
	}
	include, exclude := resolveTableFilterArgs(r.IncludeTable, r.ExcludeTable, cfg)
	filter, err := migcore.NewTableFilter(include, exclude)
	if err != nil {
		return err
	}

	ctx := kongContext()

	// ADR-0155: whether the pretty TTY view will render. Computed up front —
	// before the pre-run INFO lines and the telemetry build below — so those
	// lines can be suppressed on the pretty path; they fire before
	// runWithProgress installs the TTY slog gate and would otherwise leak above
	// the live view. (The v238 backup fix, extended to `restore`.)
	pretty := wantPrettyProgress(g, env.jsonMode, false, false)

	// --control-keyspace is a TARGET concept (the control tables live on the
	// target): a chain restore's incremental replay creates the CDC control
	// tables, which a sharded PlanetScale/Vitess target rejects without a
	// primary vindex. Resolve it (explicit flag, else auto-detect the unsharded
	// sidecar) and record it on the target engine BEFORE it opens the applier —
	// this is the same engine that reaches ChainRestore's OpenChangeApplier, so
	// EnsureControlTable + the incremental position writes land in the sidecar
	// keyspace. Inert on non-MySQL targets and on a single-full restore.
	if target, err = applyControlKeyspace(ctx, target, r.ControlKeyspace, r.Target); err != nil {
		return err
	}

	store, storeDesc, closer, err := openBackupStore(ctx, r.FromDir, r.From, blobcodec.BlobStoreOptions{
		Endpoint:  r.BackupEndpoint,
		Region:    r.BackupRegion,
		PathStyle: r.BackupPathStyle,
	})
	if err != nil {
		return err
	}
	if closer != nil {
		defer func() { _ = closer() }()
	}

	if !pretty {
		// Suppressed on the pretty path so it does not leak above the panel; the
		// live view is the output there (the panel header carries the context).
		slog.InfoContext(
			ctx, "restore: starting full restore",
			slog.String("target_engine", target.Name()),
			slog.String("source", storeDesc),
		)
	}

	// Phase 6.1: read the chain-root manifest first to extract any
	// recorded Argon2id params, so the restore-side envelope's KEK
	// derivation matches the backup's.
	rootManifest, err := lineage.ReadRootManifest(ctx, store)
	if err != nil {
		return fmt.Errorf("restore: read root manifest: %w", err)
	}
	envelope, err := r.buildReadEnvelope(rootManifest)
	if err != nil {
		return err
	}

	// ADR-0148 / audit MED-A1: arm the automatic deploy-request index-build
	// fallback when the target is planetscale and the control-plane
	// credentials resolve; nil (unarmed) leaves the index phase
	// byte-identical to before.
	indexFallback := composePlanetScaleIndexFallback(indexFallbackParams{
		targetDriver:  r.TargetDriver,
		targetDSN:     r.Target,
		org:           r.PlanetScaleOrg,
		database:      r.PlanetScaleDatabase,
		branch:        r.PlanetScaleBranch,
		tokenID:       r.PlanetScaleServiceTokenID,
		token:         r.PlanetScaleServiceToken,
		deployTimeout: r.PlanetScaleDeployTimeout,
	})

	// OPTIONAL PlanetScale telemetry (ADR-0107) — used here only to clamp the
	// AUTO parallelism product by live headroom (ADR-0115). (nil, nil) when
	// off; an org without a complete token pair is a loud refusal — except a
	// fallback-intent arming (org + service-token pair, no metrics token
	// piece), which telemetryParamsSharedOrg routes to
	// telemetry-off-with-WARN instead — on ANY target engine (Bug 192).
	// Closed at return so its background poller stops. quiet=pretty: no
	// telemetry-enabled INFO above the panel (ADR-0156 polish).
	telemetryProvider, err := r.buildTargetTelemetry(ctx, pretty)
	if err != nil {
		return err
	}
	if telemetryProvider != nil {
		defer func() { _ = telemetryProvider.Close() }()
	}

	verifyKey, err := r.resolveVerifyKey()
	if err != nil {
		return err
	}
	restore := &backup.Restore{
		Target:           target,
		TargetDSN:        r.Target,
		Store:            store,
		Filter:           filter,
		MaxBufferBytes:   r.MaxBufferBytes,
		TableParallelism: r.TableParallelism,
		ChunkParallelism: r.BulkParallelism,
		ApplyConcurrency: r.ApplyConcurrency,
		Envelope:         envelope,
		VerifyKey:        verifyKey,
		RequireSignature: r.RequireSignature,
		TargetSchema:     r.TargetSchema,
		// --format json envelope hookup; nil in text mode (no-ops).
		Summary: env.summary,
		// telemetryProviderOrNil returns a TRUE nil interface when off, so the
		// restore's `TargetTelemetry != nil` guard stays exact (no typed-nil trap).
		TargetTelemetry: telemetryProviderOrNil(telemetryProvider),
		// ADR-0148 / audit MED-A1: nil unless armed (see above).
		IndexBuildFallback: indexFallback,
	}
	// Validation is done; errors past this point classify as "failed"
	// (not "refused") in the --format json envelope.
	env.markEngaged()
	// ADR-0155: pretty TTY view for an interactive, non-envelope run. `pretty`
	// was computed up front (before the pre-run INFO / telemetry build) so those
	// lines could be suppressed on the panel path.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	return runWithProgress(pretty, cancel, backup.RestoreProgressSpec,
		func(s progress.Sink) { restore.Progress = s },
		func() error { return restore.Run(runCtx) })
}
