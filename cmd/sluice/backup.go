// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/crypto"
	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/pipeline"
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

	EncryptMode string `name:"encrypt-mode" enum:"per-chain,per-chunk" default:"per-chain" help:"Encryption mode: 'per-chain' (single CEK per chain; default; one KEK derive / KMS Decrypt per restore) or 'per-chunk' (one CEK per chunk; defense-in-depth at the cost of per-chunk wrap)."`
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

// buildBackupEncryption constructs a [pipeline.BackupEncryption] for
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
func (e *EncryptionFlags) buildBackupEncryption() (*pipeline.BackupEncryption, error) {
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
	mode := e.EncryptMode
	if mode == "" {
		mode = crypto.EncryptModePerChain
	}
	switch {
	case e.KMSKeyARN != "":
		env, err := crypto.NewKMSEnvelope(kongContext(), e.KMSKeyARN, kmsOpts(e.KMSRegion)...)
		if err != nil {
			return nil, fmt.Errorf("encryption: build aws kms envelope: %w", err)
		}
		return &pipeline.BackupEncryption{
			Envelope: env,
			Mode:     mode,
			KEKRef:   e.KMSKeyARN,
		}, nil
	case e.GCPKMSKeyResource != "":
		env, err := crypto.NewGCPKMSEnvelope(kongContext(), e.GCPKMSKeyResource)
		if err != nil {
			return nil, fmt.Errorf("encryption: build gcp kms envelope: %w", err)
		}
		return &pipeline.BackupEncryption{
			Envelope: env,
			Mode:     mode,
			KEKRef:   e.GCPKMSKeyResource,
		}, nil
	case e.AzureKeyVaultID != "":
		env, err := crypto.NewAzureKMSEnvelope(kongContext(), e.AzureKeyVaultID, azureKMSOpts(e.AzureWrapAlgorithm)...)
		if err != nil {
			return nil, fmt.Errorf("encryption: build azure kms envelope: %w", err)
		}
		return &pipeline.BackupEncryption{
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
	return &pipeline.BackupEncryption{
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
func passphraseRebuildForChain(passphrase string) func(*ir.Argon2idParams) (crypto.EnvelopeEncryption, error) {
	return func(p *ir.Argon2idParams) (crypto.EnvelopeEncryption, error) {
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
func (e *EncryptionFlags) buildReadEnvelope(rootManifest *ir.Manifest) (crypto.EnvelopeEncryption, error) {
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

// BackupCmd groups the backup verbs. Phase 1 shipped `full` and
// `verify`; Phase 3 (v0.17.0) adds `incremental` for chained backups
// taken on top of a previous full or incremental; Phase 4 (v0.19.0)
// adds `stream` for continuous-incremental long-running streams. See
// `docs/dev/design-logical-backups.md`,
// `docs/dev/design-logical-backups-phase-3.md`, and
// `docs/dev/design-logical-backups-phase-4.md` for the staged plan.
type BackupCmd struct {
	Full        BackupFullCmd        `cmd:"" help:"Take a full logical backup of a source database to a local directory."`
	Incremental BackupIncrementalCmd `cmd:"" help:"Take an incremental backup chained off a previous full or incremental (Phase 3)."`
	Stream      BackupStreamCmdGroup `cmd:"" help:"Long-running stream that produces rolling incrementals (Phase 4)."`
	Verify      BackupVerifyCmd      `cmd:"" help:"Re-checksum every chunk in an existing backup chain and report any mismatches."`
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

	SlotName string `help:"Replication-slot name suffix on engines with a slot concept (Postgres). Used to label the EndPosition recorded on the manifest so a Phase 3 incremental chained off this full opens CDC against a slot of the same name. Default 'sluice_slot'. Engines without slots (MySQL: binlog stream is the slot) ignore this flag." placeholder:"NAME"`

	ForceOverwrite bool `help:"Replace an existing completed backup at the destination. By default 'sluice backup full' refuses to overwrite a successful prior backup; pass this to discard the prior contents and start fresh. Partial (in-progress) backups always resume regardless of this flag."`

	EncryptionFlags
}

// Run implements `sluice backup full`.
func (b *BackupFullCmd) Run(g *Globals) error {
	cfg, err := config.Load(g.Config)
	if err != nil {
		return err
	}

	source, err := resolveEngine(b.SourceDriver)
	if err != nil {
		return fmt.Errorf("--source-driver: %w", err)
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
	filter, err := pipeline.NewTableFilter(include, exclude)
	if err != nil {
		return err
	}

	ctx := kongContext()
	store, storeDesc, closer, err := openBackupStore(ctx, b.OutputDir, b.Target, pipeline.BlobStoreOptions{
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

	slog.InfoContext(ctx, "backup: starting full backup",
		slog.String("source_engine", source.Name()),
		slog.String("destination", storeDesc),
		slog.Int("chunk_size", b.ChunkSize),
	)

	encConfig, err := b.buildBackupEncryption()
	if err != nil {
		return err
	}
	backup := &pipeline.Backup{
		Source:         source,
		SourceDSN:      b.Source,
		Store:          store,
		Filter:         filter,
		ChunkRows:      b.ChunkSize,
		SluiceVersion:  version,
		SlotName:       pipeline.ResolveSlotName(b.SlotName),
		ForceOverwrite: b.ForceOverwrite,
		Encryption:     encConfig,
	}
	return backup.Run(ctx)
}

// openBackupStore opens the right [ir.BackupStore] for the operator's
// flag combination. Returns the store, a human-readable destination
// description (for log lines), and an optional closer for backends
// that need cleanup. The S3-only options are validated against the
// URL scheme inside [pipeline.OpenBlobStore].
func openBackupStore(
	ctx context.Context,
	outputDir, target string,
	opts pipeline.BlobStoreOptions,
) (store ir.BackupStore, description string, closer func() error, err error) {
	switch {
	case outputDir != "":
		s, err := pipeline.NewLocalStore(outputDir)
		if err != nil {
			return nil, "", nil, fmt.Errorf("open output directory: %w", err)
		}
		root := s.Root()
		return s, root, nil, nil
	case target != "":
		s, err := pipeline.OpenBlobStore(ctx, target, opts)
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

	EncryptionFlags
}

// Run implements `sluice backup incremental`.
func (b *BackupIncrementalCmd) Run(_ *Globals) error {
	source, err := resolveEngine(b.SourceDriver)
	if err != nil {
		return fmt.Errorf("--source-driver: %w", err)
	}
	if b.OutputDir == "" && b.Target == "" {
		return errors.New("one of --output-dir or --target is required")
	}
	if b.OutputDir != "" && b.Target != "" {
		return errors.New("--output-dir and --target are mutually exclusive")
	}
	ctx := kongContext()
	store, storeDesc, closer, err := openBackupStore(ctx, b.OutputDir, b.Target, pipeline.BlobStoreOptions{
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

	slog.InfoContext(ctx, "backup: starting incremental",
		slog.String("source_engine", source.Name()),
		slog.String("destination", storeDesc),
		slog.String("since", b.Since),
		slog.Duration("window", b.Window),
	)

	encConfig, err := b.buildBackupEncryption()
	if err != nil {
		return err
	}
	incr := &pipeline.IncrementalBackup{
		Source:        source,
		SourceDSN:     b.Source,
		Store:         store,
		ParentRef:     b.Since,
		SlotName:      b.SlotName,
		Window:        b.Window,
		MaxChanges:    b.MaxChanges,
		ChunkChanges:  b.ChunkSize,
		SluiceVersion: version,
		Encryption:    encConfig,
	}
	return incr.Run(ctx)
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

	EncryptionFlags
}

// Run implements `sluice backup stream run`.
func (b *BackupStreamCmd) Run(_ *Globals) error {
	source, err := resolveEngine(b.SourceDriver)
	if err != nil {
		return fmt.Errorf("--source-driver: %w", err)
	}
	if b.OutputDir == "" && b.Target == "" {
		return errors.New("one of --output-dir or --target is required")
	}
	if b.OutputDir != "" && b.Target != "" {
		return errors.New("--output-dir and --target are mutually exclusive")
	}
	ctx := kongContext()
	store, storeDesc, closer, err := openBackupStore(ctx, b.OutputDir, b.Target, pipeline.BlobStoreOptions{
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

	slog.InfoContext(ctx, "backup: starting stream",
		slog.String("source_engine", source.Name()),
		slog.String("destination", storeDesc),
		slog.String("since", b.Since),
		slog.Duration("rollover_window", b.RolloverWindow),
	)

	encConfig, err := b.buildBackupEncryption()
	if err != nil {
		return err
	}
	stream := &pipeline.BackupStream{
		Source:                source,
		SourceDSN:             b.Source,
		Store:                 store,
		ParentRef:             b.Since,
		SlotName:              b.SlotName,
		RolloverWindow:        b.RolloverWindow,
		RolloverMaxChanges:    b.RolloverMaxChanges,
		RolloverMaxBytes:      b.RolloverMaxBytes,
		ChunkChanges:          b.ChunkSize,
		IncludeEmptyRollovers: b.IncludeEmpty,
		Force:                 b.Force,
		RolloverHook:          b.RolloverHook,
		SluiceVersion:         version,
		Encryption:            encConfig,
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
	store, storeDesc, closer, err := openBackupStore(ctx, b.OutputDir, b.Target, pipeline.BlobStoreOptions{
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
	slog.InfoContext(ctx, "backup stream stop: signal written; running stream will exit on next rollover-tick",
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
type BackupVerifyCmd struct {
	FromDir string `help:"Directory containing the backup to verify (the same directory --output-dir wrote to). Mutually exclusive with --from." placeholder:"DIR"`
	From    string `help:"URL of the backup to verify (s3://, gs://, azblob://, file:///). Mutually exclusive with --from-dir." placeholder:"URL"`

	BackupEndpoint  string `help:"Override the S3 endpoint for S3-compatible providers. Only meaningful when --from is an s3:// URL." placeholder:"URL"`
	BackupRegion    string `help:"Override the S3 region. Only meaningful when --from is an s3:// URL." placeholder:"REGION"`
	BackupPathStyle bool   `help:"Force path-style addressing. Only meaningful when --from is an s3:// URL."`

	RebuildCatalog bool `help:"Rebuild the chain.json catalog from scratch by walking every manifest, then exit. Use after manual chain mutation (operator-driven prune) or to seed a catalog on a legacy chain produced by sluice older than v0.47.0."`
}

// Run implements `sluice backup verify`.
func (v *BackupVerifyCmd) Run(_ *Globals) error {
	if v.FromDir == "" && v.From == "" {
		return errors.New("one of --from-dir or --from is required")
	}
	if v.FromDir != "" && v.From != "" {
		return errors.New("--from-dir and --from are mutually exclusive")
	}
	ctx := kongContext()
	store, _, closer, err := openBackupStore(ctx, v.FromDir, v.From, pipeline.BlobStoreOptions{
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
		entries, err := pipeline.RebuildChainCatalogAt(ctx, store)
		if err != nil {
			return fmt.Errorf("rebuild chain catalog: %w", err)
		}
		slog.InfoContext(ctx, "chain catalog rebuilt",
			slog.Int("entries", entries),
		)
		return nil
	}
	total, mismatches, err := pipeline.VerifyBackup(ctx, store)
	if err != nil {
		return err
	}
	if mismatches > 0 {
		return fmt.Errorf("verify: %d of %d chunk(s) failed SHA-256 check", mismatches, total)
	}
	slog.InfoContext(ctx, "backup verify: all chunks OK",
		slog.Int("chunks", total),
	)
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

	EncryptionFlags
}

// Run implements `sluice restore`.
func (r *RestoreCmd) Run(g *Globals) error {
	cfg, err := config.Load(g.Config)
	if err != nil {
		return err
	}

	target, err := resolveEngine(r.TargetDriver)
	if err != nil {
		return fmt.Errorf("--target-driver: %w", err)
	}

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
	filter, err := pipeline.NewTableFilter(include, exclude)
	if err != nil {
		return err
	}

	ctx := kongContext()
	store, storeDesc, closer, err := openBackupStore(ctx, r.FromDir, r.From, pipeline.BlobStoreOptions{
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

	slog.InfoContext(ctx, "restore: starting full restore",
		slog.String("target_engine", target.Name()),
		slog.String("source", storeDesc),
	)

	// Phase 6.1: read the chain-root manifest first to extract any
	// recorded Argon2id params, so the restore-side envelope's KEK
	// derivation matches the backup's.
	rootManifest, err := pipeline.ReadRootManifest(ctx, store)
	if err != nil {
		return fmt.Errorf("restore: read root manifest: %w", err)
	}
	envelope, err := r.buildReadEnvelope(rootManifest)
	if err != nil {
		return err
	}

	restore := &pipeline.Restore{
		Target:         target,
		TargetDSN:      r.Target,
		Store:          store,
		Filter:         filter,
		MaxBufferBytes: r.MaxBufferBytes,
		Envelope:       envelope,
	}
	return restore.Run(ctx)
}
