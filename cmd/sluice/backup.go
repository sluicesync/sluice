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

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline"
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

// BackupCmd groups the backup verbs. Phase 1 shipped `full` and
// `verify`; Phase 3 (v0.17.0) adds `incremental` for chained backups
// taken on top of a previous full or incremental; Phase 4 (v0.19.0)
// adds `stream` for continuous-incremental long-running streams. See
// `docs/dev/design/logical-backups.md`,
// `docs/dev/design/logical-backups-phase-3.md`, and
// `docs/dev/design/logical-backups-phase-4.md` for the staged plan.
type BackupCmd struct {
	Full        BackupFullCmd        `cmd:"" help:"Take a full logical backup of a source database to a local directory."`
	Incremental BackupIncrementalCmd `cmd:"" help:"Take an incremental backup chained off a previous full or incremental (Phase 3)."`
	Stream      BackupStreamCmdGroup `cmd:"" help:"Long-running stream that produces rolling incrementals (Phase 4)."`
	Verify      BackupVerifyCmd      `cmd:"" help:"Re-checksum every chunk in an existing backup chain and report any mismatches."`
	Prune       BackupPruneCmd       `cmd:"" help:"Drop the oldest incrementals from an existing chain to bound disk + restore time (GitHub #20 chunk 14c)."`
	Compact     BackupCompactCmd     `cmd:"" help:"Concatenate consecutive segments whose CreatedAt gaps fall within --merge-window into a single segment (GitHub #20 chunk 14d, Task #15)."`
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

	TableParallelism int `help:"Number of tables read CONCURRENTLY during the backup row sweep (the read-side analog of pg_dump -j / migrate --table-parallelism). Engages only when the source surfaces a SHAREABLE exported snapshot (Postgres) — every parallel reader is pinned to the ONE snapshot, so cross-table consistency is identical to the serial sweep. MySQL's snapshot is per-session, so MySQL backups stay serial (an INFO log names the reason). The resolved value is bounded by the source's connection budget, reserving one slot for the snapshot's replication conn. 0 (default) = auto: 4. 1 disables cross-table concurrency. See ADR-0084." default:"0" placeholder:"N"`

	Compression string `help:"Per-segment chunk compression codec: none | gzip | zstd. Default zstd (klauspost/compress at SpeedDefault — 55-85% faster restore, the DR-critical axis; ~1-5% larger than gzip on representative data). 'none' leaves chunks as human-readable .jsonl on a local-FS target; 'gzip' is the pre-v0.67.0 codec. Recorded in lineage.json and read back from there on restore (never inferred from bytes)." default:"zstd" enum:"none,gzip,zstd" placeholder:"CODEC"`

	SlotName string `help:"Replication-slot name suffix on engines with a slot concept (Postgres). Used to label the EndPosition recorded on the manifest so a Phase 3 incremental chained off this full opens CDC against a slot of the same name. Default 'sluice_slot'. Engines without slots (MySQL: binlog stream is the slot) ignore this flag." placeholder:"NAME"`

	ChainSlot bool `help:"Provision incremental-chain prerequisites at backup time (Postgres): create the PERSISTENT replication slot (named by --slot-name) as the snapshot anchor and ensure the pgoutput publication exists before the anchor. 'backup incremental' then chains with zero gap, no manual slot management. The slot is kept once the run's in-progress manifest records the anchor — including across an interruption, where re-running the same command resumes and ADOPTS the slot (ADR-0085); it is dropped only when the run fails before that point. Costs source-side WAL retention until the next incremental consumes the slot (drop via 'sluice slot drop' to abandon the chain). Refuses if the slot already exists on a fresh (non-resume) run. Loud no-op on engines without slots (MySQL)."`

	ForceOverwrite bool `help:"Discard whatever is at the destination and start fresh. By default 'sluice backup full' refuses to overwrite a successful prior backup and RESUMES a partial (in-progress) one, adopting the interrupted attempt's chain anchor (ADR-0085); pass this to discard either. It is also the escape hatch the resume guards (schema drift, keyless re-stream, chain-slot preflight) name. A discarded --chain-slot attempt's slot must be dropped separately ('sluice slot drop')."`

	Redact       []string `help:"Redact a PII column in backup chunks (repeatable). Format: '[schema.]table.column=STRATEGY[:options]'. Strategies: null, static:<v>, hash:sha256, hash:hmac-sha256[:<keyname>], truncate:<n>, mask:inner:<m1>,<m2>[,<char>], mask:outer:<m1>,<m2>[,<char>], mask:ssn, mask:pan, mask:pan-relaxed, mask:email, mask:ca-sin, mask:uk-nin, mask:iban, mask:uuid, randomize:int:<min>,<max>, randomize:email, randomize:us-phone, randomize:uuid, randomize:ssn, randomize:pan[:<brand>], randomize:ca-sin, randomize:uk-nin, randomize:iban[:<country-code>], randomize:dict:<name>, tokenize:dict:<name>[:<keyname>] (Phase 3 v0.61.0+, keyset-sourced Phase 4 v0.62.0+; dictionaries declared in YAML) — same set as 'sluice migrate --redact'. PII Phase 1.5 (v0.55.0+): redaction applies during chunk write, so the on-disk backup is PII-clean. Restore from a redacted chain produces the same redacted shape; restore does NOT re-apply redactions (they were applied at backup time). See docs/dev/notes/prep-pii-redaction-phase-1.md." placeholder:"RULE" sep:"none"`
	KeysetSource string   `help:"Operator keyset source (file:PATH | env:VARNAME | db:DSN) for keyset-using strategies (hash:hmac-sha256, tokenize:dict). PII Phase 4 (ADR-0041). Same forms as 'sluice migrate --keyset-source'." placeholder:"SRC"`

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
	codec, err := pipeline.ParseCompression(b.Compression)
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

	slog.InfoContext(
		ctx, "backup: starting full backup",
		slog.String("source_engine", source.Name()),
		slog.String("destination", storeDesc),
		slog.Int("chunk_size", b.ChunkSize),
	)

	encConfig, err := b.buildBackupEncryption()
	if err != nil {
		return err
	}
	backup := &pipeline.Backup{
		Source:           source,
		SourceDSN:        b.Source,
		Store:            store,
		Filter:           filter,
		ChunkRows:        b.ChunkSize,
		SluiceVersion:    version,
		SlotName:         pipeline.ResolveSlotName(b.SlotName),
		ChainSlot:        b.ChainSlot,
		TableParallelism: b.TableParallelism,
		ForceOverwrite:   b.ForceOverwrite,
		Encryption:       encConfig,
		Codec:            codec,
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
	backup.Redactor = redactor
	logKeysetLoaded(keyset)
	logRedactionConfig(redactor, "backup full")
	return backup.Run(ctx)
}

// openBackupStore opens the right [irbackup.BackupStore] for the operator's
// flag combination. Returns the store, a human-readable destination
// description (for log lines), and an optional closer for backends
// that need cleanup. The S3-only options are validated against the
// URL scheme inside [pipeline.OpenBlobStore].
func openBackupStore(
	ctx context.Context,
	outputDir, target string,
	opts pipeline.BlobStoreOptions,
) (store irbackup.BackupStore, description string, closer func() error, err error) {
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

	slog.InfoContext(
		ctx, "backup: starting incremental",
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
		SlotName:      pipeline.ResolveSlotName(b.SlotName),
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

	RetryAttempts    int           `help:"Cap on consecutive retriable rollover failures the stream will absorb before giving up. Mirrors the sync-stream's --apply-retry-attempts. GitHub #22: transient source-side errors that v0.46.0 fixed for sync streams now also retry on backup-stream. 1 disables retry." default:"8" placeholder:"N"`
	RetryBackoffBase time.Duration `help:"Base interval for exponential backoff between retriable rollover failures. Doubles each attempt, capped at --retry-backoff-cap." default:"100ms" placeholder:"DUR"`
	RetryBackoffCap  time.Duration `help:"Upper bound on each retriable rollover backoff interval." default:"30s" placeholder:"DUR"`

	RetainRotateAt            time.Duration `help:"In-process backup-segment rotation (ADR-0046): once the open segment reaches this age, the stream caps it and opens a fresh segment over the SAME CDC handle (no operator wrapper, no stream exit). Pair with 'sluice backup prune' to bound total disk. 0 disables (unbounded single segment)." placeholder:"DUR"`
	RetainRotateAtChainLength int           `help:"Rotate the open segment after this many incrementals are committed to it. Either rotation threshold firing wins. 0 disables." placeholder:"N"`

	Compression string `help:"Per-segment chunk compression codec: none | gzip | zstd. Default zstd (klauspost/compress at SpeedDefault — 55-85% faster restore, the DR-critical axis; ~1-5% larger than gzip on representative data). 'none' leaves chunks as human-readable .jsonl on a local-FS target; 'gzip' is the pre-v0.67.0 codec. Recorded per segment in lineage.json and read back from there on restore (never inferred from bytes)." default:"zstd" enum:"none,gzip,zstd" placeholder:"CODEC"`

	// Phase-1 rotation flags removed in v0.67.0 (ADR-0046 §6). Kept as
	// hidden no-value sentinels so the operator gets a CLEAR
	// migration error (clean break, not a silent ignore — kong's
	// generic "unknown flag" is less actionable).
	ExitAfterAge         string `name:"exit-after-age" hidden:"" help:"REMOVED in v0.67.0."`
	ExitAfterChainLength string `name:"exit-after-chain-length" hidden:"" help:"REMOVED in v0.67.0."`

	EncryptionFlags
}

// Run implements `sluice backup stream run`.
func (b *BackupStreamCmd) Run(_ *Globals) error {
	if b.ExitAfterAge != "" || b.ExitAfterChainLength != "" {
		return errors.New("--exit-after-age / --exit-after-chain-length were REMOVED in v0.67.0 (ADR-0046): rotation is now always in-process. Use --retain-rotate-at=DUR and/or --retain-rotate-at-chain-length=N instead — the stream caps the open segment and opens a fresh one over the same CDC handle, no operator wrapper needed")
	}
	source, err := resolveEngine(b.SourceDriver)
	if err != nil {
		return fmt.Errorf("--source-driver: %w", err)
	}
	codec, err := pipeline.ParseCompression(b.Compression)
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

	slog.InfoContext(
		ctx, "backup: starting stream",
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

	RebuildCatalog bool `help:"Rebuild lineage.json from scratch by walking the conventional one-segment layout (manifest.json + manifests/incr-*.json), then exit. Use after manual mutation of a single-segment backup. NOTE: a multi-segment (rotated) lineage's sub-dir structure is NOT reconstructable from a bare walk by design — lineage.json IS the structural record for a rotated backup."`

	EncryptionFlags
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
		segments, manifests, err := pipeline.RebuildLineageCatalogAt(ctx, store)
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
	// Bug 117 closure (v0.94.1): when --encrypt is on, load the
	// chain-root manifest so the read envelope re-derives the same
	// Argon2id KEK the writer used, then thread the envelope into
	// VerifyBackupWith. SHA-only verify silently accepted per-chunk
	// passphrase rotation; the decrypt probe refuses it.
	rootManifest, err := pipeline.ReadRootManifest(ctx, store)
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
			ctx, "backup verify: chain is encrypted but no envelope supplied — running SHA-only verify; passphrase rotation (Bug 117) is undetectable in this mode. Re-run with --encrypt + the chain's passphrase / KMS reference to enable the per-chunk decrypt probe.",
			slog.String("kek_mode", rootManifest.ChainEncryption.KEKMode),
			slog.String("kek_ref", rootManifest.ChainEncryption.KEKRef),
		)
	}
	total, mismatches, err := pipeline.VerifyBackupWith(ctx, store, pipeline.VerifyOptions{
		Envelope: envelope,
	})
	if err != nil {
		return err
	}
	if mismatches > 0 {
		return fmt.Errorf("verify: %d of %d chunk(s) failed verification", mismatches, total)
	}
	slog.InfoContext(
		ctx, "backup verify: all chunks OK",
		slog.Int("chunks", total),
		slog.Bool("decrypt_probe", envelope != nil),
	)
	return nil
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
	store, _, closer, err := openBackupStore(ctx, p.FromDir, p.From, pipeline.BlobStoreOptions{
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

	res, err := pipeline.PruneChain(ctx, store, pipeline.PruneOpts{
		KeepIncrementals: p.KeepIncrementals,
		KeepDuration:     p.KeepDuration,
		DryRun:           p.DryRun,
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
	store, _, closer, err := openBackupStore(ctx, c.FromDir, c.From, pipeline.BlobStoreOptions{
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

	res, err := pipeline.CompactChain(ctx, store, pipeline.CompactOpts{
		MergeWindow:     c.MergeWindow,
		DryRun:          c.DryRun,
		SmartCompaction: c.SmartCompaction,
		PKStrategy:      pipeline.PKStrategy(c.CompactionPKStrategy),
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

	TableParallelism int `help:"Number of tables bulk-applied CONCURRENTLY during the restore (the write-side analog of pg_restore -j / migrate --table-parallelism). Engine-generic: each concurrent table writes through its own dedicated connection — no snapshot sharing is involved on the write side, so it engages for EVERY target (Postgres, MySQL). The resolved value is bounded by the TARGET's connection budget and clamped to the table count. Applies to chain restores too (each segment full's bulk-apply; incremental change replay stays strictly ordered). 0 (default) = auto: 4. 1 disables cross-table concurrency. See ADR-0084." default:"0" placeholder:"N"`

	TargetSchema string `help:"Per-source target schema namespace (Postgres-only). When set, restored tables land in the named schema rather than the DSN's default. Mirrors 'sluice migrate --target-schema' / 'sync start --target-schema' (ADR-0031). PG-only: flat-namespace engines (MySQL) refuse at validate time — operators use a different --target DSN database instead. The schema is auto-created on the target if it doesn't exist. v0.56.0+ closure of the v0.55.0 cycle's UX-gap finding." placeholder:"NAME"`

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

	slog.InfoContext(
		ctx, "restore: starting full restore",
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
		Target:           target,
		TargetDSN:        r.Target,
		Store:            store,
		Filter:           filter,
		MaxBufferBytes:   r.MaxBufferBytes,
		TableParallelism: r.TableParallelism,
		Envelope:         envelope,
		TargetSchema:     r.TargetSchema,
	}
	return restore.Run(ctx)
}
