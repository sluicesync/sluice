// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/pipeline"
)

// BackupCmd groups the backup verbs. Phase 1 shipped `full` and
// `verify`; Phase 3 (v0.17.0) adds `incremental` for chained backups
// taken on top of a previous full or incremental. See
// `docs/dev/design-logical-backups.md` and
// `docs/dev/design-logical-backups-phase-3.md` for the staged plan.
type BackupCmd struct {
	Full        BackupFullCmd        `cmd:"" help:"Take a full logical backup of a source database to a local directory."`
	Incremental BackupIncrementalCmd `cmd:"" help:"Take an incremental backup chained off a previous full or incremental (Phase 3)."`
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

	ForceOverwrite bool `help:"Replace an existing completed backup at the destination. By default 'sluice backup full' refuses to overwrite a successful prior backup; pass this to discard the prior contents and start fresh. Partial (in-progress) backups always resume regardless of this flag."`
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

	backup := &pipeline.Backup{
		Source:         source,
		SourceDSN:      b.Source,
		Store:          store,
		Filter:         filter,
		ChunkRows:      b.ChunkSize,
		SluiceVersion:  version,
		ForceOverwrite: b.ForceOverwrite,
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
	}
	return incr.Run(ctx)
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

	restore := &pipeline.Restore{
		Target:         target,
		TargetDSN:      r.Target,
		Store:          store,
		Filter:         filter,
		MaxBufferBytes: r.MaxBufferBytes,
	}
	return restore.Run(ctx)
}
