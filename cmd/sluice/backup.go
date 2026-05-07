// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/pipeline"
)

// BackupCmd groups the backup verbs. Phase 1 ships the `full` and
// `verify` subcommands; Phase 3 will add `incremental`. See
// `docs/dev/design-logical-backups.md` for the staged plan.
type BackupCmd struct {
	Full   BackupFullCmd   `cmd:"" help:"Take a full logical backup of a source database to a local directory."`
	Verify BackupVerifyCmd `cmd:"" help:"Re-checksum every chunk in an existing backup directory and report any mismatches."`
}

// BackupFullCmd runs `sluice backup full`. Reads the source schema,
// streams each table's rows to one or more JSON-Lines + gzip chunk
// files under --output-dir, and writes a manifest.json describing
// the schema + chunks + per-chunk SHA-256.
//
// Phase 1 limitations (called out so operators set expectations
// correctly):
//
//   - Local filesystem only. Cloud backends (S3 / GCS / Azure) are
//     Phase 2; the `BackupStore` interface is designed for them but
//     the CLI doesn't expose --target=s3:// yet.
//   - Full snapshot only. Incremental backups are Phase 3.
//   - No client-side encryption. Backups rest on disk unencrypted;
//     operators relying on filesystem-level encryption (LUKS /
//     BitLocker / FileVault) carry that responsibility today.
//     KMS-backed encryption is Phase 6.
type BackupFullCmd struct {
	SourceDriver string `help:"Source engine name (e.g. mysql, postgres). See 'sluice engines'." required:"" placeholder:"NAME" group:"source"`
	Source       string `help:"Source database DSN." required:"" env:"SLUICE_SOURCE" placeholder:"DSN" group:"source"`

	OutputDir string `help:"Directory the backup is written to. Created if it doesn't exist. Manifest lives at <DIR>/manifest.json; chunks live under <DIR>/chunks/<table>/." required:"" placeholder:"DIR"`

	IncludeTable []string `help:"Only back up these tables (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --exclude-table." sep:"," placeholder:"TABLE"`
	ExcludeTable []string `help:"Back up every table except these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-table." sep:"," placeholder:"TABLE"`

	ChunkSize int `help:"Maximum rows per chunk file. The writer rolls over to a new file whenever the current chunk hits this row count. Smaller chunks restore faster (per-chunk SHA-256 verification can fail-fast on the smallest possible unit) but inflate the manifest. Default 100000." default:"100000" placeholder:"N"`
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
	include, exclude := resolveTableFilterArgs(b.IncludeTable, b.ExcludeTable, cfg)
	filter, err := pipeline.NewTableFilter(include, exclude)
	if err != nil {
		return err
	}

	store, err := pipeline.NewLocalStore(b.OutputDir)
	if err != nil {
		return fmt.Errorf("open output directory: %w", err)
	}

	slog.InfoContext(kongContext(), "backup: starting full backup",
		slog.String("source_engine", source.Name()),
		slog.String("output_dir", store.Root()),
		slog.Int("chunk_size", b.ChunkSize),
	)

	backup := &pipeline.Backup{
		Source:        source,
		SourceDSN:     b.Source,
		Store:         store,
		Filter:        filter,
		ChunkRows:     b.ChunkSize,
		SluiceVersion: version,
	}
	return backup.Run(kongContext())
}

// BackupVerifyCmd runs `sluice backup verify`. Walks an existing
// backup directory, recomputes every chunk's SHA-256, and reports
// any that don't match the manifest. Useful for cron probes against
// archived backups — confirms the bits are still good without needing
// a target database to restore into.
type BackupVerifyCmd struct {
	FromDir string `help:"Directory containing the backup to verify (the same directory --output-dir wrote to)." required:"" placeholder:"DIR"`
}

// Run implements `sluice backup verify`.
func (v *BackupVerifyCmd) Run(_ *Globals) error {
	store, err := pipeline.NewLocalStore(v.FromDir)
	if err != nil {
		return fmt.Errorf("open backup directory: %w", err)
	}
	ctx := kongContext()
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
// --from-dir, applies the schema (with cross-engine retargeting if
// the target differs from the backup's source engine), bulk-copies
// every chunk's rows back, and creates indexes / constraints / views.
//
// Cross-engine restore (PG backup → MySQL target, etc.) is supported
// via `translate.RetargetForEngine` — the same machinery `sluice
// schema diff` uses to bridge type differences.
type RestoreCmd struct {
	FromDir string `help:"Directory containing the backup to restore from. Manifest is at <DIR>/manifest.json." required:"" placeholder:"DIR"`

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
	include, exclude := resolveTableFilterArgs(r.IncludeTable, r.ExcludeTable, cfg)
	filter, err := pipeline.NewTableFilter(include, exclude)
	if err != nil {
		return err
	}

	store, err := pipeline.NewLocalStore(r.FromDir)
	if err != nil {
		return fmt.Errorf("open backup directory: %w", err)
	}

	slog.InfoContext(kongContext(), "restore: starting full restore",
		slog.String("target_engine", target.Name()),
		slog.String("from_dir", store.Root()),
	)

	restore := &pipeline.Restore{
		Target:         target,
		TargetDSN:      r.Target,
		Store:          store,
		Filter:         filter,
		MaxBufferBytes: r.MaxBufferBytes,
	}
	return restore.Run(kongContext())
}
