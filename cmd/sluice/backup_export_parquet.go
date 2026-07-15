// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// BackupExportAsParquetCmd runs `sluice backup export-as-parquet`
// (ADR-0163): a one-shot, read-only transcode of an existing backup's
// row chunks into one zstd-compressed Parquet file per table plus a
// parquet_index.json export manifest — the analytics exit surface over
// the chain sluice already captured. Exit-only: sluice never reads its
// Parquet output back; `sluice restore` keeps the JSON-Lines path.
//
// The export represents ONE snapshot — the latest full by default, or
// the full named by --backup-id. Incremental change-windows after that
// full are not folded in (a loud WARN names the count); operators who
// need point-in-time state restore the chain and re-export.
//
// Encryption + signature flags mirror `sluice restore`: an encrypted
// chain needs --encrypt + the chain's passphrase / KMS reference; a
// signed chain is verified (and --require-signature makes that
// strict) before any chunk is decoded. The Parquet files themselves
// are written PLAINTEXT — the operator chose the analytics
// destination's encryption posture separately (ADR-0163).
type BackupExportAsParquetCmd struct {
	FromDir string `help:"Directory containing the backup to export (the same directory --output-dir wrote to). Mutually exclusive with --from." placeholder:"DIR"`
	From    string `help:"URL of the backup to export (s3://, gs://, azblob://, file:///). Mutually exclusive with --from-dir." placeholder:"URL"`

	OutputDir string `help:"Directory the Parquet files + parquet_index.json are written to (local filesystem). Created if it doesn't exist. Mutually exclusive with --output." placeholder:"DIR"`
	Output    string `help:"Destination URL for the Parquet files (s3://bucket/prefix, gs://, azblob://, file:///). Mutually exclusive with --output-dir." placeholder:"URL"`

	BackupEndpoint  string `help:"Override the S3 endpoint for S3-compatible providers. Applies to BOTH --from and --output when they are s3:// URLs." placeholder:"URL"`
	BackupRegion    string `help:"Override the S3 region. Applies to BOTH --from and --output when they are s3:// URLs." placeholder:"REGION"`
	BackupPathStyle bool   `help:"Force path-style addressing. Applies to BOTH --from and --output when they are s3:// URLs."`

	IncludeTable []string `help:"Only export these tables (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --exclude-table." sep:"," placeholder:"TABLE"`
	ExcludeTable []string `help:"Export every table except these (comma-separated, repeatable). Glob patterns allowed. Mutually exclusive with --include-table." sep:"," placeholder:"TABLE"`

	BackupID string `name:"backup-id" help:"Export the segment FULL snapshot with this BackupID instead of the latest one (chain-to-a-point at snapshot granularity; find ids in the chain's manifests or lineage.json). Incremental ids are refused — their change-windows are not exportable." placeholder:"ID"`

	ForceOverwrite bool `help:"Replace a prior export at the destination. By default the command refuses when parquet_index.json is already present."`

	EncryptionFlags
}

// Run implements `sluice backup export-as-parquet`.
func (x *BackupExportAsParquetCmd) Run(g *Globals) error {
	cfg, err := config.Load(g.Config)
	if err != nil {
		return err
	}
	if x.FromDir == "" && x.From == "" {
		return errors.New("one of --from-dir or --from is required")
	}
	if x.FromDir != "" && x.From != "" {
		return errors.New("--from-dir and --from are mutually exclusive")
	}
	if x.OutputDir == "" && x.Output == "" {
		return errors.New("one of --output-dir or --output is required")
	}
	if x.OutputDir != "" && x.Output != "" {
		return errors.New("--output-dir and --output are mutually exclusive")
	}
	if len(x.IncludeTable) > 0 && len(x.ExcludeTable) > 0 {
		return errors.New("--include-table and --exclude-table are mutually exclusive")
	}
	include, exclude := resolveTableFilterArgs(x.IncludeTable, x.ExcludeTable, cfg)
	filter, err := migcore.NewTableFilter(include, exclude)
	if err != nil {
		return err
	}

	ctx := kongContext()
	blobOpts := blobcodec.BlobStoreOptions{
		Endpoint:  x.BackupEndpoint,
		Region:    x.BackupRegion,
		PathStyle: x.BackupPathStyle,
	}
	store, storeDesc, closer, err := openBackupStore(ctx, x.FromDir, x.From, blobOpts)
	if err != nil {
		return err
	}
	if closer != nil {
		defer func() { _ = closer() }()
	}
	output, outputDesc, outCloser, err := openBackupStore(ctx, x.OutputDir, x.Output, blobOpts)
	if err != nil {
		return fmt.Errorf("open export destination: %w", err)
	}
	if outCloser != nil {
		defer func() { _ = outCloser() }()
	}

	// Read-side envelope: same chain-root Argon2id re-derivation as
	// restore (nil for plaintext chains / when --encrypt is unset).
	rootManifest, err := lineage.ReadRootManifest(ctx, store)
	if err != nil {
		return fmt.Errorf("export-as-parquet: read root manifest: %w", err)
	}
	envelope, err := x.buildReadEnvelope(rootManifest)
	if err != nil {
		return err
	}
	verifyKey, err := x.resolveVerifyKey()
	if err != nil {
		return err
	}

	slog.InfoContext(
		ctx, "export-as-parquet: reading backup",
		slog.String("source", storeDesc),
		slog.String("destination", outputDesc),
	)
	export := &backup.ParquetExport{
		Store:            store,
		Output:           output,
		Filter:           filter,
		BackupID:         x.BackupID,
		ForceOverwrite:   x.ForceOverwrite,
		Envelope:         envelope,
		VerifyKey:        verifyKey,
		RequireSignature: x.RequireSignature,
		SluiceVersion:    version,
	}
	return export.Run(ctx)
}
