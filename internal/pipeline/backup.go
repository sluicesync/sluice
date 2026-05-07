// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Backup orchestrator. Phase 1 of the logical-backup feature
// (`docs/dev/design-logical-backups.md`): full snapshot to a
// [ir.BackupStore], one chunk file per N rows per table, plus a
// JSON manifest that lists schema + chunks + per-chunk SHA-256.
//
// Shape mirrors [Migrator]:
//
//   - Construct the value with engine + DSN + filter + chunk size.
//   - Call [Backup.Run] with a context.
//   - Errors are wrapped with phase names so a failed run pinpoints
//     where it failed without parsing strings.
//
// The orchestrator is sequential per table (Phase 2 will add
// parallel reads — same shape as the parallel bulk-copy path —
// once cloud backends with multipart upload land).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// DefaultBackupChunkRows is the per-chunk row count when [Backup]'s
// ChunkRows is left at zero. 100,000 rows is the proto-ADR default;
// big enough to amortise per-chunk JSON-Lines + gzip overhead, small
// enough that a single chunk fits comfortably in memory at restore
// time on commodity hardware. Operators tune via --chunk-size.
const DefaultBackupChunkRows = 100_000

// ManifestFileName is the filename of the manifest within a backup
// directory. Convention; restore looks here first.
const ManifestFileName = "manifest.json"

// Backup runs a single Phase 1 full backup against Source / SourceDSN
// and writes to Store. Construct the value, then call Run.
//
// Backup does not retain state between Run calls. Concurrent calls
// on the same value are not supported.
type Backup struct {
	// Source is the engine the source DSN belongs to. Required.
	Source ir.Engine

	// SourceDSN is the source-engine-native connection string.
	// Required.
	SourceDSN string

	// Store is the [ir.BackupStore] backup chunks and manifest are
	// written to. Required.
	Store ir.BackupStore

	// Filter selects which source tables participate in the backup.
	// Empty filter (zero value) keeps every table the schema reader
	// returns.
	Filter TableFilter

	// ChunkRows is the per-chunk row count. Zero falls back to
	// [DefaultBackupChunkRows]. The writer rolls over to a new chunk
	// file whenever the current one hits this row count.
	ChunkRows int

	// SluiceVersion is the build identifier of the running binary,
	// recorded in the manifest. Optional — empty leaves the field
	// blank in the manifest.
	SluiceVersion string

	// Now, when set, overrides the wall-clock-time source for
	// [Manifest.CreatedAt]. Used by tests to pin timestamps; in
	// production callers leave it nil and the default uses time.Now.
	Now func() time.Time
}

// Run executes the backup. Returns nil on success; a wrapped error on
// any phase failure.
//
// On success: the Store contains exactly one `manifest.json` and one
// chunk file per (table, chunk-index) in the source schema.
func (b *Backup) Run(ctx context.Context) error {
	if err := b.validate(); err != nil {
		return err
	}

	// Engine-default exclusions (Bug 22): merge in PlanetScale's
	// `_vt_*` shadow tables when the source signals them via the
	// optional [ir.DefaultTableExcluder] surface.
	if eff, added := effectiveTableFilter(b.Filter, b.Source, b.SourceDSN); len(added) > 0 {
		slog.InfoContext(ctx, "backup: applying engine-default table exclusions",
			slog.String("engine", b.Source.Name()),
			slog.Any("patterns", added),
		)
		b.Filter = eff
	}

	// 1. Read source schema.
	sr, err := b.Source.OpenSchemaReader(ctx, b.SourceDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("backup: open source schema reader: %w", err))
	}
	defer closeIf(sr)

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("backup: read source schema: %w", err))
	}
	if len(schema.Tables) == 0 {
		slog.InfoContext(ctx, "backup: source schema has no tables; manifest with empty table list will be written")
	}

	// 2. Apply table filter.
	if err := applyTableFilter(ctx, schema, b.Filter); err != nil {
		// applyTableFilter errors when the filter excludes everything.
		// For backups that's still a valid intent in some workflows
		// (e.g. "snapshot-time-only"), but matching the migrate shape
		// — surface the error so the operator notices.
		return err
	}

	// 3. Open row reader.
	rr, err := b.Source.OpenRowReader(ctx, b.SourceDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("backup: open source row reader: %w", err))
	}
	defer closeIf(rr)

	// 4. Stream each table to chunk file(s).
	chunkRows := b.ChunkRows
	if chunkRows <= 0 {
		chunkRows = DefaultBackupChunkRows
	}

	tableEntries := make([]*ir.TableManifest, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		entry, err := b.backupTable(ctx, rr, table, chunkRows)
		if err != nil {
			return wrapWithHint(PhaseBulkCopy, fmt.Errorf("backup: table %q: %w", table.Name, err))
		}
		tableEntries = append(tableEntries, entry)
	}

	// 5. Write manifest.
	now := time.Now
	if b.Now != nil {
		now = b.Now
	}
	manifest := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		SluiceVersion: b.SluiceVersion,
		CreatedAt:     now().UTC(),
		SourceEngine:  b.Source.Name(),
		Schema:        schema,
		Tables:        tableEntries,
	}
	if err := writeManifest(ctx, b.Store, manifest); err != nil {
		return fmt.Errorf("backup: write manifest: %w", err)
	}

	totalRows := int64(0)
	totalChunks := 0
	for _, t := range tableEntries {
		totalRows += t.RowCount
		totalChunks += len(t.Chunks)
	}
	slog.InfoContext(ctx, "backup complete",
		slog.Int("tables", len(tableEntries)),
		slog.Int64("rows", totalRows),
		slog.Int("chunks", totalChunks),
	)
	return nil
}

// validate checks that all required fields are populated.
func (b *Backup) validate() error {
	switch {
	case b.Source == nil:
		return errors.New("backup: Source engine is nil")
	case b.SourceDSN == "":
		return errors.New("backup: SourceDSN is empty")
	case b.Store == nil:
		return errors.New("backup: Store is nil")
	}
	return nil
}

// backupTable streams every row of table from rr through one or more
// chunk files in the Store, returning the manifest entry that
// describes them.
//
// One subtle point worth flagging: the chunk-roll boundary is checked
// AFTER each row write. A table whose row count is an exact multiple
// of chunkRows ends with a fully-written chunk and no extra trailing
// chunk. Empty tables produce zero chunks (the manifest entry has
// RowCount=0 and Chunks=nil) — keeps the storage layout tidy.
func (b *Backup) backupTable(
	ctx context.Context,
	rr ir.RowReader,
	table *ir.Table,
	chunkRows int,
) (*ir.TableManifest, error) {
	rows, err := rr.ReadRows(ctx, table)
	if err != nil {
		return nil, fmt.Errorf("read rows: %w", err)
	}

	cols := nonGeneratedTableColumns(table)
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Name
	}

	entry := &ir.TableManifest{
		Schema: table.Schema,
		Name:   table.Name,
	}

	var (
		writer    *chunkWriter
		buf       *bytes.Buffer
		chunkIdx  int
		rowsTotal int64
	)

	flush := func() error {
		if writer == nil {
			return nil
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("close chunk: %w", err)
		}
		chunkPath := chunkFilePath(table, chunkIdx)
		if err := b.Store.Put(ctx, chunkPath, buf); err != nil {
			return fmt.Errorf("store put %q: %w", chunkPath, err)
		}
		entry.Chunks = append(entry.Chunks, &ir.ChunkInfo{
			File:     chunkPath,
			RowCount: writer.RowCount(),
			SHA256:   writer.Hash(),
		})
		writer = nil
		buf = nil
		chunkIdx++
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case row, ok := <-rows:
			if !ok {
				if err := flush(); err != nil {
					return nil, err
				}
				// Surface any sticky error captured by the reader's
				// streaming goroutine. Mirrors the pattern other
				// pipeline phases follow.
				if errReader, ok := rr.(interface{ Err() error }); ok {
					if e := errReader.Err(); e != nil {
						return nil, fmt.Errorf("row reader: %w", e)
					}
				}
				entry.RowCount = rowsTotal
				slog.InfoContext(ctx, "backup: table complete",
					slog.String("table", table.Name),
					slog.Int64("rows", rowsTotal),
					slog.Int("chunks", len(entry.Chunks)),
				)
				return entry, nil
			}
			if writer == nil {
				buf = &bytes.Buffer{}
				w, err := newChunkWriter(buf, colNames)
				if err != nil {
					return nil, fmt.Errorf("open chunk: %w", err)
				}
				writer = w
			}
			if err := writer.WriteRow(row, cols); err != nil {
				return nil, fmt.Errorf("write row: %w", err)
			}
			rowsTotal++
			if writer.RowCount() >= int64(chunkRows) {
				if err := flush(); err != nil {
					return nil, err
				}
			}
		}
	}
}

// chunkFilePath returns the conventional path of chunk index for
// table within the backup store. Forward-slash separated, kept short
// because the path lands in the manifest verbatim.
func chunkFilePath(table *ir.Table, idx int) string {
	name := table.Name
	if table.Schema != "" {
		// Schema-qualified tables get a `<schema>__<name>` directory
		// to avoid collisions when two PG schemas have a same-named
		// table. Underscore-double rather than slash because some
		// object stores reserve specific path shapes.
		name = table.Schema + "__" + table.Name
	}
	return path.Join("chunks", name, fmt.Sprintf("%s-%d.jsonl.gz", name, idx))
}

// nonGeneratedTableColumns returns the columns of table that are NOT
// generated columns. Mirrors the engine row readers' filtering — the
// row stream omits generated values, so the chunk writer must skip
// them too or it'd emit nil for every generated column slot. The
// restore path does the same skip on insert.
func nonGeneratedTableColumns(table *ir.Table) []*ir.Column {
	out := make([]*ir.Column, 0, len(table.Columns))
	for _, c := range table.Columns {
		if c.IsGenerated() {
			continue
		}
		out = append(out, c)
	}
	return out
}

// writeManifest serialises manifest as JSON (indented for human
// readability) and writes it to the store. The manifest is the
// public contract; readability matters.
func writeManifest(ctx context.Context, store ir.BackupStore, manifest *ir.Manifest) error {
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	return store.Put(ctx, ManifestFileName, bytes.NewReader(b))
}

// readManifest loads and decodes the manifest from store. Used by
// both restore and `sluice backup verify`.
func readManifest(ctx context.Context, store ir.BackupStore) (*ir.Manifest, error) {
	rc, err := store.Get(ctx, ManifestFileName)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read manifest body: %w", err)
	}
	var m ir.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	if m.FormatVersion > ir.BackupFormatVersion {
		return nil, fmt.Errorf("backup: manifest format version %d is newer than this build supports (%d); upgrade sluice",
			m.FormatVersion, ir.BackupFormatVersion)
	}
	return &m, nil
}
