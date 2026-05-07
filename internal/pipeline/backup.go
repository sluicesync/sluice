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
//
// # Resumable backups (Phase 2)
//
// A re-run of `sluice backup full` against the same destination is
// resumable: the orchestrator reads any pre-existing manifest at the
// destination and decides whether to start fresh, resume, or refuse.
//
//   - If no manifest exists, the run starts fresh.
//   - If the prior manifest is `partial_state == "complete"` (a
//     successful prior run), the new run refuses unless
//     [Backup.ForceOverwrite] is set. Operators trigger this from the
//     CLI with `--force-overwrite`, mirroring the friction tier of
//     `migrate --reset-target-data` (ADR-0023).
//   - If the prior manifest is `partial_state == "in_progress"`, the
//     new run resumes: tables already fully written in the prior run
//     are kept verbatim (their chunks are HEAD-checked for presence),
//     and the run picks up at the next un-completed table. Within a
//     table, chunk writes that find an existing object with a
//     matching SHA-256 are skipped; mismatches overwrite (treating
//     the prior bytes as a corrupted partial upload).
//
// The manifest is committed to the store after every table completes,
// so a crashed run leaves at most one table's worth of work to redo.

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

	// ForceOverwrite, when true, lets a re-run replace a previously-
	// completed backup at the same destination. Without it, finding a
	// `partial_state == "complete"` manifest at the destination is an
	// operator-actionable error. This is the analog of
	// `migrate --reset-target-data` for the backup verb. In-progress
	// manifests always resume regardless of this flag.
	ForceOverwrite bool

	// Now, when set, overrides the wall-clock-time source for
	// [Manifest.CreatedAt]. Used by tests to pin timestamps; in
	// production callers leave it nil and the default uses time.Now.
	Now func() time.Time
}

// Run executes the backup. Returns nil on success; a wrapped error on
// any phase failure.
//
// On success: the Store contains exactly one `manifest.json` (with
// `partial_state == "complete"`) and one chunk file per (table,
// chunk-index) in the source schema.
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

	// 0. Resume detection: if a manifest already exists in the store,
	// decide whether to fresh-start, resume, or refuse.
	prior, priorErr := readManifestIfPresent(ctx, b.Store)
	if priorErr != nil {
		return fmt.Errorf("backup: inspect existing manifest: %w", priorErr)
	}
	if prior != nil {
		switch prior.PartialState {
		case ir.BackupStateInProgress:
			slog.InfoContext(ctx, "backup: resuming partial backup",
				slog.Int("completed_tables", len(prior.Tables)),
			)
		case ir.BackupStateComplete, "":
			if !b.ForceOverwrite {
				return fmt.Errorf("backup: a completed backup already exists at this destination (created %s); pass --force-overwrite to replace it",
					prior.CreatedAt.UTC().Format(time.RFC3339))
			}
			slog.InfoContext(ctx, "backup: --force-overwrite set; replacing existing complete backup",
				slog.Time("prior_created_at", prior.CreatedAt),
			)
			prior = nil // discard so we start from scratch
		}
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

	now := time.Now
	if b.Now != nil {
		now = b.Now
	}

	// Pre-build the in-progress manifest with the schema. Tables get
	// appended (or copied from the prior run) as they finish; the
	// manifest is committed after each table completes.
	manifest := &ir.Manifest{
		FormatVersion: ir.BackupFormatVersion,
		SluiceVersion: b.SluiceVersion,
		CreatedAt:     now().UTC(),
		SourceEngine:  b.Source.Name(),
		Schema:        schema,
		Tables:        make([]*ir.TableManifest, 0, len(schema.Tables)),
		PartialState:  ir.BackupStateInProgress,
	}
	if prior != nil {
		// Preserve the original CreatedAt across resume so the
		// "when was this backup taken?" answer is the snapshot point,
		// not the resume point.
		manifest.CreatedAt = prior.CreatedAt
	}
	priorTables := indexManifestTables(priorCompletedTables(prior))

	for _, table := range schema.Tables {
		key := manifestTableKey(table.Schema, table.Name)
		if existing, ok := priorTables[key]; ok {
			ok, err := tableChunksAllPresent(ctx, b.Store, existing)
			if err != nil {
				return fmt.Errorf("backup: re-validate prior table %q: %w", table.Name, err)
			}
			if ok {
				slog.InfoContext(ctx, "backup: table already complete in prior run; skipping",
					slog.String("table", table.Name),
					slog.Int64("rows", existing.RowCount),
					slog.Int("chunks", len(existing.Chunks)),
				)
				manifest.Tables = append(manifest.Tables, existing)
				continue
			}
			slog.WarnContext(ctx, "backup: prior-run chunks missing on store; re-running table",
				slog.String("table", table.Name),
			)
		}
		entry, err := b.backupTable(ctx, rr, table, chunkRows)
		if err != nil {
			return wrapWithHint(PhaseBulkCopy, fmt.Errorf("backup: table %q: %w", table.Name, err))
		}
		manifest.Tables = append(manifest.Tables, entry)

		// Per-table checkpoint: commit the manifest with PartialState
		// = "in_progress" after each table so a crash before the next
		// table loses at most one table of work.
		if err := writeManifest(ctx, b.Store, manifest); err != nil {
			return fmt.Errorf("backup: checkpoint manifest after %q: %w", table.Name, err)
		}
	}

	// 5. Final manifest write — flip to complete.
	manifest.PartialState = ir.BackupStateComplete
	if err := writeManifest(ctx, b.Store, manifest); err != nil {
		return fmt.Errorf("backup: write final manifest: %w", err)
	}

	totalRows := int64(0)
	totalChunks := 0
	for _, t := range manifest.Tables {
		totalRows += t.RowCount
		totalChunks += len(t.Chunks)
	}
	slog.InfoContext(ctx, "backup complete",
		slog.Int("tables", len(manifest.Tables)),
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
		hash := writer.Hash()
		// Resumable-write defence: if a chunk file is already at this
		// path (from a prior interrupted run) AND its bytes hash to
		// the same value as the chunk we just produced, skip the
		// upload. Mismatches overwrite — a corrupted partial-upload
		// shouldn't survive a re-run.
		skip, err := chunkAlreadyMatches(ctx, b.Store, chunkPath, hash)
		if err != nil {
			return fmt.Errorf("inspect existing chunk %q: %w", chunkPath, err)
		}
		if !skip {
			if err := b.Store.Put(ctx, chunkPath, buf); err != nil {
				return fmt.Errorf("store put %q: %w", chunkPath, err)
			}
		}
		entry.Chunks = append(entry.Chunks, &ir.ChunkInfo{
			File:     chunkPath,
			RowCount: writer.RowCount(),
			SHA256:   hash,
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

// readManifestIfPresent returns the prior manifest if one exists in
// store, or (nil, nil) when no manifest is on disk. Distinct from
// [readManifest] which surfaces a NotFound as an error: resume code
// needs to distinguish "no prior backup" (fresh start) from "prior
// manifest is unreadable" (operator-actionable failure).
func readManifestIfPresent(ctx context.Context, store ir.BackupStore) (*ir.Manifest, error) {
	exists, err := store.Exists(ctx, ManifestFileName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return readManifest(ctx, store)
}

// priorCompletedTables returns the prior manifest's table list when
// the manifest is in a state where its entries can be trusted as
// "this table is fully done." Returns nil for a nil manifest.
//
// Only "in_progress" manifests carry per-table progress; "complete"
// manifests are the same shape but get rejected up-stack unless
// --force-overwrite is set, in which case the caller has already
// nilled out prior. Empty PartialState (Phase 1 manifests) is treated
// the same as "complete" — those manifests are immutable backups, not
// resume points.
func priorCompletedTables(prior *ir.Manifest) []*ir.TableManifest {
	if prior == nil || prior.PartialState != ir.BackupStateInProgress {
		return nil
	}
	return prior.Tables
}

// tableChunksAllPresent verifies every chunk listed in entry is still
// present in store. Used when resuming to confirm a "fully completed
// table from a prior run" is still backed by its bytes — operators
// who manually deleted chunks between runs trip this and the table
// gets re-streamed instead of silently appearing in the manifest with
// no actual data behind it.
func tableChunksAllPresent(ctx context.Context, store ir.BackupStore, entry *ir.TableManifest) (bool, error) {
	for _, c := range entry.Chunks {
		exists, err := store.Exists(ctx, c.File)
		if err != nil {
			return false, fmt.Errorf("exists %q: %w", c.File, err)
		}
		if !exists {
			return false, nil
		}
	}
	return true, nil
}

// chunkAlreadyMatches reports whether key already exists in store and
// hashes to expectedSHA256. Returns (false, nil) when the key is
// absent (the common cold-start case). Returns (false, nil) when the
// key is present but mismatches — the caller treats that as a stale
// partial-upload and overwrites.
//
// The fetch-and-hash cost is bounded to chunk size (default 100k rows,
// typically a few MB compressed); cheap relative to a full re-upload
// of the same chunk over a slow link.
func chunkAlreadyMatches(ctx context.Context, store ir.BackupStore, key, expectedSHA256 string) (bool, error) {
	exists, err := store.Exists(ctx, key)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	rc, err := store.Get(ctx, key)
	if err != nil {
		return false, fmt.Errorf("get %q: %w", key, err)
	}
	defer func() { _ = rc.Close() }()
	got, err := hashChunkBytes(ctx, rc)
	if err != nil {
		return false, fmt.Errorf("hash %q: %w", key, err)
	}
	return got == expectedSHA256, nil
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
