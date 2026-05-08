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

	// SlotName is the source-side replication-slot name to record on
	// the manifest's [ir.Manifest.EndPosition] for engines with a slot
	// concept (Postgres). Phase 3.3: the captured EndPosition pairs the
	// slot name with the source's current LSN at end-of-backup so a
	// subsequent incremental can resume CDC from that LSN against a
	// slot of the same name. Empty falls back to the engine's default
	// (`sluice_slot` on PG); engines without slots (MySQL) ignore the
	// field. The slot need not exist at backup time — Phase 3.3's
	// `sluice sync start --position-from-manifest` pre-flights slot
	// state before resuming CDC.
	SlotName string

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
			// Bug 34a: emit a clear "resuming" log line so operators
			// can see resume happened. The detailed per-table fan-out
			// (which tables to skip vs resume) follows once the schema
			// is read; this is the headline.
			slog.InfoContext(ctx, "resuming from partial backup",
				slog.String("backup_dir", backupStoreDescriptor(b.Store)),
				slog.Int("tables_in_prior_manifest", len(prior.Tables)),
				slog.Time("prior_created_at", prior.CreatedAt),
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
		Kind:          ir.BackupKindFull,
	}
	if prior != nil {
		// Preserve the original CreatedAt across resume so the
		// "when was this backup taken?" answer is the snapshot point,
		// not the resume point.
		manifest.CreatedAt = prior.CreatedAt
	}
	priorTables := indexManifestTables(priorResumableTables(prior))

	// Bug 34a: Once the schema is in hand, fan out the resume decision
	// per-table so operators can see exactly what's being resumed and
	// what's being re-run. Decisions are surface-level only here; the
	// actual skip / per-chunk-resume happens inside the loop below.
	//
	// A prior entry with Partial=false (or omitted, for backward compat
	// with pre-v0.16.1 manifests) AND all its chunks still on the store
	// is a "fully complete" table (skip whole table). Partial=true with
	// some chunks present falls into the per-chunk resume path.
	if prior != nil && prior.PartialState == ir.BackupStateInProgress {
		var alreadyComplete, toResume []string
		for _, table := range schema.Tables {
			key := manifestTableKey(table.Schema, table.Name)
			existing, ok := priorTables[key]
			if !ok {
				toResume = append(toResume, table.Name)
				continue
			}
			full, err := tableManifestFullyComplete(ctx, b.Store, existing)
			if err != nil {
				return fmt.Errorf("backup: re-validate prior table %q: %w", table.Name, err)
			}
			if full {
				alreadyComplete = append(alreadyComplete, table.Name)
			} else {
				toResume = append(toResume, table.Name)
			}
		}
		slog.InfoContext(ctx, "resume plan",
			slog.Int("tables_already_complete", len(alreadyComplete)),
			slog.Any("tables_to_resume", toResume),
		)
	}

	for _, table := range schema.Tables {
		key := manifestTableKey(table.Schema, table.Name)
		var priorTable *ir.TableManifest
		if existing, ok := priorTables[key]; ok {
			full, err := tableManifestFullyComplete(ctx, b.Store, existing)
			if err != nil {
				return fmt.Errorf("backup: re-validate prior table %q: %w", table.Name, err)
			}
			if full {
				slog.InfoContext(ctx, "skipping table — already complete in partial backup",
					slog.String("table", table.Name),
					slog.Int64("rows", existing.RowCount),
					slog.Int("chunks", len(existing.Chunks)),
				)
				manifest.Tables = append(manifest.Tables, existing)
				continue
			}
			// Partial: pass the prior entry through so backupTable can
			// per-chunk-skip already-uploaded chunks (Bug 34b).
			priorTable = existing
			slog.InfoContext(ctx, "resuming table mid-stream — partial chunks present in prior backup",
				slog.String("table", table.Name),
				slog.Int("prior_chunks", len(existing.Chunks)),
				slog.Bool("prior_partial_flag", existing.Partial),
			)
		}
		// backupTable stages its returned entry into manifest.Tables
		// up-front so per-chunk checkpoints record progress as it
		// accrues (Bug 34b's per-chunk granularity). The orchestrator
		// must NOT append again here.
		if _, err := b.backupTable(ctx, rr, table, chunkRows, priorTable, manifest); err != nil {
			return wrapWithHint(PhaseBulkCopy, fmt.Errorf("backup: table %q: %w", table.Name, err))
		}

		// Per-table checkpoint: commit the manifest with PartialState
		// = "in_progress" after each table so a crash before the next
		// table loses at most one table of work. (Per-chunk checkpoints
		// inside backupTable already capture sub-table progress; this
		// final per-table commit ensures the manifest is up to date
		// once the table fully closes — the per-chunk checkpoint after
		// the last chunk usually has the same effect, but empty-table
		// runs and tables whose row count is an exact chunk multiple
		// rely on this final write.)
		if err := writeManifest(ctx, b.Store, manifest); err != nil {
			return fmt.Errorf("backup: checkpoint manifest after %q: %w", table.Name, err)
		}
	}

	// 4.5. Capture end-of-backup CDC position into manifest.EndPosition
	// so a Phase 3 incremental chained off this full has a resume
	// cursor (Phase 3.3). Engines without CDC support — or without an
	// implementation of [ir.BackupPositionCapturer] — leave the field
	// empty; the incremental orchestrator already handles that case
	// with a clear "parent has no EndPosition" warning.
	if err := b.captureEndPosition(ctx, manifest); err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("backup: capture end position: %w", err))
	}

	// Compute BackupID once EndPosition is known. The id is used by
	// Phase 3 incrementals to link via ParentBackupID; pre-Phase-3
	// fulls leave it empty, which the chain-restore walker tolerates by
	// computing on demand. Filling it in here means the full's manifest
	// carries the same id the incremental would compute when chaining.
	manifest.BackupID = ir.ComputeBackupID(manifest)

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

// captureEndPosition queries the source for its current CDC position
// and stores it on manifest.EndPosition. Engines that don't support
// CDC (Capabilities.CDC == ir.CDCNone) skip the capture; engines that
// do but don't implement [ir.BackupPositionCapturer] also skip with a
// debug log line so the gap is visible to operators running with
// --log-level=debug.
//
// The capture happens AFTER the per-table row sweep so the recorded
// position reflects "the source has produced everything up to here at
// the moment the backup completes." Source writes during the backup
// window are read by the row sweep itself; the EndPosition captures
// the resume point for the chain's next link.
func (b *Backup) captureEndPosition(ctx context.Context, manifest *ir.Manifest) error {
	if b.Source.Capabilities().CDC == ir.CDCNone {
		slog.DebugContext(ctx, "backup: source does not support CDC; skipping EndPosition capture",
			slog.String("engine", b.Source.Name()),
		)
		return nil
	}
	// We need a SchemaReader-shaped surface for the optional capturer
	// (it lives there to share the engine's *sql.DB pool). Open one
	// just for the capture; the engine handles connection lifetime
	// via Close.
	sr, err := b.Source.OpenSchemaReader(ctx, b.SourceDSN)
	if err != nil {
		return fmt.Errorf("open schema reader for position capture: %w", err)
	}
	defer closeIf(sr)

	capturer, ok := sr.(ir.BackupPositionCapturer)
	if !ok {
		slog.DebugContext(ctx, "backup: source SchemaReader does not implement BackupPositionCapturer; manifest EndPosition will be empty",
			slog.String("engine", b.Source.Name()),
		)
		return nil
	}
	pos, err := capturer.CaptureBackupPosition(ctx, b.SlotName)
	if err != nil {
		return fmt.Errorf("capture position: %w", err)
	}
	manifest.EndPosition = pos
	slog.InfoContext(ctx, "backup: recorded end position",
		slog.String("engine", manifest.SourceEngine),
		slog.String("position_token", pos.Token),
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
//
// Bug 34b: when priorTable is non-nil, its chunk entries are checked
// before each new chunk gets a writer. If chunk N's prior info exists,
// the recorded chunk is still on the store, AND its on-store SHA-256
// matches the manifest's recorded SHA-256, the chunk is skipped — the
// orchestrator advances the row cursor by chunk N's recorded RowCount
// (reading-and-discarding) without opening a writer or hitting Put.
// fullManifest, when non-nil, is the in-flight manifest to checkpoint
// after each newly-written chunk so a mid-table crash leaves a manifest
// recording exactly which chunks completed.
func (b *Backup) backupTable(
	ctx context.Context,
	rr ir.RowReader,
	table *ir.Table,
	chunkRows int,
	priorTable *ir.TableManifest,
	fullManifest *ir.Manifest,
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
		Schema:  table.Schema,
		Name:    table.Name,
		Partial: true, // flips to false on natural EOF; per-chunk checkpoints persist it as true
	}
	// Stage the entry into the in-flight manifest now so per-chunk
	// checkpoints capture progress as it accrues. The same pointer is
	// returned to the orchestrator at the end.
	if fullManifest != nil {
		fullManifest.Tables = append(fullManifest.Tables, entry)
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
		// Per-chunk checkpoint: commit the manifest now so a mid-table
		// crash (or kill) before the next table starts leaves an
		// up-to-date record of exactly which chunks completed. The cost
		// is one extra Put per chunk; benefit is the per-chunk skip on
		// resume sees the right state. Bug 34b.
		if fullManifest != nil {
			if err := writeManifest(ctx, b.Store, fullManifest); err != nil {
				return fmt.Errorf("checkpoint manifest after chunk %d: %w", chunkIdx-1, err)
			}
		}
		return nil
	}

	// trySkipChunk inspects priorTable for a recorded entry at chunk
	// chunkIdx. If the recorded entry is still on the store and its
	// on-store bytes hash to the recorded SHA-256, the chunk is reused
	// verbatim — the orchestrator advances the row cursor over chunk's
	// rows without opening a writer or issuing a Put.
	//
	// Returns (skipped, nil) on a successful skip; (false, nil) when
	// either no prior entry exists or the prior bytes mismatch (caller
	// then writes the chunk normally — overwrite-on-mismatch).
	trySkipChunk := func() (bool, error) {
		if priorTable == nil || chunkIdx >= len(priorTable.Chunks) {
			return false, nil
		}
		priorChunk := priorTable.Chunks[chunkIdx]
		if priorChunk == nil || priorChunk.SHA256 == "" {
			return false, nil
		}
		matches, err := chunkAlreadyMatches(ctx, b.Store, priorChunk.File, priorChunk.SHA256)
		if err != nil {
			return false, fmt.Errorf("inspect prior chunk %q: %w", priorChunk.File, err)
		}
		if !matches {
			// Either the chunk is missing on the store or its bytes
			// don't match the recorded SHA-256 (corrupted partial
			// upload). Surface the second case loudly per the loud-
			// failure tenet; either way the chunk gets re-written.
			exists, existsErr := b.Store.Exists(ctx, priorChunk.File)
			if existsErr == nil && exists {
				slog.WarnContext(ctx, "prior chunk SHA-256 mismatch — overwriting on resume",
					slog.String("table", table.Name),
					slog.Int("chunk", chunkIdx),
					slog.String("path", priorChunk.File),
				)
			}
			return false, nil
		}
		// Reuse the prior entry verbatim and advance the cursor.
		entry.Chunks = append(entry.Chunks, priorChunk)
		// Discard the rows the skipped chunk covered so the row stream
		// stays aligned with the chunk index. The chunk-row-range is
		// deterministic ([N*chunkRows, (N+1)*chunkRows) in PK order),
		// so reading-and-discarding priorChunk.RowCount rows from the
		// channel preserves alignment for the next chunk.
		discardN := priorChunk.RowCount
		for discardN > 0 {
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case _, ok := <-rows:
				if !ok {
					return false, fmt.Errorf("row stream ended early while skipping chunk %d (had %d rows of %d to discard)", chunkIdx, priorChunk.RowCount-discardN, priorChunk.RowCount)
				}
				discardN--
			}
		}
		rowsTotal += priorChunk.RowCount
		slog.InfoContext(ctx, "skipping chunk — already complete in partial backup",
			slog.String("table", table.Name),
			slog.Int("chunk", chunkIdx),
			slog.Int64("rows", priorChunk.RowCount),
			slog.String("sha256", priorChunk.SHA256),
		)
		chunkIdx++
		return true, nil
	}

	// Drive the per-chunk skip up-front before the row loop opens a
	// new writer for chunk chunkIdx. We loop because successive chunks
	// may all be skippable.
	skipUntilWrite := func() error {
		for {
			skipped, err := trySkipChunk()
			if err != nil {
				return err
			}
			if !skipped {
				return nil
			}
		}
	}

	if err := skipUntilWrite(); err != nil {
		return nil, err
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
				entry.Partial = false // table EOF reached naturally; flip the partial flag off
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
				// After flushing, the next chunk index might also have
				// been pre-uploaded in a prior run — try to skip again.
				if err := skipUntilWrite(); err != nil {
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

// priorResumableTables returns the prior manifest's table list when
// the manifest is in a state where its entries are eligible for
// resume. Returns nil for a nil manifest.
//
// Only "in_progress" manifests carry per-table progress; "complete"
// manifests are the same shape but get rejected up-stack unless
// --force-overwrite is set, in which case the caller has already
// nilled out prior. Empty PartialState (Phase 1 manifests) is treated
// the same as "complete" — those manifests are immutable backups, not
// resume points.
//
// Renamed from priorCompletedTables in v0.16.1: per-chunk-resume needs
// partial-table entries too, not just fully-completed-tables. Callers
// must check chunk-level state themselves before reusing entries.
func priorResumableTables(prior *ir.Manifest) []*ir.TableManifest {
	if prior == nil || prior.PartialState != ir.BackupStateInProgress {
		return nil
	}
	return prior.Tables
}

// backupStoreDescriptor returns a short human-readable identifier of
// the destination store for log lines. LocalStore reports the absolute
// root directory; BlobStore reports the (annotated) URL with credentials
// stripped. Other implementations of [ir.BackupStore] fall back to
// `<unknown-store>` so log shape is stable.
func backupStoreDescriptor(s ir.BackupStore) string {
	type rooted interface{ Root() string }
	type urled interface{ URL() string }
	if r, ok := s.(rooted); ok {
		return r.Root()
	}
	if u, ok := s.(urled); ok {
		return u.URL()
	}
	return "<unknown-store>"
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

// tableManifestFullyComplete reports whether a prior table entry
// represents a fully-completed table backup (so the resume can skip
// the table whole) vs a mid-stream partial state (resume per-chunk
// inside the table).
//
// Two conditions both required: (1) the entry's Partial flag is false
// (the row stream reached natural EOF when the entry was written; the
// flag is omitted in pre-v0.16.1 manifests, defaulting to false there
// too — those manifests only ever persisted entries at table-completion
// boundaries, so the default is correct); (2) every chunk listed is
// still present on the store (an operator who manually deleted chunks
// between runs forces a re-stream).
func tableManifestFullyComplete(ctx context.Context, store ir.BackupStore, entry *ir.TableManifest) (bool, error) {
	if entry.Partial {
		return false, nil
	}
	return tableChunksAllPresent(ctx, store, entry)
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
