// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Restore orchestrator. The inverse of [Backup]: read manifest, apply
// the schema (with cross-engine retargeting if needed), bulk-copy
// every chunk's rows into the target with per-chunk SHA-256
// verification, then create indexes / constraints / views.
//
// The restore-phase order mirrors [Migrator]:
//
//   1. CreateTablesWithoutConstraints
//   2. Bulk-copy rows from chunks (per-chunk SHA-256 verified)
//   3. SyncIdentitySequences
//   4. CreateIndexes
//   5. CreateConstraints
//   6. CreateViews
//
// Cross-engine restore is supported via [translate.RetargetForEngine]:
// a PG-source backup restoring into MySQL gets PG-native types
// rewritten to their MySQL-storage equivalents (UUID → CHAR(36), etc.)
// before the schema is applied. Same shape `sluice schema diff` uses.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/translate"
)

// Restore runs a single Phase 1 full restore from Store into Target /
// TargetDSN. Construct the value, then call Run.
type Restore struct {
	// Target is the engine the target DSN belongs to. Required.
	// May differ from the backup's source engine — the orchestrator
	// runs `translate.RetargetForEngine` to bridge type differences.
	Target ir.Engine

	// TargetDSN is the target-engine-native connection string.
	// Required.
	TargetDSN string

	// Store is the [ir.BackupStore] to read manifest + chunks from.
	// Required.
	Store ir.BackupStore

	// Filter selects which tables from the manifest participate.
	// Empty (zero value) restores every table.
	Filter TableFilter

	// MaxBufferBytes is the soft byte cap on per-batch buffered
	// memory in the row writer. Same semantics as [Migrator.MaxBufferBytes].
	// Zero means "no cap".
	MaxBufferBytes int64
}

// Run executes the restore. Returns nil on success; a wrapped error
// pointing at the failed phase otherwise.
func (r *Restore) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}

	// 1. Read manifest.
	manifest, err := readManifest(ctx, r.Store)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("restore: %w", err))
	}
	slog.InfoContext(ctx, "restore: loaded manifest",
		slog.Int("format_version", manifest.FormatVersion),
		slog.String("source_engine", manifest.SourceEngine),
		slog.String("target_engine", r.Target.Name()),
		slog.Int("tables", len(manifest.Tables)),
		slog.Time("created_at", manifest.CreatedAt),
	)

	if manifest.Schema == nil {
		return errors.New("restore: manifest carries no schema")
	}

	// 2. Filter tables — both at the schema level (so unwanted
	//    tables never get created) and at the manifest-table level
	//    (so unwanted chunks never get streamed).
	if err := applyTableFilter(ctx, manifest.Schema, r.Filter); err != nil {
		return err
	}
	manifest.Tables = filterManifestTables(manifest.Tables, r.Filter)

	// 3. Cross-engine retarget (identity for same-engine).
	schema := translate.RetargetForEngine(manifest.Schema, manifest.SourceEngine, r.Target.Name())

	// 4. Open target writers.
	sw, err := r.Target.OpenSchemaWriter(ctx, r.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("restore: open target schema writer: %w", err))
	}
	defer closeIf(sw)

	rw, err := r.Target.OpenRowWriter(ctx, r.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("restore: open target row writer: %w", err))
	}
	applyMaxBufferBytes(rw, r.MaxBufferBytes)
	defer closeIf(rw)

	// 5. Phase 1: tables.
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		return wrapWithHint(PhaseSchemaApply, fmt.Errorf("restore: create tables: %w", err))
	}
	slog.InfoContext(ctx, "restore: tables created", slog.Int("count", len(schema.Tables)))

	// 6. Phase 2: bulk-copy from chunks.
	tablesByName := indexManifestTables(manifest.Tables)
	for _, table := range schema.Tables {
		key := manifestTableKey(table.Schema, table.Name)
		entry, ok := tablesByName[key]
		if !ok {
			slog.InfoContext(ctx, "restore: table not in manifest; skipping bulk-copy",
				slog.String("table", table.Name))
			continue
		}
		if err := r.restoreTable(ctx, rw, table, entry); err != nil {
			return wrapWithHint(PhaseBulkCopy, fmt.Errorf("restore: table %q: %w", table.Name, err))
		}
	}

	// 7. Phase 3: identity-sequence sync.
	if err := sw.SyncIdentitySequences(ctx, schema); err != nil {
		return wrapWithHint(PhaseSchemaApply, fmt.Errorf("restore: sync identity sequences: %w", err))
	}

	// 8. Phase 4: indexes.
	if err := sw.CreateIndexes(ctx, schema); err != nil {
		return wrapWithHint(PhaseIndexes, fmt.Errorf("restore: create indexes: %w", err))
	}

	// 9. Phase 5: constraints.
	if err := sw.CreateConstraints(ctx, schema); err != nil {
		return wrapWithHint(PhaseConstraints, fmt.Errorf("restore: create constraints: %w", err))
	}

	// 10. Phase 6: views.
	if err := runViewsPhase(ctx, schema, sw); err != nil {
		return wrapWithHint(PhaseViews, err)
	}

	slog.InfoContext(ctx, "restore complete", slog.Int("tables", len(schema.Tables)))
	return nil
}

// validate sanity-checks required fields.
func (r *Restore) validate() error {
	switch {
	case r.Target == nil:
		return errors.New("restore: Target engine is nil")
	case r.TargetDSN == "":
		return errors.New("restore: TargetDSN is empty")
	case r.Store == nil:
		return errors.New("restore: Store is nil")
	}
	return nil
}

// restoreTable bulk-copies every chunk's rows into the target via the
// row writer, verifying each chunk's SHA-256 along the way. The
// orchestrator opens one [chunkReader] per chunk; rows from all
// chunks of a table flow through a single channel into a single
// [ir.RowWriter.WriteRows] call so the writer's batching / commit
// logic doesn't reset per chunk.
//
// Per-chunk SHA-256 verification is the load-bearing layer-1 check
// of the proto-ADR's "100% confidence" story. A mismatch is a hard
// failure — silent corruption is not acceptable.
func (r *Restore) restoreTable(
	ctx context.Context,
	rw ir.RowWriter,
	table *ir.Table,
	entry *ir.TableManifest,
) error {
	if len(entry.Chunks) == 0 {
		slog.InfoContext(ctx, "restore: empty table; no chunks to apply",
			slog.String("table", table.Name))
		return nil
	}

	rowCh := make(chan ir.Row)
	errCh := make(chan error, 1)

	go func() {
		defer close(rowCh)
		var rowsApplied int64
		for chunkIdx, chunk := range entry.Chunks {
			chunkRows, err := r.streamChunkRows(ctx, chunk, rowCh)
			if err != nil {
				errCh <- fmt.Errorf("chunk %d (%s): %w", chunkIdx, chunk.File, err)
				return
			}
			rowsApplied += chunkRows
			slog.DebugContext(ctx, "restore: chunk verified and streamed",
				slog.String("table", table.Name),
				slog.Int("chunk", chunkIdx),
				slog.Int64("rows", chunkRows),
			)
		}
		if entry.RowCount > 0 && rowsApplied != entry.RowCount {
			errCh <- fmt.Errorf("layer-2 row-count mismatch on table %q: manifest says %d, streamed %d",
				table.Name, entry.RowCount, rowsApplied)
			return
		}
		errCh <- nil
	}()

	if err := rw.WriteRows(ctx, table, rowCh); err != nil {
		// Drain the error channel so the goroutine exits even on
		// writer-side failure.
		<-errCh
		return fmt.Errorf("write rows: %w", err)
	}
	if err := <-errCh; err != nil {
		return err
	}
	slog.InfoContext(ctx, "restore: table complete",
		slog.String("table", table.Name),
		slog.Int64("rows", entry.RowCount),
		slog.Int("chunks", len(entry.Chunks)),
	)
	return nil
}

// streamChunkRows opens chunk in the store, decodes every row, sends
// each into rowCh, and verifies the SHA-256 on close. Returns the
// row count read from this chunk, which the caller compares against
// the manifest entry's RowCount for layer-2 verification.
func (r *Restore) streamChunkRows(
	ctx context.Context,
	chunk *ir.ChunkInfo,
	rowCh chan<- ir.Row,
) (int64, error) {
	src, err := r.Store.Get(ctx, chunk.File)
	if err != nil {
		return 0, fmt.Errorf("open chunk: %w", err)
	}
	cr, err := newChunkReader(src, chunk.SHA256)
	if err != nil {
		return 0, fmt.Errorf("open chunk reader: %w", err)
	}

	var rows int64
	for {
		row, err := cr.ReadRow()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = cr.Close()
			return rows, fmt.Errorf("read row: %w", err)
		}
		select {
		case <-ctx.Done():
			_ = cr.Close()
			return rows, ctx.Err()
		case rowCh <- row:
			rows++
		}
	}
	if err := cr.Close(); err != nil {
		// SHA-256 mismatch surfaces here as a wrapped
		// ErrChunkHashMismatch; loud-failure tenet means we surface
		// it directly rather than continuing.
		return rows, err
	}
	if chunk.RowCount > 0 && rows != chunk.RowCount {
		return rows, fmt.Errorf("layer-2 chunk row-count mismatch on %s: manifest says %d, decoded %d",
			chunk.File, chunk.RowCount, rows)
	}
	return rows, nil
}

// filterManifestTables filters the manifest's table list against the
// supplied filter, mirroring the schema-side filtering. Empty filter
// returns the input unchanged.
func filterManifestTables(in []*ir.TableManifest, filter TableFilter) []*ir.TableManifest {
	if filter.IsEmpty() {
		return in
	}
	out := in[:0]
	for _, t := range in {
		if t == nil {
			continue
		}
		if filter.Allows(t.Name) {
			out = append(out, t)
		}
	}
	return out
}

// indexManifestTables returns a "schema.name" → entry map. Used by
// [Restore.Run] to look up each schema-table's manifest entry in O(1).
func indexManifestTables(tables []*ir.TableManifest) map[string]*ir.TableManifest {
	out := make(map[string]*ir.TableManifest, len(tables))
	for _, t := range tables {
		if t == nil {
			continue
		}
		out[manifestTableKey(t.Schema, t.Name)] = t
	}
	return out
}

func manifestTableKey(schema, name string) string {
	if schema == "" {
		return name
	}
	return schema + "." + name
}

// VerifyBackup walks every chunk referenced by the manifest in store,
// rehashes each chunk's bytes, and reports any mismatches. Used by
// `sluice backup verify` for "is my backup still intact?" cron probes
// against archived backups — no decoding of rows, just byte-level
// hash comparison against the manifest.
//
// Returns the total number of chunks, the count of failed chunks, and
// an error. A non-nil error pinpoints an irrecoverable problem
// (manifest unreadable, ctx cancelled); per-chunk hash failures are
// reported via slog at error level AND counted in the failed return,
// so the caller can report "N of M chunks failed verification"
// without needing the full list.
func VerifyBackup(ctx context.Context, store ir.BackupStore) (total, failed int, err error) {
	manifest, err := readManifest(ctx, store)
	if err != nil {
		return 0, 0, fmt.Errorf("verify: %w", err)
	}
	for _, table := range manifest.Tables {
		for _, chunk := range table.Chunks {
			total++
			if err := verifyChunk(ctx, store, chunk); err != nil {
				failed++
				slog.ErrorContext(ctx, "verify: chunk failed",
					slog.String("table", table.Name),
					slog.String("file", chunk.File),
					slog.String("error", err.Error()),
				)
				continue
			}
			slog.DebugContext(ctx, "verify: chunk OK",
				slog.String("table", table.Name),
				slog.String("file", chunk.File),
			)
		}
	}
	return total, failed, nil
}

// verifyChunk fetches a chunk and recomputes its SHA-256, returning
// nil on match or a wrapped [ErrChunkHashMismatch] on mismatch.
func verifyChunk(ctx context.Context, store ir.BackupStore, chunk *ir.ChunkInfo) error {
	src, err := store.Get(ctx, chunk.File)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = src.Close() }()
	got, err := hashChunkBytes(ctx, src)
	if err != nil {
		return fmt.Errorf("hash: %w", err)
	}
	if got != chunk.SHA256 {
		return fmt.Errorf("%w: expected %s, got %s", ErrChunkHashMismatch, chunk.SHA256, got)
	}
	return nil
}
