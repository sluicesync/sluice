// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Within-table read chunking for the full-backup row sweep (ADR-0149).
//
// ADR-0084 gave the backup its cross-table axis; a corpus dominated by
// ONE huge table still backed up single-stream while migrate split the
// same table into PK ranges (ADR-0019/0096/0123) — perf-parity matrix
// gap #1. This file closes it by reusing migrate's chunk machinery
// verbatim — [migcore.CanParallelChunkTable], [migcore.ComputeChunkBoundaries],
// [migcore.ComputeKeysetChunkBoundaries], [migcore.ReadChunkBatch],
// [migcore.ClampParallelChunkCount] — inside the ADR-0084 table pool: an
// eligible table's pool worker plans N disjoint half-open PK ranges
// ((lower, upper]; nil = unbounded) on its snapshot-pinned reader,
// then streams them concurrently, every range reader minted against
// the SAME exported snapshot through the pool's free-reader channel +
// importer factory.
//
// Eligibility is deliberately narrow (all must hold):
//
//   - The snapshot is shareable via the LAZY importer path — the same
//     presence gate cross-table parallelism uses ([backupParallelEligible]
//     minus the parallelism clause): snap.SnapshotName != "" AND the
//     source implements [ir.SnapshotImporterOpener]. The MySQL EAGER
//     FTWRL path has a FIXED reader supply (no importer to mint range
//     readers) and the v0.17.x non-snapshot fallback has no consistent
//     multi-reader view at all — both stay single-stream per table,
//     named by one INFO at run start ([Backup.resolveBackupWithinTable]).
//   - [migcore.CanParallelChunkTable] returns a strategy, the reader implements
//     the strategy's surfaces (+[ir.BatchedRowReader] for the range
//     paging itself), and the reader does not veto cursor reads via
//     [ir.BatchedReadDisqualifier].
//   - The row-count estimate clears the same [migcore.ResolveBulkParallelMinRows]
//     threshold migrate uses. On the snapshot-pinned PG readers the
//     estimate runs off-snapshot with the exact-COUNT never-ANALYZEd
//     fallback (the 59c55e27 estimate/bounds split; see
//     [ir.ExactCountEstimateOptIn]) — without it a freshly-loaded
//     source reports 0 and silently loses chunking.
//
// Anything else falls through to today's single-stream [Backup.backupTable],
// with the reason recorded (DEBUG + the test-only dispatch observer).
//
// Chunk FILES keep their `chunkFilePath(table, idx)` naming; idx is
// allocated from a per-table atomic counter shared by the table's range
// workers, so paths are collision-free and gapless while the manifest's
// per-entry chunk list accrues in ARRIVAL order (nothing downstream
// assumes index order or contiguity — restore's [partitionChunks] slices
// the list positionally, verify/compact walk it, and the chunk SET is
// order-independent by the Bug 135 doctrine). Resume posture is
// UNCHANGED: a partial table re-streams from scratch (Bug 135); there is
// deliberately NO per-chunk resume — flush's content-addressed same-path
// SHA skip is the only chunk-level reuse, exactly as before.

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// backupChunkDispatchObserver is a TEST-ONLY seam (nil in production —
// a single nil check): it fires once per swept table with the resolved
// within-table dispatch — ranges > 1 means the chunked read engaged;
// reason carries the fallback clause ("" when engaged). Lets the
// integration pins assert WHICH path ran instead of inferring it from
// chunk counts. Mirrors [backupDispatchObserver].
var backupChunkDispatchObserver func(table string, ranges int, reason string)

// backupWithinTable is the run-level within-table chunking
// configuration the orchestrator threads into the table pool (nil ⇒
// the feature is not engaged this run and every table streams through
// the single-reader path unchanged).
type backupWithinTable struct {
	// parallelism is the resolved --bulk-parallelism (>1 by
	// construction — 1 resolves to a nil config).
	parallelism int

	// minRows is the chunk-eligibility row threshold, resolved via
	// [migcore.ResolveBulkParallelMinRows] exactly as migrate resolves it.
	minRows int64

	// readBudget is the tableParallelism × parallelism product the
	// run's reader connections are bounded by; it also caps the finer
	// per-table chunk count ([migcore.ClampParallelChunkCount]) so one large
	// table can fill the whole budget at the tail (ADR-0123).
	readBudget int

	// gate is the run's SINGLE shared reader-budget gate, sized to
	// readBudget. Every table pool worker draws one BASE token for its
	// held reader and every extra range worker draws one before minting,
	// so cross-table + within-table readers never exceed the budget —
	// and a large table's surplus ranges steal tokens freed by finished
	// peer tables (the migrate copyGate discipline, ADR-0123).
	gate *migcore.CopyParallelismGate

	// freeReader / factory are the pool's reader supply, filled in by
	// [Backup.runBackupTablePool] before the sweep starts: range workers
	// claim an idle pooled reader first and mint a dedicated
	// snapshot-pinned one via the importer factory otherwise — the same
	// [acquireBackupReader] path cross-table peers use.
	freeReader chan ir.RowReader
	factory    backupReaderFactory
}

// backupWithinChunkingEligible is the MODE half of the ADR-0149
// eligibility gate: whether this run's snapshot can supply the
// additional same-view readers within-table chunking needs. It is the
// [backupParallelEligible] LAZY clause without the parallelism check —
// presence-driven, never an engine-name string. The per-table half
// (PK shape, surfaces, size) lives in [Backup.planBackupTableChunks].
func backupWithinChunkingEligible(snap *irbackup.Snapshot, source ir.Engine) (ok bool, reason string) {
	if snap != nil && snap.SnapshotName != "" {
		if _, ok := source.(ir.SnapshotImporterOpener); ok {
			return true, ""
		}
		return false, "source engine has no snapshot importer"
	}
	if snap != nil && len(snap.ExtraReaders) > 0 {
		return false, "eager coordinated readers (MySQL FTWRL) are a fixed supply; no importer to mint per-range snapshot readers"
	}
	return false, "source snapshot is not shareable (per-session / single-stream / non-snapshot fallback)"
}

// resolveBackupWithinTable builds the run's within-table chunking
// configuration, or nil when it cannot engage — with one loud INFO
// naming the reason (the ADR-0079/0084 disposition: a silent fallback
// leaves operators wondering why a big table streamed single-reader).
// requestedWithin is the budget-split --bulk-parallelism from
// [Backup.resolveBackupTableParallelism]; tableParallelism is the
// GATED cross-table fan-out the read budget is sized against.
func (b *Backup) resolveBackupWithinTable(
	ctx context.Context,
	snap *irbackup.Snapshot,
	tableParallelism, requestedWithin, taskCount int,
) *backupWithinTable {
	if requestedWithin <= 1 {
		if b.BulkParallelism == 1 {
			slog.DebugContext(ctx, "backup: within-table read chunking disabled (--bulk-parallelism=1)")
		}
		return nil
	}
	if ok, reason := backupWithinChunkingEligible(snap, b.Source); !ok {
		slog.InfoContext(
			ctx, "backup: within-table read chunking not engaged; large tables stream single-reader (ADR-0149)",
			slog.String("reason", reason),
			slog.Int("requested_bulk_parallelism", requestedWithin),
		)
		return nil
	}
	if tableParallelism < 1 {
		tableParallelism = 1
	}
	budget := tableParallelism * requestedWithin
	w := &backupWithinTable{
		parallelism: requestedWithin,
		minRows:     migcore.ResolveBulkParallelMinRows(b.BulkParallelMinRows, taskCount),
		readBudget:  budget,
		gate:        migcore.NewCopyParallelismGate(budget, migcore.DefaultCopyBackoffPolicy),
	}
	slog.InfoContext(
		ctx, "backup: within-table read chunking enabled (ADR-0149)",
		slog.Int("bulk_parallelism", requestedWithin),
		slog.Int64("min_rows", w.minRows),
		slog.Int("read_budget", budget),
	)
	return w
}

// backupTableDispatch routes one table between the single-stream
// [Backup.backupTable] path (unchanged) and the chunked
// [Backup.backupTableRanges] path, per the ADR-0149 eligibility plan.
// rr is the pool worker's held snapshot-pinned reader; it plans the
// ranges pre-stream and then serves range 0 itself.
func (b *Backup) backupTableDispatch(
	ctx context.Context,
	rr ir.RowReader,
	task backupTableTask,
	chunkRows int,
	committer *manifestCommitter,
	chainCEK []byte,
	within *backupWithinTable,
) error {
	bounds, reason, err := b.planBackupTableChunks(ctx, rr, task.table, within)
	if err != nil {
		return err
	}
	if len(bounds) <= 1 {
		if within != nil {
			slog.DebugContext(
				ctx, "backup: within-table chunking not applicable; streaming table single-reader",
				slog.String("table", task.table.Name),
				slog.String("reason", reason),
			)
		}
		if backupChunkDispatchObserver != nil {
			backupChunkDispatchObserver(task.table.Name, 1, reason)
		}
		return b.backupTable(ctx, rr, task, chunkRows, committer, chainCEK)
	}
	slog.InfoContext(
		ctx, "backup: within-table chunked read engaged (ADR-0149)",
		slog.String("table", task.table.Name),
		slog.Int("ranges", len(bounds)),
	)
	if backupChunkDispatchObserver != nil {
		backupChunkDispatchObserver(task.table.Name, len(bounds), "")
	}
	return b.backupTableRanges(ctx, rr, task, chunkRows, committer, chainCEK, within, bounds)
}

// planBackupTableChunks runs the per-table half of the ADR-0149
// eligibility gate and, when every condition holds, computes the
// table's chunk boundaries on the held snapshot-pinned reader —
// strictly pre-stream, single-goroutine, mirroring migrate's
// [shouldParallelChunk] + [resolveChunks] (same reason strings where
// the condition is shared). Returns (bounds, "", nil) with len > 1 to
// chunk; (nil-or-single, reason, nil) to fall back to single-stream;
// an error only for a boundary-computation failure (loud, like
// migrate's resolveChunks — never a silent mis-split).
func (b *Backup) planBackupTableChunks(
	ctx context.Context,
	rr ir.RowReader,
	table *ir.Table,
	within *backupWithinTable,
) (bounds []migcore.ChunkBoundary, reason string, err error) {
	if within == nil {
		return nil, "within-table chunking not engaged for this run", nil
	}
	eligible, strategy, reason := migcore.CanParallelChunkTable(table, within.parallelism)
	if !eligible {
		return nil, reason, nil
	}
	// The range workers page via migcore.ReadChunkBatch, so the paging surface is
	// required for BOTH strategies; each strategy additionally needs its
	// boundary surface, and keyset needs the SQL-side upper-bound clip
	// (ADR-0096 exactly-once — the Go bytewise clip diverges from a
	// non-C collation).
	if _, ok := rr.(ir.BatchedRowReader); !ok {
		return nil, "reader does not implement BatchedRowReader; single-reader path", nil
	}
	switch strategy {
	case migcore.StrategyMinMaxDivide:
		if _, ok := rr.(migcore.RangeQuerier); !ok {
			return nil, "reader does not implement RangeBoundsQuerier; single-reader path", nil
		}
	case migcore.StrategyKeysetSample:
		if _, ok := rr.(migcore.KeysetSampler); !ok {
			return nil, "reader does not implement KeysetSampler (non-integer/composite PK); single-reader path", nil
		}
		if _, ok := rr.(ir.BoundedBatchedRowReader); !ok {
			return nil, "reader does not implement BoundedBatchedRowReader; keyset upper-bound clip would diverge from DB collation; single-reader path", nil
		}
	}
	if d, ok := rr.(ir.BatchedReadDisqualifier); ok {
		if disq, why := d.DisqualifiesBatchedRead(table); disq {
			return nil, fmt.Sprintf("reader disqualifies cursor reads for this table: %s", why), nil
		}
	}
	est, err := migcore.ApproximateRowCount(ctx, rr, table)
	if err != nil {
		// Best-effort fallback, mirroring shouldParallelChunk: the data
		// path is the load-bearing thing; chunking is a perf detail.
		slog.WarnContext(ctx, "backup: row-count probe failed; streaming table single-reader",
			slog.String("table", table.Name),
			slog.String("err", err.Error()))
		return nil, "row-count probe failed", nil
	}
	if est < within.minRows {
		return nil, fmt.Sprintf("table has ~%d rows; below --bulk-parallel-min-rows=%d", est, within.minRows), nil
	}

	m := migcore.ClampParallelChunkCount(est, within.minRows, within.parallelism, within.readBudget)
	switch strategy {
	case migcore.StrategyMinMaxDivide:
		bounds, err = migcore.ComputeChunkBoundaries(ctx, rr.(migcore.RangeQuerier), table, m)
	case migcore.StrategyKeysetSample:
		bounds, err = migcore.ComputeKeysetChunkBoundaries(ctx, rr.(migcore.KeysetSampler), table, m)
	default:
		return nil, "", fmt.Errorf("pipeline: backup: table %q has unknown chunk strategy %d", table.Name, strategy)
	}
	if err != nil {
		return nil, "", fmt.Errorf("backup: compute chunk boundaries for %q: %w", table.Name, err)
	}
	if len(bounds) <= 1 {
		return bounds, "table too small to split (computed a single range)", nil
	}
	return bounds, "", nil
}

// backupTableRanges streams one table through N concurrent PK-range
// workers. Range 0 rides the pool worker's already-held reader
// (covered by the worker's base gate token); every other range draws a
// gate token, claims an idle pooled reader or mints a dedicated
// snapshot-pinned one ([acquireBackupReader]), and streams its
// half-open range. Chunk indexes and the terminal row count are shared
// atomics; every manifest mutation routes through the committer's
// mutex exactly as the single-stream path's do. The errgroup's derived
// ctx cancels peers on the first error, leaving the entry Partial so a
// resume re-streams the table from scratch (Bug 135 posture,
// unchanged).
func (b *Backup) backupTableRanges(
	ctx context.Context,
	rr ir.RowReader,
	task backupTableTask,
	chunkRows int,
	committer *manifestCommitter,
	chainCEK []byte,
	within *backupWithinTable,
	bounds []migcore.ChunkBoundary,
) error {
	table, entry := task.table, task.entry
	pkCols := migcore.PrimaryKeyColumnNames(table)

	var (
		chunkIdx  atomic.Int64
		rowsTotal atomic.Int64
	)
	g, gctx := errgroup.WithContext(ctx)
	for _, bound := range bounds {
		bound := bound
		g.Go(func() error {
			s := b.newBackupChunkStreamer(table, entry, chunkRows, committer, chainCEK, &chunkIdx, &rowsTotal)
			if bound.ChunkIndex == 0 {
				// Range 0 streams on the worker's held reader — already
				// budgeted by the worker's base gate token.
				return b.backupRange(gctx, rr, table, s, bound, pkCols)
			}
			if err := within.gate.Acquire(gctx); err != nil {
				return err
			}
			defer within.gate.Release()
			rangeRR, release, err := acquireBackupReader(gctx, within.freeReader, within.factory)
			if err != nil {
				return err
			}
			defer release()
			return b.backupRange(gctx, rangeRR, table, s, bound, pkCols)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	if err := committer.finishTable(ctx, entry, rowsTotal.Load()); err != nil {
		return fmt.Errorf("checkpoint manifest after table: %w", err)
	}
	slog.InfoContext(
		ctx, "backup: table complete (chunked read)",
		slog.String("table", table.Name),
		slog.Int64("rows", rowsTotal.Load()),
		slog.Int("ranges", len(bounds)),
		slog.Int("chunks", len(entry.Chunks)),
	)
	return nil
}

// backupRange streams one half-open PK range ((lower, upper]; nil =
// unbounded) into chunk files through the shared streamer. It pages
// the reader with the same cursor-driven [migcore.ReadChunkBatch] migrate's
// copyChunk uses — SQL-side upper bound via [ir.BoundedBatchedRowReader]
// where implemented, so the range partition is collation-correct and
// exactly-once by construction (ADR-0096).
func (b *Backup) backupRange(
	ctx context.Context,
	rr ir.RowReader,
	table *ir.Table,
	s *backupChunkStreamer,
	bound migcore.ChunkBoundary,
	pkCols []string,
) error {
	br, ok := rr.(ir.BatchedRowReader)
	if !ok {
		// Unreachable: planBackupTableChunks asserted the surface on the
		// planning reader and every range reader is minted from the same
		// engine. Loud rather than a silently-skipped range.
		return fmt.Errorf("pipeline: backupRange: row reader %T does not implement BatchedRowReader", rr)
	}
	cursor := bound.LowerPK
	for {
		filtered, err := migcore.ReadChunkBatch(ctx, br, table, cursor, bound.UpperPK, pkCols, migcore.DefaultBulkBatchSize)
		if err != nil {
			return fmt.Errorf("read range %d batch: %w", bound.ChunkIndex, err)
		}
		var batchCount int64
		tracker := migcore.NewPKTracker(pkCols)
		for row := range filtered {
			tracker.Observe(row)
			batchCount++
			if err := s.writeRow(ctx, row); err != nil {
				return err
			}
		}
		// Loud-failure gate (Bug 68): the batched reader aborts a page by
		// closing the channel on a per-row failure — indistinguishable
		// from a clean short/empty page. Check before interpreting
		// batchCount.
		if err := migcore.ReaderStreamErr(rr, table); err != nil {
			return err
		}
		// A ctx-cancellation must NOT read as a clean end-of-range (the
		// ADR-0109 sibling-cancel class): a peer range's failure cancels
		// gctx and the reader closes this range's page channel early. The
		// errgroup error keeps the entry Partial, so a resume re-streams
		// the whole table — but only if the cancellation is surfaced here
		// rather than read as EOF.
		if err := ctx.Err(); err != nil {
			return err
		}
		if batchCount == 0 {
			break // end of range
		}
		newCursor, ok := tracker.LastPK()
		if !ok {
			return errors.New("pipeline: backupRange: batch produced rows but PK tracker captured none")
		}
		cursor = newCursor
		if batchCount < int64(migcore.DefaultBulkBatchSize) {
			break // short page ⇒ end of data within the range
		}
	}
	return s.flush(ctx)
}

// backupChunkStreamer accumulates a row stream into rolling chunk
// files: open-on-demand, roll at chunkRows, content-addressed same-path
// upload skip, per-chunk manifest checkpoint through the committer —
// the flush/committer plumbing [Backup.backupTable] and the ADR-0149
// range workers share (one implementation, two callers; the pre-ADR
// single-stream behaviour is byte-identical).
//
// chunkIdx and rowsTotal are SHARED by every streamer writing the same
// table (each range worker holds its own streamer): chunk-file indexes
// are allocated atomically at flush time — collision-free and gapless
// across concurrent flushes, arrival-ordered in the manifest — and the
// terminal row count is the atomic sum [finishTable] records. All
// other state is streamer-local.
type backupChunkStreamer struct {
	b         *Backup
	table     *ir.Table
	entry     *irbackup.TableManifest
	committer *manifestCommitter
	chainCEK  []byte
	chunkRows int

	cols     []*ir.Column
	colNames []string
	pkCols   []string

	chunkIdx  *atomic.Int64
	rowsTotal *atomic.Int64

	writer        *blobcodec.ChunkWriter
	buf           *bytes.Buffer
	curWrappedCEK []byte // populated only in per-chunk encryption mode
}

// newBackupChunkStreamer builds one streamer for table. chunkIdx and
// rowsTotal are the table-scoped shared counters (see the type doc).
func (b *Backup) newBackupChunkStreamer(
	table *ir.Table,
	entry *irbackup.TableManifest,
	chunkRows int,
	committer *manifestCommitter,
	chainCEK []byte,
	chunkIdx, rowsTotal *atomic.Int64,
) *backupChunkStreamer {
	cols := nonGeneratedTableColumns(table)
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Name
	}
	return &backupChunkStreamer{
		b:         b,
		table:     table,
		entry:     entry,
		committer: committer,
		chainCEK:  chainCEK,
		chunkRows: chunkRows,
		cols:      cols,
		colNames:  colNames,
		pkCols:    migcore.TablePKColumns(table),
		chunkIdx:  chunkIdx,
		rowsTotal: rowsTotal,
	}
}

// writeRow redacts row (PII Phase 1.5 — so on-disk backups are
// PII-clean; nil/empty Registry is a zero-cost passthrough), writes it
// to the open chunk (opening one on demand), and rolls the chunk when
// it reaches chunkRows.
func (s *backupChunkStreamer) writeRow(ctx context.Context, row ir.Row) error {
	if s.writer == nil {
		s.buf = &bytes.Buffer{}
		cek, wrapped, err := s.b.resolveChunkCEK(s.chainCEK)
		if err != nil {
			return fmt.Errorf("resolve chunk cek: %w", err)
		}
		s.curWrappedCEK = wrapped
		w, err := blobcodec.NewChunkWriter(s.buf, s.colNames, cek, s.b.Codec)
		if err != nil {
			return fmt.Errorf("open chunk: %w", err)
		}
		s.writer = w
	}
	// streamID is empty for full-backup runs (one-shot snapshots); the
	// per-row randomize:* seed is determined purely by table + column +
	// PK values, so re-running produces the same redacted values.
	if err := migcore.RedactRow(s.b.Redactor, s.table.Schema, s.table.Name, row, s.cols, s.pkCols, ""); err != nil {
		return fmt.Errorf("redact row: %w", err)
	}
	if err := s.writer.WriteRow(row, s.cols); err != nil {
		return fmt.Errorf("write row: %w", err)
	}
	s.rowsTotal.Add(1)
	if s.writer.RowCount() >= int64(s.chunkRows) {
		return s.flush(ctx)
	}
	return nil
}

// flush closes the open chunk (no-op when none is open — empty tables
// produce zero chunks), allocates its manifest-unique index, uploads it
// unless the content-addressed same-path SHA comparison says the bytes
// are already there (resumable-write defence; mismatches overwrite —
// a corrupted partial upload must not survive a re-run), and
// checkpoints the chunk on the entry through the committer's mutex.
func (s *backupChunkStreamer) flush(ctx context.Context) error {
	if s.writer == nil {
		return nil
	}
	if err := s.writer.Close(); err != nil {
		return fmt.Errorf("close chunk: %w", err)
	}
	idx := int(s.chunkIdx.Add(1) - 1)
	chunkPath := chunkFilePath(s.table, idx)
	hash := s.writer.Hash()
	skip, err := chunkAlreadyMatches(ctx, s.b.Store, chunkPath, hash)
	if err != nil {
		return fmt.Errorf("inspect existing chunk %q: %w", chunkPath, err)
	}
	if !skip {
		if err := s.b.Store.Put(ctx, chunkPath, s.buf); err != nil {
			return fmt.Errorf("store put %q: %w", chunkPath, err)
		}
	}
	ci := &irbackup.ChunkInfo{
		File:     chunkPath,
		RowCount: s.writer.RowCount(),
		SHA256:   hash,
	}
	if s.b.Encryption != nil {
		ci.Encryption = &irbackup.ChunkEncryption{
			Algorithm:  crypto.AlgorithmAESGCM,
			NonceLen:   crypto.NonceLen,
			AuthTagLen: crypto.AuthTagLen,
			WrappedCEK: s.curWrappedCEK, // empty for per-chain mode
		}
	}
	s.writer = nil
	s.buf = nil
	s.curWrappedCEK = nil
	// Per-chunk checkpoint: observability + it keeps the same-path SHA
	// fast-skip effective across a re-run. Resume never REUSES partial
	// chunk lists (Bug 135). The committer serializes the entry mutation
	// + manifest write against peer tables AND peer range workers.
	if err := s.committer.appendChunk(ctx, s.entry, ci); err != nil {
		return fmt.Errorf("checkpoint manifest after chunk %d: %w", idx, err)
	}
	return nil
}

// applyExactCountEstimate opts a minted snapshot-pinned reader into the
// exact-COUNT never-ANALYZEd estimate fallback (see
// [ir.ExactCountEstimateOptIn]) so the ADR-0149 chunk decision doesn't
// silently report 0 on a fresh source. nil-safe no-op for readers
// without the surface.
func applyExactCountEstimate(rr ir.RowReader) {
	if o, ok := rr.(ir.ExactCountEstimateOptIn); ok {
		o.EnableExactCountEstimate()
	}
}
