// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Fast parallel cold-start for the `sync` path (ADR-0079, roadmap item 3d).
//
// `sluice migrate` copies fast: a cross-table worker pool (ADR-0076),
// index-build overlap (ADR-0077), and a same-engine raw-copy passthrough
// (ADR-0078). `sluice sync start`'s own initial cold-start, by contrast,
// has always run the serial `runBulkCopyWithOpts` "by design" — the
// resumable durable-watermark + idempotent-COPY coupling on the VStream
// path makes parallelising THAT path delicate.
//
// But that coupling lives ONLY on the VStream path. A Postgres source
// exports a SHAREABLE snapshot (CREATE_REPLICATION_SLOT … EXPORT_SNAPSHOT),
// and any number of connections can `SET TRANSACTION SNAPSHOT '<name>'` to
// observe the EXACT same consistent_point view — pgcopydb's parallel-worker
// mechanism. So when (and only when) the source surfaces such a snapshot
// name AND implements [ir.SnapshotImporterOpener], the sync cold-start can
// reuse migrate's fast machinery verbatim, with every parallel reader
// pinned to the one exported snapshot (gap-free; strictly better than
// migrate's per-connection-snapshot v1 window).
//
// This file is the gate + the orchestration. The capability gate
// ([coldStartFastEligible]) is interface-/field-presence-driven, never an
// engine-name check (the IR-first tenet); MySQL and VStream don't qualify
// and stay on the serial path with a loud INFO log naming the reason. The
// resumable cold-start (ADR-0072) and `--schema-already-applied` paths are
// excluded too — both keep `runBulkCopyWithOpts` unchanged.

package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"runtime"

	"sluicesync.dev/sluice/internal/ir"
)

// coldStartDispatchObserver is a TEST-ONLY seam: when non-nil it fires
// with the cold-start dispatch decision (fast == true → parallel path,
// false → serial fallback) the moment [Streamer.coldStart] chooses. It
// lets the serial-fallback integration tests (MySQL / VStream) assert the
// SERIAL path was taken without inferring it from timing — a green
// zero-loss test alone can't distinguish the two paths. nil in production
// (a single nil check). Mirrors the onTableCopiedObserver / rawCopyTaken-
// Observer disposition.
var coldStartDispatchObserver func(fast bool)

// errColdStartNoImporter is the loud precondition guard for
// [Streamer.runColdStartParallel]: it should only ever be called after
// [coldStartFastEligible] asserted the source implements
// [ir.SnapshotImporterOpener], so reaching the nil-importer branch is a
// programming error, surfaced rather than silently deref'd.
var errColdStartNoImporter = errors.New("pipeline: cold-start fast path: source engine has no snapshot importer (gate bypassed)")

// coldStartFastEligible is the ADR-0079 capability gate: it decides whether
// the sync cold-start may take the fast parallel path. It returns
// (true, "") only when EVERY precondition holds; otherwise (false, reason)
// where reason is a single operator-facing clause for the loud INFO log.
//
// The four predicates, in order:
//   - !resumingCopy — the resumable cold-start (ADR-0072) carries a durable
//     mid-COPY watermark + idempotent-COPY coupling that only the serial
//     path honours; resume stays serial.
//   - !schemaAlreadyApplied — `--schema-already-applied` (GitHub #17)
//     suppresses the DDL phases and promises a prepared target; the serial
//     path owns that contract.
//   - snapshotName != "" — the source surfaced a SHAREABLE exported
//     snapshot (Postgres does; MySQL/VStream leave it empty).
//   - source implements [ir.SnapshotImporterOpener] — the engine can mint N
//     readers pinned to that snapshot.
//
// Pure and table-unit-testable: no I/O, no state mutation.
func coldStartFastEligible(resumingCopy, schemaAlreadyApplied bool, snapshotName string, source ir.Engine) (ok bool, reason string) {
	if resumingCopy {
		return false, "resumable cold-start (durable watermark stays serial)"
	}
	if schemaAlreadyApplied {
		return false, "--schema-already-applied (serial DDL-skipping path)"
	}
	if snapshotName == "" {
		return false, "source snapshot is not shareable (per-session / single-stream)"
	}
	if _, ok := source.(ir.SnapshotImporterOpener); !ok {
		return false, "source engine has no snapshot importer"
	}
	return true, ""
}

// resolveColdStartCopyBudget resolves the cross-table × within-table copy
// budget and the overlapped index-build slice for the sync cold-start,
// mirroring migrate's chokepoint (migrate.go) with ONE difference: it
// RESERVES one connection for the CDC reader that goes live immediately
// after bulk-copy, so the copy/index pool can't starve it.
//
// The reservation is taken off the measured CopyBudget BEFORE the
// copy/index split, so the load-bearing invariant
//
//	tableP × withinP + indexBudget + 1(CDC) ≤ CopyBudget
//
// holds at this single chokepoint (ADR-0076/0077 discipline) — no runtime
// semaphore. On a non-prober target or a degraded probe (CopyBudget < 1)
// there is no measured ceiling to divide; the reservation is a no-op and
// the axes resolve to their requested values exactly as migrate's do.
func resolveColdStartCopyBudget(
	ctx context.Context,
	s *Streamer,
	overlapsIndexes bool,
) (tableP, withinP, indexBudget int, err error) {
	withinRequested := resolveBulkParallelism(s.BulkParallelism, runtime.NumCPU())
	copyParallelism, budgetReport, err := resolveTargetCopyParallelism(
		ctx, s.Target, s.TargetDSN, withinRequested, s.MaxTargetConnections,
	)
	if err != nil {
		return 0, 0, 0, err
	}

	// Reserve one slot for the CDC connection (the ADR-0079 delta vs
	// migrate). Only meaningful when there IS a measured ceiling; a 0/neg
	// CopyBudget means "no ceiling" and the reservation is moot.
	copyBudget := budgetReport.CopyBudget
	if copyBudget >= 1 {
		copyBudget--
		if copyBudget < 1 {
			// A budget of exactly 1 left only the CDC slot; the copy axes
			// still need at least one connection. Floor at 1 and accept
			// that copy + CDC briefly share the headroom — the cold-start
			// copy finishes and ReleaseRows fires before CDC's steady
			// state, so the overlap is transient (and the loud refusal in
			// resolveTargetCopyParallelism already fired if the target had
			// truly zero free slots).
			copyBudget = 1
		}
	}

	copyBudgetForAxes := copyBudget
	if overlapsIndexes && copyBudget >= 1 {
		ib, copyRemaining := splitCopyAndIndexBudget(copyBudget, copyParallelism)
		if ib > 0 {
			indexBudget = ib
			copyBudgetForAxes = copyRemaining
		}
	}

	tableP, withinP = resolveCopyParallelismBudget(
		copyParallelism,
		resolveTableParallelism(s.TableParallelism),
		copyBudgetForAxes,
		s.MaxTargetConnections,
	)
	return tableP, withinP, indexBudget, nil
}

// runColdStartParallel drives the sync cold-start through migrate's fast
// machinery (ADR-0079): the cross-table pool + index-build overlap +
// (for a same-engine no-transform copy) the raw-copy passthrough, with all
// N parallel readers minted via the source's [ir.SnapshotImporter] so they
// share the ONE exported snapshot. It is invoked only when
// [coldStartFastEligible] held.
//
// It reuses [runBulkCopyPhases] directly — the SAME orchestrator migrate
// uses — so every phase (tables → copy/index → identity-sync → constraints
// → views) and every concurrency-safety property comes for free. The two
// migrate-vs-sync differences are confined to the [parallelBulkCopyDeps]
// it builds:
//
//   - chunkReaderFactory mints snapshot-pinned readers (not independent
//     OpenRowReader connections); and
//   - the resume state store is disabled (a zero-value resumeContext),
//     because the fast path is fresh-cold-start-only (resume stays serial).
//
// Lifecycle: the snapshot-importer pool is closed when this returns. The
// chunk/table readers it minted are closed by the pool's own release paths
// (the same closeIf path a normal chunk reader uses). The exported-snapshot
// transaction itself is held open by the caller's `stream` until its
// ReleaseRows runs AFTER bulk-copy — so every parallel reader's
// `SET TRANSACTION SNAPSHOT` resolves against a still-live snapshot.
func (s *Streamer) runColdStartParallel(
	ctx context.Context,
	stream *ir.SnapshotStream,
	sw ir.SchemaWriter,
	rw ir.RowWriter,
	schema *ir.Schema,
	streamID string,
) error {
	opener, ok := s.Source.(ir.SnapshotImporterOpener)
	if !ok {
		// Unreachable: coldStartFastEligible already asserted this. Loud
		// rather than a silent fall-through to a nil-importer deref.
		return wrapWithHint(PhaseSnapshot, errColdStartNoImporter)
	}
	importer, err := opener.OpenSnapshotImporter(ctx, s.SourceDSN)
	if err != nil {
		return wrapWithHint(PhaseSnapshot, err)
	}
	defer func() {
		if c, ok := importer.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// chunkReaderFactory mints one snapshot-pinned reader per call. Each is
	// a fresh connection inside its own REPEATABLE READ tx with
	// SET TRANSACTION SNAPSHOT '<name>' — so it observes the SAME view as
	// stream.Rows (chunk 0 / the free pair). The factory is called
	// concurrently by peer chunk/table goroutines; ImportSnapshot opens its
	// own *sql.Conn per call, so concurrent calls are safe. The returned
	// reader's lifecycle is owned by the caller's closeIf release path.
	//
	// INVARIANT (load-bearing): these importer-minted readers are
	// SINGLE-SCHEMA (qualifyBySchema=false). The fast lane therefore must
	// NEVER be reached by a spanning / multi-schema snapshot stream (ADR-0075
	// Phase 2b): such a stream qualifies table names by schema, and a
	// single-schema parallel reader would silently read only the default
	// schema while CDC delivered all of them — a silent divergence. This
	// holds by construction today: the spanning opener
	// (OpenMultiDatabaseSnapshotStream) is reached ONLY via
	// coldStartMultiDatabase, which copies serially through
	// runBulkCopyWithOpts and never calls runColdStartParallel; coldStart's
	// fast dispatch only ever sees the non-spanning (qualifyBySchema=false)
	// OpenSnapshotStream. If a future change routes a spanning stream into
	// coldStart's fast path, this becomes a live bug — re-gate here first.
	snapshotName := stream.SnapshotName
	maxBuffer := s.MaxBufferBytes
	readerFactory := func(rctx context.Context) (ir.RowReader, error) {
		readers, err := importer.ImportSnapshot(rctx, snapshotName, 1)
		if err != nil {
			return nil, err
		}
		applyMaxBufferBytes(readers[0], maxBuffer)
		return readers[0], nil
	}

	// Index-build overlap engages iff the target writer implements the
	// incremental builder (PG). That also drives the budget split.
	_, overlapsIndexes := sw.(ir.IncrementalIndexBuilder)

	tableParallelism, withinParallelism, indexBudget, err := resolveColdStartCopyBudget(ctx, s, overlapsIndexes)
	if err != nil {
		return err
	}
	applyIndexBuildBudget(sw, indexBudget)

	// Raw-copy passthrough (ADR-0078) is governed by the SAME gate as
	// migrate (ADR-0079), populated from the Streamer's transform fields.
	rawCopyOK, rawCopyReason := rawCopyGate(rawCopyConfigForStreamer(s))
	rawCopyFormat := ir.RawCopyText
	if rawCopyOK {
		if exp, imp, ok := asRawCopyEndpoints(stream.Rows, rw); ok {
			rawCopyFormat = negotiateRawCopyFormat(ctx, s.RawCopyFormat, exp, imp)
		}
	}

	slog.InfoContext(ctx, "sync cold-start: fast parallel copy engaged (ADR-0079)",
		slog.Int("table_parallelism", tableParallelism),
		slog.Int("within_table_parallelism", withinParallelism),
		slog.Int("index_build_budget", indexBudget),
		slog.Bool("raw_copy_eligible", rawCopyOK),
		slog.String("raw_copy_reason", rawCopyReason))

	// ADR-0110: one coordinated grow-pause gate for the whole cold-copy run.
	// Constructed unconditionally (no EnableX config — the v0.99.51 trap);
	// inert until a trip source fires. The sync cold-start path has a
	// TargetTelemetry seam, so this gate gets a recovery probe: a PROACTIVE
	// storage-headroom trip (from the gated headroom watch below) reopens on
	// the earlier of {max-hold | storage headroom recovered}. nil provider ⇒
	// the probe is nil and the gate is signal-driven only.
	gate := growGateOrNil(newGrowGate(ctx, storageRecoveredProbe(ctx, s.TargetTelemetry)))
	// Run a cold-copy-phase storage-headroom watch that trips the gate
	// proactively on the rising edge. Scoped to the cold-copy ctx so it
	// exits when the copy completes / unwinds; nil provider ⇒ no goroutine.
	s.startStorageHeadroomWatch(ctx, streamID, gate)

	deps := &parallelBulkCopyDeps{
		source:             s.Source,
		target:             s.Target,
		sourceDSN:          s.SourceDSN,
		targetDSN:          s.TargetDSN,
		parallelism:        withinParallelism,
		minRows:            resolveBulkParallelMinRows(s.BulkParallelMinRows, len(schema.Tables)),
		maxBufferBytes:     s.MaxBufferBytes,
		forceColdStart:     false, // fresh cold-start: the fast non-upsert loader is safe (Bug 9 preflight ran)
		rawCopyOK:          rawCopyOK,
		rawCopyFormat:      rawCopyFormat,
		chunkReaderFactory: readerFactory,
		growGate:           gate,
	}

	// A disabled resume context: the fast path is fresh-cold-start-only
	// (resume stays serial via the gate), so the migrate-state store is
	// inert — markPhase / writeState / markComplete all no-op when
	// rc.enabled is false. resuming=false drives the cold (non-upsert /
	// raw) loader gates.
	rc := resumeContext{}
	state := ir.MigrationState{}
	return runBulkCopyPhases(
		ctx, rc, &state, schema, stream.Rows, sw, rw,
		false, // resuming
		s.BulkBatchSize,
		deps, tableParallelism,
		s.Redactor, s.InjectShardColumn,
		false, // upfrontIndexes: --upfront-indexes is a migrate-path flag; the sync cold-start keeps the deferred post-copy index build
	)
}
