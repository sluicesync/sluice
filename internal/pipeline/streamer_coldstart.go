// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// coldStart performs the original §4 flow: read schema → ensure
// publication scope → snapshot → bulk-copy → start CDC from
// snapshot's position.
//
// lsnTracker is the opaque applied-LSN feedback channel (Bug 15,
// ADR-0020) — attached to the snapshot stream's CDC reader before
// StreamChanges so the keepalive path uses applied-LSN from the
// first ack onwards.
//
// applier and streamID are the engine-side handles for the optional
// `--reset-target-data` recovery path (ADR-0023): when [s.ResetTargetData]
// is set, the cdc-state row is cleared via [ir.StreamCleaner] and dest
// tables are dropped via [ir.TableDropper] before the bulk-copy phase
// begins. Both surfaces are optional; an engine that doesn't expose
// them surfaces a clear refusal rather than running a partial reset.
//
// stop mirrors [warmResume]'s teardown contract: a non-nil closure the
// caller MUST defer. It closes the snapshot stream, whose CloseFn
// closes the CDC reader and thus terminates the engine's binlog/
// replication goroutine deterministically. See warmResume's doc for
// why ctx cancellation alone leaks that goroutine into the next test
// (cross-test slog.Default() DATA RACE under `-race`).
//
// resumeFrom is the INTERRUPTED-cold-start resume cursor (v0.99.8). The
// zero Position (empty Engine+Token) is the normal fresh cold-start: the
// snapshot opens from the beginning and the populated-target preflight
// gates as usual. A non-zero resumeFrom means a process restart caught an
// in-flight cold-start COPY whose persisted position carries a mid-COPY
// cursor ([ir.SnapshotStreamResumer.PositionCarriesCopyCursor]); coldStart
// then (a) opens the snapshot via [ir.SnapshotStreamResumer.
// OpenSnapshotStreamFromPosition] so the bulk COPY continues from the
// cursor (NOT from row 0) through the batched bulk-COPY writer, and (b)
// SKIPS the populated-target cold-start preflight — the partial copy on
// the target is the expected state, and the idempotent COPY writer
// absorbs the overlap. Everything else (schema read, publication scope,
// the bulk-copy machinery, the CDC handoff) is shared verbatim with the
// fresh path. The completed-cold-start warm-resume (cursor-less position)
// never reaches here — it stays on the fast plain-CDC warmResume path.
func (s *Streamer) coldStart(ctx context.Context, lsnTracker any, applier ir.ChangeApplier, streamID string, resumeFrom ir.Position) (changes <-chan ir.Change, stop func(), err error) {
	// resumingCopy is the interrupted-cold-start discriminator: a non-zero
	// resume position. It gates the seeded snapshot open and the preflight
	// skip in the phases below. Read once here so the call sites can't drift.
	resumingCopy := resumeFrom.Engine != "" || resumeFrom.Token != ""
	stop = func() {}

	// Read + gate the source schema: open the source SchemaReader, read
	// + filter the schema, and run every source-side preflight against
	// the still-open reader. A nil schema with nil error is the
	// empty-source case — nothing to stream (the (nil, nil) contract
	// runOnce checks for).
	schema, snapshotTables, err := s.coldStartReadSourceSchema(ctx)
	if err != nil {
		return nil, stop, err
	}
	if schema == nil {
		return nil, stop, nil
	}

	// ---- Scope the source-side publication to the filtered table
	// list (Bug 13, ADR-0021). On engines that don't have
	// publications (MySQL), this is a no-op; on Postgres, this is
	// what stops a CREATE TABLE on the source mid-sync from
	// crashing the applier with "table public.X has no columns".
	// Run BEFORE OpenSnapshotStream so the snapshot's slot pins a
	// catalog snapshot that already has the scoped publication.
	if pe, ok := s.Source.(publicationEnsurer); ok {
		tables := tableNamesForPublication(schema)
		if err := pe.EnsurePublication(ctx, s.SourceDSN, tables); err != nil {
			return nil, stop, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: ensure publication scope: %w", err))
		}
	}

	// Shape the schema for the target: per-column type overrides,
	// expression overrides, the ADR-0054 Bug 83 cold-start seed
	// capture, the ADR-0048 Shape-A discriminator injection, and the
	// Bug 60 redaction-type preflight — in that order (the phase
	// documents each ordering constraint).
	schema, err = s.coldStartPrepareSchema(schema)
	if err != nil {
		return nil, stop, err
	}

	// Open the snapshot stream — seeded from the persisted mid-COPY
	// cursor when resuming an interrupted cold-start (v0.99.8), from
	// the beginning otherwise.
	stream, err := s.coldStartOpenSnapshot(ctx, applier, streamID, resumeFrom, resumingCopy, snapshotTables)
	if err != nil {
		return nil, stop, err
	}

	// Open the target-side writers and run the target-side preflights
	// (stale backends, connection budget, RLS) against them. On error
	// the phase has already closed the writers AND the snapshot stream.
	sw, rw, err := s.coldStartOpenTargetWriters(ctx, schema, stream)
	if err != nil {
		return nil, stop, err
	}

	// Gate the cold start: --reset-target-data recovery,
	// --schema-already-applied skip, interrupted-COPY resume skip, or
	// the default populated-target preflight (Bug 9 / ADR-0048 DP-2).
	if err := s.coldStartGatePreflight(ctx, schema, sw, rw, stream, applier, streamID, resumingCopy); err != nil {
		return nil, stop, err
	}

	// Bulk-copy: the ADR-0079 FAST parallel path when eligible, the
	// serial path otherwise. Releases the writers and the snapshot
	// transaction (Bug 21) on success.
	if err := s.coldStartRunCopy(ctx, schema, stream, sw, rw, streamID, resumingCopy); err != nil {
		return nil, stop, err
	}

	// Persist the cold-start CDC anchor (GitHub #15), then start CDC
	// from the snapshot's position. The returned stop closure closes
	// the snapshot stream when Run unwinds.
	return s.coldStartBeginCDC(ctx, stream, applier, streamID, lsnTracker)
}

// coldStartReadSourceSchema opens the source SchemaReader, reads and
// filters the schema, and runs every source-side preflight (RLS,
// replication capability, XID wraparound, partitioned tables) against
// the still-open reader before closing it. Returns the filtered
// schema plus the surviving table names for snapshot/publication
// scoping. A (nil, nil, nil) return is the empty-source case: the
// source has no tables and there is nothing to stream (already
// logged here).
func (s *Streamer) coldStartReadSourceSchema(ctx context.Context) (*ir.Schema, []string, error) {
	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		return nil, nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open source schema reader: %w", err))
	}
	if err := applyEnabledPGExtensions(ctx, sr, s.EnabledPGExtensions); err != nil {
		closeIf(sr)
		return nil, nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: enable PG extensions on source: %w", err))
	}
	// ADR-0047 tier (b): live PG → PG sync may carry uncatalogued
	// extension types verbatim. Engine-name-only determination.
	applyVerbatimExtensionPassthrough(sr, verbatimLiveSameEnginePG(s.Source, s.Target))
	// catalog Bug 76: scope per-column type validation to the filtered
	// table set (s.Filter already has engine defaults merged in Run).
	applyTableScope(sr, s.Filter)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		closeIf(sr)
		return nil, nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: read source schema: %w", err))
	}
	if len(schema.Tables) == 0 {
		closeIf(sr)
		slog.InfoContext(ctx, "source schema has no tables; nothing to stream")
		return nil, nil, nil
	}

	// Prune by table filter before mappings + bulk-copy so the
	// excluded tables never reach the target schema-apply phase.
	if err := applyTableFilter(ctx, schema, s.Filter); err != nil {
		closeIf(sr)
		return nil, nil, err
	}
	applyViewFilter(ctx, schema, s.ViewFilter, s.SkipViews)

	// Collect the surviving (filtered) table names so a source that
	// implements ir.TableScopedSnapshotOpener (PlanetScale VStream) can
	// scope its COPY to exactly these tables. Without this the VStream
	// snapshot copies EVERY table in the keyspace regardless of
	// --include-table, so a large unrelated table in the same keyspace
	// gets streamed/buffered and overflows --max-buffer-bytes (ADR-0071).
	// Unqualified names — matches the VStream filter rule's Match scope.
	snapshotTables := tableNamesForPublication(schema)

	// Source-side RLS preflight (task #52 sub-deliverable 1). Refuses
	// loudly when any in-scope source table has RLS enabled AND the
	// connecting role lacks BYPASSRLS — the silent-snapshot-filter
	// class. Runs against the still-open source SchemaReader (sr)
	// AFTER the table filter so `--exclude-table` of an RLS table
	// short-circuits the refusal (one of the recovery hints). No-op
	// on non-PG sources.
	if err := preflightRLS(ctx, schema, sr, rlsSideSource); err != nil {
		closeIf(sr)
		return nil, nil, err
	}

	// Source-side replication-capability preflight (task #61). The
	// slot-based `postgres` CDC engine creates a logical replication
	// slot at cold start (openSnapshotStreamWithOptionalSlot below);
	// that requires the connecting role to be a superuser or carry the
	// REPLICATION attribute. Refuses loudly UPFRONT — naming the role
	// and pointing managed-PG operators at `--source-driver=postgres-
	// trigger` — instead of failing mid-cold-start with a raw permission
	// error. Gated on the declared CDC capability: fires only for
	// slot-creating CDC (ir.CDCLogicalReplication — today `postgres`),
	// never for the slot-less `postgres-trigger` (which delegates the
	// same SchemaReader, so interface-presence alone wouldn't exclude
	// it; its CDCTriggers capability does) nor for MySQL. Runs against
	// the still-open source SchemaReader (sr) before it's closed below.
	if err := preflightSourceReplication(ctx, sr, s.Source.Capabilities()); err != nil {
		closeIf(sr)
		return nil, nil, err
	}
	// XID-wraparound preflight (pgcopydb PR #17 adoption). Refuses
	// upfront when the source PG database is near the 32-bit wraparound
	// horizon — long-running CDC against such a source either races
	// PG's global write-block or holds back autovacuum and makes it
	// worse. Gated on the PostgresBackend capability (postgres +
	// postgres-trigger both declare it).
	if err := preflightSourceXIDWraparound(ctx, sr, s.Source.Capabilities()); err != nil {
		closeIf(sr)
		return nil, nil, err
	}
	// Partition preflight (Bug 100 / v0.92.0). Same shape as the
	// migrate preflight — refuses upfront when the source schema
	// contains declaratively-partitioned tables.
	if err := preflightPartitionedTables(ctx, sr, s.Source.Capabilities(), schema); err != nil {
		closeIf(sr)
		return nil, nil, err
	}
	closeIf(sr)

	return schema, snapshotTables, nil
}

// coldStartPrepareSchema shapes the post-filter source schema for the
// target: per-column type overrides, generated-expression overrides,
// the ADR-0054 Bug 83 cold-start seed capture (which must precede the
// shard-column rewrite), the ADR-0048 Shape-A discriminator-column
// injection, and the Bug 60 redaction-type preflight. Warm resume
// never runs this — by then the target schema is already shaped from
// the cold-start run.
func (s *Streamer) coldStartPrepareSchema(schema *ir.Schema) (*ir.Schema, error) {
	// Apply per-column type overrides before the schema-write phase
	// sees the schema. Warm resume skips this step — by then the
	// target schema is already shaped from the cold-start run.
	schema, err := translate.ApplyMappings(schema, s.Mappings)
	if err != nil {
		return nil, fmt.Errorf("pipeline: apply mappings: %w", err)
	}
	schema, err = translate.ApplyExpressionOverrides(schema, s.ExpressionMappings)
	if err != nil {
		return nil, fmt.Errorf("pipeline: apply expression overrides: %w", err)
	}
	// ADR-0054 Bug 83 fix — capture the pre-Shape-A-rewrite source IR
	// per filtered table as the intercept's cold-start cache seed.
	// Must run BEFORE InjectShardColumn so the seed IR matches what the
	// CDC reader will later emit (CDC emits source schema; the source
	// doesn't have the discriminator column — only the target does
	// after Shape A rewrite). The intercept uses the seed to recognize
	// the FIRST CDC SchemaSnapshot as a true boundary (rather than
	// treating it as the cold-start anchor, which was the v0.73.0
	// Bug 83 root cause).
	//
	// ADR-0058 Bug 89 fix — the same seed feeds [interceptAddColumnForward]
	// when --forward-schema-add-column is set and Shape A is NOT
	// engaged. Without it, the MySQL CDC reader's first SchemaSnapshot
	// (emitted only on the FIRST observed DDL boundary, with the
	// already-post-DDL schema) seeds the intercept's cache with
	// hadPre=false, so the ALTER is silently treated as the anchor
	// rather than classified and forwarded. Seeding from the cold-start
	// source IR gives the classifier a real pre-state to diff against.
	// PG sources already work without this seed because pgoutput emits
	// RelationMessage on first-touch (before any DDL); MySQL's binlog
	// has no first-touch equivalent.
	if (s.InjectShardColumn.Engaged() && s.CoordinateLiveDDL) ||
		(s.forwardSchemaEnabled() && !s.InjectShardColumn.Engaged() && !s.multiDatabaseMode()) {
		s.coldStartSeedSnapshots = synthesizeColdStartSeedSnapshots(schema, s.Source)
	}

	// ADR-0048 Shape A discriminator-column injection. Runs after
	// ApplyMappings / ApplyExpressionOverrides and BEFORE the
	// target-side schema writer opens, so CREATE TABLE on the cold-
	// start branch sees the rewritten composite PK + the
	// SluiceInjected column. No-op when --inject-shard-column is
	// unset.
	if s.InjectShardColumn.Engaged() {
		schema, err = translate.InjectShardColumn(schema, s.InjectShardColumn.Name, ir.Varchar{Length: 64})
		if err != nil {
			return nil, wrapWithHint(PhaseSchemaApply, fmt.Errorf("pipeline: inject shard column: %w", err))
		}
	}

	// Redaction-type pre-flight (Bug 60, v0.58.1): catch
	// mask:uuid on UUID-typed columns before the target schema
	// gets created. Runs after ApplyMappings so the operator's
	// `--type-override=col=text` workaround short-circuits the
	// refusal.
	if err := preflightRedactTypes(s.Redactor, schema); err != nil {
		return nil, wrapWithHint(PhaseConnect, err)
	}

	return schema, nil
}

// coldStartOpenSnapshot opens the snapshot+CDC stream — seeded from
// the persisted mid-COPY cursor when resuming an interrupted
// cold-start (v0.99.8), via the slot-/table-scoped default open
// otherwise — then applies the operator byte cap and wires the
// resumable COPY-cursor checkpoint sink (ADR-0072 Phase B).
func (s *Streamer) coldStartOpenSnapshot(ctx context.Context, applier ir.ChangeApplier, streamID string, resumeFrom ir.Position, resumingCopy bool, snapshotTables []string) (*ir.SnapshotStream, error) {
	// Interrupted-cold-start resume (v0.99.8): seed the bulk snapshot
	// stream from the persisted mid-COPY cursor so vtgate continues the
	// COPY from the last-copied PK through the batched bulk-COPY writer,
	// instead of the plain CDC reader's per-row apply path (~10 rows/sec —
	// the silent-degrade this fixes). Fresh cold-starts open from the
	// beginning as before. The resumer surface is gated by the caller
	// (runOnce only sets resumeFrom when the source implements
	// SnapshotStreamResumer AND the position carries a cursor), but we
	// re-assert the type here so a misrouted call fails loudly rather than
	// silently re-copying from row 0.
	var stream *ir.SnapshotStream
	var err error
	if resumingCopy {
		resumer, ok := s.Source.(ir.SnapshotStreamResumer)
		if !ok {
			return nil, wrapWithHint(PhaseSnapshot, fmt.Errorf(
				"pipeline: source engine %q does not support resumable cold-start COPY but a resume cursor was supplied",
				s.Source.Name(),
			))
		}
		slog.InfoContext(
			ctx, "resuming interrupted cold-start COPY from persisted cursor (bulk path)",
			slog.String("stream_id", streamID),
			slog.String("position_token", truncateDryRunToken(resumeFrom.Token, 60)),
		)
		// Pass the filtered table allowlist (snapshotTables, computed above)
		// so the resumed COPY is scoped to --include-table exactly as a fresh
		// cold-start is — Vitess's TablePKs cursor is per-table, so the scope
		// composes with the cursor without any manual reconciliation.
		stream, err = resumer.OpenSnapshotStreamFromPosition(ctx, s.SourceDSN, resumeFrom, snapshotTables)
	} else {
		stream, err = openSnapshotStreamScoped(ctx, s.Source, s.SourceDSN, s.SlotName, snapshotTables)
	}
	if err != nil {
		return nil, wrapWithHint(PhaseSnapshot, fmt.Errorf("pipeline: open snapshot stream: %w", err))
	}
	// The snapshot+CDC handle stays alive past this function; the
	// returned stop closure (set on the success path below) closes it
	// so the engine-side streaming goroutine is joined deterministically
	// when Streamer.Run unwinds.
	//
	// stream.Position is intentionally NOT read here. The VStream
	// snapshot reader (ADR-0071) finalises Position asynchronously at
	// COPY_COMPLETED (after bulk-copy), so reading the field at open
	// time would both be meaningless (zero token) and — because the
	// COPY pump may write it concurrently — a data race. The captured
	// position IS logged once it's load-bearing, after bulk-copy (the
	// "cold-start CDC anchor persisted" line below). Engines that
	// populate Position synchronously at open are unaffected by the
	// missing token on this one line.
	slog.InfoContext(ctx, "cold start; snapshot captured")
	// Bound the snapshot row reader's in-flight buffer (ADR-0071). The
	// VStream COPY-phase reader streams rows under a byte cap; engines
	// without a buffered snapshot reader (PG, vanilla MySQL) no-op the
	// setter. Applied before bulk-copy drains the stream so the pump's
	// backpressure uses the operator's cap rather than the 64 MiB
	// default the engine seeds at open.
	applyMaxBufferBytes(stream.Rows, s.MaxBufferBytes)

	// Wire the resumable COPY-cursor checkpoint sink (ADR-0072 Phase B).
	// Engines whose snapshot reader carries a mid-COPY resume cursor (the
	// VStream cold-start reader, whose position round-trips Vitess's
	// per-shard TablePKs) persist that cursor to the control table on a
	// bounded cadence, so a fault mid-COPY resumes from the checkpoint
	// instead of re-copying the table from row 0. Engines without the
	// cursor (PG, vanilla MySQL) don't implement CopyCheckpointer and the
	// sink is simply not wired. Requires a PositionWriter applier (every
	// shipping engine implements it); without one we skip the wiring (the
	// checkpoint would have nowhere durable to land).
	applyCopyCheckpoint(stream.Rows, applier, streamID)

	return stream, nil
}

// coldStartOpenTargetWriters opens the target SchemaWriter +
// RowWriter, threads the operator knobs onto them, and runs the
// target-side preflights (stale backends, connection budget, RLS)
// against them. On any error the writers AND the snapshot stream are
// closed here before returning — the caller propagates without
// further cleanup.
func (s *Streamer) coldStartOpenTargetWriters(ctx context.Context, schema *ir.Schema, stream *ir.SnapshotStream) (ir.SchemaWriter, ir.RowWriter, error) {
	sw, err := s.Target.OpenSchemaWriter(ctx, s.TargetDSN)
	if err != nil {
		_ = stream.Close()
		return nil, nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open target schema writer: %w", err))
	}
	applyTargetSchema(sw, s.TargetSchema)
	applyIndexBuildMem(sw, s.IndexBuildMem)
	applyIndexBuildParallelism(sw, s.IndexBuildParallelism)
	if err := applyEnabledPGExtensions(ctx, sw, s.EnabledPGExtensions); err != nil {
		closeIf(sw)
		_ = stream.Close()
		return nil, nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: enable PG extensions on target: %w", err))
	}
	rw, err := s.Target.OpenRowWriter(ctx, s.TargetDSN)
	if err != nil {
		closeIf(sw)
		_ = stream.Close()
		return nil, nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open target row writer: %w", err))
	}
	applyTargetSchema(rw, s.TargetSchema)
	applyMaxBufferBytes(rw, s.MaxBufferBytes)

	// Stale-backend preflight (connection-resilience Phase 2, item 2).
	// Detect sluice's OWN orphaned backends on the target before the
	// budget probe so a reap frees slots the budget math then sees.
	// Detection runs always and reports loudly; --reap-stale-backends
	// authorises terminating them. No-op on engines without a backend
	// model (MySQL).
	if err := preflightStaleBackends(
		ctx, s.Target, s.TargetDSN,
		targetWriteSchemas(schema, s.TargetSchema),
		s.ReapStaleBackends,
	); err != nil {
		closeIf(rw)
		closeIf(sw)
		_ = stream.Close()
		return nil, nil, err
	}

	// Connection-budget preflight (connection-resilience item 4). The
	// streamer's cold-start is single-reader (requested=1), so there's
	// no parallelism to cap — but the loud refusal still fires when the
	// target has no free slot for the copy + CDC connections, and an
	// operator --max-target-connections ceiling is honoured. No-op on
	// engines without a connection-slot model (MySQL). Discarded
	// effective value (it can only be 1 here); we run it for the refusal.
	if _, _, err := resolveTargetCopyParallelism(ctx, s.Target, s.TargetDSN, 1, s.MaxTargetConnections); err != nil {
		closeIf(rw)
		closeIf(sw)
		_ = stream.Close()
		return nil, nil, err
	}

	// Target-side RLS preflight (task #52 sub-deliverable 1). Refuses
	// when any in-scope target table has RLS enabled AND the connecting
	// role lacks BYPASSRLS — the INSERT-blocked-by-WITH-CHECK class.
	// Skipped under --schema-already-applied (GitHub #17): the operator
	// promised the target is fully set up including permissions, so the
	// RLS gate is the operator's responsibility on that path. No-op on
	// non-PG targets.
	if !s.SchemaAlreadyApplied {
		if err := preflightRLS(ctx, schema, rw, rlsSideTarget); err != nil {
			closeIf(rw)
			closeIf(sw)
			_ = stream.Close()
			return nil, nil, err
		}
	}

	return sw, rw, nil
}

// coldStartGatePreflight is the cold-start gate switch:
// --reset-target-data destructive recovery (ADR-0023),
// --schema-already-applied operator-promise skip (GitHub #17), the
// interrupted-COPY resume skip (v0.99.8 — the partial copy is the
// expected state), or the default path's populated-target preflights
// (ADR-0048 Shape-A three-point check / Bug 9 cold-start refusal).
// On any error the writers and the snapshot stream are closed here.
func (s *Streamer) coldStartGatePreflight(ctx context.Context, schema *ir.Schema, sw ir.SchemaWriter, rw ir.RowWriter, stream *ir.SnapshotStream, applier ir.ChangeApplier, streamID string, resumingCopy bool) error {
	switch {
	case s.ResetTargetData:
		if err := resetTargetDataForStream(ctx, schema, rw, applier, streamID); err != nil {
			closeIf(rw)
			closeIf(sw)
			_ = stream.Close()
			return err
		}
	case s.SchemaAlreadyApplied:
		// GitHub issue #17: operator promises every source table
		// exists on the target with a compatible schema, and that
		// the sluice_cdc_state control table has been pre-created.
		// Skip the preflight refusal — the operator's promise is
		// "everything I need is already there with no data"; we
		// can't validate that without round-trips that the operator
		// has explicitly opted out of. Bulk-copy runs into the
		// operator-prepared empty tables.
		slog.InfoContext(
			ctx, "schema-already-applied: skipping cold-start preflight + DDL phases (GitHub #17)",
			slog.String("stream_id", streamID),
		)
	case resumingCopy:
		// Interrupted-cold-start resume (v0.99.8): the target already
		// holds the PARTIAL copy from the run that was interrupted — that
		// is precisely the expected state, so the populated-target
		// cold-start preflight (Bug 9) MUST NOT fire here. The resumed
		// COPY continues from the persisted cursor and the idempotent COPY
		// writer (CreateTablesWithoutConstraints uses IF NOT EXISTS;
		// copyTableColdStartIdempotent upserts) absorbs the overlap with
		// rows already on the target. We do NOT drop or truncate the
		// target tables — re-copying from row 0 destructively would defeat
		// the whole resume. This branch is reached only when the persisted
		// position carried a mid-COPY cursor (gated in runOnce), so it
		// cannot mask a genuine "operator pointed at a populated target by
		// mistake" — that path has no cursor and stays on the default
		// preflight below.
		slog.InfoContext(
			ctx, "cold-start COPY resume: skipping populated-target preflight (partial copy is the expected state)",
			slog.String("stream_id", streamID),
		)
	default:
		// ADR-0048 Shape A populated-target preflight (DP-2). When
		// --inject-shard-column is set, this is the LOUD replacement
		// for `--force-cold-start`'s silent skip. No-op when the
		// flag is unset.
		// Bug 152: refuse a multi-shard source merging into a single
		// non-discriminated, collision-capable target (silent cross-shard
		// overwrite). No-op when --inject-shard-column is set or for a
		// single-shard/non-sharded source. Runs first for the clearest
		// diagnostic.
		if err := preflightCrossShardCollision(ctx, s.Source, s.SourceDSN, schema, s.InjectShardColumn.Engaged(), s.AllowCrossShardMerge); err != nil {
			closeIf(rw)
			closeIf(sw)
			_ = stream.Close()
			return err
		}
		if err := preflightShardConsolidation(ctx, schema, rw, s.InjectShardColumn.Name, s.InjectShardColumn.Value); err != nil {
			closeIf(rw)
			closeIf(sw)
			_ = stream.Close()
			return err
		}
		// Cold-start pre-flight: refuse if any target table already
		// contains data. See preflight.go for the rationale (Bug 9).
		// Streamer's cold-start branch is the analogue of Migrator's
		// non-resume cold-start path; warm-resume doesn't run bulk-copy
		// and is therefore not gated by this check.
		// When --inject-shard-column is engaged, Shape-A's three-point
		// check above is the operator-opted-in replacement; the
		// classic cold-start preflight is suppressed in that case.
		if !s.InjectShardColumn.Engaged() {
			// --restart-from-scratch implies the pre-flight skip: the operator
			// is deliberately re-copying onto existing rows (idempotently).
			if err := preflightColdStart(ctx, schema, rw, s.ForceColdStart || s.RestartFromScratch, preflightModeSync); err != nil {
				closeIf(rw)
				closeIf(sw)
				_ = stream.Close()
				return err
			}
		}
	}
	return nil
}

// coldStartRunCopy runs the bulk-copy phase: the ADR-0079 FAST
// parallel cold-start (migrate's cross-table pool + index-build
// overlap + raw passthrough) when the source's exported snapshot
// makes it eligible, the serial runBulkCopyWithOpts path otherwise.
// On success the target writers are closed and the snapshot
// transaction is released (Bug 21); on error everything (writers +
// stream) is closed here before returning.
func (s *Streamer) coldStartRunCopy(ctx context.Context, schema *ir.Schema, stream *ir.SnapshotStream, sw ir.SchemaWriter, rw ir.RowWriter, streamID string, resumingCopy bool) error {
	// ADR-0079: take the FAST parallel cold-start (migrate's cross-table
	// pool + index-build overlap + same-engine raw passthrough) when the
	// source surfaced a SHAREABLE exported snapshot AND implements the
	// snapshot importer — so the one-command copy+follow workflow gets the
	// fast copy, with all N parallel readers pinned to the ONE snapshot.
	// Otherwise (MySQL, VStream, resume, --schema-already-applied) the
	// existing serial path runs, with a loud INFO naming the reason — the
	// resumable durable-watermark + idempotent-COPY coupling lives ONLY on
	// the serial path and is left untouched.
	fast, fastReason := coldStartFastEligible(resumingCopy, s.SchemaAlreadyApplied, stream.SnapshotName, s.Source)
	if coldStartDispatchObserver != nil {
		coldStartDispatchObserver(fast)
	}
	var copyErr error
	if fast {
		copyErr = s.runColdStartParallel(ctx, stream, sw, rw, schema)
	} else {
		slog.InfoContext(ctx, "sync cold-start: "+fastReason+"; using serial cold-start",
			slog.String("stream_id", streamID))
		bulkOpts := bulkCopyOpts{
			SkipSchemaApply: s.SchemaAlreadyApplied,
			Redactor:        s.Redactor,
			Shard:           s.InjectShardColumn,
		}
		copyErr = runBulkCopyWithOpts(ctx, schema, stream.Rows, sw, rw, bulkOpts)
	}
	if copyErr != nil {
		closeIf(rw)
		closeIf(sw)
		_ = stream.Close()
		return copyErr
	}
	closeIf(rw)
	closeIf(sw)
	// Release the snapshot transaction and import-side connections
	// now that bulk-copy is done — without this, Postgres holds the
	// snapshot tx as `idle in transaction` for the entire CDC
	// lifetime (Bug 21), keeping AccessShareLock on every snapshotted
	// table and blocking ALTER on the source. The slot's logical
	// position is independent of the exporting tx; CDC continues on
	// its own connection.
	if err := stream.ReleaseRows(); err != nil {
		slog.WarnContext(
			ctx, "release snapshot rows failed; CDC will continue but the snapshot tx may stay open",
			slog.String("error", err.Error()),
		)
	}
	slog.InfoContext(ctx, "bulk-copy complete; entering CDC mode")
	return nil
}

// coldStartBeginCDC persists the snapshot's anchor position on the
// target (GitHub issue #15 — BEFORE the first CDC batch lands), then
// starts CDC from that position. The returned stop closure closes the
// snapshot stream so the engine-side streaming goroutine is joined
// deterministically when Run unwinds; on error paths the stream is
// closed here and the returned stop is the no-op.
func (s *Streamer) coldStartBeginCDC(ctx context.Context, stream *ir.SnapshotStream, applier ir.ChangeApplier, streamID string, lsnTracker any) (changes <-chan ir.Change, stop func(), err error) {
	stop = func() {}
	// GitHub issue #15: persist the snapshot's anchor position on the
	// target BEFORE the first CDC batch lands. Without this write, the
	// cdc-state row stays absent through the entire window between
	// "entering CDC mode" and the first successful batch commit. A
	// crash, transient applier failure, or operator interrupt in that
	// window wedges the operator: warm-resume can't recover (no row),
	// and cold-start refuses (target tables already populated). The
	// only escape is `--reset-target-data` which re-runs the whole
	// bulk-copy.
	//
	// The position written here is the snapshot's anchor — the same
	// position StreamChanges resumes from on the next call. CDC from
	// this position is gapless and idempotent (ADR-0007, ADR-0010), so
	// a restart that reads this row and warm-resumes is correct: it
	// re-opens the slot at the same anchor and replays the same change
	// stream the failed run would have processed.
	//
	// Idempotent: this row is later overwritten by the first
	// applier.commitBatch — same row shape, monotonic position, same
	// (streamID, source_fingerprint, target_schema) tuple, so the
	// applier's writePositionTx absorbs the duplicate without conflict.
	if pw, ok := applier.(ir.PositionWriter); ok {
		if err := pw.WritePosition(ctx, streamID, stream.Position); err != nil {
			_ = stream.Close()
			return nil, stop, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: persist cold-start CDC anchor position: %w", err))
		}
		slog.DebugContext(
			ctx, "cold-start CDC anchor persisted",
			slog.String("stream_id", streamID),
			slog.String("position_token", stream.Position.Token),
		)
	} else {
		// Shipping engines all implement PositionWriter; an engine
		// that doesn't would have shipped with the issue #15 wedge,
		// but the fall-through preserves pre-fix behaviour rather than
		// hard-erroring.
		slog.WarnContext(
			ctx, "applier does not implement ir.PositionWriter; cold-start CDC anchor cannot be persisted — GitHub issue #15 wedge risk",
			slog.String("stream_id", streamID),
		)
	}

	if lsnTracker != nil {
		if attacher, ok := stream.Changes.(lsnTrackerAttacher); ok {
			attacher.AttachLSNTracker(lsnTracker)
		}
	}
	// Roadmap item 18(c): apply operator-supplied --poll-interval to
	// poll-based CDC readers on the cold-start path too. Same
	// type-assert/silent-ignore shape as warmResume.
	if s.PollInterval > 0 {
		if setter, ok := stream.Changes.(pollIntervalSetter); ok {
			setter.SetPollInterval(s.PollInterval)
		}
	}
	// ADR-0091 F7a (cold-start mirror of warmResume): relax the reader's
	// mid-stream schema-change gate when single-stream forwarding is active
	// so the unambiguous shapes reach the forward intercept rather than
	// being refused / swallowed at the source-read level. Same type-assert/
	// silent-ignore shape as the poll-interval setter above. PG implements
	// it for DROP COLUMN / ALTER COLUMN TYPE (GAP #1); MySQL implements it
	// for ALTER COLUMN NULLABILITY (GAP #2 — the nullability-only change
	// that does not move the decode signature).
	if setter, ok := stream.Changes.(schemaForwardModeSetter); ok {
		setter.SetSchemaForward(s.singleStreamSchemaForwardActive())
	}

	changes, err = stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		_ = stream.Close()
		return nil, stop, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: start cdc: %w", err))
	}
	// Close the snapshot stream when Streamer.Run unwinds. stream.Close
	// runs the engine CloseFn, which closes the CDC reader and joins the
	// engine-side streaming goroutine (go-mysql BinlogSyncer / PG slot
	// reader). Relying on ctx cancel alone left that goroutine running
	// to its reconnect budget after Run returned — a cross-test leak
	// that raced slog.Default() under `-race`.
	stop = func() { _ = stream.Close() }
	// GitHub issue #19: capture the reader's Err method so runOnce
	// can surface a pump error into the ADR-0038 retry loop after the
	// changes channel closes. See [warmResume] for the rationale —
	// same optional-interface probe pattern.
	if errer, ok := stream.Changes.(interface{ Err() error }); ok {
		s.sourceErrFn = errer.Err
	}
	// stream stays alive for the rest of Run; the returned stop closure
	// closes it when Run unwinds, joining the engine-side streaming
	// goroutine deterministically (no longer left to process-exit
	// reclaim — see the stop assignment above).
	return changes, stop, nil
}

// tableNamesForPublication returns the bare table names from a
// post-filter schema, in declaration order. Used by the publication-
// scope step (Bug 13, ADR-0021) — schema-qualifying happens in the
// engine because schema is an engine-side concept (PG namespaces vs.
// MySQL databases vs. future engines).
func tableNamesForPublication(schema *ir.Schema) []string {
	if schema == nil {
		return nil
	}
	out := make([]string, 0, len(schema.Tables))
	for _, t := range schema.Tables {
		out = append(out, t.Name)
	}
	return out
}

// openSnapshotStreamScoped opens the snapshot stream, preferring (in
// order) the slot-aware surface, then the table-scoped surface, then the
// plain default open. It is the snapshot-stream sibling of
// openCDCReaderWithOptionalSlot, extended with the table-scope dispatch.
//
//   - slotName != "" → [ir.SnapshotStreamWithSlotOpener] when the engine
//     implements it (Postgres), else a debug note + default open. The
//     slot is created at open time, so the name must flow in here.
//   - len(tables) > 0 → [ir.TableScopedSnapshotOpener] when the engine
//     implements it (PlanetScale VStream), scoping the COPY to the
//     filtered tables so a large unrelated keyspace table is never
//     streamed/buffered (ADR-0071).
//   - otherwise → the plain [ir.Engine.OpenSnapshotStream].
//
// Slot and table-scope never coexist on one engine (Postgres has the
// slot; PlanetScale has the tables), but if both are somehow set the slot
// wins — it's the more specific lifecycle requirement.
func openSnapshotStreamScoped(ctx context.Context, source ir.Engine, dsn, slotName string, tables []string) (*ir.SnapshotStream, error) {
	if slotName != "" {
		if opener, ok := source.(ir.SnapshotStreamWithSlotOpener); ok {
			return opener.OpenSnapshotStreamWithSlot(ctx, dsn, slotName)
		}
		slog.DebugContext(
			ctx, "engine does not implement SnapshotStreamWithSlotOpener; --slot-name silently ignored",
			slog.String("engine", source.Name()),
		)
		return source.OpenSnapshotStream(ctx, dsn)
	}
	if len(tables) > 0 {
		if opener, ok := source.(ir.TableScopedSnapshotOpener); ok {
			return opener.OpenSnapshotStreamForTables(ctx, dsn, tables)
		}
	}
	return source.OpenSnapshotStream(ctx, dsn)
}
