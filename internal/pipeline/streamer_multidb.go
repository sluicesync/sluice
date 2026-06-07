// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// Multi-database fan-out CDC (ADR-0074 Phase 1b.2). This file holds the
// `sync start` counterpart to migrate_multidb.go: a cold-start that
// captures N source databases under ONE spanning consistent snapshot,
// copies each to a same-named target namespace, then hands off to the
// single server-wide binlog CDC stream — scoped to the selected set and
// routed per-change to the right namespace.
//
// The crux versus Phase 1a's migrate (which took N independent
// per-database snapshots) is the SINGLE spanning snapshot: the
// server-wide binlog position handed to the CDC reader must be captured
// at one consistent cut across ALL selected databases, or the snapshot →
// CDC handoff loses or double-applies rows. The engine surface
// [ir.MultiDatabaseSnapshotOpener] provides exactly that — one snapshot
// transaction, one position, a RowReader that reads across every database
// at the same view.

// multiDatabaseMode reports whether any database-scope flag engages the
// fan-out path for this Streamer. Single-database mode (the default)
// returns false and the streamer runs byte-identically to its
// pre-ADR-0074 shape — the load-bearing back-compat guard.
func (s *Streamer) multiDatabaseMode() bool {
	return s.AllDatabases || !s.DatabaseFilter.IsEmpty()
}

// validateMultiDatabaseStream enforces the multi-database-mode
// preconditions that don't fit the single-database [Streamer.validate].
// Surfaced as caller-facing errors before any I/O.
func (s *Streamer) validateMultiDatabaseStream() error {
	if s.AllDatabases && !s.DatabaseFilter.IsEmpty() {
		return errors.New(
			"pipeline: --all-databases is mutually exclusive with --include-database / --exclude-database",
		)
	}
	if s.TargetSchema != "" {
		return errors.New(
			"pipeline: --target-schema is incompatible with multi-database sync mode; " +
				"each source database routes to a same-named target namespace automatically (ADR-0074)",
		)
	}
	if s.InjectShardColumn.Engaged() {
		return errors.New(
			"pipeline: --inject-shard-column is not supported in multi-database sync mode (ADR-0074)",
		)
	}
	if s.SchemaAlreadyApplied {
		return errors.New(
			"pipeline: --schema-already-applied is not supported in multi-database sync mode (ADR-0074); " +
				"the cold-start owns per-namespace creation across the selected databases",
		)
	}
	return nil
}

// resolveStreamDatabases enumerates the source server's databases, applies
// the operator's include/exclude globs, and returns the concrete selected
// set (sorted for deterministic logs) plus an inScope predicate over the
// set. Refuses loudly when the source engine can't enumerate databases or
// the selection is empty.
func (s *Streamer) resolveStreamDatabases(ctx context.Context) (selected []string, inScope func(string) bool, err error) {
	lister, ok := s.Source.(ir.DatabaseLister)
	if !ok {
		return nil, nil, fmt.Errorf(
			"pipeline: --all-databases / --include-database / --exclude-database require a source engine "+
				"that can enumerate databases, but %q does not (this is a MySQL-source feature; ADR-0074)",
			s.Source.Name(),
		)
	}

	all, err := lister.ListDatabases(ctx, s.SourceDSN)
	if err != nil {
		return nil, nil, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: list source databases: %w", err))
	}

	selected = make([]string, 0, len(all))
	for _, db := range all {
		if s.DatabaseFilter.Allows(db) {
			selected = append(selected, db)
		}
	}
	sort.Strings(selected)
	if len(selected) == 0 {
		return nil, nil, errors.New(
			"pipeline: no source databases matched the database scope; nothing to sync " +
				"(check --include-database / --exclude-database, or that the source server has non-system databases)",
		)
	}

	selectedSet := make(map[string]struct{}, len(selected))
	for _, db := range selected {
		selectedSet[db] = struct{}{}
	}
	inScope = func(database string) bool {
		_, ok := selectedSet[database]
		return ok
	}
	return selected, inScope, nil
}

// coldStartMultiDatabase is the ADR-0074 Phase 1b.2 multi-database
// cold-start. Shape:
//
//  1. Resolve the selected database set + inScope predicate.
//  2. (MySQL → MySQL) EnsureDatabase each same-named target database, so
//     the per-namespace bulk-copy + CDC apply write into existing
//     namespaces. (MySQL → PG) the PG writer auto-creates the schema when
//     Table.Schema is set, so no orchestrator-level creation is needed.
//  3. Open the ONE spanning consistent snapshot (one tx, one binlog
//     position) via [ir.MultiDatabaseSnapshotOpener].
//  4. For each selected database, read its schema (scoped so Table.Schema
//     is stamped + the FK carve-out is lifted) and bulk-copy its tables
//     from the SHARED spanning RowReader into its target namespace.
//  5. Release the snapshot tx, persist the single anchor position.
//  6. Wire the CDC phase: SetCDCDatabaseScope(inScope) on the reader
//     (scope the server-wide binlog to the selected set) and
//     SetMultiDatabaseRouting(true) on the applier (per-change namespace
//     routing). StreamChanges from the single anchor.
//
// The returned changes channel + stop closure follow the same contract as
// [Streamer.coldStart].
func (s *Streamer) coldStartMultiDatabase(
	ctx context.Context,
	lsnTracker any,
	applier ir.ChangeApplier,
	streamID string,
) (changes <-chan ir.Change, stop func(), err error) {
	stop = func() {}

	if err := s.validateMultiDatabaseStream(); err != nil {
		return nil, stop, err
	}

	opener, ok := s.Source.(ir.MultiDatabaseSnapshotOpener)
	if !ok {
		return nil, stop, fmt.Errorf(
			"pipeline: source engine %q does not support a multi-database spanning snapshot (ADR-0074 Phase 1b.2); "+
				"multi-database `sync start` requires a server-wide-binlog source (MySQL)",
			s.Source.Name(),
		)
	}

	selected, inScope, err := s.resolveStreamDatabases(ctx)
	if err != nil {
		return nil, stop, err
	}
	slog.InfoContext(
		ctx, "multi-database sync: resolved database set",
		slog.String("stream_id", streamID),
		slog.Int("count", len(selected)),
		slog.Any("databases", selected),
	)

	// ---- 2. Ensure each target namespace exists BEFORE cold-start so the
	// applier (which does NOT create namespaces) lands CDC events into
	// existing ones. MySQL → MySQL: CREATE DATABASE IF NOT EXISTS per
	// source database (the TARGET engine exposes DatabaseDSNDeriver). PG
	// target: the PG writer's ensureSchema (driven by Table.Schema during
	// bulk-copy) creates the schema, so no orchestrator-level creation.
	//
	// Target namespacing note: the applier's bound namespace is the target
	// DSN's database — for MySQL that database also hosts the per-target
	// sluice_cdc_state control table (the "home" database; runOnce's
	// EnsureControlTable created it). routedSchema sends each change to its
	// Change.Schema (source database) whenever that differs from the bound
	// home database, so user data lands per-source-database while control
	// metadata stays in the home database. A source database that equals
	// the home database simply routes to the bound namespace (same name —
	// correct). ----
	targetDeriver, targetCanDeriveDB := s.Target.(ir.DatabaseDSNDeriver)
	if targetCanDeriveDB {
		for _, database := range selected {
			if err := targetDeriver.EnsureDatabase(ctx, s.TargetDSN, database); err != nil {
				return nil, stop, wrapWithHint(PhaseSchemaApply,
					fmt.Errorf("pipeline: ensure target database %q: %w", database, err))
			}
		}
	}

	// ---- 3. Open the SINGLE spanning consistent snapshot. One tx, one
	// binlog position spanning ALL selected databases. This is the crux:
	// the position handed to CDC below is captured at one consistent cut. ----
	stream, err := opener.OpenMultiDatabaseSnapshotStream(ctx, s.SourceDSN, selected)
	if err != nil {
		return nil, stop, wrapWithHint(PhaseSnapshot, fmt.Errorf("pipeline: open multi-database snapshot stream: %w", err))
	}
	// Once the snapshot is open every error path must close it.
	closeStream := func() { _ = stream.Close() }

	slog.InfoContext(ctx, "multi-database cold start; single spanning snapshot captured",
		slog.String("stream_id", streamID))
	applyMaxBufferBytes(stream.Rows, s.MaxBufferBytes)

	// ---- 4. Per-database schema read + bulk-copy from the SHARED spanning
	// RowReader. Each database's tables carry Table.Schema = its source
	// database (stamped by MultiDatabaseScoper), and the spanning RowReader
	// qualifies its SELECT by that schema, so the single pinned snapshot
	// connection reads across every database at the one consistent view. ----
	for _, database := range selected {
		if err := s.coldStartCopyOneDatabase(ctx, stream, applier, streamID, database, inScope, targetDeriver, targetCanDeriveDB); err != nil {
			closeStream()
			return nil, stop, err
		}
	}

	// Release the snapshot transaction + import-side connections now that
	// every database is copied. CDC continues on its own connection.
	if err := stream.ReleaseRows(); err != nil {
		slog.WarnContext(
			ctx, "multi-database: release snapshot rows failed; CDC will continue but the snapshot tx may stay open",
			slog.String("stream_id", streamID),
			slog.String("error", err.Error()),
		)
	}
	slog.InfoContext(ctx, "multi-database bulk-copy complete; entering CDC mode",
		slog.String("stream_id", streamID))

	// ---- 5. Persist the single anchor position (GitHub #15: write it
	// BEFORE the first CDC batch so a crash in the handoff window can
	// warm-resume rather than wedge). One position for the whole stream —
	// the binlog coordinate is server-wide. ----
	if pw, ok := applier.(ir.PositionWriter); ok {
		if err := pw.WritePosition(ctx, streamID, stream.Position); err != nil {
			closeStream()
			return nil, stop, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: persist multi-database cold-start CDC anchor position: %w", err))
		}
		slog.DebugContext(
			ctx, "multi-database cold-start CDC anchor persisted",
			slog.String("stream_id", streamID),
			slog.String("position_token", stream.Position.Token),
		)
	} else {
		slog.WarnContext(
			ctx, "applier does not implement ir.PositionWriter; multi-database cold-start CDC anchor cannot be persisted — GitHub issue #15 wedge risk",
			slog.String("stream_id", streamID),
		)
	}

	// ---- 6. Wire the CDC phase. ----
	// (a) Scope the server-wide binlog to the selected database set. The
	//     reader emits row/truncate events from EVERY selected database
	//     (each tagged with its source database in Change.Schema) and drops
	//     events outside the set.
	if scoper, ok := stream.Changes.(ir.CDCDatabaseScoper); ok {
		scoper.SetCDCDatabaseScope(inScope)
	} else {
		closeStream()
		return nil, stop, fmt.Errorf(
			"pipeline: source CDC reader for %q does not implement ir.CDCDatabaseScoper; "+
				"cannot scope the server-wide binlog to the selected database set (ADR-0074 Phase 1b.2)",
			s.Source.Name(),
		)
	}
	// (b) Enable per-change namespace routing on the applier. The applier's
	//     bound namespace is the target DSN's database — for a multi-
	//     database run that is a SERVER DSN (empty bound), so routedSchema
	//     routes every change to its Change.Schema namespace. Refuse loudly
	//     if the applier can't route (it would silently write every database
	//     into one namespace — cross-database bleed / silent loss).
	if router, ok := applier.(ir.MultiDatabaseRouter); ok {
		router.SetMultiDatabaseRouting(true)
	} else {
		closeStream()
		return nil, stop, fmt.Errorf(
			"pipeline: target applier for %q does not implement ir.MultiDatabaseRouter; "+
				"cannot route CDC changes per source database to same-named target namespaces (ADR-0074 Phase 1b.2)",
			s.Target.Name(),
		)
	}

	if lsnTracker != nil {
		if attacher, ok := stream.Changes.(lsnTrackerAttacher); ok {
			attacher.AttachLSNTracker(lsnTracker)
		}
	}
	if s.PollInterval > 0 {
		if setter, ok := stream.Changes.(pollIntervalSetter); ok {
			setter.SetPollInterval(s.PollInterval)
		}
	}

	changes, err = stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		closeStream()
		return nil, stop, wrapWithHint(PhaseCDC, fmt.Errorf("pipeline: start multi-database cdc: %w", err))
	}
	stop = func() { _ = stream.Close() }
	if errer, ok := stream.Changes.(interface{ Err() error }); ok {
		s.sourceErrFn = errer.Err
	}
	return changes, stop, nil
}

// coldStartCopyOneDatabase reads one selected database's schema (scoped so
// Table.Schema is stamped + the FK carve-out lifted) and bulk-copies its
// tables — read from the SHARED spanning snapshot RowReader — into its
// same-named target namespace.
//
//   - MySQL → MySQL: a per-database target SchemaWriter / RowWriter opened
//     against the WithDatabase-derived target DSN, so DDL + writes land in
//     the same-named target database.
//   - MySQL → PG: a SchemaWriter / RowWriter with TargetSchema = database,
//     so the PG writer auto-creates the schema and emits Schema-qualified
//     DDL.
//
// The READ side is always the shared spanning RowReader (one snapshot tx
// across all databases); only the WRITE side is per-database. The applier
// in this run is the server-level CDC applier; the per-database writers
// here are bulk-copy-only and closed at the end of this call.
func (s *Streamer) coldStartCopyOneDatabase(
	ctx context.Context,
	stream *ir.SnapshotStream,
	applier ir.ChangeApplier,
	streamID, database string,
	inScope func(string) bool,
	targetDeriver ir.DatabaseDSNDeriver,
	targetCanDeriveDB bool,
) error {
	// Per-database source DSN so the scoped SchemaReader reads the right
	// database's information_schema. The bulk-copy ROW reads come from the
	// shared spanning snapshot, not this reader.
	deriver, ok := s.Source.(ir.DatabaseDSNDeriver)
	if !ok {
		return fmt.Errorf(
			"pipeline: source engine %q cannot derive a per-database DSN for multi-database sync (ADR-0074)",
			s.Source.Name(),
		)
	}
	srcDSN, err := deriver.WithDatabase(s.SourceDSN, database)
	if err != nil {
		return fmt.Errorf("pipeline: derive source DSN for database %q: %w", database, err)
	}

	sr, err := s.Source.OpenSchemaReader(ctx, srcDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open source schema reader for %q: %w", database, err))
	}
	applyTableScope(sr, s.Filter)
	applyMultiDatabaseScope(sr, &multiDBScope{database: database, inScope: inScope})
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		closeIf(sr)
		return fmt.Errorf("pipeline: read source schema for %q: %w", database, err)
	}
	closeIf(sr)

	if len(schema.Tables) == 0 {
		slog.InfoContext(ctx, "multi-database: database has no tables; skipping copy",
			slog.String("stream_id", streamID), slog.String("database", database))
		return nil
	}

	if err := applyTableFilter(ctx, schema, s.Filter); err != nil {
		return fmt.Errorf("pipeline: filter tables for %q: %w", database, err)
	}
	applyViewFilter(ctx, schema, s.ViewFilter, s.SkipViews)

	// Apply per-column type / expression overrides before schema-apply.
	schema, err = translate.ApplyMappings(schema, s.Mappings)
	if err != nil {
		return fmt.Errorf("pipeline: apply mappings for %q: %w", database, err)
	}
	schema, err = translate.ApplyExpressionOverrides(schema, s.ExpressionMappings)
	if err != nil {
		return fmt.Errorf("pipeline: apply expression overrides for %q: %w", database, err)
	}

	// Foreign keys are deferred to keep the first cut symmetric with the
	// multi-database migrate path: a same-named cross-database FK may
	// reference a database whose tables are created in a later iteration.
	// CDC apply (the steady state) does not depend on FKs, and a follow-on
	// cross-database FK pass can be added when demand surfaces. Strip them
	// so the per-database CreateConstraints emits nothing cross-database
	// that doesn't yet exist on the target.
	stripForeignKeys(schema)

	// ---- Per-database target writers. ----
	targetDSN := s.TargetDSN
	targetSchema := ""
	if targetCanDeriveDB {
		// MySQL → MySQL: re-point the target DSN at the same-named database
		// (already EnsureDatabase'd by the caller).
		targetDSN, err = targetDeriver.WithDatabase(s.TargetDSN, database)
		if err != nil {
			return fmt.Errorf("pipeline: derive target DSN for database %q: %w", database, err)
		}
	} else {
		// MySQL → PG (or any namespaced target): route to a same-named
		// target schema; the PG writer auto-creates it.
		targetSchema = database
	}

	sw, err := s.Target.OpenSchemaWriter(ctx, targetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open target schema writer for %q: %w", database, err))
	}
	applyTargetSchema(sw, targetSchema)
	applyIndexBuildMem(sw, s.IndexBuildMem)
	applyIndexBuildParallelism(sw, s.IndexBuildParallelism)

	rw, err := s.Target.OpenRowWriter(ctx, targetDSN)
	if err != nil {
		closeIf(sw)
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: open target row writer for %q: %w", database, err))
	}
	applyTargetSchema(rw, targetSchema)
	applyMaxBufferBytes(rw, s.MaxBufferBytes)

	// Cold-start preflight: refuse if any target table already holds data
	// (Bug 9). --force-cold-start / --restart-from-scratch / --reset-target-
	// data override. ResetTargetData is the destructive variant (drop +
	// re-copy) handled per-database here for parity with single-database.
	switch {
	case s.ResetTargetData:
		if err := resetTargetDataForStream(ctx, schema, rw, applier, streamID); err != nil {
			closeIf(rw)
			closeIf(sw)
			return fmt.Errorf("pipeline: reset target data for %q: %w", database, err)
		}
	default:
		if err := preflightColdStart(ctx, schema, rw, s.ForceColdStart || s.RestartFromScratch, preflightModeSync); err != nil {
			closeIf(rw)
			closeIf(sw)
			return fmt.Errorf("pipeline: cold-start preflight for %q: %w", database, err)
		}
	}

	slog.InfoContext(ctx, "multi-database: copying database from the spanning snapshot",
		slog.String("stream_id", streamID),
		slog.String("database", database),
		slog.Int("tables", len(schema.Tables)))

	bulkOpts := bulkCopyOpts{Redactor: s.Redactor}
	if err := runBulkCopyWithOpts(ctx, schema, stream.Rows, sw, rw, bulkOpts); err != nil {
		closeIf(rw)
		closeIf(sw)
		return fmt.Errorf("pipeline: bulk-copy database %q: %w", database, err)
	}
	closeIf(rw)
	closeIf(sw)
	return nil
}
