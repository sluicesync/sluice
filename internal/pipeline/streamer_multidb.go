// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
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
	return s.AllDatabases || !s.DatabaseFilter.IsEmpty() || !s.NamespaceMap.IsEmpty()
}

// namespaceRenameFunc returns the per-change source → target namespace
// rename the multi-database CDC applier applies inside its routedSchema
// (ADR-0142), or nil when no rename is configured. nil is the load-bearing
// identity default: [ir.MultiDatabaseRouter.SetMultiDatabaseRouting] with a
// nil rename is byte-identical to the pre-ADR-0142 same-named routing. The
// rename lives in the applier (not a Change.Schema rewrite in the
// orchestrator) so each change's source Schema — and thus source-keyed
// --redact rule matching — stays intact while only the target table
// reference is qualified with the renamed namespace.
func (s *Streamer) namespaceRenameFunc() func(string) string {
	if s.NamespaceMap.IsEmpty() {
		return nil
	}
	return s.NamespaceMap.Apply
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
		return nil, nil, connectHint(fmt.Errorf("pipeline: list source databases: %w", err))
	}

	// Resolve the selected SOURCE set (ADR-0142: map-only ⇒ the map keys ARE
	// the selection; otherwise the filter decides and the map renames within
	// it), then refuse loudly if a rename-map key is not in that selection
	// (typo guard) or two sources resolve to one target (many-to-one).
	selected = selectNamespaces(all, s.DatabaseFilter, s.AllDatabases, s.NamespaceMap)
	if err := crossCheckMapSelection(selected, s.NamespaceMap); err != nil {
		return nil, nil, err
	}
	if len(selected) == 0 {
		return nil, nil, errors.New(
			"pipeline: no source databases matched the database scope; nothing to sync " +
				"(check --include-database / --exclude-database / --map-database / --map-schema, " +
				"or that the source server has non-system databases)",
		)
	}
	// Resolve each source namespace's TARGET name through the rename map and
	// refuse loudly on an exact many-to-one collision (engine-agnostic; fires
	// even on a case-sensitive target).
	targets, err := resolveTargetNamespaces(selected, s.NamespaceMap)
	if err != nil {
		return nil, nil, err
	}
	// Refuse LOUDLY when two distinct MAPPED/unmapped target names FOLD to the
	// same identifier on a folding MySQL target (ADR-0075 decision #1 /
	// ADR-0142) — the silent-merge hazard — on the SYNC path too, before any
	// cold-start or CDC moves data. Mirrors migrate_multidb.go's preflight on
	// the mapped target names; kept alongside the exact many-to-one guard above
	// (defense in depth). No-op on a non-folding (Postgres) target; identity
	// map ⇒ targets == selected ⇒ byte-identical to before.
	if err := preflightNamespaceFoldCollisions(ctx, s.Target, s.TargetDSN, targets); err != nil {
		return nil, nil, err
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
// forceFresh has the same meaning as in [Streamer.coldStart]: a deliberate
// re-copy onto an expectedly-populated target — the operator's
// --restart-from-scratch OR the automatic ADR-0093 auto-resnapshot
// (warm-resume → ir.ErrPositionInvalid fall-through). It suppresses the
// populated-target refusal (idempotent absorbs via UPSERT; non-idempotent
// drops + recreates first), keeping the cdc-state row.
func (s *Streamer) coldStartMultiDatabase(
	ctx context.Context,
	lsnTracker any,
	applier ir.ChangeApplier,
	streamID string,
	fresh freshCopyReason,
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

	// ---- 1.4. UNLOGGED-table census (G2): refuse BEFORE anything is
	// created on either side — the spanning snapshot opener (step 3)
	// creates the FOR ALL TABLES publication that silently excludes
	// unlogged tables, and the per-database copy would include them, so
	// each would freeze at its cold-copy snapshot forever at exit 0.
	// Filter-aware (an --exclude-table'd unlogged table passes). ----
	if err := s.preflightSpanningUnloggedTables(ctx, selected); err != nil {
		return nil, stop, err
	}

	// ---- 1.5. A target that cannot namespace at all is refused before the
	// spanning snapshot is taken (roadmap item 148, route 2 — the `migrate`
	// sibling in runMultiDatabase). The `else` arm in
	// coldStartCopyOneDatabase sets targetSchema on the same premise the
	// migrate path did, and [migcore.ApplyTargetSchema] discards it on an
	// engine with no [ir.SchemaSetter]. Every source database would then
	// write bare names into ONE target namespace. Placed before step 3's
	// FLUSH TABLES WITH READ LOCK for the same reason the connection-budget
	// preflight is: a run that cannot succeed should not freeze the source
	// first. ----
	if err := migcore.ValidateMultiNamespaceTarget(s.Target, "multi-namespace sync", selected); err != nil {
		return nil, stop, err
	}

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
			// Route to the (possibly renamed) TARGET namespace (ADR-0142);
			// identity when the source is unmapped.
			target := s.NamespaceMap.Apply(database)
			if err := targetDeriver.EnsureDatabase(ctx, s.TargetDSN, target); err != nil {
				return nil, stop, migcore.WrapWithHint(migcore.PhaseSchemaApply,
					fmt.Errorf("pipeline: ensure target database %q: %w", target, err))
			}
		}
	}

	// Connection-budget preflight, ONCE for the run (item 144). Every
	// per-database copy below fans out D writers at the SAME target server —
	// only the database in the DSN differs — so the target's declared write
	// fan-out ceiling is a property of the run, and probing per database
	// would open one control connection per database for the same answer.
	// This is the multi-database sibling of the single-database preflight in
	// [Streamer.coldStartOpenTargetWriters]; without it the multi-database
	// cold-start was the one W x D fan-out path that never heard the target's
	// capacity at all.
	//
	// Placed BEFORE the spanning snapshot deliberately, unlike its
	// single-database sibling: step 3 takes a FLUSH TABLES WITH READ LOCK
	// that briefly freezes writes on the SOURCE, and a target with no free
	// slot for the fan-out should refuse without paying that.
	_, budgetReport, err := migcore.ResolveTargetCopyParallelism(
		ctx, s.Target, s.TargetDSN, resolveCopyFanoutDegree(s.CopyFanoutDegree), s.MaxTargetConnections,
	)
	if err != nil {
		return nil, stop, err
	}

	// ---- 3. Open the SINGLE spanning consistent snapshot. One tx, one
	// binlog position spanning ALL selected databases. This is the crux:
	// the position handed to CDC below is captured at one consistent cut. ----
	stream, err := opener.OpenMultiDatabaseSnapshotStream(ctx, s.SourceDSN, selected)
	if err != nil {
		return nil, stop, migcore.WrapWithHint(migcore.PhaseSnapshot, fmt.Errorf("pipeline: open multi-database snapshot stream: %w", err))
	}
	// Once the snapshot is open every error path must tear it down. The
	// pre-anchor rule (Bug 177, see coldStartOpenTargetWriters) picks
	// which teardown: before the CDC anchor position is persisted (step
	// 5), Abandon — no resume is possible, so a durable slot the open
	// created would be orphaned debris; after the anchor write, Close —
	// the slot is the warm-resume anchor. Today's multi-database sources
	// are MySQL (no AbandonFn ⇒ Abandon ≡ Close), but the PG spanning
	// snapshot (ADR-0075) shares this opener surface and DOES create a
	// slot.
	closeStream := func() { _ = stream.Close() }
	abandonStream := func() { _ = stream.Abandon() }

	slog.InfoContext(ctx, "multi-database cold start; single spanning snapshot captured",
		slog.String("stream_id", streamID))
	migcore.ApplyMaxBufferBytes(stream.Rows, s.MaxBufferBytes)

	// ---- 4. Per-database schema read + bulk-copy from the SHARED spanning
	// RowReader. Each database's tables carry Table.Schema = its source
	// database (stamped by MultiDatabaseScoper), and the spanning RowReader
	// qualifies its SELECT by that schema, so the single pinned snapshot
	// connection reads across every database at the one consistent view. ----
	for _, database := range selected {
		if err := s.coldStartCopyOneDatabase(ctx, stream, applier, streamID, database, inScope, targetDeriver, targetCanDeriveDB, fresh, budgetReport.CopyFanoutCeiling); err != nil {
			abandonStream()
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
			abandonStream()
			return nil, stop, migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("pipeline: persist multi-database cold-start CDC anchor position: %w", err))
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
		// Best-effort companion (ir.CDCDatabaseListSetter): the concrete
		// selected set, so the reader's binlog-filter preflight can prove
		// the scope sits inside a server-side --binlog-do-db allow-list —
		// the predicate alone cannot answer that subset question.
		if lister, ok := stream.Changes.(ir.CDCDatabaseListSetter); ok {
			lister.SetCDCDatabaseList(selected)
		}
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
	//     routes every change to its Change.Schema namespace. The optional
	//     rename (ADR-0142) is threaded through the SAME call so the applier
	//     maps each change's source namespace to its TARGET namespace inside
	//     routedSchema — keeping the change's source Schema (and thus
	//     source-keyed --redact rules) intact. nil ⇒ identity. Refuse loudly
	//     if the applier can't route (it would silently write every database
	//     into one namespace — cross-database bleed / silent loss).
	if router, ok := applier.(ir.MultiDatabaseRouter); ok {
		router.SetMultiDatabaseRouting(true, s.namespaceRenameFunc())
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
	// Bug 246: reader-side scope predicate, multidb cold-start mirror.
	s.wireCDCScopePredicate(stream.Changes)

	changes, err = stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		closeStream()
		return nil, stop, migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("pipeline: start multi-database cdc: %w", err))
	}
	stop = func() { _ = stream.Close() }
	if errer, ok := stream.Changes.(interface{ Err() error }); ok {
		s.sourceErrFn = errer.Err
	}
	return changes, stop, nil
}

// warmResumeMultiDatabase is the ADR-0074 Phase 1b.3 multi-database
// WARM-RESUME (stop → restart). A multi-database stream that has a
// persisted server-wide binlog position must RESUME the single stream —
// re-scoped to the selected database set, routing on — NOT re-cold-start
// (which would re-snapshot + re-copy every database). Shape:
//
//  1. Re-resolve the selected database set + inScope predicate via the
//     SAME resolveStreamDatabases the cold-start uses (the operator
//     passes the same --include-database / --all-databases flags on
//     restart; the live server is the source of truth, identical to
//     cold-start). A newly-appeared in-scope database is admitted by the
//     predicate; see the loud-failure note below.
//  2. Open a BARE server-wide CDC reader (database-optional DSN) via
//     [ir.ServerCDCReaderOpener] — no snapshot, no bulk-copy.
//  3. Wire the CDC phase exactly as the cold-start does at handoff:
//     SetCDCDatabaseScope(inScope) on the reader (scope the server-wide
//     binlog to the selected set) and SetMultiDatabaseRouting(true) on
//     the applier (per-change namespace routing). Refuse loudly if either
//     surface is absent — without scoping the reader would emit only its
//     (empty) bound database's events, and without routing the applier
//     would write every database into one namespace (cross-database bleed
//     / silent loss).
//  4. Resume StreamChanges(ctx, persisted) from the ONE persisted
//     server-wide position. Resume advances the one position regardless
//     of which database an event came from (the binlog coordinate is
//     server-wide).
//
// Loud-failure note (newly-appeared in-scope database): the selected set
// is re-resolved from the LIVE server on restart, so a database created
// since cold-start that matches the include globs IS admitted by inScope
// and its CDC events flow. Its target namespace was never cold-started,
// so the applier writes into an assumed-existing namespace; if it does
// not exist the apply fails LOUDLY (no silent drop). Live add-database
// (the cold-start analogue) is a future phase — 1b.3 only guarantees the
// failure is loud, never silent.
//
// The returned changes channel + stop closure follow the same contract
// as [Streamer.warmResume] / [Streamer.coldStartMultiDatabase].
func (s *Streamer) warmResumeMultiDatabase(
	ctx context.Context,
	persisted ir.Position,
	lsnTracker any,
	applier ir.ChangeApplier,
	streamID string,
) (changes <-chan ir.Change, stop func(), err error) {
	stop = func() {}

	if err := s.validateMultiDatabaseStream(); err != nil {
		return nil, stop, err
	}

	opener, ok := s.Source.(ir.ServerCDCReaderOpener)
	if !ok {
		return nil, stop, fmt.Errorf(
			"pipeline: source engine %q does not support a server-wide CDC reader (ADR-0074 Phase 1b.3); "+
				"multi-database `sync start` warm-resume requires a server-wide-binlog source (MySQL)",
			s.Source.Name(),
		)
	}

	// Re-resolve the selected database set from the live server — same
	// source of truth as cold-start. The operator passed the same scope
	// flags on restart; re-enumerate + re-filter rather than trusting any
	// stale snapshot-time set (there is none persisted; the position model
	// is one server-wide coordinate, not a per-database set).
	selected, inScope, err := s.resolveStreamDatabases(ctx)
	if err != nil {
		return nil, stop, err
	}
	slog.InfoContext(
		ctx, "multi-database sync: warm-resume; re-resolved database set",
		slog.String("stream_id", streamID),
		slog.Int("count", len(selected)),
		slog.Any("databases", selected),
		slog.String("position_token", persisted.Token),
	)

	// UNLOGGED-table census at the warm-resume open too (G2): `ALTER
	// TABLE … SET UNLOGGED` succeeds mid-sync under the spanning FOR ALL
	// TABLES publication and silently drops the table from it (observed
	// on PG 16), so the cold-start census alone leaves every later open
	// blind to the flip. A flip DURING a live window is still
	// undetectable until this next open — the documented residual.
	if err := s.preflightSpanningUnloggedTables(ctx, selected); err != nil {
		return nil, stop, err
	}

	// Open the bare server-wide CDC reader (no snapshot, no copy).
	cdc, err := opener.OpenServerCDCReader(ctx, s.SourceDSN)
	if err != nil {
		return nil, stop, migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("pipeline: open server-wide cdc reader: %w", err))
	}
	// Once the reader is open every error path must close it.
	closeReader := func() { migcore.CloseIf(cdc) }

	// Scope the server-wide binlog to the selected database set. The reader
	// emits row/truncate events from EVERY selected database (each tagged
	// with its source database in Change.Schema) and drops events outside
	// the set. Refuse loudly when absent — an unscoped reader would emit
	// only its (empty) bound database's events, silently losing every
	// selected database's changes.
	if scoper, ok := cdc.(ir.CDCDatabaseScoper); ok {
		scoper.SetCDCDatabaseScope(inScope)
		// Best-effort companion (ir.CDCDatabaseListSetter) — same rationale
		// as the cold-start wiring: the concrete set lets the reader's
		// binlog-filter preflight prove scope ⊆ --binlog-do-db.
		if lister, ok := cdc.(ir.CDCDatabaseListSetter); ok {
			lister.SetCDCDatabaseList(selected)
		}
	} else {
		closeReader()
		return nil, stop, fmt.Errorf(
			"pipeline: source CDC reader for %q does not implement ir.CDCDatabaseScoper; "+
				"cannot scope the server-wide binlog to the selected database set (ADR-0074 Phase 1b.3)",
			s.Source.Name(),
		)
	}

	// Enable per-change namespace routing on the applier, threading the
	// optional ADR-0142 rename through the same call (nil ⇒ identity). The
	// rename map is re-resolved from the same flags on restart, so warm-resume
	// routes to the same TARGET namespaces the cold-start did. Refuse loudly
	// if absent — it would silently write every database into one namespace
	// (cross-database bleed / silent loss).
	if router, ok := applier.(ir.MultiDatabaseRouter); ok {
		router.SetMultiDatabaseRouting(true, s.namespaceRenameFunc())
	} else {
		closeReader()
		return nil, stop, fmt.Errorf(
			"pipeline: target applier for %q does not implement ir.MultiDatabaseRouter; "+
				"cannot route CDC changes per source database to same-named target namespaces (ADR-0074 Phase 1b.3)",
			s.Target.Name(),
		)
	}

	if lsnTracker != nil {
		if attacher, ok := cdc.(lsnTrackerAttacher); ok {
			attacher.AttachLSNTracker(lsnTracker)
		}
	}
	if s.PollInterval > 0 {
		if setter, ok := cdc.(pollIntervalSetter); ok {
			setter.SetPollInterval(s.PollInterval)
		}
	}
	// Bug 246: reader-side scope predicate, multidb warm-resume mirror.
	s.wireCDCScopePredicate(cdc)

	changes, err = cdc.StreamChanges(ctx, persisted)
	if err != nil {
		closeReader()
		return nil, stop, migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("pipeline: resume multi-database cdc: %w", err))
	}
	stop = func() { migcore.CloseIf(cdc) }
	if errer, ok := cdc.(interface{ Err() error }); ok {
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
	fresh freshCopyReason,
	fanoutCeiling int,
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
		return connectHint(fmt.Errorf("pipeline: open source schema reader for %q: %w", database, err))
	}
	migcore.ApplyTableScope(sr, s.Filter)
	applyMultiDatabaseScope(sr, &multiDBScope{database: database, inScope: inScope})
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		migcore.CloseIf(sr)
		return fmt.Errorf("pipeline: read source schema for %q: %w", database, err)
	}
	migcore.CloseIf(sr)

	if len(schema.Tables) == 0 {
		slog.InfoContext(ctx, "multi-database: database has no tables; skipping copy",
			slog.String("stream_id", streamID), slog.String("database", database))
		return nil
	}

	if err := migcore.ApplyTableFilter(ctx, schema, s.Filter); err != nil {
		return fmt.Errorf("pipeline: filter tables for %q: %w", database, err)
	}
	applyViewFilter(ctx, schema, s.ViewFilter, s.SkipViews)
	// ADR-0143: skip ORM/framework migration-bookkeeping tables in the
	// per-database sync fan-out too. No-op unless SkipORMTables is set.
	applyORMTableSkip(ctx, schema, s.SkipORMTables, s.Filter)

	// Apply per-column type / expression overrides before schema-apply.
	schema, err = translate.ApplyMappings(schema, s.Mappings)
	if err != nil {
		return fmt.Errorf("pipeline: apply mappings for %q: %w", database, err)
	}
	schema, err = translate.ApplyExpressionOverrides(schema, s.ExpressionMappings)
	if err != nil {
		return fmt.Errorf("pipeline: apply expression overrides for %q: %w", database, err)
	}

	// Cross-engine schema-narrowing advisory notices (Bug 157 Q2). Emitted
	// PER DATABASE — once each database's schema is finalized (mappings +
	// expression overrides applied) and before its target tables are created
	// — so a MySQL→PG multi-database sync surfaces the same up-front WARNs
	// the migrate path emits, scoped to the affected database. The helper's
	// scanners self-short-circuit by engine pair (MySQL→MySQL multi-database
	// — the common fan-out shape — emits ZERO notices). FK stripping below
	// does not affect column-type scanning, so order versus stripForeignKeys
	// is immaterial.
	emitCrossEngineTranslationNotices(ctx, schema, s.Source.Name(), s.Target.Name(), "sync cold-start")

	// Index-emit pre-flight refusal (roadmap item 118), per database and
	// before this database's target tables are created — the multi-database
	// sibling of the single-database cold-start's call.
	//
	// What it prevents, stated exactly, because the earlier wording here
	// claimed more than it delivers (audit backlog C-6): it stops THIS
	// database's rows from landing before the refusal. It does NOT stop the
	// run from having copied the preceding databases — this call is inside
	// the per-database copy loop, and each database's schema is read here, so
	// an unrepresentable index in database N is still discovered only when the
	// loop reaches N, with N-1 databases already fully copied.
	//
	// That residual is FILED, not fixed (see the C-6 entry): hoisting it means
	// reading all N schemas before the copy loop, which either doubles the
	// schema reads or needs a cache threaded through. The cost of the residual
	// is wasted copy time and a target holding N-1 complete databases — loud,
	// and resumable — which is why it did not outrank the silent-loss work.
	if err := migcore.PreflightTableEmit(ctx, s.Target, schema, "sync cold-start"); err != nil {
		return fmt.Errorf("pipeline: preflight tables for %q: %w", database, err)
	}
	// Item 149, per database. The fan-out's DATABASE-name fold is already
	// refused once up front (preflightNamespaceFoldCollisions); this is the
	// TABLE-name axis of the same server setting, and it is per-database
	// because each iteration carries its own schema. s.TargetDSN is the server
	// DSN rather than this database's derived one on purpose — the setting is
	// global, and the derived DSN is not built until further down.
	if err := migcore.PreflightTableNameFold(ctx, s.Target, s.TargetDSN, schema, "sync cold-start"); err != nil {
		return fmt.Errorf("pipeline: preflight table-name fold for %q: %w", database, err)
	}
	if err := migcore.PreflightIndexEmit(ctx, s.Target, schema, "sync cold-start"); err != nil {
		return fmt.Errorf("pipeline: preflight indexes for %q: %w", database, err)
	}
	if err := migcore.PreflightViewEmit(ctx, s.Target, schema, "sync cold-start"); err != nil {
		return fmt.Errorf("pipeline: preflight views for %q: %w", database, err)
	}
	// The COLUMN-TYPE member. The same per-database residual noted above
	// applies to it unchanged: database N's unrenderable column is still found
	// when the loop reaches N.
	if err := migcore.PreflightColumnTypeEmit(ctx, s.Target, schema, "sync cold-start"); err != nil {
		return fmt.Errorf("pipeline: preflight column types for %q: %w", database, err)
	}

	// Foreign keys are deferred to keep the first cut symmetric with the
	// multi-database migrate path: a same-named cross-database FK may
	// reference a database whose tables are created in a later iteration.
	// CDC apply (the steady state) does not depend on FKs, and a follow-on
	// cross-database FK pass can be added when demand surfaces. Strip them
	// so the per-database CreateConstraints emits nothing cross-database
	// that doesn't yet exist on the target.
	//
	// --skip-foreign-keys additionally synthesizes the backing indexes for
	// each FK's referencing columns before stripping (this multi-database
	// path never re-creates the FKs, so index synthesis is the only extra
	// work versus the plain deferral).
	if s.SkipForeignKeys {
		logSkipForeignKeys(ctx, applySkipForeignKeys(schema))
	} else {
		stripForeignKeys(schema)
	}

	// ---- Per-database target writers. ----
	// target is the source namespace's renamed TARGET (ADR-0142; identity
	// when unmapped). Only the target writers route to it — the source
	// schema read above stays keyed on `database`.
	target := s.NamespaceMap.Apply(database)
	targetDSN := s.TargetDSN
	targetSchema := ""
	if targetCanDeriveDB {
		// MySQL → MySQL: re-point the target DSN at the (possibly renamed)
		// target database (already EnsureDatabase'd by the caller).
		targetDSN, err = targetDeriver.WithDatabase(s.TargetDSN, target)
		if err != nil {
			return fmt.Errorf("pipeline: derive target DSN for database %q: %w", target, err)
		}
	} else {
		// MySQL → PG (or any namespaced target): route to the (possibly
		// renamed) target schema; the PG writer auto-creates it.
		targetSchema = target
	}

	sw, err := s.Target.OpenSchemaWriter(ctx, targetDSN)
	if err != nil {
		return connectHint(fmt.Errorf("pipeline: open target schema writer for %q: %w", database, err))
	}
	migcore.ApplyTargetSchema(sw, targetSchema)
	applyIndexBuildMem(sw, s.IndexBuildMem)
	applyIndexBuildParallelism(sw, s.IndexBuildParallelism)
	migcore.ApplyIndexBuildFallback(sw, s.IndexBuildFallback)
	// Item 112 is deliberately NOT armed on the multi-database cold-start: the
	// item-109 metadata-only FK add is only safe when its bounded orphan scan
	// runs, and that scan is wired into the single-DB coldStartRunCopy path, not
	// coldStartMultiDatabase. Arming here without the scan would add unvalidated
	// FKs and silently accept a dirty source's orphans. So the multi-DB path
	// keeps full target-side FK validation (safe, just not wall-optimized) —
	// wiring the scan into the multi-DB path is a follow-up if demand appears.

	rw, err := s.Target.OpenRowWriter(ctx, targetDSN)
	if err != nil {
		migcore.CloseIf(sw)
		return connectHint(fmt.Errorf("pipeline: open target row writer for %q: %w", database, err))
	}
	migcore.ApplyTargetSchema(rw, targetSchema)
	migcore.ApplyMaxBufferBytes(rw, s.MaxBufferBytes)

	// Cold-start preflight: refuse if any target table already holds data
	// (Bug 9). --force-cold-start / --restart-from-scratch / --reset-target-
	// data override. ResetTargetData is the destructive variant (drop +
	// re-copy) handled per-database here for parity with single-database.
	switch {
	case s.ResetTargetData:
		if err := resetTargetDataForStream(ctx, schema, rw, applier, streamID); err != nil {
			migcore.CloseIf(rw)
			migcore.CloseIf(sw)
			return fmt.Errorf("pipeline: reset target data for %q: %w", database, err)
		}
	case fresh.forcesFresh() && (!copyReaderIsIdempotent(stream.Rows) || fresh.clearsTarget()):
		// restart-from-scratch / auto-resnapshot onto a NON-idempotent reader
		// (multi-database fan-out is MySQL-source native binlog, plain
		// INSERT). forceFresh covers BOTH --restart-from-scratch AND the
		// automatic auto-resnapshot fall-through. Drop the in-scope target
		// tables first so the fresh cold-start doesn't dup-key (Error 1062) on
		// the prior copy's rows. Mirrors the single-database gate in
		// coldStartGatePreflight; the idempotent path (none in multi-DB today,
		// but guarded for parity) keeps the absorb-the-overlap behaviour via
		// the default branch.
		if err := resetTargetTablesForRestart(ctx, schema, rw); err != nil {
			migcore.CloseIf(rw)
			migcore.CloseIf(sw)
			return fmt.Errorf("pipeline: restart-from-scratch reset for %q: %w", database, err)
		}
	default:
		if err := preflightColdStart(ctx, schema, rw, s.ForceColdStart || fresh.forcesFresh(), preflightModeSync); err != nil {
			migcore.CloseIf(rw)
			migcore.CloseIf(sw)
			return fmt.Errorf("pipeline: cold-start preflight for %q: %w", database, err)
		}
		// Roadmap item 140, the multi-database sibling. This fan-out is the
		// one copy entry point of the six that was neither covered nor named
		// by the first cut; a target database branched from an existing one
		// dies on Error 1452 / SQLSTATE 23503 here exactly as it does on the
		// single-database path, N-1 databases into the run. See
		// migrate_existing_tables_fks.go for the full entry-point roster and
		// the three exemptions.
		//
		// Only this branch: the two above drop the in-scope target tables
		// first, so nothing pre-existing survives to find — the same reason
		// the single-database gate lives in its own default branch alone.
		// The gate is built with THIS database's resolved targetDSN /
		// targetSchema, not the server DSN, because a catalog is per
		// database. Shape-compare is deliberately not run here (the fan-out
		// creates every in-scope table); this is the FK verdict only.
		fkGate := &existingTablesGate{
			Source:              s.Source,
			Target:              s.Target,
			TargetDSN:           targetDSN,
			TargetSchema:        targetSchema,
			EnabledPGExtensions: s.EnabledPGExtensions,
			Mode:                "sync cold-start",
		}
		if err := fkGate.readAndCheckPreExistingForeignKeys(ctx, schema); err != nil {
			migcore.CloseIf(rw)
			migcore.CloseIf(sw)
			return err
		}
	}

	slog.InfoContext(ctx, "multi-database: copying database from the spanning snapshot",
		slog.String("stream_id", streamID),
		slog.String("database", database),
		slog.Int("tables", len(schema.Tables)))

	// ADR-0110 (v0.99.103): wire the coordinated grow-gate onto this
	// database's cold-copy writer — runBulkCopyWithOpts reuses this rw
	// across all fan-out workers (see coldStartRunCopy for the rationale).
	// One gate per database cold-copy; nil provider ⇒ signal-driven only.
	gate := migcore.GrowGateOrNil(migcore.NewGrowGate(ctx, storageRecoveredProbe(ctx, s.TargetTelemetry)))
	migcore.ApplyGrowGate(rw, gate)
	s.startStorageHeadroomWatch(ctx, streamID, gate)

	// CopyFanoutDegree joined the list 2026-08-01: runBulkCopyWithOpts already
	// resolved a fan-out degree here (the zero value resolves to the
	// conservative DEFAULT, never "no workers"), so the multi-database
	// cold-copy always fanned out — it just could not hear the operator's
	// --copy-fanout-degree. Same one-sided-knob class as the rest of this
	// change; found by the new bulkCopyOpts assignment gate.
	bulkOpts := bulkCopyOpts{
		Redactor:             s.Redactor,
		UpfrontIndexes:       s.UpfrontIndexes,
		AnalyzeAfter:         s.AnalyzeAfter,
		CopyFanoutDegree:     s.CopyFanoutDegree,
		CopyFanoutCeiling:    fanoutCeiling,
		NoIntraTableStealing: s.NoIntraTableStealing,
		GrowGate:             gate,
	}
	if err := runBulkCopyWithOpts(ctx, schema, stream.Rows, sw, rw, bulkOpts); err != nil {
		migcore.CloseIf(rw)
		migcore.CloseIf(sw)
		return fmt.Errorf("pipeline: bulk-copy database %q: %w", database, err)
	}
	migcore.CloseIf(rw)
	migcore.CloseIf(sw)
	return nil
}
