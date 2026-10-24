// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Mid-stream add-table orchestrator (Phase 1 MVP).
//
// Operators run `CREATE TABLE foo` on a CDC source. Today, sluice
// silently drops events for foo (defence-in-depth WARN, no schema
// propagation — see ADR-0021). The AddTable orchestrator brings the
// new table into an active stream's scope without a destructive
// `--reset-target-data` cycle.
//
// Strategy A from `docs/dev/design/mid-stream-add-table.md`: the
// operator drains the running stream first (`sluice sync stop --wait`),
// then runs `sluice schema add-table SOURCE.NAME`, then resumes
// (re-run `sluice sync start` with the same --stream-id; there is no
// resume flag). AddTable refuses if the stream is
// still active. Live add-table without the drain (Phase 2, `--no-drain`)
// is withdrawn on every source (Bug 332): it refuses before anything is
// read or written.
//
// The flow:
//
//   1. Validate inputs and confirm the stream's cdc-state row exists
//      on the target (refuse cleanly if the operator typo'd the
//      stream-id or pointed at the wrong target).
//   2. Refuse if the stream looks active (stop_requested_at IS NOT
//      NULL — a sync stop is in flight).
//   3. Read the source schema, filter to just the named table.
//   4. Refuse if the table doesn't exist on the source.
//   5. Apply per-column type / expression overrides.
//   6. Open target writers; refuse if the target table already
//      exists with rows (TableEmptyChecker).
//   6a. Record the table as pending on the stream's control row
//      (INCLUDE-TABLE-SCOPE-DRIFT, Bug 321). Everything above refuses
//      without changing anything; everything below changes state, so a
//      failure from here leaves the record, `sync start` refuses a warm
//      resume that includes the table, and on PG the publication add of
//      step 7 is taken back. Cleared after step 9 (and the MySQL
//      filter-flip) succeed.
//   7. For PG: ALTER PUBLICATION ... ADD TABLE so the slot picks up
//      events on the new table from this LSN forward (idempotent).
//      MySQL has no publication; the binlog already covers everything.
//   8. Open a snapshot stream restricted to the new table. PG uses a
//      temporary slot (the main `sluice_slot` is left untouched);
//      MySQL uses START TRANSACTION WITH CONSISTENT SNAPSHOT.
//   9. Bulk-copy the rows via runBulkCopy (single-table schema).
//  10. Drop the temp PG slot if one was created. The persisted
//      cdc-state position is NOT updated — the stream's existing
//      position is still the right resume point for the other
//      tables, and the applier's idempotent upsert handles the
//      [persisted_LSN, snapshot_LSN] overlap on the new table.
//  11. Operator restarts via `sluice sync start` (same --stream-id) — the
//      stream picks up CDC for every table including the new one
//      from its persisted position.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/redact"
	"sluicesync.dev/sluice/internal/translate"
)

// publicationAdder is the optional engine-side surface for the
// additive `ALTER PUBLICATION ... ADD TABLE` shape used by mid-
// stream add-table. Postgres implements it; MySQL does not (no
// publication concept). Discovered structurally so the orchestrator
// stays engine-neutral.
//
// Implementations MUST be idempotent — a re-run after a partial-add
// (publication updated, bulk-copy crashed) lands cleanly. Tables
// already in the publication are skipped.
type publicationAdder interface {
	AddPublicationTables(ctx context.Context, dsn string, tables []string) error
}

// snapshotSlotOpener is the optional engine-side surface for opening
// a snapshot stream with an operator-supplied slot name. Same shape
// as ir.SnapshotStreamWithSlotOpener; re-declared here to avoid the
// pipeline package leaking ir import-cycle weirdness for what's a
// structural-interface check.
type snapshotSlotOpener interface {
	OpenSnapshotStreamWithSlot(ctx context.Context, dsn, slotName string) (*ir.SnapshotStream, error)
}

// slotDropper is the optional engine-side surface for dropping a
// named replication slot. Used by the add-table orchestrator's
// cleanup path on Postgres — the temp snapshot slot is short-lived
// and dropped after bulk-copy completes so it doesn't pin WAL.
// MySQL doesn't implement it (no slot concept).
type slotDropper interface {
	OpenSlotManager(ctx context.Context, dsn string) (ir.SlotManager, error)
}

// currentWALPositionReader is an optional engine-side surface for
// reading the source's current WAL position against a supplied DSN.
// ADR-0036 (Path D Phase A) instrumentation: the diagnostic flow logs
// pg_current_wal_lsn() before AND after publication-add so the
// captured logs name a concrete LSN window for the catalog change.
// Independent from the SchemaReader.SourceCurrentPosition surface
// (which requires a held SchemaReader) because the add flow
// closes the SchemaReader before publication-add runs.
//
// Implemented on PG; engines without WAL semantics omit it. Returns
// the position's bare token (LSN string for PG); callers are responsible
// for engine-specific parsing.
type currentWALPositionReader interface {
	ReadCurrentWALPosition(ctx context.Context, dsn string) (string, error)
}

// snapshotLSNExtractor is the optional engine-side surface for
// extracting the consistent-point LSN out of a snapshot stream's
// opaque [ir.Position] token, for the ADR-0036 debug line that places
// the snapshot against the publication-add LSN window
// ([AddTable.diagSnapshotConsistentPoint]). PG implements via
// JSON-decoding its position envelope; engines without LSN-shaped
// positions return ("", false, nil), and an engine without the surface
// skips the line.
type snapshotLSNExtractor interface {
	ExtractSnapshotLSN(pos ir.Position) (lsn string, ok bool, err error)
}

// AddTable is the orchestrator for the mid-stream add-table flow.
// Construct the value, then call Run with a context. AddTable does
// not retain state between Run calls — call it once per add.
//
// The Source/Target engines should be the same engines the active
// stream uses; AddTable validates this against the cdc-state row's
// source-side engine encoding when possible (Phase 1: same-engine
// pairs; cross-engine validation is best-effort).
type AddTable struct {
	// Source / Target are the engines the source / target DSNs
	// belong to. Required.
	Source ir.Engine
	Target ir.Engine

	// SourceDSN / TargetDSN are the engine-native connection strings.
	// Required. Must match the active stream's source / target DSNs
	// (host + database) — the orchestrator doesn't validate the DSN
	// strings byte-for-byte but the stream-id lookup against the
	// target's cdc-state surfaces the most important shape of
	// mismatch.
	SourceDSN string
	TargetDSN string

	// StreamID is the active stream's identifier — the key in
	// sluice_cdc_state on the target. Required: the orchestrator
	// refuses if no row exists for this stream.
	StreamID string

	// TableName is the unqualified name of the new source table to
	// bring into scope. The schema (PG namespace / MySQL database)
	// is inferred from the source DSN. Required.
	TableName string

	// Mappings is the per-column type-override list applied during
	// schema translation, mirroring [Migrator.Mappings].
	Mappings []config.Mapping

	// ExpressionMappings is the per-column generated-expression
	// override list, mirroring [Migrator.ExpressionMappings].
	ExpressionMappings []config.ExpressionMapping

	// SlotName, when non-empty, overrides the temporary
	// replication-slot name used for the snapshot capture on
	// engines with a slot concept (Postgres). Default is an
	// auto-generated `sluice_addtable_<table>` slot. Engines
	// without slots ignore this field.
	SlotName string

	// DryRun, when true, prints the plan (which table, target DDL
	// summary) without modifying the source publication, the
	// target schema, or capturing a snapshot.
	DryRun bool

	// LiveMode is `--no-drain` on `sluice schema add-table`: an add
	// against a stream that is still running. It is WITHDRAWN on every
	// source and always refuses, before the source is read or anything is
	// written, naming the drained add-table flow
	// ([noDrainRefusalForStream]). Each live path it once took lost the
	// changes made to the table during the add: Postgres's publication add
	// (NO-DRAIN-POSTGRES-SOURCE, Bug 332), the binlog filter-flip
	// (NO-DRAIN-BINLOG-SOURCE) and the VStream copy
	// (NO-DRAIN-VSTREAM-SOURCE). The field and the flag stay so a script
	// that still passes it gets the refusal rather than an unknown-flag
	// error or, worse, a drained add against a running stream.
	LiveMode bool

	// TargetSchema is the operator-supplied per-source target schema
	// namespace (`--target-schema NAME`, ADR-0031). Bug 46 fix:
	// when the active stream was started with `--target-schema=NAME`,
	// add-table must route the new table into the same namespace —
	// otherwise the table lands in `public.<table>` but the
	// stream's CDC applier routes events to `<NAME>.<table>` and
	// they silently drop with a WARN log.
	//
	// Resolution rule:
	//   - If empty AND the recorded stream's target_schema is
	//     non-empty, inherit the recorded value.
	//   - If non-empty AND matches the recorded value, proceed.
	//   - If non-empty AND differs from the recorded non-empty
	//     value, refuse loudly with a clear mismatch error.
	//   - If empty AND the recorded value is empty (legacy / no
	//     `--target-schema` at start time), fall through to the
	//     DSN's default schema — pre-Bug-46 behaviour preserved.
	//
	// Engines that don't implement [ir.SchemaSetter] (MySQL) are
	// rejected upstream by the validate gate. Threaded into the
	// schema writer / row writer / change applier via the
	// [ir.SchemaSetter] surface after resolution.
	TargetSchema string

	// Redactor is the operator-configured PII redaction policy
	// (Phase 1, roadmap item 15a). Same shape as
	// [Migrator.Redactor] / [Streamer.Redactor] — see those for the
	// design. Threaded into the bulk-copy phase of the add-table
	// path so a newly-added table is PII-clean from the first row.
	Redactor *redact.Registry

	// AcceptTargetOnlyUniqueKeys is [Streamer.AcceptTargetOnlyUniqueKeys]
	// for the table being added: unique keys a pre-created target table
	// carries beyond the source's, accepted with its fingerprint (`<table>.<key>=<fingerprint>`).
	AcceptTargetOnlyUniqueKeys []string

	// sourceKeyFacts is the source reader's [ir.UniqueKeyFact]s, read in
	// Run while the source reader is open and folded into a COPY of the
	// added table by the unique-key door ([AddTable.refuseTargetOnlyUniqueKeys]).
	sourceKeyFacts map[string]map[string]ir.UniqueKeyFact
	// sourceAsRead is the added table before --type-override, whose column
	// collations the unique-key door judges direction on.
	sourceAsRead *ir.Table
	// publicationAddedByThisRun is whether this run's publication add is what
	// put the table in the publication ([AddTable.notePublicationAdd]), so a
	// failed add may take it back out ([AddTable.undoFailedAdd]).
	publicationAddedByThisRun bool

	// Abandon retires an add-table instead of adding the table (`sluice
	// schema add-table --abandon`; see [AddTable.runAbandon]): it marks a
	// pending record ABANDONED, drops the dead add's inactive snapshot slot,
	// takes the table out of an unshared publication (no other sluice slot on
	// the same database) unless the stream established it, and takes it out
	// of the live-added set; it refuses while the add's heartbeat is fresh.
	// The operator then restarts the stream.
	Abandon bool
	// pendingHeartbeatEvery overrides [ir.PendingAddHeartbeatInterval]
	// (tests); zero is the default.
	pendingHeartbeatEvery time.Duration
	// pendingToken identifies this run's pending entry; every later write to
	// it compares-and-sets on it ([ir.PendingAddedTablesStore]).
	pendingToken string
	// copyTimeID is the table's engine id when its copy started.
	copyTimeID string
}

// Run executes the add-table flow. Returns nil on success or a
// wrapped error pointing at the phase that failed.
//
// Per the loud-failure tenet, every refuse-path surfaces a clear
// operator-facing message: missing stream row, active stream, table
// already exists with rows, table missing on source, publication
// extension failure. None silently degrade.
func (a *AddTable) Run(ctx context.Context) error {
	if err := a.validate(); err != nil {
		return err
	}
	if a.Abandon {
		return a.runAbandon(ctx)
	}

	// Item 135 / audit B-2: add-table is the SECOND entry point that
	// hands its own Mappings to a CDC lane — it creates the target column
	// from the mapped type and then gives the table to the LIVE stream
	// (ADR-0030), so `--type-override t.c=binary_uuid` here reproduces
	// exactly the configuration `sync` refuses: this command's own copy
	// stores the source's bytes, and the running applier — whose column
	// descriptors come from the target — stores the hex reading of the
	// same value. Refused here, before any connection, for the same
	// reasons and with the same message. The roster that keeps this list
	// honest is TestMappingsCDCLaneRoster.
	if err := preflightBinaryTypeOverrideOnCDC(a.Mappings); err != nil {
		return err
	}

	slog.InfoContext(
		ctx, "add-table starting",
		slog.String("source", a.Source.Name()),
		slog.String("source_host", redactedHost(a.SourceDSN)),
		slog.String("target", a.Target.Name()),
		slog.String("target_host", redactedHost(a.TargetDSN)),
		slog.String("stream_id", a.StreamID),
		slog.String("table", a.TableName),
		slog.Bool("dry_run", a.DryRun),
	)

	// ---- 1. Verify the stream exists and is not actively running.
	// --no-drain (LiveMode) refuses here on every source.
	applier, err := a.Target.OpenChangeApplier(ctx, a.TargetDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: add-table: open target applier: %w", err))
	}
	defer migcore.CloseIf(applier)

	// A name the target's live-added set cannot hold refuses here, before
	// anything changes (Bug 321 R9 L-1): the set is written after the copy on
	// a source without a publication.
	if checker, ok := applier.(liveAddedTableNameChecker); ok {
		if err := checker.CheckLiveAddedTableName(a.TableName); err != nil {
			return fmt.Errorf("pipeline: add-table: %w", err)
		}
	}

	preflight, err := a.preflightStream(ctx, applier)
	if err != nil {
		return err
	}

	// ---- 2. Read the source schema; isolate the new table.
	sr, err := a.Source.OpenSchemaReader(ctx, a.SourceDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: add-table: open source schema reader: %w", err))
	}
	fullSchema, err := sr.ReadSchema(ctx)
	if err != nil {
		migcore.CloseIf(sr)
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: add-table: read source schema: %w", err))
	}
	// Source-side replica-identity preflight (roadmap item 93). This is
	// the OTHER door into the publication: extendPublicationScope below
	// runs `ALTER PUBLICATION … ADD TABLE`, after which Postgres refuses
	// the source application's own UPDATE/DELETE on a table with no
	// usable replica identity. Same refusal, same remedies, run against
	// the still-open reader and ahead of the ALTER so a refusal leaves
	// the publication untouched.
	if err := preflightSourceReplicaIdentity(ctx, sr, a.Source.Capabilities(), []string{a.TableName}); err != nil {
		migcore.CloseIf(sr)
		return err
	}
	// Isolate the table BEFORE closing the reader: the three source-side
	// shape preflights below need both the scoped schema and the still-open
	// handle, and the handle is what makes them able to ask the catalog.
	scoped, err := isolateTable(fullSchema, a.TableName)
	if err != nil {
		migcore.CloseIf(sr)
		return err
	}

	if err := a.preflightAddTableShape(ctx, sr, scoped); err != nil {
		migcore.CloseIf(sr)
		return err
	}
	if a.sourceKeyFacts, err = readUniqueKeyFacts(ctx, sr); err != nil {
		migcore.CloseIf(sr)
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: add-table: read source unique-key facts: %w", err))
	}
	migcore.CloseIf(sr)

	// UNLOGGED-table registration door (audit 2026-08-27 A7; see
	// preflightUnloggedAddTable). Must run before the dry-run report and
	// before any target DDL / publication change: an unlogged table can
	// never be streamed, so registering it on a live stream is the
	// backfill-then-freeze shape the G2 census refuses at stream open.
	if err := a.preflightUnloggedAddTable(ctx); err != nil {
		return err
	}

	// Apply per-column type / expression overrides (CLI parity with
	// migrate / sync-start), then the redaction preflight on the result.
	// The unique-key door judges collation direction on the table as READ:
	// an override toward a looser collation is the very weakening it looks
	// for (round-5 review LOW-6).
	a.sourceAsRead = cloneTableColumns(scoped.Tables[0])
	scoped, err = a.translateScopedSchema(scoped)
	if err != nil {
		return err
	}

	if a.DryRun {
		return a.logDryRun(ctx, scoped, preflight.resolvedTargetSchema)
	}

	// ---- 3. Open target writers; refuse if the new table already
	// has rows (TableEmptyChecker — same shape as cold-start
	// preflight). A re-run on a target whose CREATE TABLE landed but
	// whose bulk-copy didn't proceed is allowed: the table exists
	// but is empty.
	sw, err := a.Target.OpenSchemaWriter(ctx, a.TargetDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: add-table: open target schema writer: %w", err))
	}
	defer migcore.CloseIf(sw)

	rw, err := a.Target.OpenRowWriter(ctx, a.TargetDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: add-table: open target row writer: %w", err))
	}
	defer migcore.CloseIf(rw)

	// Bug 46: thread the resolved target-schema namespace into the
	// schema writer + row writer + change applier so the new table
	// lands in the same namespace the active stream is routing CDC
	// events to. Resolution (operator-supplied vs recorded vs
	// mismatch) ran in preflightStream above; an empty value here
	// preserves the DSN-default schema (pre-Bug-46 behaviour for
	// streams that didn't pass --target-schema).
	if preflight.resolvedTargetSchema != "" {
		migcore.ApplyTargetSchema(sw, preflight.resolvedTargetSchema)
		migcore.ApplyTargetSchema(rw, preflight.resolvedTargetSchema)
		migcore.ApplyTargetSchema(applier, preflight.resolvedTargetSchema)
		slog.InfoContext(
			ctx, "add-table: resolved target schema",
			slog.String("target_schema", preflight.resolvedTargetSchema),
			slog.String("source", schemaSourceLabel(a.TargetSchema, preflight.resolvedTargetSchema)),
		)
	}

	if err := preflightAddTable(ctx, a.Source, scoped, rw); err != nil {
		return err
	}

	// What the table's target counterpart must be before the live stream is
	// handed it (the binary-column halves, the unique-key door).
	if err := a.preflightTargetForStream(ctx, scoped, preflight.resolvedTargetSchema); err != nil {
		return err
	}

	// ---- 3a. Create the target table BEFORE publication-add
	// (ADR-0036 Phase B fix). The Phase A diagnose work pinned the
	// v0.24.0 residual loss surface to the PG applier's
	// `errUnknownTable` silent-drop path: events on the new table
	// reach the applier after publication-add, but if the target
	// table has not yet been created (the original order ran
	// publication-add → snapshot → runBulkCopy, with CREATE TABLE
	// inside runBulkCopy), the applier silently skips them with a
	// "skipping CDC event for unknown target table" warning. The
	// missing rows in Phase A.3's verdict captures (1-3 per run,
	// always from the loader tail of the burst) are exactly the
	// events received in this race window.
	//
	// Reordering CREATE TABLE to before publication-add eliminates
	// the race: by the time pgoutput begins decoding events on the
	// new table, the target table is already in place and the
	// applier's dispatch path succeeds. The CREATE TABLE statement
	// itself is idempotent (CREATE TABLE IF NOT EXISTS — both
	// engines); the later runBulkCopy phase no-ops on the
	// already-existing table. See ADR-0036 § Phase B for the full
	// drop-site analysis.
	//
	// MySQL: this step is engine-symmetric; the schema writer's
	// CreateTablesWithoutConstraints uses CREATE TABLE IF NOT EXISTS
	// on both engines (internal/engines/mysql/schema_writer.go,
	// internal/engines/postgres/schema_writer.go). MySQL's
	// filter-flip mechanism (ADR-0034) gates dispatch on
	// `live_added_tables` membership, so the same race didn't
	// manifest there in v0.24.0; the early-create is still the
	// correct shape — it removes engine-specific timing assumptions
	// from the orchestrator.
	// Index-emit pre-flight refusal (roadmap item 118). runBulkCopyForAddTable
	// below copies the table and THEN emits its indexes, so the new table's
	// unrepresentable index was discovered after its rows had landed — and
	// mid-sync, with the publication already extended. Same one-pass check the
	// migrate and cold-start paths run, on the single-table scoped schema.
	if err := migcore.PreflightTableEmit(ctx, a.Target, scoped, "add-table"); err != nil {
		return err
	}
	if err := migcore.PreflightIndexEmit(ctx, a.Target, scoped, "add-table"); err != nil {
		return err
	}
	if err := migcore.PreflightViewEmit(ctx, a.Target, scoped, "add-table"); err != nil {
		return err
	}
	// The COLUMN-TYPE member, and on this path it is the one with teeth: a
	// table joining a running sync brings columns the original cold start never
	// saw, so an unrenderable type here is genuinely NEW information rather
	// than something the cold start would already have refused.
	if err := migcore.PreflightColumnTypeEmit(ctx, a.Target, scoped, "add-table"); err != nil {
		return err
	}
	// Item 149. On this path the scoped schema carries ONE table, so the
	// implementations return before probing the server — the call is here so
	// the entry-point roster stays the answer to "which paths ask all four",
	// not because add-table can collide with itself.
	if err := migcore.PreflightTableNameFold(ctx, a.Target, a.TargetDSN, scoped, "add-table"); err != nil {
		return err
	}

	// INCLUDE-TABLE-SCOPE-DRIFT (Bug 321): every check that can refuse
	// without changing anything has run. From the CREATE below on, a
	// failure leaves the table half brought in — on the target, in the
	// publication — so it is recorded as pending first and cleared only once
	// the copy and the hand-off have succeeded. `sync start` refuses a warm
	// resume whose scope includes a pending table.
	if err := a.recordPendingAdd(ctx, applier); err != nil {
		return err
	}
	if err := a.bringInTableUnderHeartbeat(ctx, applier, preflight, scoped, sw, rw); err != nil {
		if errors.Is(err, ir.ErrPendingAddSuperseded) {
			return a.supersededError(err)
		}
		return a.undoFailedAdd(ctx, applier, err)
	}
	if err := a.recordCompletedAdd(ctx, applier); err != nil {
		return err
	}

	a.logAddComplete(ctx)
	return nil
}

// bringInTable is the part of the add that changes state: it creates the
// target table, extends the publication, copies the rows from a fresh
// snapshot and hands the table to the stream. Carved out of Run so the
// pending record brackets exactly this span ([AddTable.recordPendingAdd]).
func (a *AddTable) bringInTable(ctx context.Context, applier ir.ChangeApplier, preflight addTablePreflight, scoped *ir.Schema, sw ir.SchemaWriter, rw ir.RowWriter) error {
	a.publicationAddedByThisRun = false
	if err := sw.CreateTablesWithoutConstraints(ctx, scoped); err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf("pipeline: add-table: create target table: %w", err))
	}

	// ---- 4. Extend the publication scope (Postgres) BEFORE the
	// snapshot's slot is created so the slot's pinned catalog snapshot
	// already includes the new table (Bug 13 / ADR-0021 ordering), and
	// capture the ADR-0036 pg_current_wal_lsn() window around the ALTER.
	if err := a.notePublicationAdd(ctx); err != nil {
		return err
	}
	// Bug 321 R6 A: on a publication source the table joins the stream's
	// scope before the publication starts delivering it.
	if _, publicationSource := a.Source.(publicationAdder); publicationSource {
		if err := a.recordAddedTable(ctx, applier); err != nil {
			return err
		}
	}
	lsnBeforePubAdd, lsnAfterPubAdd, err := a.extendPublicationScope(ctx)
	if err != nil {
		return err
	}

	// ---- 5. Open a snapshot stream restricted to the new table.
	// PG: temp slot to avoid colliding with the main `sluice_slot`
	// that the active stream owns. MySQL: no slot; the snapshot is
	// just a REPEATABLE READ tx with CONSISTENT SNAPSHOT.
	tempSlot, stream, err := a.openSnapshotForAdd(ctx)
	if err != nil {
		return err
	}
	// The table's id as its copy starts, verified when the add is recorded
	// complete (coldstart_copy_identity.go).
	a.copyTimeID = a.tableIdentity(ctx)
	defer func() {
		_ = stream.Close()
		if tempSlot != "" {
			a.dropTempSlot(ctx, tempSlot)
		}
	}()

	slog.InfoContext(
		ctx, "add-table: snapshot captured",
		slog.String("table", a.TableName),
		slog.String("position_token", stream.Position.Token),
	)

	// ---- 5b. ADR-0036 Path D Phase A M2 diagnostics: the snapshot's
	// consistent-point LSN against the publication-add LSN window.
	a.diagSnapshotConsistentPoint(ctx, stream.Position, lsnBeforePubAdd, lsnAfterPubAdd, tempSlot)

	// ---- 6. Bulk-copy the single table. runBulkCopyForAddTable
	// re-runs CreateTablesWithoutConstraints (idempotent no-op given
	// the early-create in step 3a above), copies rows via the
	// idempotent INSERT path when the row writer exposes one,
	// syncs identity sequences, emits indexes and FKs scoped to
	// scoped.Tables. FKs to existing target tables are added; FKs to
	// tables not present on the target surface a clear engine-side
	// error.
	//
	// ADR-0036 Phase B: the idempotent INSERT path is what makes
	// the early-create step (3a) safe under load. With the table
	// in place before publication-add, the active stream's applier
	// may already have applied a small number of CDC INSERTs by
	// the time bulk-copy reaches those rows. A plain COPY would
	// fail with a duplicate-key error on those rows and abort the
	// entire COPY. WriteRowsIdempotent generates INSERT ... ON
	// CONFLICT (pk) DO UPDATE so the bulk-copy absorbs the
	// [publication-add, snapshot-open] overlap window cleanly.
	// Engines without [ir.IdempotentRowWriter] (none today; PG and
	// MySQL both implement it) fall back to plain WriteRows with a
	// debug log noting the fallback.
	if err := runBulkCopyForAddTable(ctx, scoped, stream.Rows, sw, rw, a.Redactor, a.StreamID); err != nil {
		return err
	}

	// Release the snapshot tx promptly — same rationale as the
	// cold-start path (Bug 21). The temp slot itself stays alive
	// just long enough to be dropped in our defer; the slot's
	// position is independent of the exporting tx.
	if err := stream.ReleaseRows(); err != nil {
		slog.WarnContext(
			ctx, "add-table: release snapshot rows failed; the snapshot tx may stay open until process exit",
			slog.String("error", err.Error()),
		)
	}

	// ---- 7. MySQL-side filter-flip (ADR-0034): record the new table on
	// cdc-state.live_added_tables so the running streamer's poll merges
	// it into the dispatch filter. No-op on the drained and PG paths.
	return a.recordLiveAddedTable(ctx, applier, preflight)
}

// recordPendingAdd records the table as pending on the stream's control row
// ([ir.PendingAddedTablesStore]) before the add changes anything. A failure
// to record refuses the add: an add whose failure could not be remembered
// must not start. An applier without the surface (test stubs only — both
// shipping appliers implement it) skips it.
func (a *AddTable) recordPendingAdd(ctx context.Context, applier ir.ChangeApplier) error {
	store, ok := applier.(ir.PendingAddedTablesStore)
	if !ok {
		slog.DebugContext(ctx, "add-table: target applier keeps no pending-add record; skipping it", slog.String("target", a.Target.Name()))
		return nil
	}
	a.pendingToken = newPendingToken()
	if err := store.RecordPendingAddedTable(ctx, a.StreamID, a.TableName, a.pendingToken); err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf(
			"pipeline: add-table: record %q as pending on the stream's control row before changing anything "+
				"(nothing was changed): %w", a.TableName, err,
		))
	}
	return nil
}

// notePublicationAdd records, before the publication add, whether the table
// is outside the publication now — that is, whether this run's add is what
// puts it there, and so what [AddTable.undoFailedAdd] may take back. A table
// already a member (a re-run, another stream's table) is never removed.
func (a *AddTable) notePublicationAdd(ctx context.Context) error {
	a.publicationAddedByThisRun = false
	checker, ok := a.Source.(publicationMembershipChecker)
	if !ok {
		return nil
	}
	m, err := checker.UnpublishedTables(ctx, a.SourceDSN, []string{a.TableName})
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: add-table: read publication membership: %w", err))
	}
	a.publicationAddedByThisRun = m.Publication != "" && len(m.Missing) == 1
	return nil
}

// translateScopedSchema applies the operator's per-column type /
// expression overrides to the single-table schema and then runs the
// redaction preflight on the result — the same order as migrate and
// sync cold-start, so `--type-override=col=text` short-circuits the
// mask:uuid refusal here too. Carved out of Run for the funlen ceiling
// when the preflight joined it (audit 2026-08-27 NEW-1).
func (a *AddTable) translateScopedSchema(scoped *ir.Schema) (*ir.Schema, error) {
	scoped, err := translate.ApplyMappings(scoped, a.Mappings)
	if err != nil {
		return nil, fmt.Errorf("pipeline: add-table: apply mappings: %w", err)
	}
	scoped, err = translate.ApplyExpressionOverrides(scoped, a.ExpressionMappings)
	if err != nil {
		return nil, fmt.Errorf("pipeline: add-table: apply expression overrides: %w", err)
	}
	if err := a.preflightRedact(scoped); err != nil {
		return nil, err
	}
	return scoped, nil
}

// extendPublicationScope extends the source publication to include the
// new table (Postgres) before the snapshot slot is created, so the
// slot's pinned catalog snapshot already sees it in scope (Bug 13 /
// ADR-0021 ordering). Engines without publications are a no-op. Returns
// the pg_current_wal_lsn() readings taken immediately before and after
// the ALTER (ADR-0036 Path D Phase A M2/M3 instrumentation) so the
// snapshot diagnostic can name the catalog-change LSN window: the
// "before" value bounds where pgoutput's per-LSN catalog snapshot must
// still exclude the new table; the "after" value upper-bounds the ALTER
// PUBLICATION commit-LSN.
func (a *AddTable) extendPublicationScope(ctx context.Context) (lsnBefore, lsnAfter string, err error) {
	lsnBefore = a.diagReadCurrentWAL(ctx, "before-publication-add")
	if pa, ok := a.Source.(publicationAdder); ok {
		slog.InfoContext(
			ctx, "add-table: extending source publication scope",
			slog.String("table", a.TableName),
		)
		if err := pa.AddPublicationTables(ctx, a.SourceDSN, []string{a.TableName}); err != nil {
			return "", "", migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: add-table: extend publication: %w", err))
		}
	} else {
		slog.DebugContext(
			ctx, "add-table: source engine has no publication concept; skipping extend step",
			slog.String("source", a.Source.Name()),
		)
	}
	lsnAfter = a.diagReadCurrentWAL(ctx, "after-publication-add")
	slog.DebugContext(
		ctx, "addtable.diag: publication-add LSN window",
		slog.String("phase", "pub-add-window"),
		slog.String("table", a.TableName),
		slog.String("lsn_before_pub_add", lsnBefore),
		slog.String("lsn_after_pub_add", lsnAfter),
	)
	return lsnBefore, lsnAfter, nil
}

// diagSnapshotConsistentPoint logs the snapshot's consistent-point LSN
// next to the publication-add LSN window (ADR-0036 Path D Phase A M2) so
// the diagnostic test can compare LSN_S against the pub-add window. Pure
// instrumentation; best-effort (a missing extractor or decode error is
// silently skipped).
func (a *AddTable) diagSnapshotConsistentPoint(ctx context.Context, pos ir.Position, lsnBefore, lsnAfter, tempSlot string) {
	extractor, ok := a.Source.(snapshotLSNExtractor)
	if !ok {
		return
	}
	if snapLSN, present, err := extractor.ExtractSnapshotLSN(pos); err == nil && present {
		slog.DebugContext(
			ctx, "addtable.diag: snapshot consistent-point",
			slog.String("phase", "snapshot-open"),
			slog.String("table", a.TableName),
			slog.String("lsn_snapshot", snapLSN),
			slog.String("lsn_before_pub_add", lsnBefore),
			slog.String("lsn_after_pub_add", lsnAfter),
			slog.String("temp_slot", tempSlot),
		)
	}
}

// recordLiveAddedTable records the new table in the stream's live-added set
// (sluice_cdc_state.live_added_tables), which makes it part of the stream's
// scope on every later start whatever --include-table says, and on the
// running streamer's next poll tick (ADR-0034's filter-flip). Bug 321 R6 A
// made this every source and path: it used to run only on MySQL's --no-drain
// path, so a restart of a Postgres stream (or after any drained add) with the
// stream's original --include-table — the documented staged-wave flow —
// dropped every change to the added table at the dispatch filter, silently.
//
// On a publication source it runs before the publication add (the
// [AddTable.bringInTable] call site): from that point the publication
// delivers the table, and a running stream must apply what it delivers, as
// an unfiltered stream always did. Elsewhere it runs here, after the copy,
// where ADR-0034's MySQL --no-drain path always ran it. With --no-drain
// withdrawn (Bug 332) the stream is meant to be stopped for the whole add,
// so the order no longer decides what a running stream applies; it is kept
// so the drained path's records and undo are unchanged by the withdrawal.
func (a *AddTable) recordLiveAddedTable(ctx context.Context, applier ir.ChangeApplier, _ addTablePreflight) error {
	if _, publicationSource := a.Source.(publicationAdder); publicationSource {
		return nil // recorded before the publication add
	}
	return a.recordAddedTable(ctx, applier)
}

// recordAddedTable writes the live-added entry. A target applier without
// the surface (SQLite/D1) cannot carry it, and the WARN says what that means.
func (a *AddTable) recordAddedTable(ctx context.Context, applier ir.ChangeApplier) error {
	writer, ok := applier.(liveAddedTablesWriter)
	if !ok {
		slog.WarnContext(ctx, "add-table: the target keeps no live-added table set, so the stream applies this table only "+
			"while its --include-table names it; name it on every restart", slog.String("table", a.TableName))
		return nil
	}
	if err := writer.RecordLiveAddedTable(ctx, a.StreamID, a.TableName); err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf(
			"pipeline: add-table: record %q in the stream's live-added table set (sluice_cdc_state.live_added_tables): %w", a.TableName, err,
		))
	}
	slog.InfoContext(ctx, "add-table: recorded the table in the stream's live-added set: it is in the stream's scope on every "+
		"start from now on (only `schema add-table --abandon` takes it out); the stream must be stopped for the whole add on every "+
		"source: a stream still running drops the table's changes until its next poll tick, or on an unfiltered Postgres stream applies "+
		"them while the copy runs, which a later copied row undoes (Bug 332); "+
		"a stream whose --exclude-table names it refuses (LIVE-ADDED-TABLE-EXCLUDED) until one of the two changes",
		slog.String("table", a.TableName), slog.String("stream_id", a.StreamID))
	return nil
}

// logAddComplete emits the terminal success line: the operator restarts
// the stream (every add is drained; --no-drain refuses before Run gets
// here).
func (a *AddTable) logAddComplete(ctx context.Context) {
	slog.InfoContext(
		ctx, "add-table: complete; resume the stream by re-running `sluice sync start --source-driver=... --source=... "+
			"--target-driver=... --target=...` with the same --stream-id (there is no resume flag) "+
			"to pick up CDC for the new table",
		slog.String("table", a.TableName),
		slog.String("stream_id", a.StreamID),
	)
}

// preflightTargetForStream runs the checks the table's target counterpart
// owes before the live stream is handed it. Carved out of Run for the
// funlen ceiling when the unique-key door joined it.
func (a *AddTable) preflightTargetForStream(ctx context.Context, scoped *ir.Schema, targetSchema string) error {
	// The SCHEMA-keyed half of the same refusal (see
	// preflightBinaryTargetColumnsOnCDC): a pre-existing EMPTY target
	// table whose column is already binary passes the emptiness check
	// above and is then fed by the live stream, with no override on this
	// command line for the config check to see. The usual case is that
	// the table does not exist yet, and the catalog read finds nothing.
	addGate := &existingTablesGate{
		Source:       a.Source,
		Target:       a.Target,
		TargetDSN:    a.TargetDSN,
		TargetSchema: targetSchema,
		Mode:         "add-table",
	}
	if actual, ok := addGate.readTargetTablesForShapeGate(ctx); ok {
		if err := preflightBinaryTargetColumnsOnCDC(scoped, actual, "add-table"); err != nil {
			return err
		}
	}
	if err := a.refuseTargetOnlyUniqueKeys(ctx, scoped.Tables[0], targetSchema); err != nil {
		return err
	}
	// The byte-shaped array-leaf half of the same shape — a source element
	// type the applier's target-derived columns cannot carry. Keyed on the
	// target ENGINE, not the catalog, because the table being added
	// normally does not exist on the target yet.
	return preflightArrayBytesLeafOnCDC(scoped, engineNameOrEmpty(a.Target), "add-table")
}

// refuseTargetOnlyUniqueKeys runs the unique-key door for the added table on
// the door's own reads, not the shape gate's: the plain schema read carries
// no [ir.UniqueKeyFact]s, so a pre-created Postgres target's NULLS NOT
// DISTINCT, index-level COLLATE and partitioned keys were invisible to it,
// and a failed read was a WARN and a skip (round-3 review MEDIUM). Both
// sides now have their facts folded in — the source's into copies of the
// table this command will create and of the table as read (whose array key
// columns' element collation only the facts classify: round-7 review
// MEDIUM-3 judged collation direction on an unfolded copy, so a target
// `text[] COLLATE <nondeterministic>` key passed), so its keys stay as read —
// and a read that fails refuses the command: the next run reads again.
func (a *AddTable) refuseTargetOnlyUniqueKeys(ctx context.Context, source *ir.Table, targetSchema string) error {
	src := &ir.Schema{Tables: []*ir.Table{cloneTableKeys(cloneTableColumns(source))}}
	foldUniqueKeyFacts(a.sourceKeyFacts, src)
	asRead := a.sourceAsRead
	if asRead == nil {
		asRead = source
	}
	read := &ir.Schema{Tables: []*ir.Table{cloneTableKeys(cloneTableColumns(asRead))}}
	foldUniqueKeyFacts(a.sourceKeyFacts, read)
	target, err := a.readTargetKeys(ctx, source.Name, targetSchema)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: add-table: unique-key door: %w", err))
	}
	define, closeDefiner := openKeyDefiner(a.Target, a.TargetDSN, targetSchema)
	defer closeDefiner()
	err = refuseAddTableTargetOnlyUniqueKeysAsRead(ctx, addTableKeyDoor{
		source: src.Tables[0], asRead: read.Tables[0], target: target, sourceEngine: a.Source, targetEngine: a.Target,
		accept: a.AcceptTargetOnlyUniqueKeys, define: define, namespaces: []string{source.Schema, targetSchema},
	})
	var refusal *ir.SourceUniqueKeyAbsentError
	if err != nil && !errors.As(err, &refusal) {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: add-table: unique-key door: %w", err))
	}
	return err
}

// readTargetKeys reads the target's counterpart of table, with its
// unique-key facts folded in, the way the stream's door reads a target.
func (a *AddTable) readTargetKeys(ctx context.Context, table, targetSchema string) (map[string]*ir.Table, error) {
	tr, err := a.Target.OpenSchemaReader(ctx, a.TargetDSN)
	if err != nil {
		return nil, fmt.Errorf("open target schema reader: %w", err)
	}
	defer migcore.CloseIf(tr)
	if targetSchema != "" {
		migcore.ApplyTargetSchema(tr, targetSchema)
	}
	migcore.ApplyVerbatimExtensionPassthrough(tr, true)
	if scoper, ok := tr.(ir.TableScoper); ok {
		scoper.SetTableScope(func(name string) bool { return strings.EqualFold(name, table) })
	}
	schema, err := tr.ReadSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("read target schema: %w", err)
	}
	if schema == nil {
		return nil, nil
	}
	if err := applyUniqueKeyFacts(ctx, tr, schema); err != nil {
		return nil, fmt.Errorf("read target unique-key facts: %w", err)
	}
	out := map[string]*ir.Table{}
	for _, t := range schema.Tables {
		if t != nil && strings.EqualFold(t.Name, table) {
			out[t.Name] = t
		}
	}
	return out, nil
}

// addTableKeyDoor is one add-table run of the unique-key door's inputs.
type addTableKeyDoor struct {
	// source is the scoped table after the operator's overrides, which
	// change column types and generated expressions, never keys; asRead is
	// the table before --type-override, on which collation direction is
	// judged (nil: source).
	source, asRead *ir.Table
	target         map[string]*ir.Table
	sourceEngine   ir.Engine
	targetEngine   ir.Engine
	accept         []string
	// define reads a target key's definition for the acceptance fingerprint.
	define keyDefiner
	// namespaces are those an acceptance may name: the source's and the
	// target's (--target-schema), the one sync start's refusal prints.
	namespaces []string
}

// refuseAddTableTargetOnlyUniqueKeysAsRead is the add-table entry of the
// unique-key door (unique_key_door.go, UNIQUE-KEY-STOPPED-WINDOW-DOOR): a
// PRE-CREATED target table joins the stream with whatever unique keys it
// has, and one the source does not enforce is a merge waiting for its first
// duplicate, so each must be implied by a source key or accepted with the
// fingerprint of its definition. A table sluice creates copies the source's
// keys, which imply themselves. It judges through [judgeUncoveredKeysWith],
// the stream door's own judge, with the same namespace rule.
func refuseAddTableTargetOnlyUniqueKeysAsRead(ctx context.Context, d addTableKeyDoor) error {
	asRead := d.asRead
	if asRead == nil {
		asRead = d.source
	}
	tables := make([]*ir.Table, 0, len(d.target))
	for _, t := range d.target {
		if t != nil {
			tables = append(tables, t)
		}
	}
	target := matchTargetTable(tables, d.source.Name)
	if target == nil {
		return nil
	}
	accepted := parseAcceptedUniqueKeys(d.accept)
	weakened := keyColumnWeakening(d.sourceEngine, d.targetEngine, asRead, target)
	var uncovered []uncoveredKey
	for _, key := range uncoveredTargetKeys(d.source, target, engineNameOrEmpty(d.targetEngine), weakened) {
		u := uncoveredKey{display: target.Name, table: target, key: key, weakened: weakened}
		if entry, ok := accepted.match(target.Name, key.Name, d.namespaces...); ok {
			u.accepted = &accepted[entry]
		}
		uncovered = append(uncovered, u)
	}
	absent, err := judgeUncoveredKeysWith(ctx, d.define, uncovered)
	if err != nil || len(absent) == 0 {
		return err
	}
	return &ir.SourceUniqueKeyAbsentError{Keys: absent, When: ir.UniqueKeyDoorAddTable}
}

// validate enforces the required-fields contract.
func (a *AddTable) validate() error {
	if err := validateAcceptedUniqueKeys(a.AcceptTargetOnlyUniqueKeys); err != nil {
		return err
	}
	switch {
	case a.Source == nil:
		return errors.New("pipeline: add-table: Source engine is nil")
	case a.Target == nil:
		return errors.New("pipeline: add-table: Target engine is nil")
	case a.SourceDSN == "":
		return errors.New("pipeline: add-table: SourceDSN is empty")
	case a.TargetDSN == "":
		return errors.New("pipeline: add-table: TargetDSN is empty")
	case a.StreamID == "":
		return errors.New("pipeline: add-table: StreamID is empty (must match the active stream)")
	case strings.TrimSpace(a.TableName) == "":
		return errors.New("pipeline: add-table: TableName is empty")
	case a.Source.Capabilities().CDC == ir.CDCNone:
		return cdcUnsupportedError(a.Source,
			fmt.Errorf("pipeline: add-table: Source engine %q declares CDC=None; mid-stream add-table only applies to CDC sources", a.Source.Name()))
	}
	// --target-schema is PG-only (ADR-0031, validated by migcore.ValidateTargetSchema
	// against the target's SchemaScope capability). MySQL operators get a
	// clear refusal naming the DSN-choice workaround.
	return migcore.ValidateTargetSchema(a.Target, a.TargetSchema)
}

// addTablePreflight is the orchestrator-side state captured during
// the stream pre-flight.
//
// resolvedTargetSchema is the post-resolution value to thread into
// the schema writer / row writer / change applier — either inherited
// from the recorded value, supplied by the operator, or (when both
// sides agreed-empty) the empty string for "use the DSN default."
type addTablePreflight struct {
	resolvedTargetSchema string
}

// preflightStream verifies the active stream's row exists on the
// target and refuses if a `sync stop` is still in flight, surfacing the
// operator-friendly "drain first" message. `--no-drain` (LiveMode)
// refuses on every source, before anything is read or written
// ([noDrainRefusalForStream]; Bug 332 withdrew the last live path).
//
// Always resolves the target-schema namespace (Bug 46): inherits
// the recorded value when the operator omits the flag, refuses on
// mismatch, accepts agreement.
//
// Returns the captured pre-flight state for use later in Run.
func (a *AddTable) preflightStream(ctx context.Context, applier ir.ChangeApplier) (addTablePreflight, error) {
	streams, err := applier.ListStreams(ctx)
	if err != nil {
		return addTablePreflight{}, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: add-table: list streams: %w", err))
	}
	var (
		found  bool
		status ir.StreamStatus
	)
	for _, st := range streams {
		if st.StreamID == a.StreamID {
			found = true
			status = st
			break
		}
	}
	if !found {
		return addTablePreflight{}, fmt.Errorf("pipeline: add-table: no stream %q on target — verify --stream-id matches the active stream's id (run `sluice sync status` to list streams)", a.StreamID)
	}
	// --no-drain refuses here, on every source, before the source is read
	// or anything is written (Bug 332). The stream's row is read first only
	// because its recorded position picks the marker.
	if a.LiveMode {
		return addTablePreflight{}, noDrainRefusalForStream(a.Source, status)
	}

	// ADR-0176 prerequisite: point the source engine at the stream's
	// RECORDED publication so the publication-extend step lands on the
	// publication the stream actually reads through — not the shared
	// default. Without this, add-table against a stream on a
	// per-stream publication (explicit --publication-name since
	// ADR-0175, or the derived filtered-stream default) would extend
	// `sluice_pub`: loudly when it doesn't exist, SILENTLY WRONG when
	// another stream's `sluice_pub` does — the new table would
	// bulk-copy once and then never receive CDC. An empty recorded
	// name (legacy / MySQL-family) is a no-op: the engine keeps its
	// default, byte-identical to before. Mirrors how the slot name is
	// recovered from the same row (activeSlotName), which also serves
	// as the guard's own-slot exclusion here.
	if status.PublicationName != "" {
		if scoper, ok := a.Source.(ir.PublicationScoper); ok {
			a.Source = scoper.WithPublicationScope(status.PublicationName, activeSlotName(status))
			slog.InfoContext(
				ctx, "add-table: using the stream's recorded publication",
				slog.String("stream_id", a.StreamID),
				slog.String("publication", status.PublicationName),
			)
		}
	}

	// Bug 46: reconcile the operator-supplied --target-schema flag
	// with the active stream's recorded target_schema. Refuses on
	// mismatch; inherits the recorded value when the flag is empty.
	resolvedTS, err := resolveAddTableTargetSchema(a.StreamID, a.TargetSchema, status.TargetSchema)
	if err != nil {
		return addTablePreflight{}, err
	}

	if err := a.preflightDrained(ctx, applier); err != nil {
		return addTablePreflight{}, err
	}
	return addTablePreflight{resolvedTargetSchema: resolvedTS}, nil
}

// resolveAddTableTargetSchema reconciles the operator-supplied
// `--target-schema` flag with the active stream's recorded
// target_schema. Bug 46 fix: closes both the silent-event-drop
// failure mode (operator forgets the flag → table lands in `public`
// while CDC routes to <recorded>) and the ADR-0031 caveat about
// mid-flight namespace changes (operator passes a different
// namespace → loud refusal).
//
// Resolution table:
//
//	flag    recorded   result
//	-----   --------   ----------------------------------------
//	""      ""         "" (DSN default, pre-Bug-46 behavior)
//	""      "X"        "X" (inherit recorded)
//	"X"     ""         "X" (recorded is legacy/empty, accept)
//	"X"     "X"        "X" (agreement, proceed)
//	"X"     "Y"        refuse with mismatch error
func resolveAddTableTargetSchema(streamID, operatorFlag, recorded string) (string, error) {
	switch {
	case operatorFlag == "":
		// Inherit recorded value (or empty if no record).
		return recorded, nil
	case recorded == "":
		// Operator override; recorded was legacy / empty. Accept the
		// operator's value — the cdc-state row will be back-filled on
		// the next position-write after add-table runs.
		return operatorFlag, nil
	case operatorFlag == recorded:
		return operatorFlag, nil
	default:
		return "", fmt.Errorf(
			"pipeline: add-table: --target-schema=%q does not match the active stream's recorded target_schema=%q for stream %q "+
				"(set --target-schema to match the stream, or omit the flag to inherit the recorded namespace)",
			operatorFlag, recorded, streamID,
		)
	}
}

// preflightDrained applies the Phase 1 conservative refusal: the
// stream must not have an in-flight `sync stop`. The operator is
// expected to have run `sluice sync stop --wait` before invoking
// add-table, which leaves the row present and the stop flag
// cleared.
func (a *AddTable) preflightDrained(ctx context.Context, applier ir.ChangeApplier) error {
	reader, ok := applier.(stopFlagReader)
	if !ok {
		// Engine doesn't expose the stop-flag surface. We can't
		// detect an in-flight stop; the operator runs at their own
		// risk. Log so the absence is visible under --log-level=debug.
		slog.DebugContext(
			ctx, "add-table: applier does not expose ReadStopRequested; cannot pre-flight stream-stopped status",
			slog.String("engine", a.Target.Name()),
		)
		return nil
	}
	stopRequested, err := reader.ReadStopRequested(ctx, a.StreamID)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: add-table: read stop flag: %w", err))
	}
	if stopRequested {
		// No --no-drain alternative is offered: it is withdrawn on every
		// source (Bug 332).
		return fmt.Errorf("pipeline: add-table: stream %q has an in-flight stop request (stop_requested_at IS NOT NULL) — wait for `sluice sync stop --wait` to drain before running add-table", a.StreamID)
	}
	return nil
}

// noDrainRefusalForStream is the --no-drain refusal. `--no-drain` is
// withdrawn on EVERY source: no live add-table path is left. The marker
// names the source class and the live path it took, picked for the
// publication path by the source implementing [publicationAdder] and
// otherwise from the stream's CDC method ([streamSourceCDC]: the declared
// driver and the stream's recorded position). Each message names the
// drained add-table flow. Pinned across every registered engine by
// TestNoDrainRefusal_EngineMatrix.
func noDrainRefusalForStream(source ir.Engine, status ir.StreamStatus) error {
	if _, publicationSource := source.(publicationAdder); publicationSource {
		return fmt.Errorf(
			"pipeline: add-table: %s: --no-drain is withdrawn on a Postgres source (source=%q): the running stream does not apply the new table's changes in order after the add's copy. "+
				"A stream started with --include-table drops them until it admits the table, and an unfiltered stream applies them while the copy runs, "+
				"so a delete or update made during the add is lost or undone by the copy, and on a filtered stream inserts are lost too. %s",
			noDrainPostgresSourceMarker, source.Name(), drainedAddTableFlow,
		)
	}
	sourceCDC, cdcFrom := streamSourceCDC(source, status)
	return noDrainRefusal(source.Name(), sourceCDC, cdcFrom)
}

// noDrainRefusal is the --no-drain refusal for a source without a
// publication. Each source class that had a live path gets its own marker,
// because each lost data its own way:
//
//   - a Postgres source (NO-DRAIN-POSTGRES-SOURCE, [noDrainRefusalForStream])
//     took ADR-0030's publication add. Bug 332:
//     TestAddTableNoDrainWindow_PG_* measured, on pg->pg and pg->mysql,
//     every change made between the add's snapshot and the stream admitting
//     the table lost on a filtered stream (50 deletes kept, 20 updates
//     stale, 20 inserts missing), and on an unfiltered one the deletes and
//     updates that reached a row before the copy wrote it undone (40-50
//     kept, 10-15 stale), at exit 0 with no WARN. ADR-0036's "strict
//     zero-loss" was pinned with inserts only.
//   - a BINLOG source (MySQL, MariaDB; NO-DRAIN-BINLOG-SOURCE) took
//     ADR-0034's filter-flip. The copy reads a snapshot fixed when it
//     opens, the table joins the running stream's scope only after the
//     copy, and the stream drops the table's changes until its poll admits
//     it while its position moves past them. ADR-0034 called that window
//     the poll lag; it is the whole copy.
//     TestAddTableNoDrainWindow_Binlog_MySQLToMySQL_Live measured every
//     change made between the snapshot and admission lost (60 deletes kept,
//     30 updates stale, 30 inserts missing), with a 200ms poll as with 5s,
//     on MySQL and MariaDB, at exit 0 with no WARN.
//   - a VStream source (planetscale, vitess; NO-DRAIN-VSTREAM-SOURCE) took
//     the same path with a keyspace-wide copy that also drops its own
//     catch-up deletes (TestStreamer_VStreamAddTableKeyspaceWideCopy: 80
//     deletes and 10 updates stale, surviving restarts).
//
// Every other source (the trigger-CDC engines, and engines with no CDC,
// which validate refuses earlier) gets the generic refusal.
func noDrainRefusal(source string, sourceCDC ir.CDCMethod, cdcFrom string) error {
	switch sourceCDC {
	case ir.CDCBinlog:
		return fmt.Errorf(
			"pipeline: add-table: %s: --no-drain is not supported on a binlog source (source=%q; binlog per %s): the add copies the table from a snapshot fixed when it opens, and the running stream drops every change to the table until it admits the table after the copy, so every delete, update and insert made to it during the add would be lost. %s",
			noDrainBinlogSourceMarker, source, cdcFrom, drainedAddTableFlow,
		)
	case ir.CDCVStream:
		return fmt.Errorf(
			"pipeline: add-table: %s: --no-drain is not supported on a VStream source (source=%q; VStream per %s): its add-table copy is one keyspace-wide snapshot that drops the deletes made during it, and the running stream drops every change to the table until the copy finishes, so the table would be left with rows the source deleted or changed. %s",
			noDrainVStreamSourceMarker, source, cdcFrom, drainedAddTableFlow,
		)
	}
	return fmt.Errorf(
		"pipeline: add-table: --no-drain is withdrawn on every source; source=%q (cdc=%s) never had a live add-table path. %s",
		source, sourceCDC, drainedAddTableFlow,
	)
}

// drainedAddTableFlow is the remedy every --no-drain refusal names. The
// stop must hold for the whole add: add-table does not detect a stream
// that is still running (DRAINED-ADD-TABLE-RUNNING-STREAM-UNCHECKED), and a
// fleet supervisor's Reconcile restarts a stopped leg that is still in its
// config on the next reload.
const drainedAddTableFlow = "Use the drained add-table flow: `sluice sync stop --wait`, then `sluice schema add-table` without --no-drain, then start the stream again: " +
	"`sluice sync start` with the same --stream-id (it warm-resumes from the stream's position and replays what changed during the add), " +
	"or, for a `sluice sync run` fleet leg, the fleet (a leg restarts from its fleet config, not from `sync start --stream-id`). " +
	"Keep the stream stopped until add-table returns: no fleet reload, no supervisor or systemd restart in between. " +
	"add-table does not detect a stream that is still running, and an add without --no-drain against one loses the same changes"

// positionCDCClassifier reports which CDC method wrote a persisted stream
// position. The MySQL engine implements it for every flavor, because its
// flavors share one set of position codecs and differ in CDC method.
type positionCDCClassifier interface {
	PositionCDCMethod(pos ir.Position) (method ir.CDCMethod, known bool)
}

// streamSourceCDC is the CDC method of the stream add-table joins, and
// where that answer came from. The engine passed to add-table
// (--source-driver) is not proof: a stream started from planetscale or
// vitess can be handed `--source-driver mysql` (self-hosted Vitess's
// underlying mysqld, say), whose declared capability is binlog while the
// running streamer is VStream. So either one saying VStream decides: a
// declared VStream source refuses whatever the position says, and
// otherwise the stream's RECORDED position decides when the source can
// classify it. The declared capability is the fallback for a stream with no
// position yet, a source that cannot classify, or a token it does not
// recognise.
func streamSourceCDC(source ir.Engine, status ir.StreamStatus) (method ir.CDCMethod, from string) {
	declared := source.Capabilities().CDC
	if declared == ir.CDCVStream {
		return declared, "--source-driver"
	}
	if c, ok := source.(positionCDCClassifier); ok {
		if m, known := c.PositionCDCMethod(status.Position); known {
			return m, "the stream's recorded position"
		}
	}
	return declared, "--source-driver"
}

// noDrainVStreamSourceMarker is the grep-stable marker the --no-drain
// refusal for a VStream (PlanetScale/Vitess) source carries.
const noDrainVStreamSourceMarker = "NO-DRAIN-VSTREAM-SOURCE"

// noDrainBinlogSourceMarker is the grep-stable marker the --no-drain
// refusal for a MySQL or MariaDB binlog source carries.
const noDrainBinlogSourceMarker = "NO-DRAIN-BINLOG-SOURCE"

// noDrainPostgresSourceMarker is the grep-stable marker the --no-drain
// refusal for a Postgres (publication) source carries (Bug 332).
const noDrainPostgresSourceMarker = "NO-DRAIN-POSTGRES-SOURCE"

// schemaSourceLabel returns a diagnostic tag for the
// resolveAddTableTargetSchema branch the operator hit — useful when
// a successful add-table inherits the recorded namespace, so the
// log line surfaces "did the operator pass the flag?". `operator`
// when the operator-supplied flag was non-empty (and matched the
// recorded value, or the recorded value was legacy/empty);
// `inherited` when the recorded value supplied the resolved name.
func schemaSourceLabel(operatorFlag, resolved string) string {
	if operatorFlag != "" {
		return "operator"
	}
	if resolved != "" {
		return "inherited"
	}
	return "default"
}

// activeSlotName returns the slot name to query for the active
// stream's confirmed_flush_lsn. Recovered from the cdc-state row
// populated by the streamer's per-position SetSlotName plumbing
// (ADR-0030). Falls back to the engine default `sluice_slot` for
// empty values — covers (a) legacy rows that pre-date the slot_name
// column, and (b) streamers that ran with the default slot name.
func activeSlotName(status ir.StreamStatus) string {
	if status.SlotName != "" {
		return status.SlotName
	}
	return defaultActiveSlotName
}

// defaultActiveSlotName mirrors the engine-side default in
// internal/engines/postgres/cdc_reader.go's defaultSlot constant.
// Held in the pipeline package because the orchestrator stays
// engine-neutral; bound to the engine's value by
// TestStoppedSlotAdviceNamesTheRealDefault rather than kept in sync by
// hand.
const defaultActiveSlotName = "sluice_slot"

// diagReadCurrentWAL is the ADR-0036 (Path D Phase A) instrumentation
// helper that reads the source engine's current WAL position via the
// optional currentWALPositionReader surface. Best-effort: a missing
// surface or query failure produces an empty string rather than
// aborting the add. Logs at DEBUG with the supplied phase tag.
func (a *AddTable) diagReadCurrentWAL(ctx context.Context, phase string) string {
	reader, ok := a.Source.(currentWALPositionReader)
	if !ok {
		slog.DebugContext(
			ctx, "addtable.diag: source engine does not expose ReadCurrentWALPosition; skipping LSN sample",
			slog.String("phase", phase),
			slog.String("source", a.Source.Name()),
		)
		return ""
	}
	lsn, err := reader.ReadCurrentWALPosition(ctx, a.SourceDSN)
	if err != nil {
		slog.DebugContext(
			ctx, "addtable.diag: ReadCurrentWALPosition failed (best-effort, continuing)",
			slog.String("phase", phase),
			slog.String("err", err.Error()),
		)
		return ""
	}
	slog.DebugContext(
		ctx, "addtable.diag: WAL position sample",
		slog.String("phase", phase),
		slog.String("lsn", lsn),
	)
	return lsn
}

// isolateTable returns a new schema containing only the named table
// from src.Tables. Returns a clear error when the table is not
// present on the source — the most common operator failure mode is
// running add-table before the CREATE TABLE has actually landed on
// the source.
//
// Views, sequences, and unrelated tables are dropped so the
// downstream phases (translate, schema-write, bulk-copy) operate
// strictly on the new table. A new table whose default references a
// standalone sequence that does NOT already exist on the target
// therefore fails loudly at CREATE TABLE (undefined relation) rather
// than silently collapsing — carrying referenced sequences through
// the add-table path is a deliberate deferral (item-51 notes).
func isolateTable(src *ir.Schema, tableName string) (*ir.Schema, error) {
	if src == nil {
		return nil, errors.New("pipeline: add-table: source schema is nil")
	}
	for _, t := range src.Tables {
		if t == nil {
			continue
		}
		if t.Name == tableName {
			return &ir.Schema{Tables: []*ir.Table{t}}, nil
		}
	}
	available := make([]string, 0, len(src.Tables))
	for _, t := range src.Tables {
		if t != nil {
			available = append(available, t.Name)
		}
	}
	return nil, fmt.Errorf("pipeline: add-table: table %q not found on source — run `CREATE TABLE %s ...` on the source first, then re-run add-table (source has %d tables: %v)", tableName, tableName, len(available), available)
}

// preflightAddTable refuses the add when the target table already
// contains rows. Mirrors the cold-start preflight's shape but with
// a message tailored to the add-table flow (the recovery path is
// different: the operator either drops the stale dest table or
// uses a different name).
//
// Engines that don't implement TableEmptyChecker silently skip the
// check — same pattern as preflightColdStart. A table that doesn't
// yet exist on the target counts as empty (the IsTableEmpty
// contract).
func preflightAddTable(ctx context.Context, source ir.Engine, scoped *ir.Schema, rw ir.RowWriter) error {
	// Source-side CDC-scope guard (ADR-0170): the live stream already
	// passed its stream-start CDC preflight, but add-table extends that
	// stream's scope without re-opening it — so a newly-added table with a
	// column the source's CDC path cannot faithfully stream (MariaDB
	// native uuid/inet) must be refused HERE, or it would be mis-decoded
	// mid-stream (silently on a MySQL-family target). Optional surface;
	// engines that decode every column they read do not implement it.
	if pf, ok := source.(ir.CDCScopePreflighter); ok {
		if err := pf.PreflightCDCScope(ctx, scoped.Tables); err != nil {
			return err
		}
	}

	checker, ok := rw.(ir.TableEmptyChecker)
	if !ok {
		slog.DebugContext(ctx, "add-table: target row writer does not implement TableEmptyChecker; skipping pre-flight")
		return nil
	}
	for _, t := range scoped.Tables {
		empty, err := checker.IsTableEmpty(ctx, t)
		if err != nil {
			return fmt.Errorf("pipeline: add-table: pre-flight probe of target table %q: %w", t.Name, err)
		}
		if !empty {
			return migcore.WrapWithHint(migcore.PhaseSchemaApply, fmt.Errorf(
				"pipeline: add-table: target table %q already exists with rows — drop it on the target (or pick a different table name on the source) and re-run add-table; this guard prevents accidentally double-copying onto a previously-imported table",
				t.Name,
			))
		}
	}
	return nil
}

// openSnapshotForAdd opens a snapshot stream scoped to the new
// table. On Postgres the engine creates a fresh slot at snapshot-
// open time; we pass a temp slot name so the active stream's main
// `sluice_slot` is left untouched. On MySQL the snapshot is just a
// REPEATABLE READ tx; the slot name is ignored.
//
// Returns the temp-slot name (empty on engines without slots) so
// the caller can drop it after bulk-copy.
func (a *AddTable) openSnapshotForAdd(ctx context.Context) (string, *ir.SnapshotStream, error) {
	tempSlot := a.tempSlotName()
	if opener, ok := a.Source.(snapshotSlotOpener); ok && tempSlot != "" {
		stream, err := opener.OpenSnapshotStreamWithSlot(ctx, a.SourceDSN, tempSlot)
		if err != nil {
			return "", nil, migcore.WrapWithHint(migcore.PhaseSnapshot, fmt.Errorf("pipeline: add-table: open snapshot stream: %w", err))
		}
		return tempSlot, stream, nil
	}
	stream, err := a.Source.OpenSnapshotStream(ctx, a.SourceDSN)
	if err != nil {
		return "", nil, migcore.WrapWithHint(migcore.PhaseSnapshot, fmt.Errorf("pipeline: add-table: open snapshot stream: %w", err))
	}
	// Drained (the only mode a VStream source allows since
	// NO-DRAIN-VSTREAM-SOURCE): the stream was stopped before this add and
	// resumes from its pre-add persisted position, so it replays every change
	// the source makes during the copy, in order. A change Vitess replays
	// INSIDE this keyspace-wide copy (the catch-up before each later table)
	// that the copy cannot apply in order is therefore left to that stream
	// instead of failing the add (ir.CopyReplayDeferrer; measured by
	// TestStreamer_VStreamAddTableKeyspaceWideCopy's Drained cells). Never in
	// live mode, whose running stream is past the copy (unreachable since
	// --no-drain refuses on every source, Bug 332; the guard stays so a
	// future live path cannot inherit the deferral). Kept beside the open
	// so TestCopyReplayDeferralRoster can see it.
	if d, ok := stream.Rows.(ir.CopyReplayDeferrer); ok && !a.LiveMode {
		d.DeferCopyReplaysToStream()
	}
	return "", stream, nil
}

// tempSlotName returns the temporary slot name used for the add-
// table snapshot capture. Empty when the engine doesn't have a slot
// concept (caller's structural check skips the slot path).
//
// Default name combines the sluice prefix with the table name and
// an `addtable_` infix so operators inspecting `pg_replication_slots`
// during the brief window the slot exists can immediately see what
// it's for. Truncated to a reasonable length so PG's 63-char ident
// limit isn't exceeded on long table names.
func (a *AddTable) tempSlotName() string {
	if a.SlotName != "" {
		return resolveSlotName(a.SlotName)
	}
	const maxLen = 60
	tableHint := strings.ToLower(a.TableName)
	tableHint = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			return r
		case r == '_':
			return r
		}
		return '_'
	}, tableHint)
	name := "sluice_addtable_" + tableHint
	if len(name) > maxLen {
		name = name[:maxLen]
	}
	return name
}

// dropTempSlot best-effort drops the temp snapshot slot via the
// engine's SlotManager. Logs (but doesn't return) failure — the
// add-table itself succeeded; a leftover slot is an operational
// problem the operator can clean up via `sluice slot drop`. Logs
// loudly enough to surface in monitoring.
func (a *AddTable) dropTempSlot(ctx context.Context, slot string) {
	opener, ok := a.Source.(slotDropper)
	if !ok {
		return
	}
	mgr, err := opener.OpenSlotManager(ctx, a.SourceDSN)
	if err != nil {
		slog.WarnContext(
			ctx, "add-table: open slot manager to drop temp slot failed; drop the slot manually via `sluice slot drop`",
			slog.String("slot", slot),
			slog.String("error", err.Error()),
		)
		return
	}
	defer migcore.CloseIf(mgr)

	if err := mgr.Drop(ctx, slot, false); err != nil {
		slog.WarnContext(
			ctx, "add-table: drop temp snapshot slot failed; the slot may pin source WAL until manually dropped via `sluice slot drop`",
			slog.String("slot", slot),
			slog.String("error", err.Error()),
		)
		return
	}
	slog.InfoContext(
		ctx, "add-table: temp snapshot slot dropped",
		slog.String("slot", slot),
	)
}

// logDryRun describes what Run would do without doing it. Mirrors
// the shape of logDryRunPlan (streamer.go) and Migrator.logPlan.
func (a *AddTable) logDryRun(ctx context.Context, scoped *ir.Schema, resolvedTargetSchema string) error {
	if len(scoped.Tables) == 0 {
		return errors.New("pipeline: add-table: dry-run: scoped schema has no tables; this is a bug")
	}
	t := scoped.Tables[0]
	slog.InfoContext(
		ctx, "dry run: add-table",
		slog.String("source", a.Source.Name()),
		slog.String("target", a.Target.Name()),
		slog.String("stream_id", a.StreamID),
		slog.String("table", t.Name),
		slog.Int("columns", len(t.Columns)),
		slog.Bool("primary_key", t.PrimaryKey != nil),
		slog.Int("secondary_indexes", len(t.Indexes)),
		slog.Int("foreign_keys", len(t.ForeignKeys)),
		slog.String("target_schema", resolvedTargetSchema),
	)
	if _, ok := a.Source.(publicationAdder); ok {
		slog.InfoContext(
			ctx, "dry run: add-table: would extend source publication via ALTER PUBLICATION ... ADD TABLE",
			slog.String("table", t.Name),
		)
	}
	slog.InfoContext(ctx, "dry run: add-table: would capture snapshot, bulk-copy rows, drop temp slot, then nudge operator to re-run `sluice sync start --source-driver=... --source=... --target-driver=... --target=...` with the same --stream-id")
	return nil
}

// preflightAddTableShape runs the three SOURCE-SHAPE preflights on the one
// table being added, against the still-open reader.
//
// Extracted from Run only because Run outgrew the funlen limit. That is worth
// a sentence, because moving a guarded call OUT of the function a roster
// greps is exactly how this repo has lost coverage before -- the cold-start
// roster carries the same warning about a fix that landed in a helper outside
// its listed set and left the gate green while still exempt. So
// TestAddTableRosterReachesTheShapePreflights lists BOTH (*AddTable).Run and
// this helper as consumers; a future extraction owes the same edit.
func (a *AddTable) preflightAddTableShape(ctx context.Context, sr ir.SchemaReader, scoped *ir.Schema) error {
	// The three source-shape preflights every OTHER row-emitting entry point
	// runs (audit 2026-09-06 W4 H3). add-table ran none of them, and the RLS
	// one is the sharp end: preflightRLS's own documented remedy is to
	// `--exclude-table` the offending table, and `schema add-table` is how an
	// operator brings a table back afterwards. So the advice sluice gives on
	// one door routed straight through the door with no check — an RLS-enabled
	// table copied by a role without BYPASSRLS returns only the rows the
	// policy admits, which is a silent PARTIAL copy at exit 0.
	//
	// Scoped to the one table being added, not the whole schema: an unrelated
	// partitioned or RLS-enabled table elsewhere in the source is not this
	// command's business and refusing on it would be a false halt.
	if err := migcore.PreflightRLS(ctx, scoped, sr, migcore.RLSSideSource); err != nil {
		return err
	}
	if err := migcore.PreflightPartitionedTables(ctx, sr, a.Source.Capabilities(), scoped); err != nil {
		return err
	}
	return migcore.PreflightInheritanceTables(ctx, sr, a.Source.Capabilities(), scoped)
}
