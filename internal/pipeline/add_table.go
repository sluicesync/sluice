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
// (`sluice sync start --resume`). AddTable refuses if the stream is
// still active. Live add-table without the drain is Phase 2.
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
//  11. Operator restarts via `sluice sync start --resume` — the
//      stream picks up CDC for every table including the new one
//      from its persisted position.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
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

// slotPositionReader is the optional engine-side surface for reading
// the active stream's slot position before a live mid-stream add-
// table. Used by [AddTable.LiveMode] to capture the slot's
// confirmed_flush_lsn at the moment publication-add is about to
// run, so the orchestrator can verify the subsequent snapshot LSN
// has not regressed past it (which would silently drop events on
// the new table). PG implements via SELECT confirmed_flush_lsn FROM
// pg_replication_slots WHERE slot_name = ...; engines without slots
// leave it unimplemented and live mode refuses on those engines via
// the publicationAdder gate. See ADR-0030.
type slotPositionReader interface {
	ReadSlotPosition(ctx context.Context, dsn, slotName string) (string, error)
}

// currentWALPositionReader is an optional engine-side surface for
// reading the source's current WAL position against a supplied DSN.
// ADR-0036 (Path D Phase A) instrumentation: the diagnostic flow logs
// pg_current_wal_lsn() before AND after publication-add so the
// captured logs name a concrete LSN window for the catalog change.
// Independent from the SchemaReader.SourceCurrentPosition surface
// (which requires a held SchemaReader) because the live-add flow
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
// opaque [ir.Position] token. Used by [AddTable.LiveMode] to verify
// the snapshot-LSN ≥ slot-confirmed-flush-LSN invariant (ADR-0030).
// PG implements via JSON-decoding its position envelope; engines
// without LSN-shaped positions return ("", false, nil).
//
// The orchestrator stays engine-neutral by treating absence of this
// interface as "no LSN floor available, skip the check". Live mode
// refuses up-front on engines that don't implement publicationAdder,
// so in practice the only engine that opts into live mode also
// implements this surface.
type snapshotLSNExtractor interface {
	ExtractSnapshotLSN(pos ir.Position) (lsn string, ok bool, err error)
}

// lsnComparer is the optional engine-side surface for comparing two
// position-token LSN strings. Used in conjunction with
// [snapshotLSNExtractor] and [slotPositionReader] to enforce the
// snapshot-LSN ≥ slot-confirmed-flush-LSN invariant on live add-
// table without the orchestrator needing to parse engine-specific
// LSN encodings.
//
// Returns -1 if a < b, 0 if equal, +1 if a > b. PG implements via
// pglogrepl.ParseLSN comparison; engines without slots omit it.
type lsnComparer interface {
	CompareLSN(a, b string) (int, error)
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

	// LiveMode, when true, lifts the Phase 1 conservative refusal
	// of an active stream and runs add-table against a stream that
	// is currently consuming WAL. Phase 2 of the mid-stream add-
	// table feature; see ADR-0030 for the correctness story
	// (publication-add-then-snapshot ordering on PG; the idempotent
	// applier absorbs the [snapshot-LSN, slot-LSN] overlap on the
	// new table).
	//
	// Trade-off: LiveMode skips the `stop_requested_at` refusal in
	// favour of an explicit invariant check (snapshot-LSN ≥ slot
	// confirmed_flush_lsn captured at publication-add time). PG-only
	// in this phase — engines without publications refuse with a
	// clear error directing the operator at the drained add-table
	// flow. The CLI flag is `--no-drain` on `sluice schema add-table`.
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
	// In live mode the active-stream refusal is skipped in favour of
	// the snapshot-LSN ≥ slot-LSN invariant captured below.
	applier, err := a.Target.OpenChangeApplier(ctx, a.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: add-table: open target applier: %w", err))
	}
	defer closeIf(applier)

	preflight, err := a.preflightStream(ctx, applier)
	if err != nil {
		return err
	}

	// ---- 2. Read the source schema; isolate the new table.
	sr, err := a.Source.OpenSchemaReader(ctx, a.SourceDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: add-table: open source schema reader: %w", err))
	}
	fullSchema, err := sr.ReadSchema(ctx)
	closeIf(sr)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: add-table: read source schema: %w", err))
	}

	scoped, err := isolateTable(fullSchema, a.TableName)
	if err != nil {
		return err
	}

	// Apply per-column type / expression overrides (CLI parity with
	// migrate / sync-start).
	scoped, err = translate.ApplyMappings(scoped, a.Mappings)
	if err != nil {
		return fmt.Errorf("pipeline: add-table: apply mappings: %w", err)
	}
	scoped, err = translate.ApplyExpressionOverrides(scoped, a.ExpressionMappings)
	if err != nil {
		return fmt.Errorf("pipeline: add-table: apply expression overrides: %w", err)
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
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: add-table: open target schema writer: %w", err))
	}
	defer closeIf(sw)

	rw, err := a.Target.OpenRowWriter(ctx, a.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: add-table: open target row writer: %w", err))
	}
	defer closeIf(rw)

	// Bug 46: thread the resolved target-schema namespace into the
	// schema writer + row writer + change applier so the new table
	// lands in the same namespace the active stream is routing CDC
	// events to. Resolution (operator-supplied vs recorded vs
	// mismatch) ran in preflightStream above; an empty value here
	// preserves the DSN-default schema (pre-Bug-46 behaviour for
	// streams that didn't pass --target-schema).
	if preflight.resolvedTargetSchema != "" {
		applyTargetSchema(sw, preflight.resolvedTargetSchema)
		applyTargetSchema(rw, preflight.resolvedTargetSchema)
		applyTargetSchema(applier, preflight.resolvedTargetSchema)
		slog.InfoContext(
			ctx, "add-table: resolved target schema",
			slog.String("target_schema", preflight.resolvedTargetSchema),
			slog.String("source", schemaSourceLabel(a.TargetSchema, preflight.resolvedTargetSchema)),
		)
	}

	if err := preflightAddTable(ctx, scoped, rw); err != nil {
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
	if err := sw.CreateTablesWithoutConstraints(ctx, scoped); err != nil {
		return wrapWithHint(PhaseSchemaApply, fmt.Errorf("pipeline: add-table: create target table: %w", err))
	}

	// ---- 4. Extend the publication scope (Postgres) BEFORE the
	// snapshot's slot is created so the slot's pinned catalog
	// snapshot already includes the new table in publication scope.
	// Same ordering rationale as cold-start's EnsurePublication
	// (Bug 13, ADR-0021). Engines without publications are no-ops.
	//
	// ADR-0036 (Path D Phase A) M2/M3 instrumentation: capture
	// pg_current_wal_lsn() before AND after publication-add so the
	// diagnostic test can name a concrete LSN window for the catalog
	// change. The "before" value bounds where pgoutput's per-LSN
	// catalog snapshot must still exclude the new table; the "after"
	// value is an upper bound on LSN_pubadd (the actual commit-LSN
	// of the ALTER PUBLICATION DDL). Together with the BEGIN/COMMIT
	// LSN logging in cdc_reader's dispatchWAL, the captured trace
	// answers M1 (long txns straddling the boundary) and M3
	// (pgoutput catalog-snapshot lag).
	lsnBeforePubAdd := a.diagReadCurrentWAL(ctx, "before-publication-add")
	if pa, ok := a.Source.(publicationAdder); ok {
		slog.InfoContext(
			ctx, "add-table: extending source publication scope",
			slog.String("table", a.TableName),
		)
		if err := pa.AddPublicationTables(ctx, a.SourceDSN, []string{a.TableName}); err != nil {
			return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: add-table: extend publication: %w", err))
		}
	} else {
		slog.DebugContext(
			ctx, "add-table: source engine has no publication concept; skipping extend step",
			slog.String("source", a.Source.Name()),
		)
	}
	lsnAfterPubAdd := a.diagReadCurrentWAL(ctx, "after-publication-add")
	slog.DebugContext(
		ctx, "addtable.diag: publication-add LSN window",
		slog.String("phase", "pub-add-window"),
		slog.String("table", a.TableName),
		slog.String("lsn_before_pub_add", lsnBeforePubAdd),
		slog.String("lsn_after_pub_add", lsnAfterPubAdd),
	)

	// ---- 5. Open a snapshot stream restricted to the new table.
	// PG: temp slot to avoid colliding with the main `sluice_slot`
	// that the active stream owns. MySQL: no slot; the snapshot is
	// just a REPEATABLE READ tx with CONSISTENT SNAPSHOT.
	tempSlot, stream, err := a.openSnapshotForAdd(ctx)
	if err != nil {
		return err
	}
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

	// ADR-0036 (Path D Phase A) M2: surface the snapshot's
	// consistent-point LSN alongside the publication-add LSN window
	// so the diagnostic test can compare LSN_S against LSN_before_pubadd
	// and LSN_after_pubadd. The "snapshot consistent-point race"
	// hypothesis predicts loss when LSN_S < LSN_pubadd; the captured
	// trace answers it directly.
	if extractor, ok := a.Source.(snapshotLSNExtractor); ok {
		if snapLSN, present, err := extractor.ExtractSnapshotLSN(stream.Position); err == nil && present {
			slog.DebugContext(
				ctx, "addtable.diag: snapshot consistent-point",
				slog.String("phase", "snapshot-open"),
				slog.String("table", a.TableName),
				slog.String("lsn_snapshot", snapLSN),
				slog.String("lsn_before_pub_add", lsnBeforePubAdd),
				slog.String("lsn_after_pub_add", lsnAfterPubAdd),
				slog.String("temp_slot", tempSlot),
			)
		}
	}

	// ---- 5a. Live-mode invariant (PG path only): snapshot-LSN ≥ slot
	// confirmed_flush_lsn captured before publication-add. ADR-0030.
	// The standard ordering (publication-add → snapshot) cannot trip
	// this in practice, but the explicit check pins the invariant so
	// a future regression in the flow's ordering fails loudly rather
	// than producing silent drift on the new table.
	//
	// MySQL's filter-flip path (ADR-0034) has no equivalent invariant —
	// the binlog auto-includes every table by construction, so the
	// snapshot's binlog position is the only floor and no separate
	// "before-the-mechanism" position needs comparing against it.
	if a.LiveMode && preflight.mode == liveModePublicationAdd {
		if err := a.verifyLiveModeLSNInvariant(ctx, preflight.slotConfirmedFlushLSN, stream.Position); err != nil {
			return err
		}
	}

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

	// ---- 7. MySQL-side filter-flip: record the new table on the per-
	// target sluice_cdc_state.live_added_tables column so the running
	// streamer's poll goroutine merges it into the dispatch filter on
	// its next tick. ADR-0034.
	//
	// Runs only in live mode and only on the binlog-source path
	// (preflight.mode == liveModeBinlogFilterFlip). The PG path's
	// equivalent step is the `ALTER PUBLICATION ... ADD TABLE` issued
	// in step 4 BEFORE the snapshot — pgoutput's per-LSN catalog
	// snapshot semantics make pre-snapshot publication-add the right
	// ordering (see ADR-0030 for the correctness story). MySQL has no
	// publication concept; the streamer's filter is the gate, so the
	// flip lands AFTER bulk-copy succeeds.
	if a.LiveMode && preflight.mode == liveModeBinlogFilterFlip {
		writer, ok := applier.(liveAddedTablesWriter)
		if !ok {
			// Shouldn't happen — preflightLive's structural-interface
			// check already guarded this — but be loud if a regression
			// in the dispatch ladder slips through.
			return errors.New("pipeline: add-table: --no-drain (binlog-source path): target applier no longer implements RecordLiveAddedTable; this is a regression in the dispatch ladder")
		}
		if err := writer.RecordLiveAddedTable(ctx, a.StreamID, a.TableName); err != nil {
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf(
				"pipeline: add-table: --no-drain: record live-added table on cdc-state: %w", err,
			))
		}
		slog.InfoContext(
			ctx, "add-table: live mode: recorded table on cdc-state.live_added_tables; running streamer's poll will merge into dispatch filter on next tick (ADR-0034)",
			slog.String("table", a.TableName),
			slog.String("stream_id", a.StreamID),
		)
	}

	if a.LiveMode {
		slog.InfoContext(
			ctx, "add-table: live add complete; the active stream's tail will pick up new-table events on its next consumption",
			slog.String("table", a.TableName),
			slog.String("stream_id", a.StreamID),
		)
	} else {
		slog.InfoContext(
			ctx, "add-table: complete; resume the stream with `sluice sync start --resume` to pick up CDC for the new table",
			slog.String("table", a.TableName),
			slog.String("stream_id", a.StreamID),
		)
	}
	return nil
}

// validate enforces the required-fields contract.
func (a *AddTable) validate() error {
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
		return fmt.Errorf("pipeline: add-table: Source engine %q declares CDC=None; mid-stream add-table only applies to CDC sources", a.Source.Name())
	}
	// --target-schema is PG-only (ADR-0031, validated by validateTargetSchema
	// against the target's SchemaScope capability). MySQL operators get a
	// clear refusal naming the DSN-choice workaround.
	return validateTargetSchema(a.Target, a.TargetSchema)
}

// liveAddMode encodes which Phase 2 mechanism the orchestrator picked
// based on engine capabilities. Drives the post-bulk-copy step
// (publication-add → no-op for MySQL filter-flip; record column → no-op
// for PG). Zero value is the PG path (preserves pre-ADR-0034 behaviour).
type liveAddMode int

const (
	// liveModePublicationAdd is the PG mechanism (ADR-0030):
	// publication-add-then-snapshot, idempotent applier absorbs the
	// [snapshot-LSN, slot-LSN] overlap. Zero value.
	liveModePublicationAdd liveAddMode = iota

	// liveModeBinlogFilterFlip is the MySQL mechanism (ADR-0034): the
	// orchestrator records the new table on the per-target
	// sluice_cdc_state.live_added_tables column after bulk-copy
	// succeeds; the running streamer's poll goroutine merges into the
	// dispatch filter on its next tick.
	liveModeBinlogFilterFlip
)

// addTablePreflight is the orchestrator-side state captured during
// the stream pre-flight: the active stream exists, the recorded
// target-schema namespace, and (in live mode) the active stream's
// slot confirmed_flush_lsn at the moment publication-add is about
// to run. The captured LSN feeds the snapshot-LSN ≥ slot-LSN
// invariant check after the snapshot opens.
//
// Empty slotConfirmedFlushLSN means "no floor" — either the engine
// doesn't expose the surface (drained mode), or the slot exists but
// has not yet acked any consumer progress.
//
// resolvedTargetSchema is the post-resolution value to thread into
// the schema writer / row writer / change applier — either inherited
// from the recorded value, supplied by the operator, or (when both
// sides agreed-empty) the empty string for "use the DSN default."
//
// mode encodes which Phase 2 mechanism the orchestrator chose based
// on engine capabilities (PG publication-add vs MySQL filter-flip).
// Drained-mode runs leave the field at the zero value; only live-mode
// runs consult it for post-bulk-copy dispatch.
type addTablePreflight struct {
	slotConfirmedFlushLSN string
	resolvedTargetSchema  string
	mode                  liveAddMode
}

// preflightStream verifies the active stream's row exists on the
// target. In drained (default) mode it also refuses if no `sync
// stop` is in flight, surfacing the operator-friendly "drain first"
// message. In live mode (`LiveMode=true`, ADR-0030) the active-
// stream refusal is skipped in favour of three tighter checks:
//
//  1. the source engine must implement publicationAdder — engines
//     without publications (MySQL) get a clear PG-only error here;
//  2. the source engine must implement slotPositionReader — needed
//     to capture the slot's confirmed_flush_lsn for the invariant
//     check that fires after the snapshot opens;
//  3. the slot's confirmed_flush_lsn is captured (best-effort: an
//     empty string means "no floor", which the invariant check
//     accepts).
//
// Always resolves the target-schema namespace (Bug 46): inherits
// the recorded value when the operator omits the flag, refuses on
// mismatch, accepts agreement.
//
// Returns the captured pre-flight state for use later in Run.
func (a *AddTable) preflightStream(ctx context.Context, applier ir.ChangeApplier) (addTablePreflight, error) {
	streams, err := applier.ListStreams(ctx)
	if err != nil {
		return addTablePreflight{}, wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: add-table: list streams: %w", err))
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

	// Bug 46: reconcile the operator-supplied --target-schema flag
	// with the active stream's recorded target_schema. Refuses on
	// mismatch; inherits the recorded value when the flag is empty.
	resolvedTS, err := resolveAddTableTargetSchema(a.StreamID, a.TargetSchema, status.TargetSchema)
	if err != nil {
		return addTablePreflight{}, err
	}

	if a.LiveMode {
		live, err := a.preflightLive(ctx, applier, status)
		if err != nil {
			return addTablePreflight{}, err
		}
		live.resolvedTargetSchema = resolvedTS
		return live, nil
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
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: add-table: read stop flag: %w", err))
	}
	if stopRequested {
		return fmt.Errorf("pipeline: add-table: stream %q has an in-flight stop request (stop_requested_at IS NOT NULL) — wait for `sluice sync stop --wait` to drain before running add-table (or pass --no-drain to use Phase 2 live add)", a.StreamID)
	}
	return nil
}

// preflightLive applies the live-mode refusals and captures any
// engine-specific floor (PG slot confirmed_flush_lsn, MySQL nothing
// today) needed by later phases. Two paths, dispatched by source-
// engine capability:
//
//   - PG (`publicationAdder`): ADR-0030. Captures the active stream's
//     slot confirmed_flush_lsn so the snapshot-LSN ≥ slot-LSN
//     invariant can be checked after the snapshot opens. Refuses if
//     the source doesn't also implement `slotPositionReader` (a code-
//     side guarantee on PG; the absence indicates a build with the
//     engine surface stripped).
//
//   - MySQL (`liveAddedTablesWriter` on the target applier, since
//     MySQL's filter-flip mechanism is target-side): ADR-0034. Verifies
//     the target applier exposes the surface; no source-side floor to
//     capture (the binlog auto-includes everything by construction —
//     the snapshot's own binlog position is the only floor that
//     matters, and it's captured at open time).
//
// Engines that match neither (the rare cross-engine pair where source
// is something other than PG/MySQL) refuse with a clear error directing
// the operator at the drained add-table flow.
func (a *AddTable) preflightLive(ctx context.Context, applier ir.ChangeApplier, status ir.StreamStatus) (addTablePreflight, error) {
	if _, ok := a.Source.(publicationAdder); ok {
		return a.preflightLivePG(ctx, status)
	}
	if _, ok := applier.(liveAddedTablesWriter); ok {
		return a.preflightLiveBinlog(ctx, applier, status)
	}
	return addTablePreflight{}, fmt.Errorf(
		"pipeline: add-table: --no-drain requires either a publication-bearing source engine (PG) or a target applier exposing RecordLiveAddedTable (MySQL); source=%q target=%q expose neither. Use the drained add-table flow (`sluice sync stop --wait`, then `sluice schema add-table` without --no-drain) for unsupported engine pairs",
		a.Source.Name(), a.Target.Name(),
	)
}

// preflightLivePG is the original ADR-0030 preflight: capture the
// active stream's slot confirmed_flush_lsn so the snapshot-LSN ≥
// slot-LSN invariant has a floor to verify against. PG-only.
//
// The slot name is recovered from the StreamStatus row populated by
// the active stream's last position-write (PG ChangeApplier records
// it via SetSlotName on every Streamer startup; the sluice_cdc_state
// row's slot_name column carries it). Empty / legacy values fall
// back to the engine default `sluice_slot`.
func (a *AddTable) preflightLivePG(ctx context.Context, status ir.StreamStatus) (addTablePreflight, error) {
	reader, ok := a.Source.(slotPositionReader)
	if !ok {
		return addTablePreflight{}, fmt.Errorf(
			"pipeline: add-table: --no-drain requires the source engine to expose ReadSlotPosition; %q does not. This is a code-side guarantee on PG; the absence indicates a build with the engine surface stripped — file an issue",
			a.Source.Name(),
		)
	}

	slotName := activeSlotName(status)
	lsn, err := reader.ReadSlotPosition(ctx, a.SourceDSN, slotName)
	if err != nil {
		return addTablePreflight{}, wrapWithHint(PhaseConnect, fmt.Errorf(
			"pipeline: add-table: --no-drain: read slot position for %q: %w",
			slotName, err,
		))
	}
	slog.InfoContext(
		ctx, "add-table: live mode: captured active-stream slot position",
		slog.String("slot", slotName),
		slog.String("slot_source", slotNameSource(status)),
		slog.String("confirmed_flush_lsn", lsn),
	)
	return addTablePreflight{slotConfirmedFlushLSN: lsn, mode: liveModePublicationAdd}, nil
}

// preflightLiveBinlog is the ADR-0034 preflight for binlog-source
// engines (MySQL). The "filter-flip" mechanism is target-side: the
// orchestrator records the new table on
// `sluice_cdc_state.live_added_tables` after bulk-copy succeeds, and
// the running streamer's poll goroutine merges it into the dispatch
// filter on its next tick. There's no source-side floor to capture
// today — the binlog auto-includes every table by construction, so
// the only floor that matters is the snapshot's own binlog position,
// captured at OpenSnapshotStream time.
//
// The preflight surfaces the chosen path in the log so an operator
// inspecting the run can confirm which mechanism fired.
func (a *AddTable) preflightLiveBinlog(ctx context.Context, applier ir.ChangeApplier, _ ir.StreamStatus) (addTablePreflight, error) {
	// Defensive: the structural-interface dispatch in preflightLive
	// already type-asserted, so this should always succeed.
	if _, ok := applier.(liveAddedTablesWriter); !ok {
		return addTablePreflight{}, fmt.Errorf(
			"pipeline: add-table: --no-drain (binlog-source path): target applier does not implement RecordLiveAddedTable; %q",
			a.Target.Name(),
		)
	}
	slog.InfoContext(
		ctx, "add-table: live mode: binlog-source filter-flip path (ADR-0034)",
		slog.String("source", a.Source.Name()),
		slog.String("target", a.Target.Name()),
		slog.String("stream_id", a.StreamID),
	)
	return addTablePreflight{mode: liveModeBinlogFilterFlip}, nil
}

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

// slotNameSource is a diagnostic tag for the slot-name resolution
// path — useful when an operator's live-add succeeds against the
// fallback slot but the operator was expecting their custom slot to
// be picked up. Logs surface "recorded" when the cdc-state row
// carried a non-empty slot_name, "default-fallback" when it was
// empty (legacy row or default-named stream).
func slotNameSource(status ir.StreamStatus) string {
	if status.SlotName != "" {
		return "recorded"
	}
	return "default-fallback"
}

// defaultActiveSlotName mirrors the engine-side default in
// internal/engines/postgres/cdc_reader.go's defaultSlot constant.
// Held in the pipeline package because the orchestrator stays
// engine-neutral; the constant is small enough to keep in sync by
// hand.
const defaultActiveSlotName = "sluice_slot"

// verifyLiveModeLSNInvariant enforces the live-mode invariant
// snapshot-LSN ≥ slotConfirmedFlushLSN. ADR-0030's correctness
// story explains why publication-add → snapshot ordering keeps the
// invariant in practice; this check is the test-able shape of a
// regression in that ordering.
//
// Skipped (with a debug log) when:
//   - the active slot's confirmed_flush_lsn was empty at preflight
//     time (no floor to enforce); or
//   - the source engine doesn't expose snapshotLSNExtractor /
//     lsnComparer (fall-through; the engine surface guarantees the
//     check is meaningful on PG).
func (a *AddTable) verifyLiveModeLSNInvariant(ctx context.Context, slotLSN string, snapshotPos ir.Position) error {
	if slotLSN == "" {
		slog.DebugContext(ctx, "add-table: live mode: slot confirmed_flush_lsn empty; skipping snapshot-LSN ≥ slot-LSN invariant check")
		return nil
	}

	extractor, ok := a.Source.(snapshotLSNExtractor)
	if !ok {
		slog.DebugContext(
			ctx, "add-table: live mode: source engine does not expose ExtractSnapshotLSN; skipping invariant check",
			slog.String("source", a.Source.Name()),
		)
		return nil
	}
	snapshotLSN, ok, err := extractor.ExtractSnapshotLSN(snapshotPos)
	if err != nil {
		return fmt.Errorf("pipeline: add-table: --no-drain: extract snapshot LSN: %w", err)
	}
	if !ok || snapshotLSN == "" {
		slog.DebugContext(ctx, "add-table: live mode: snapshot position has no LSN; skipping invariant check")
		return nil
	}

	comparer, ok := a.Source.(lsnComparer)
	if !ok {
		slog.DebugContext(
			ctx, "add-table: live mode: source engine does not expose CompareLSN; skipping invariant check",
			slog.String("source", a.Source.Name()),
		)
		return nil
	}
	cmp, err := comparer.CompareLSN(snapshotLSN, slotLSN)
	if err != nil {
		return fmt.Errorf("pipeline: add-table: --no-drain: compare LSNs (snapshot=%q, slot=%q): %w", snapshotLSN, slotLSN, err)
	}
	if cmp < 0 {
		return fmt.Errorf(
			"pipeline: add-table: --no-drain: snapshot LSN %s is behind active stream's slot confirmed_flush_lsn %s; events on table %q in [snapshot, slot] would be silently dropped. This indicates a regression in the publication-add-then-snapshot ordering (ADR-0030); refusing to proceed",
			snapshotLSN, slotLSN, a.TableName,
		)
	}
	slog.InfoContext(
		ctx, "add-table: live mode: snapshot-LSN ≥ slot-LSN invariant satisfied",
		slog.String("snapshot_lsn", snapshotLSN),
		slog.String("slot_confirmed_flush_lsn", slotLSN),
	)
	return nil
}

// diagReadCurrentWAL is the ADR-0036 (Path D Phase A) instrumentation
// helper that reads the source engine's current WAL position via the
// optional currentWALPositionReader surface. Best-effort: a missing
// surface or query failure produces an empty string rather than
// aborting the live add. Logs at DEBUG with the supplied phase tag
// so the diagnostic test can correlate before/after readings.
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
// Views and unrelated tables are dropped so the downstream phases
// (translate, schema-write, bulk-copy) operate strictly on the new
// table.
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
func preflightAddTable(ctx context.Context, scoped *ir.Schema, rw ir.RowWriter) error {
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
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf(
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
			return "", nil, wrapWithHint(PhaseSnapshot, fmt.Errorf("pipeline: add-table: open snapshot stream: %w", err))
		}
		return tempSlot, stream, nil
	}
	stream, err := a.Source.OpenSnapshotStream(ctx, a.SourceDSN)
	if err != nil {
		return "", nil, wrapWithHint(PhaseSnapshot, fmt.Errorf("pipeline: add-table: open snapshot stream: %w", err))
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
	defer closeIf(mgr)

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
		slog.Bool("live_mode", a.LiveMode),
		slog.String("target_schema", resolvedTargetSchema),
	)
	if _, ok := a.Source.(publicationAdder); ok {
		slog.InfoContext(
			ctx, "dry run: add-table: would extend source publication via ALTER PUBLICATION ... ADD TABLE",
			slog.String("table", t.Name),
		)
	}
	if a.LiveMode {
		switch {
		case isPublicationAdder(a.Source):
			slog.InfoContext(ctx, "dry run: add-table: live mode (--no-drain) PG path (ADR-0030): would capture active stream's slot confirmed_flush_lsn, ALTER PUBLICATION ADD TABLE, capture snapshot, verify snapshot-LSN ≥ slot-LSN invariant, bulk-copy rows, drop temp slot")
		default:
			slog.InfoContext(ctx, "dry run: add-table: live mode (--no-drain) binlog-source path (ADR-0034): would capture snapshot, bulk-copy rows, then record table on cdc-state.live_added_tables for the running streamer's poll to merge into the dispatch filter")
		}
	} else {
		slog.InfoContext(ctx, "dry run: add-table: would capture snapshot, bulk-copy rows, drop temp slot, then nudge operator to `sluice sync start --resume`")
	}
	return nil
}

// isPublicationAdder reports whether src implements [publicationAdder]
// — the PG-path discriminator used by the dry-run log + dispatch
// helpers. Pulled out of an inline assertion so the call sites read
// declaratively.
func isPublicationAdder(src ir.Engine) bool {
	_, ok := src.(publicationAdder)
	return ok
}
