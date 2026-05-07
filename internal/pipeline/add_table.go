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
// Strategy A from `docs/dev/design-mid-stream-add-table.md`: the
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

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/translate"
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

	slog.InfoContext(ctx, "add-table starting",
		slog.String("source", a.Source.Name()),
		slog.String("source_host", redactedHost(a.SourceDSN)),
		slog.String("target", a.Target.Name()),
		slog.String("target_host", redactedHost(a.TargetDSN)),
		slog.String("stream_id", a.StreamID),
		slog.String("table", a.TableName),
		slog.Bool("dry_run", a.DryRun),
	)

	// ---- 1. Verify the stream exists and is not actively running.
	applier, err := a.Target.OpenChangeApplier(ctx, a.TargetDSN)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: add-table: open target applier: %w", err))
	}
	defer closeIf(applier)

	if err := a.preflightStream(ctx, applier); err != nil {
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
		return a.logDryRun(ctx, scoped)
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

	if err := preflightAddTable(ctx, scoped, rw); err != nil {
		return err
	}

	// ---- 4. Extend the publication scope (Postgres) BEFORE the
	// snapshot's slot is created so the slot's pinned catalog
	// snapshot already includes the new table in publication scope.
	// Same ordering rationale as cold-start's EnsurePublication
	// (Bug 13, ADR-0021). Engines without publications are no-ops.
	if pa, ok := a.Source.(publicationAdder); ok {
		slog.InfoContext(ctx, "add-table: extending source publication scope",
			slog.String("table", a.TableName),
		)
		if err := pa.AddPublicationTables(ctx, a.SourceDSN, []string{a.TableName}); err != nil {
			return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: add-table: extend publication: %w", err))
		}
	} else {
		slog.DebugContext(ctx, "add-table: source engine has no publication concept; skipping extend step",
			slog.String("source", a.Source.Name()),
		)
	}

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

	slog.InfoContext(ctx, "add-table: snapshot captured",
		slog.String("table", a.TableName),
		slog.String("position_token", stream.Position.Token),
	)

	// ---- 6. Bulk-copy the single table. runBulkCopy creates the
	// table (IF NOT EXISTS), copies rows, syncs identity sequences,
	// emits indexes and FKs scoped to scoped.Tables. FKs to existing
	// target tables are added; FKs to tables not present on the
	// target surface a clear engine-side error.
	if err := runBulkCopy(ctx, scoped, stream.Rows, sw, rw); err != nil {
		return err
	}

	// Release the snapshot tx promptly — same rationale as the
	// cold-start path (Bug 21). The temp slot itself stays alive
	// just long enough to be dropped in our defer; the slot's
	// position is independent of the exporting tx.
	if err := stream.ReleaseRows(); err != nil {
		slog.WarnContext(ctx, "add-table: release snapshot rows failed; the snapshot tx may stay open until process exit",
			slog.String("error", err.Error()),
		)
	}

	slog.InfoContext(ctx, "add-table: complete; resume the stream with `sluice sync start --resume` to pick up CDC for the new table",
		slog.String("table", a.TableName),
		slog.String("stream_id", a.StreamID),
	)
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
	return nil
}

// preflightStream verifies the active stream's row exists on the
// target and that no `sync stop` is currently in flight. Refuses
// loudly on either failure — the operator is expected to have run
// `sluice sync stop --wait` before invoking add-table, which leaves
// the row present and the stop flag cleared.
//
// We intentionally do NOT try to detect a running streamer beyond
// the in-flight stop signal: the live-detection problem is racy and
// belongs to the Phase 2 live add-table work. The CLI surface
// nudges operators toward `sync stop --wait` first; this check
// catches the obvious failure mode (operator forgot the drain).
func (a *AddTable) preflightStream(ctx context.Context, applier ir.ChangeApplier) error {
	streams, err := applier.ListStreams(ctx)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: add-table: list streams: %w", err))
	}
	var found bool
	for _, st := range streams {
		if st.StreamID == a.StreamID {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("pipeline: add-table: no stream %q on target — verify --stream-id matches the active stream's id (run `sluice sync status` to list streams)", a.StreamID)
	}

	reader, ok := applier.(stopFlagReader)
	if !ok {
		// Engine doesn't expose the stop-flag surface. We can't
		// detect an in-flight stop; the operator runs at their own
		// risk. Log so the absence is visible under --log-level=debug.
		slog.DebugContext(ctx, "add-table: applier does not expose ReadStopRequested; cannot pre-flight stream-stopped status",
			slog.String("engine", a.Target.Name()),
		)
		return nil
	}
	stopRequested, err := reader.ReadStopRequested(ctx, a.StreamID)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("pipeline: add-table: read stop flag: %w", err))
	}
	if stopRequested {
		return fmt.Errorf("pipeline: add-table: stream %q has an in-flight stop request (stop_requested_at IS NOT NULL) — wait for `sluice sync stop --wait` to drain before running add-table", a.StreamID)
	}
	return nil
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
				t.Name))
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
		slog.WarnContext(ctx, "add-table: open slot manager to drop temp slot failed; drop the slot manually via `sluice slot drop`",
			slog.String("slot", slot),
			slog.String("error", err.Error()),
		)
		return
	}
	defer closeIf(mgr)

	if err := mgr.Drop(ctx, slot, false); err != nil {
		slog.WarnContext(ctx, "add-table: drop temp snapshot slot failed; the slot may pin source WAL until manually dropped via `sluice slot drop`",
			slog.String("slot", slot),
			slog.String("error", err.Error()),
		)
		return
	}
	slog.InfoContext(ctx, "add-table: temp snapshot slot dropped",
		slog.String("slot", slot),
	)
}

// logDryRun describes what Run would do without doing it. Mirrors
// the shape of logDryRunPlan (streamer.go) and Migrator.logPlan.
func (a *AddTable) logDryRun(ctx context.Context, scoped *ir.Schema) error {
	if len(scoped.Tables) == 0 {
		return errors.New("pipeline: add-table: dry-run: scoped schema has no tables; this is a bug")
	}
	t := scoped.Tables[0]
	slog.InfoContext(ctx, "dry run: add-table",
		slog.String("source", a.Source.Name()),
		slog.String("target", a.Target.Name()),
		slog.String("stream_id", a.StreamID),
		slog.String("table", t.Name),
		slog.Int("columns", len(t.Columns)),
		slog.Bool("primary_key", t.PrimaryKey != nil),
		slog.Int("secondary_indexes", len(t.Indexes)),
		slog.Int("foreign_keys", len(t.ForeignKeys)),
	)
	if _, ok := a.Source.(publicationAdder); ok {
		slog.InfoContext(ctx, "dry run: add-table: would extend source publication via ALTER PUBLICATION ... ADD TABLE",
			slog.String("table", t.Name),
		)
	}
	slog.InfoContext(ctx, "dry run: add-table: would capture snapshot, bulk-copy rows, drop temp slot, then nudge operator to `sluice sync start --resume`")
	return nil
}
