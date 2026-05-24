// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// ADR-0058 — Online ADD COLUMN forwarding for single-stream (non-Shape-A)
// CDC apply.
//
// The Shape A multi-shard path (ADR-0054) already handles every
// recognized shape via the lease + boundary router
// (shard_consolidation_router.go). This file fills the single-stream
// gap: when --forward-schema-add-column is set, observed source ADD
// COLUMN events forward to the target via
// [ir.SchemaDeltaApplier.AlterAddColumn], with an optional source-side
// bounded backfill of already-shipped rows when
// --backfill-added-column is also set.
//
// Refuse-loudly catalog (every other shape):
//   - DROP COLUMN, ALTER COLUMN TYPE, ALTER COLUMN NULLABILITY,
//     RENAME COLUMN, CREATE INDEX, DROP INDEX, multi-shape combos →
//     refuse with the drained-model recovery hint.
//   - ADD COLUMN with [ir.DefaultExpression] → refuse (computed defaults
//     have target-session evaluation semantics that diverge from the
//     source's per-row insert values; ADR-0058 §2a).
//
// The intercept activates when:
//   - Streamer.ForwardSchemaAddColumn is true, AND
//   - Streamer.boundaryRouter is nil (i.e. not Shape A — Shape A's
//     intercept already covers ADD COLUMN via the lease).
//
// When both are set, this file's intercept replaces the pass-through
// branch in [interceptSchemaSnapshotsForCoordination]'s nil-router
// case.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/translate"
)

// schemaForwardDeps is the dependency surface
// [interceptAddColumnForward] needs. Plumbed by the Streamer's apply
// loop; the test harness constructs a minimal one with fakes.
type schemaForwardDeps struct {
	// applier issues ALTER TABLE … ADD COLUMN on the target. Both PG
	// and MySQL SchemaWriters implement this; the Streamer opens one
	// alongside the existing apply-side resources.
	applier ir.SchemaDeltaApplier

	// sourceEngineName is the source engine's [ir.Engine.Name] for the
	// translate.RetargetForEngine call (cross-engine type rewrite on
	// the ADD COLUMN's column definition).
	sourceEngineName string

	// targetEngineName is the target engine's name for the same
	// translate.RetargetForEngine call.
	targetEngineName string

	// backfill, when non-nil, enables source-side backfill of already-
	// shipped target rows after the ALTER lands. Built by the Streamer
	// when --backfill-added-column is set; nil otherwise.
	backfill *schemaForwardBackfill
}

// schemaForwardBackfill bundles the source-read + applier-write side
// of the optional backfill loop. Held by [schemaForwardDeps] when
// --backfill-added-column is set.
type schemaForwardBackfill struct {
	// reader is the source-side row reader used for the bounded
	// SELECT pk, new_col iteration. Must implement
	// [ir.BatchedRowReader]; non-implementers cause the constructor
	// to refuse loudly (every shipping engine implements it via
	// ADR-0018, but a future engine or a test stub might not).
	reader ir.BatchedRowReader

	// streamID is the per-stream identifier used for log
	// correlation.
	streamID string

	// batchSize bounds each SELECT's row count. Reuses the streamer's
	// BulkBatchSize when > 0; otherwise [defaultBulkBatchSize].
	batchSize int
}

// interceptAddColumnForward wraps the change channel for the
// non-Shape-A single-stream forwarding path (ADR-0058). On each
// [ir.SchemaSnapshot] event, the intercept compares (cached → snap.IR)
// via [ClassifyShape] and:
//
//   - [ShapeKindAddColumn] → call deps.applier.AlterAddColumn on the
//     target; optionally backfill via deps.backfill.
//   - [ShapeKindNone] → no-op pass-through (forward the snapshot to the
//     applier so the ADR-0049 schema-history row records the version).
//   - Any other recognized shape → refuse loudly with the drained-model
//     recovery hint (DROP / ALTER TYPE / RENAME / CHECK / generated /
//     CREATE INDEX / DROP INDEX / multi-shape combo).
//
// The first snapshot per table is the cache anchor (no DDL boundary
// to route — the source schema state at that point is the post-cold-
// start "pre" state). From the second snapshot onward, the
// (cached, snap.IR) delta is the routable shape.
//
// On any refuse-loudly error, the intercept closes the out-channel
// and stores the error in errStore for the streamer's
// surfaceSourceError path.
func interceptAddColumnForward(
	ctx context.Context,
	in <-chan ir.Change,
	deps schemaForwardDeps,
	errStore *atomic.Pointer[error],
) <-chan ir.Change {
	if deps.applier == nil {
		// Defensive — the engagement guard upstream should refuse
		// before reaching this point.
		return in
	}
	out := make(chan ir.Change)
	cache := map[string]*ir.Table{}
	go func() {
		defer close(out)
		for {
			select {
			case c, ok := <-in:
				if !ok {
					return
				}
				snap, isSnap := c.(ir.SchemaSnapshot)
				if !isSnap {
					if !forwardChange(ctx, out, c) {
						return
					}
					continue
				}
				key := snap.QualifiedName()
				pre, hadPre := cache[key]
				cache[key] = snap.IR
				if !hadPre {
					slog.DebugContext(
						ctx, "forward-add-column intercept: seeded table cache",
						"table", key,
					)
					if !forwardChange(ctx, out, c) {
						return
					}
					continue
				}
				if err := routeForwardBoundary(ctx, deps, key, pre, snap, out); err != nil {
					slog.ErrorContext(
						ctx, "forward-add-column intercept: refuse",
						"table", key,
						"error", err,
					)
					// Rewind the cache so a retry replays the same
					// boundary from the same pre-state.
					cache[key] = pre
					wrapped := fmt.Errorf("pipeline: forward schema add-column: %w", err)
					errStore.Store(&wrapped)
					return
				}
				// Forward the snapshot to the applier so the
				// ADR-0049 schema-history row still records the
				// version on the same tx as the position write.
				if !forwardChange(ctx, out, c) {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// forwardChange writes c to out, returning false if the context
// terminates first. Centralized so the per-branch select pattern
// doesn't repeat.
func forwardChange(ctx context.Context, out chan<- ir.Change, c ir.Change) bool {
	select {
	case out <- c:
		return true
	case <-ctx.Done():
		return false
	}
}

// routeForwardBoundary classifies the (pre, post) delta and dispatches
// the recognized shapes. Returns nil on success (ALTER landed and
// optional backfill emitted), a wrapped error on any refuse-loudly
// case.
func routeForwardBoundary(
	ctx context.Context,
	deps schemaForwardDeps,
	tableName string,
	pre *ir.Table,
	snap ir.SchemaSnapshot,
	out chan<- ir.Change,
) error {
	shape, err := ClassifyShape(pre, snap.IR)
	if err != nil {
		return fmt.Errorf("classify shape on %q: %w. %s",
			tableName, err, forwardRecoveryHint(tableName))
	}
	switch shape.Kind {
	case ShapeKindNone:
		return nil
	case ShapeKindAddColumn:
		return applyAddColumnForward(ctx, deps, tableName, snap, shape, out)
	case ShapeKindDropColumn,
		ShapeKindCreateIndex,
		ShapeKindDropIndex,
		ShapeKindAlterColumnType,
		ShapeKindAlterColumnNullability,
		ShapeKindRenameColumn,
		ShapeKindUnrecognized:
		return refuseShapeOutOfV1Scope(tableName, shape)
	}
	return fmt.Errorf("unrecognized shape kind %v on %q. %s",
		shape.Kind, tableName, forwardRecoveryHint(tableName))
}

// refuseShapeOutOfV1Scope is the operator-actionable refusal shape for
// every recognized-but-not-forwarded shape (DROP / ALTER TYPE /
// NULLABILITY / RENAME / CREATE/DROP INDEX / multi-shape combo). Names
// the table, the shape, and the drained-recovery hint per ADR-0058 §1a.
func refuseShapeOutOfV1Scope(tableName string, shape Shape) error {
	return fmt.Errorf(
		"shape %s on %q is out of --forward-schema-add-column scope "+
			"(v0.79.0 forwards ADD COLUMN only; ADR-0058 §1a documents the "+
			"scope split). %s",
		shape.Kind, tableName, forwardRecoveryHint(tableName),
	)
}

// forwardRecoveryHint is the single-stream variant of
// [RecoveryHint] for ADR-0058 refusals. The Shape A version mentions
// "every shard" and `--no-coordinate-live-ddl`, neither of which
// applies to a single-stream (non-shard) deployment. This variant
// drops the multi-shard language and names the drop-flag recovery as
// the safe path.
func forwardRecoveryHint(tableName string) string {
	return fmt.Sprintf(
		"recovery: drained model — run 'sluice sync stop --wait', "+
			"then run schema migrate (manual or 'sluice schema migrate') "+
			"against %q, then resume via 'sluice sync start --resume'. "+
			"Drop --forward-schema-add-column to keep the drained model "+
			"as the default for any subsequent source DDL.",
		tableName,
	)
}

// applyAddColumnForward executes the ADD COLUMN forward — the load-
// bearing branch of [routeForwardBoundary]. Steps:
//
//  1. Refuse loudly on any column with [ir.DefaultExpression]
//     (computed default; ADR-0058 §2a).
//  2. Retarget the post IR's added columns to the target engine's
//     dialect via [translate.RetargetForEngine] (cross-engine type
//     rewrite — same path the broker + chain-restore use).
//  3. Call deps.applier.AlterAddColumn(ctx, retargetedTable,
//     retargetedAdded). Idempotent via the engine's IF NOT EXISTS.
//  4. If deps.backfill is non-nil, emit synthetic [ir.Update] events
//     for already-shipped rows so the new column is populated from
//     source values rather than just the column's DEFAULT.
func applyAddColumnForward(
	ctx context.Context,
	deps schemaForwardDeps,
	tableName string,
	snap ir.SchemaSnapshot,
	shape Shape,
	out chan<- ir.Change,
) error {
	if err := refuseComputedDefaults(tableName, shape.AddedColumns); err != nil {
		return err
	}
	retargetedTable, retargetedAdded := retargetAddedColumns(
		snap.IR, shape.AddedColumns,
		deps.sourceEngineName, deps.targetEngineName,
	)
	if err := deps.applier.AlterAddColumn(ctx, retargetedTable, retargetedAdded); err != nil {
		return fmt.Errorf("alter add column on %q: %w. %s",
			tableName, err, forwardRecoveryHint(tableName))
	}
	slog.InfoContext(
		ctx, "forward-add-column: target ALTER applied",
		"table", tableName,
		"added_columns", columnNames(retargetedAdded),
	)
	if deps.backfill == nil {
		return nil
	}
	// Backfill uses the SOURCE IR (snap.IR), not the retargeted IR —
	// the BatchedRowReader is engine-specific (PG reader expects PG
	// types in its query). The applier consumes Update events with
	// the same source-IR column shape; cross-engine value translation
	// happens inside the applier's existing per-event dispatch path.
	if err := runBackfillForAddedColumn(ctx, deps.backfill, snap, shape.AddedColumns, out); err != nil {
		return fmt.Errorf("backfill on %q: %w. %s",
			tableName, err, forwardRecoveryHint(tableName))
	}
	return nil
}

// refuseComputedDefaults walks the added columns and returns an
// operator-actionable error if any column carries a
// [ir.DefaultExpression]. ADR-0058 §2a — computed defaults evaluate
// in the target's session at ALTER time, not the source's per-row
// insert context; silent forwarding would diverge.
//
// [ir.DefaultLiteral] and [ir.DefaultNone] pass through cleanly.
func refuseComputedDefaults(tableName string, cols []*ir.Column) error {
	for _, c := range cols {
		if c == nil {
			continue
		}
		if _, isExpr := c.Default.(ir.DefaultExpression); isExpr {
			return fmt.Errorf(
				"ADD COLUMN %q on %q has a computed DEFAULT expression "+
					"(target-session evaluation diverges from source per-row "+
					"insert; ADR-0058 §2a). %s",
				c.Name, tableName, forwardRecoveryHint(tableName),
			)
		}
	}
	return nil
}

// retargetAddedColumns wraps the post IR + the slice of added columns
// in a single-table schema, runs [translate.RetargetForEngine] to
// rewrite types for the target dialect, and returns the
// post-retarget table + the slice of added columns located in that
// table. Same call pattern as broker.go:997 and chain_restore.go:574.
//
// Same-engine pairs are a no-op pass-through inside RetargetForEngine.
func retargetAddedColumns(post *ir.Table, added []*ir.Column, sourceEngine, targetEngine string) (*ir.Table, []*ir.Column) {
	retargetedSchema := translate.RetargetForEngine(
		&ir.Schema{Tables: []*ir.Table{post}},
		sourceEngine, targetEngine,
	)
	if len(retargetedSchema.Tables) == 0 {
		return post, added
	}
	retargetedTable := retargetedSchema.Tables[0]
	// Find the retargeted columns matching the added names.
	addedNames := make(map[string]struct{}, len(added))
	for _, c := range added {
		if c != nil {
			addedNames[c.Name] = struct{}{}
		}
	}
	retargetedAdded := make([]*ir.Column, 0, len(added))
	for _, c := range retargetedTable.Columns {
		if _, ok := addedNames[c.Name]; ok {
			retargetedAdded = append(retargetedAdded, c)
		}
	}
	return retargetedTable, retargetedAdded
}

// runBackfillForAddedColumn drives the bounded source-side SELECT
// loop for the just-ALTERed table. Emits synthetic [ir.Update] events
// to out, one per source row, carrying PK columns in Before and the
// added column values in After.
//
// Idempotency: the synthesized Updates carry the SchemaSnapshot's
// Position; a crash-and-resume replays from the SchemaSnapshot, the
// ALTER is idempotent via IF NOT EXISTS, and the Updates re-issue
// against the same PK range. The applier's UPDATE path is
// idempotent (re-applying SET new_col=$1 WHERE pk=$2 is a no-op when
// the value already matches).
//
// Refuse-loudly cases:
//   - Source ReadRowsBatch error → caller wraps + persists for retry.
//   - Out-channel closed (ctx cancelled) → forwardChange returns
//     false; the function returns the ctx error.
func runBackfillForAddedColumn(
	ctx context.Context,
	bf *schemaForwardBackfill,
	snap ir.SchemaSnapshot,
	addedCols []*ir.Column,
	out chan<- ir.Change,
) error {
	if bf == nil || bf.reader == nil {
		return errors.New("backfill: missing reader")
	}
	if snap.IR == nil {
		return errors.New("backfill: snapshot has nil IR")
	}
	table := snap.IR
	if table.PrimaryKey == nil || len(table.PrimaryKey.Columns) == 0 {
		// No PK — can't safely iterate. Refuse loudly. Tables
		// without a PK are also rejected by the bulk-copy
		// orchestrator (ADR-0018); same recovery hint applies.
		return fmt.Errorf(
			"backfill: table %q has no primary key — cursor-paginated "+
				"backfill is unsafe without a PK",
			table.Name,
		)
	}
	batchSize := bf.batchSize
	if batchSize <= 0 {
		batchSize = defaultBulkBatchSize
	}
	addedNames := make(map[string]struct{}, len(addedCols))
	for _, c := range addedCols {
		if c != nil {
			addedNames[c.Name] = struct{}{}
		}
	}
	pkColNames := make([]string, len(table.PrimaryKey.Columns))
	for i, c := range table.PrimaryKey.Columns {
		pkColNames[i] = c.Column
	}
	var cursor []any
	total := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		rows, err := bf.reader.ReadRowsBatch(ctx, table, cursor, batchSize)
		if err != nil {
			return fmt.Errorf("read rows batch: %w", err)
		}
		batchCount := 0
		var lastRow ir.Row
		for r := range rows {
			update := synthesizeBackfillUpdate(snap, r, pkColNames, addedNames)
			if !forwardChange(ctx, out, update) {
				return ctx.Err()
			}
			lastRow = r
			batchCount++
		}
		if batchCount == 0 {
			break
		}
		total += batchCount
		// Advance the cursor to the last row's PK so the next batch
		// is strictly greater. Matches the bulk-copy resume cursor
		// (ADR-0018).
		nextCursor := make([]any, len(pkColNames))
		for i, name := range pkColNames {
			nextCursor[i] = lastRow[name]
		}
		cursor = nextCursor
		if batchCount < batchSize {
			// Final batch — fewer rows than the limit means we've
			// reached the end of the table.
			break
		}
	}
	slog.InfoContext(
		ctx, "forward-add-column: backfill complete",
		"table", table.Name,
		"stream_id", bf.streamID,
		"rows_backfilled", total,
		"added_columns", columnNames(addedCols),
	)
	return nil
}

// synthesizeBackfillUpdate constructs an [ir.Update] from a source
// row for the backfill loop. Before carries the PK columns (the
// UPDATE's WHERE predicate); After carries the added columns (the
// UPDATE's SET clause). The applier's existing buildUpdateSQL
// consumes this shape directly — see ADR-0058 §1c for the rationale.
//
// Position is set to the SchemaSnapshot's Position so the applier's
// position-write stays anchored at the ALTER boundary; resume after
// a crash replays the same UPDATEs against the same PK range.
func synthesizeBackfillUpdate(
	snap ir.SchemaSnapshot,
	row ir.Row,
	pkColNames []string,
	addedNames map[string]struct{},
) ir.Update {
	before := make(ir.Row, len(pkColNames))
	for _, name := range pkColNames {
		before[name] = row[name]
	}
	after := make(ir.Row, len(addedNames))
	for name := range addedNames {
		after[name] = row[name]
	}
	return ir.Update{
		Position: snap.Position,
		Schema:   snap.Schema,
		Table:    snap.Table,
		Before:   before,
		After:    after,
	}
}

// columnNames returns the Name slice for cols. For log lines.
func columnNames(cols []*ir.Column) []string {
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		if c != nil {
			out = append(out, c.Name)
		}
	}
	return out
}
