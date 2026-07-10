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
)

// # VStream-COPY FLOAT display-rounding repair (roadmap open-bug 2026-07-09)
//
// A PlanetScale/Vitess VStream cold-start COPY lands single-precision
// FLOAT values display-rounded to 6 significant digits (8388608 → 8388610):
// vttablet's rowstreamer issues a bare-column SELECT whose wire bytes are
// mysqld's own float→text rendering, and that SELECT is built inside
// vttablet, out of sluice's reach (the projection ADR-0153's SQL reader
// uses — `(col * 1E0)` — cannot be injected). The CDC leg is float32-exact
// and DOUBLE transits exactly, so only the single-precision COPY value is
// lossy.
//
// The sync cold-start REPAIRS it: after the COPY completes and BEFORE CDC
// apply begins, it re-reads the affected FLOAT columns EXACTLY from the
// source over a SEPARATE SQL connection (Engine.OpenRowReader → the
// float32-exact `(col * 1E0)` projection, vtgate MySQL port) and UPDATEs
// the target rows by primary key. Ordering is load-bearing: the re-read
// must land before CDC replays from the copy anchor, so CDC's forward
// replay correctly OVERWRITES any value that changed between copy and
// re-read (the re-read is a point-in-time read after the copy anchor;
// anything a later change touched is re-applied by CDC to its final value).
//
// The repair is best-effort within the cold-start: it runs before the CDC
// anchor position is persisted, so a crash mid-repair leaves no position
// row and the next start re-cold-starts (refused on a populated target →
// --reset-target-data), never a partially-repaired warm resume.

// floatRepairTable is one table's plan for the post-COPY FLOAT re-read.
type floatRepairTable struct {
	// name is the unqualified table name (source == target — sluice never
	// renames tables, only remaps the schema/database namespace).
	name string
	// pkColumns are the primary-key column names in PK order.
	pkColumns []string
	// floatColumns are the single-precision FLOAT column names to re-read,
	// EXCLUDING any that are themselves PK members (a float PK cannot be
	// repaired by keying on itself).
	floatColumns []string
	// srcRead is the trimmed SOURCE table (PK + FLOAT columns, SOURCE
	// types + SOURCE PK) driving the exact re-read. Its FLOAT columns carry
	// ir.Float{FloatSingle} so selectColumnExpr applies the `(col * 1E0)`
	// projection; column defs are shallow-copied so a later ApplyMappings
	// on the source schema cannot mutate the captured type.
	srcRead *ir.Table
	// repairable is true when the table has a usable PK AND at least one
	// non-PK single-precision FLOAT column. A non-repairable table (keyless,
	// or every FLOAT is a PK member) still warrants the WARN but cannot be
	// re-read — it falls back to the WARN-only (backup) posture.
	repairable bool
}

// planFloatRepair scans the pristine SOURCE schema for single-precision
// FLOAT columns and builds the per-table re-read plan. Detection is on the
// SOURCE column type (ir.Float{FloatSingle}) — NOT the post-mapping target
// type — because the VStream COPY rounds the value on the wire before it
// reaches the target, so even a FLOAT→DOUBLE `--type-override` still lands
// rounded and still needs the exact re-read. A table with no single-
// precision FLOAT column is omitted entirely.
func planFloatRepair(sourceSchema *ir.Schema) []floatRepairTable {
	if sourceSchema == nil {
		return nil
	}
	var plan []floatRepairTable
	for _, t := range sourceSchema.Tables {
		floatCols := migcore.SinglePrecisionFloatColumns(t)
		if len(floatCols) == 0 {
			continue
		}
		pkCols := migcore.PrimaryKeyColumnNames(t)
		pkSet := make(map[string]struct{}, len(pkCols))
		for _, c := range pkCols {
			pkSet[c] = struct{}{}
		}
		// FLOAT columns that are PK members can't be repaired by keying on
		// themselves; drop them from the re-read set (still WARNed via
		// floatCols below).
		repairCols := make([]string, 0, len(floatCols))
		for _, c := range floatCols {
			if _, isPK := pkSet[c.Name]; !isPK {
				repairCols = append(repairCols, c.Name)
			}
		}
		// A single-precision FLOAT anywhere in the PK makes the whole table
		// non-repairable (SL-F1): the re-read keys on the exact PK, but the
		// COPY wrote a display-rounded PK, so the PK-keyed UPDATE matches zero
		// rows and every non-PK FLOAT (repairCols) silently retains its
		// rounding. Route it to the honest cannot-repair WARN instead of the
		// false "sluice will repair it" promise.
		pkHasFloat := migcore.PrimaryKeyHasSinglePrecisionFloat(t)
		ft := floatRepairTable{
			name:         t.Name,
			pkColumns:    pkCols,
			floatColumns: floatColumnNames(floatCols),
			repairable:   len(pkCols) > 0 && len(repairCols) > 0 && !pkHasFloat,
		}
		if ft.repairable {
			ft.srcRead = trimmedFloatReadTable(t, pkCols, repairCols)
		}
		plan = append(plan, ft)
	}
	return plan
}

func floatColumnNames(cols []*ir.Column) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

// trimmedFloatReadTable builds the SOURCE-typed table the re-read scans:
// the PK columns (needed for the cursor + the WHERE) plus the FLOAT
// columns to re-read, each column shallow-copied so a later mutation of
// the source schema cannot change the captured type. The PK Index is
// rebuilt (its columns reference names, which are stable).
func trimmedFloatReadTable(src *ir.Table, pkCols, floatCols []string) *ir.Table {
	want := make(map[string]struct{}, len(pkCols)+len(floatCols))
	for _, c := range pkCols {
		want[c] = struct{}{}
	}
	for _, c := range floatCols {
		want[c] = struct{}{}
	}
	// Preserve source declaration order for the projected columns.
	cols := make([]*ir.Column, 0, len(want))
	for _, c := range src.Columns {
		if _, ok := want[c.Name]; ok {
			cp := *c
			cols = append(cols, &cp)
		}
	}
	trimmed := &ir.Table{
		Schema:  src.Schema,
		Name:    src.Name,
		Columns: cols,
	}
	if src.PrimaryKey != nil {
		pk := *src.PrimaryKey
		trimmed.PrimaryKey = &pk
	}
	return trimmed
}

// floatCopyDisplayRounds reports whether the snapshot stream's COPY reader
// display-rounds single-precision FLOAT columns (the VStream cold-start
// reader). It is the gate for both the sync repair and the backup WARN.
func floatCopyDisplayRounds(stream *ir.SnapshotStream) bool {
	if stream == nil || stream.Rows == nil {
		return false
	}
	r, ok := stream.Rows.(ir.LossyFloatCopyReader)
	return ok && r.CopyDisplayRoundsFloats()
}

// warnFloatDisplayRounding emits a once-per-column loud WARN naming every
// single-precision FLOAT column the VStream COPY display-rounds, and what
// sluice will do about it. Schema-triggered (not value-triggered): the
// wire value is already rounded, so per-value loss is undetectable — the
// column's presence is the signal. Emitted at COPY start.
func warnFloatDisplayRounding(ctx context.Context, plan []floatRepairTable, repairDisabled bool) {
	for _, ft := range plan {
		for _, col := range ft.floatColumns {
			key := ft.name + "." + col
			switch {
			case !ft.repairable:
				slog.WarnContext(ctx,
					"mysql/vstream: cold-start COPY display-rounds this single-precision FLOAT column to 6 significant "+
						"digits (vttablet's rowstreamer renders FLOAT via mysqld's float→text formatter), and it CANNOT be "+
						"repaired — the table has no primary key to target the re-read (or the FLOAT column is part of the PK). "+
						"The rounding is retained",
					slog.String("column", key))
			case repairDisabled:
				slog.WarnContext(ctx,
					"mysql/vstream: cold-start COPY display-rounds this single-precision FLOAT column to 6 significant "+
						"digits; the post-copy exact re-read repair is DISABLED (--no-float-exact-reread), so the rounding is "+
						"retained",
					slog.String("column", key))
			default:
				slog.WarnContext(ctx,
					"mysql/vstream: cold-start COPY display-rounds this single-precision FLOAT column to 6 significant "+
						"digits; sluice will repair it via a post-copy exact SQL re-read before CDC begins",
					slog.String("column", key))
			}
		}
	}
}

// repairColdStartFloats re-reads every repairable table's single-precision
// FLOAT columns EXACTLY from the source and UPDATEs the target rows by PK.
// It is the sync cold-start mitigation for the VStream-COPY display-
// rounding class; targetSchema is the finalized (post-mapping) schema whose
// tables carry the target column types the UPDATE binds against.
//
// Runs AFTER the bulk-copy completes and BEFORE the CDC anchor is
// persisted (see the file header for the ordering rationale). Returns an
// error only on a hard source-read / target-write failure — a target that
// can't do a PK-keyed UPDATE (no [ir.FloatRepairWriter]) or a source that
// isn't cursor-paginable degrades to the WARN-only posture (loud, non-fatal).
func (s *Streamer) repairColdStartFloats(ctx context.Context, plan []floatRepairTable, targetSchema *ir.Schema) error {
	tgtByName := make(map[string]*ir.Table, len(targetSchema.Tables))
	for _, t := range targetSchema.Tables {
		tgtByName[t.Name] = t
	}

	rr, err := s.Source.OpenRowReader(ctx, s.SourceDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseSnapshot, fmt.Errorf("pipeline: float repair: open source reader: %w", err))
	}
	defer migcore.CloseIf(rr)
	br, ok := rr.(ir.BatchedRowReader)
	if !ok {
		slog.WarnContext(ctx, "pipeline: float repair skipped — source reader does not support cursor-paginated reads; "+
			"single-precision FLOAT columns retain the VStream COPY display-rounding")
		return nil
	}

	rw, err := s.Target.OpenRowWriter(ctx, s.TargetDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: float repair: open target writer: %w", err))
	}
	defer migcore.CloseIf(rw)
	migcore.ApplyTargetSchema(rw, s.TargetSchema)
	fw, ok := rw.(ir.FloatRepairWriter)
	if !ok {
		slog.WarnContext(ctx, "pipeline: float repair skipped — target does not support PK-keyed FLOAT repair; "+
			"single-precision FLOAT columns retain the VStream COPY display-rounding")
		return nil
	}

	limit := s.BulkBatchSize
	if limit <= 0 {
		limit = migcore.DefaultBulkBatchSize
	}

	repaired := 0
	for _, ft := range plan {
		if !ft.repairable {
			continue
		}
		tgt, ok := tgtByName[ft.name]
		if !ok {
			slog.WarnContext(ctx, "pipeline: float repair: target table not found; skipping",
				slog.String("table", ft.name))
			continue
		}
		if err := repairFloatTable(ctx, br, rr, fw, ft, tgt, limit); err != nil {
			return migcore.WrapWithHint(migcore.PhaseBulkCopy,
				fmt.Errorf("pipeline: float repair %q: %w", ft.name, err))
		}
		repaired++
	}
	if repaired > 0 {
		slog.InfoContext(ctx, "pipeline: float repair complete — single-precision FLOAT columns re-read exactly from source",
			slog.Int("tables_repaired", repaired))
	}
	return nil
}

// repairFloatTable paginates one table's (PK + FLOAT) rows from the source
// via the cursor reader and UPDATEs the target by PK, mirroring
// copyTableWithCursor's proven keyset loop (gap-free / overlap-free
// coverage; the Bug-74 cursor round-trip contract). Each page is its own
// target transaction inside UpdateFloatColumnsByPK.
func repairFloatTable(ctx context.Context, br ir.BatchedRowReader, rr ir.RowReader, fw ir.FloatRepairWriter, ft floatRepairTable, tgt *ir.Table, limit int) error {
	var cursor []any
	for {
		batchCtx, cancel := context.WithCancel(ctx)

		rowsCh, err := br.ReadRowsBatch(batchCtx, ft.srcRead, cursor, limit)
		if err != nil {
			cancel()
			return fmt.Errorf("read batch: %w", err)
		}
		tracker := migcore.NewPKTracker(ft.pkColumns)
		var batchCount int64
		teed := teePKAndCount(batchCtx, rowsCh, tracker, &batchCount, nil)
		if err := fw.UpdateFloatColumnsByPK(batchCtx, tgt, ft.pkColumns, teed); err != nil {
			cancel()
			return fmt.Errorf("write batch: %w", err)
		}
		cancel()

		// Loud-failure gate (Bug 68): the batched reader decodes on a
		// background goroutine and aborts a batch by closing the channel,
		// indistinguishable from a clean short/empty batch. Check the
		// reader's sticky error before interpreting batchCount.
		if err := migcore.ReaderStreamErr(rr, ft.srcRead); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if batchCount == 0 {
			return nil
		}
		newCursor, ok := tracker.LastPK()
		if !ok {
			return errors.New("float repair batch produced rows but PK tracker captured none; check primary key resolution")
		}
		cursor = newCursor
	}
}
