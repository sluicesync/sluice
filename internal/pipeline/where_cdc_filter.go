// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/rowpredicate"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// ADR-0173 Phase 2 — continuous *filtered* sync (the CDC leg).
//
// Phase 1 pushes a `--where` predicate down into the source read for
// migrate. The sync SNAPSHOT (cold-start) leg reuses that push-down
// verbatim ([migcore.ApplyRowFilters] on the snapshot RowReader). The CDC
// leg is the hard part: there is no source-side stream filter, so the
// predicate is evaluated CLIENT-SIDE per [ir.Change] over the decoded
// before/after images, and translated to the correct TARGET op — the
// ADR-0173 "row-move" table.
//
// Both legs are driven by the SAME [Streamer.RowFilters] map (single
// source of truth), and the client-side evaluator ([rowpredicate])
// deliberately REFUSES any predicate it cannot evaluate faithfully — so
// the source-SQL snapshot evaluation and the client-side CDC evaluation
// agree by construction for every predicate that compiles.

// whereCDCFilter holds the compiled client-side predicates for a filtered
// continuous sync, keyed by lower-cased source table name. A nil
// *whereCDCFilter means "no --where" (the byte-identical default).
type whereCDCFilter struct {
	preds map[string]*rowpredicate.Predicate
	// pkCols is the lower-cased primary-key column set per filtered table.
	// The CDC reader emits the FULL before-image for filtered tables (so the
	// predicate can read every OLD column); after evaluating, the intercept
	// re-narrows the before-image to these key columns before forwarding, so
	// the applier's WHERE stays key-only (preserving the Bug-8/88/92 fix).
	// Empty for a PK-less table (the before-image is then left full — the
	// same PK-less fallback the readers use).
	pkCols map[string]map[string]bool
}

// buildWhereCDCFilter compiles each `--where TABLE=<predicate>` string into
// a client-side-evaluable predicate, resolving each referenced column
// against the source schema (for type/collation fidelity). Every predicate
// that cannot be faithfully evaluated client-side is refused loudly with a
// [sluicecode.CodeWhereCDCUnsupportedPredicate] coded error; a `--where`
// key that names no source table is refused too (it can never take effect).
func buildWhereCDCFilter(engineName string, rowFilters map[string]string, schema *ir.Schema, strictCollation bool) (*whereCDCFilter, error) {
	if len(rowFilters) == 0 {
		return nil, nil
	}
	byName := make(map[string]*ir.Table, len(schema.Tables))
	for _, t := range schema.Tables {
		if t != nil {
			byName[strings.ToLower(t.Name)] = t
		}
	}
	preds := make(map[string]*rowpredicate.Predicate, len(rowFilters))
	pkCols := make(map[string]map[string]bool, len(rowFilters))
	for table, predicate := range rowFilters {
		tbl, ok := byName[strings.ToLower(table)]
		if !ok {
			return nil, sluicecode.Wrap(
				sluicecode.CodeWhereCDCUnsupportedPredicate,
				"remove the --where entry or correct the table name",
				fmt.Errorf(
					"continuous filtered sync: --where names table %q, which is not in the synced source schema "+
						"(it may be misspelled or excluded by --include/--exclude-table); a filter that matches no "+
						"table would silently do nothing",
					table,
				),
			)
		}
		infos := rowpredicate.ColumnInfosFromIR(engineName, tbl.Columns, strictCollation)
		p, err := rowpredicate.Compile(table, predicate, infos)
		if err != nil {
			return nil, err
		}
		key := strings.ToLower(table)
		preds[key] = p
		pkCols[key] = primaryKeyColumnSet(tbl)
	}
	return &whereCDCFilter{preds: preds, pkCols: pkCols}, nil
}

// primaryKeyColumnSet returns the lower-cased primary-key column names of
// tbl as a set, or nil when the table has no primary key (the before-image
// is then left un-narrowed, matching the readers' PK-less fallback).
func primaryKeyColumnSet(tbl *ir.Table) map[string]bool {
	if tbl.PrimaryKey == nil || len(tbl.PrimaryKey.Columns) == 0 {
		return nil
	}
	set := make(map[string]bool, len(tbl.PrimaryKey.Columns))
	for _, c := range tbl.PrimaryKey.Columns {
		set[strings.ToLower(c.Column)] = true
	}
	return set
}

// narrowBefore returns a copy of before restricted to table's primary-key
// columns — the applier's key-only WHERE source (the reader emitted the
// full before-image so the predicate could be evaluated; the WHERE must
// still be key-only per Bug-8/88/92). Returns before unchanged when the
// table has no known primary key.
func (f *whereCDCFilter) narrowBefore(table string, before ir.Row) ir.Row {
	pk := f.pkCols[strings.ToLower(table)]
	if len(pk) == 0 || before == nil {
		return before
	}
	out := make(ir.Row, len(pk))
	for name, v := range before {
		if pk[strings.ToLower(name)] {
			out[name] = v
		}
	}
	return out
}

// fullBeforeImageSetter is the optional CDC-reader surface a filtered
// continuous sync uses to request UN-narrowed before-images for the
// filtered tables (ADR-0173 Phase 2). The reader normally narrows the
// before-image to identity-key columns (Bug-8/88/92); for the named tables
// it emits the full decoded old tuple so the client-side row-move eval can
// read every OLD column, and [whereCDCFilter.narrowBefore] re-narrows to
// the key columns before the applier builds its WHERE. MySQL and Postgres
// CDC readers implement it.
type fullBeforeImageSetter interface {
	SetFullBeforeImageTables(tables map[string]bool)
}

// applyFullBeforeImageTables wires the filtered-table set onto the CDC
// reader so it emits full before-images for those tables. It is a no-op
// when no `--where` is configured. When a filter IS configured but the
// reader does NOT implement [fullBeforeImageSetter], it refuses loudly:
// silently accepting the reader's PK-narrowed before-image would make the
// row-move evaluation mis-classify a move-OUT (before-image key-only ⇒ the
// predicate reads NULL for the filtered column ⇒ UNKNOWN ⇒ the now-out-of-
// scope row leaks on the target) — the exact silent-loss class this
// refusal exists to prevent.
func (s *Streamer) applyFullBeforeImageTables(reader any) error {
	if s.whereFilter == nil {
		return nil
	}
	setter, ok := reader.(fullBeforeImageSetter)
	if !ok {
		return sluicecode.Wrap(
			sluicecode.CodeWhereCDCBeforeImage,
			"use `sluice migrate --where` for a one-shot filtered copy instead",
			fmt.Errorf(
				"continuous filtered sync: source engine %q's CDC reader cannot emit full row before-images, "+
					"which the --where row-move evaluation requires (it narrows the before-image to primary-key "+
					"columns, so the filtered column would read NULL and a move-OUT could silently leak an "+
					"out-of-scope row)",
				s.Source.Name(),
			),
		)
	}
	tables := make(map[string]bool, len(s.RowFilters))
	for t := range s.RowFilters {
		tables[t] = true
	}
	setter.SetFullBeforeImageTables(tables)
	return nil
}

// predicateFor returns the compiled predicate for a change's table, or nil
// when the table carries no `--where` (its changes flow through unfiltered).
func (f *whereCDCFilter) predicateFor(table string) *rowpredicate.Predicate {
	if f == nil {
		return nil
	}
	return f.preds[strings.ToLower(table)]
}

// filteredTables returns the sorted list of source table names carrying a
// `--where` predicate — the set the before-image preflight must check.
func (s *Streamer) filteredTableNames() []string {
	names := make([]string, 0, len(s.RowFilters))
	for t := range s.RowFilters {
		names = append(names, t)
	}
	return names
}

// interceptWhereFilter wraps the CDC change channel with the ADR-0173
// row-move dispatch. For each row-bearing change on a filtered table it
// evaluates the predicate on the before/after images and translates to the
// correct TARGET op (see the table below); every other change (non-filtered
// table, Truncate, SchemaSnapshot, Tx boundary) flows through verbatim.
//
//	source op | before | after | target op
//	----------|--------|-------|-----------------------------
//	INSERT    |   -    |  yes  | INSERT
//	INSERT    |   -    |  no   | drop
//	DELETE    |  yes   |   -   | DELETE
//	DELETE    |  no    |   -   | drop
//	UPDATE    |  no    |  no   | drop
//	UPDATE    |  yes   |  yes  | UPDATE (as-is)
//	UPDATE    |  no    |  yes  | INSERT the after-image   (move-IN)
//	UPDATE    |  yes   |  no   | DELETE by key            (move-OUT)
//
// The move-IN → INSERT and move-OUT → DELETE cells are the load-bearing
// correctness: a naive per-event filter would drop a move-OUT (leaking a
// now-out-of-scope row on the target) and drop a move-IN (the target has no
// base row for the UPDATE, so the newly-in-scope row would never appear).
//
// A filtered UPDATE/DELETE whose before-image is absent is a
// [sluicecode.CodeWhereCDCBeforeImage] refusal: the preflight guarantees
// full before-images, but a mid-stream partial image (a slipped-past global,
// a resume replaying an old segment) must fail loudly rather than
// mis-classify the move — the same discipline as the MySQL Bug-193 belt.
//
// nil filter is a verbatim pass-through (no goroutine). On a refusal the
// intercept stores the error in errStore and closes out, mirroring
// [interceptAddColumnForward].
func interceptWhereFilter(
	ctx context.Context,
	in <-chan ir.Change,
	filter *whereCDCFilter,
	errStore *atomic.Pointer[error],
) <-chan ir.Change {
	if filter == nil {
		return in
	}
	out := make(chan ir.Change, migcore.RowChanBuffer)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case c, ok := <-in:
				if !ok {
					return
				}
				emit, err := filter.route(c)
				if err != nil {
					errStore.Store(&err)
					return
				}
				for _, e := range emit {
					if !forwardChange(ctx, out, e) {
						return
					}
				}
			}
		}
	}()
	return out
}

// route applies the row-move table to one change, returning the change(s)
// to forward (nil = drop) or a refusal. It is pure (no I/O), so the
// row-move semantics are unit-testable without a channel harness.
func (f *whereCDCFilter) route(c ir.Change) ([]ir.Change, error) {
	switch e := c.(type) {
	case ir.Insert:
		p := f.predicateFor(e.Table)
		if p == nil || p.Eval(e.Row) {
			return []ir.Change{e}, nil
		}
		return nil, nil
	case ir.Delete:
		p := f.predicateFor(e.Table)
		if p == nil {
			return []ir.Change{e}, nil
		}
		if e.Before == nil {
			return nil, missingBeforeImage("DELETE", e.Schema, e.Table)
		}
		if p.Eval(e.Before) {
			// Re-narrow the (full) before-image to the key columns for the
			// applier's WHERE.
			e.Before = f.narrowBefore(e.Table, e.Before)
			return []ir.Change{e}, nil
		}
		return nil, nil
	case ir.Update:
		p := f.predicateFor(e.Table)
		if p == nil {
			return []ir.Change{e}, nil
		}
		if e.Before == nil {
			return nil, missingBeforeImage("UPDATE", e.Schema, e.Table)
		}
		before := p.Eval(e.Before)
		after := p.Eval(e.After)
		switch {
		case before && after:
			// Stays in scope. Re-narrow Before to the key columns for the
			// applier's WHERE (After keeps every column for the SET clause).
			e.Before = f.narrowBefore(e.Table, e.Before)
			return []ir.Change{e}, nil
		case !before && !after:
			return nil, nil // never in scope
		case !before && after:
			// move-IN: the target never had this row → INSERT the after-image.
			return []ir.Change{ir.Insert{
				Position:   e.Position,
				Schema:     e.Schema,
				Table:      e.Table,
				Row:        e.After,
				CommitTime: e.CommitTime,
			}}, nil
		default:
			// move-OUT (before && !after): DELETE by key so the now-out-of-
			// scope row doesn't leak on the target. Narrow to the key columns.
			return []ir.Change{ir.Delete{
				Position:   e.Position,
				Schema:     e.Schema,
				Table:      e.Table,
				Before:     f.narrowBefore(e.Table, e.Before),
				CommitTime: e.CommitTime,
			}}, nil
		}
	default:
		// Truncate, SchemaSnapshot, TxBegin, TxCommit: not row-scoped filter
		// targets — forward verbatim.
		return []ir.Change{c}, nil
	}
}

// missingBeforeImage builds the coded refusal for a filtered UPDATE/DELETE
// that arrived without a before-image — the row-move evaluation cannot run.
func missingBeforeImage(op, schema, table string) error {
	return sluicecode.Wrap(
		sluicecode.CodeWhereCDCBeforeImage,
		"ensure MySQL binlog_row_image=FULL / PG REPLICA IDENTITY FULL on the filtered table, then restart the sync",
		fmt.Errorf(
			"continuous filtered sync: a %s on filtered table %s arrived without a before-image, so --where cannot "+
				"decide whether the row moved into or out of the filter's scope. The source must deliver full row "+
				"before-images for a filtered table (MySQL binlog_row_image=FULL, PG REPLICA IDENTITY FULL); a "+
				"partial image reached the reader anyway (a session-level override, or a resume replaying a segment "+
				"written before the setting was corrected). The stream stops here rather than mis-classify the row",
			op, qualifiedTableName(schema, table),
		),
	)
}

func qualifiedTableName(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// preflightRowFilters runs the ADR-0173 Phase 2 sync-start checks when a
// `--where` filter is set: it reads the source schema, compiles each
// predicate against it (the unsupported-predicate refusal), verifies the
// source delivers full before-images for the filtered tables (the
// before-image refusal), and stores the compiled filter on the Streamer for
// the CDC-leg intercept. A no-op when RowFilters is empty.
//
// It runs BEFORE any streaming (cold-start snapshot or warm resume), so an
// unfixable predicate / mis-configured source is refused up front — never
// after data has moved. Both refusals are coded and name the table +
// remedy.
func (s *Streamer) preflightRowFilters(ctx context.Context) error {
	if len(s.RowFilters) == 0 {
		return nil
	}
	// The client-side row-move eval requires full before-images. Refuse
	// loudly on a source engine that can't guarantee them (v1 scope:
	// mysql + postgres) or whose tables aren't configured for them.
	pf, ok := s.Source.(ir.FilteredCDCPreflighter)
	if !ok {
		return sluicecode.Wrap(
			sluicecode.CodeWhereCDCBeforeImage,
			"use `sluice migrate --where` for a one-shot filtered copy instead",
			fmt.Errorf(
				"continuous filtered sync (--where on `sync`) is not supported for source engine %q: it cannot "+
					"guarantee the full row before-images the client-side row-move evaluation requires. "+
					"MySQL and Postgres sources are supported",
				s.Source.Name(),
			),
		)
	}
	if err := pf.PreflightFilteredCDCBeforeImage(ctx, s.SourceDSN, s.filteredTableNames()); err != nil {
		return err
	}

	// Read the source schema so each predicate can be compiled against its
	// table's real column types + collations.
	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: open source schema reader for --where preflight: %w", err))
	}
	defer migcore.CloseIf(sr)
	migcore.ApplyTableScope(sr, s.Filter)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("pipeline: read source schema for --where preflight: %w", err))
	}
	filter, err := buildWhereCDCFilter(s.Source.Name(), s.RowFilters, schema, s.WhereStrictCollation)
	if err != nil {
		return err
	}
	s.whereFilter = filter
	slog.InfoContext(
		ctx, "continuous filtered sync: --where predicates compiled for the CDC leg (ADR-0173 Phase 2)",
		slog.Int("filtered_tables", len(s.RowFilters)),
	)
	return nil
}
