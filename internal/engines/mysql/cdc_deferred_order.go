// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// Deferred shapes — a schema change no boundary will carry
// (BUG301-FULL-DEFERRED-SWAP, BUG301-DEFERRED-RETYPE).
//
// Under binlog_row_metadata=FULL a reader behind the binlog head decodes
// each event by its own column names (cdc_recorded_shape.go). When one of
// those columns has since been dropped or retyped, the shape is typed in
// part from the TABLE_MAP alone, and such a shape is never a schema
// boundary: the change it records reaches the pipeline only folded into
// the next boundary built from read definitions — AFTER its rows. Two
// harms were measured (MySQL 8.0, 8.4, MariaDB 11.4, warm resume):
//
//   - a RENAME swap vanishes: `RENAME b TO c, c TO b` then `DROP COLUMN b`
//     arrives as `(id,b,c) → (id,c)`, a drop with the surviving name in
//     place, and the target keeps its pre-swap values under `c` at exit 0;
//   - a type change lands its rows under the target's OLD type before the
//     boundary that would have changed it: `MODIFY b DATETIME(6)`, a row,
//     then `DROP COLUMN c` — the target rounded the row's fractional
//     seconds away, and the folded boundary's refusal came after it.
//
// So the shape is held here, before any row of it is decoded: a shape that
// will not be a boundary must keep every pair of the names it shares with
// the table's basis in the same relative order (ir.ErrColumnOrderChanged)
// and give each of them the same IR type (ir.ErrDeferredSchemaChange). The
// type equivalence is [ir.SameColumnType], the one that decides whether a
// change is a schema boundary at all — deliberately not
// recordedColumnAccepts, which asks only whether a definition decodes the
// row image (it ignores DATETIME precision and DECIMAL scale, which the
// decoder takes from the image) and so cannot see the second harm. A
// column the shape typed from the TABLE_MAP alone is not compared: its IR
// type is approximate. It is the column dropped or retyped later, which
// the source no longer holds in that form.
//
// The basis is the shape of the table's previous decoded event, never the
// decode cache — behind the head the cache holds the catalog as of the
// read, the future (priorSig is refreshed from it, so it cannot serve).
// For a table's first event of the run it is the baseline the pipeline
// hands over ([CDCReader.SetDecodedShapeBaseline]), typed in every case:
// the schema the cold start copied from, the retained schema history at
// the warm-resume position, a backup chain's parent manifest. Once a
// baseline is given, a table missing from it has no basis, and its first
// deferred shape refuses with the re-copy remedy rather than pass
// unchecked. A reader given no baseline (the multi-database and
// foreign-lineage open sites) compares from its first decoded event on;
// a deferred shape that is a table's first event there is unchecked.
//
// What else a deferred shape can fold away, measured on MySQL 8.4 (FULL,
// warm resume), and caught downstream: a rename (then a DROP of it or of
// another column) and an ADD (then a DROP of another column) put a column
// the target lacks into the rows, and the apply refuses; a swap followed
// by an ADD is not deferred (every recorded column has a read definition),
// so the swap is its own boundary.

// decodedShape is a table's basis: its column order, and the IR type of
// each column whose type came from a read definition.
type decodedShape struct {
	names []string
	types map[string]ir.Type
}

// holdDecodedOrder holds tbl to the table's basis when tbl will not be a
// schema boundary, then makes tbl the basis.
func (r *CDCReader) holdDecodedOrder(qn string, tbl *tableSchema) error {
	cur := projectTableIR(tbl)
	approximate := make(map[string]bool, len(tbl.tableMapTyped))
	for _, n := range tbl.tableMapTyped {
		approximate[n] = true
	}
	basis, hasBasis := r.decodedShapes[qn]
	if tbl.synthesized {
		if !hasBasis && r.decodedShapeBaselineSet {
			return deferredNoBasisRefusal(qn)
		}
		if hasBasis {
			if err := basis.hold(qn, cur, approximate); err != nil {
				return err
			}
		}
	}
	next := decodedShape{names: ir.ColumnNames(cur), types: make(map[string]ir.Type, len(cur.Columns))}
	for _, c := range cur.Columns {
		switch {
		case !approximate[c.Name]:
			next.types[c.Name] = c.Type
		case basis.types[c.Name] != nil:
			// Carried: an approximate type says nothing new about the column.
			next.types[c.Name] = basis.types[c.Name]
		}
	}
	if r.decodedShapes == nil {
		r.decodedShapes = map[string]decodedShape{}
	}
	r.decodedShapes[qn] = next
	return nil
}

// hold refuses a shape (cur) that reorders or retypes any column it
// shares with the basis b.
func (b decodedShape) hold(qn string, cur *ir.Table, approximate map[string]bool) error {
	names := ir.ColumnNames(cur)
	if a, c, flipped := firstOrderFlip(b.names, names); flipped {
		return deferredOrderRefusal(qn, a, c)
	}
	for _, col := range cur.Columns {
		was, known := b.types[col.Name]
		if !known || approximate[col.Name] || ir.SameColumnType(was, col.Type) {
			continue
		}
		return deferredTypeRefusal(qn, col.Name, was, col.Type)
	}
	return nil
}

// firstOrderFlip returns a pair of names both lists hold whose relative
// order differs: a before b in prev, b before a in next.
func firstOrderFlip(prev, next []string) (a, b string, flipped bool) {
	at := make(map[string]int, len(next))
	for i, n := range next {
		at[n] = i
	}
	var shared []string
	for _, n := range prev {
		if _, ok := at[n]; ok {
			shared = append(shared, n)
		}
	}
	for i := 1; i < len(shared); i++ {
		if at[shared[i]] < at[shared[i-1]] {
			return shared[i-1], shared[i], true
		}
	}
	return "", "", false
}

// deferredWhy is what every deferred-shape refusal says about why the
// operator cannot simply apply the change on the target and resume: the
// boundary that would be judged against the target is never emitted.
const deferredWhy = "this event's shape cannot be checked against the target, because a column of it was dropped or " +
	"retyped later in the binlog and its definition is not readable, so no schema boundary will describe it before its " +
	"rows are applied. Refusing before any row of it is decoded. Re-copy the stream: `sluice sync start " +
	"--reset-target-data --yes` with the stream's other flags unchanged for a sync, or a fresh `backup full` for a " +
	"backup chain"

// deferredOrderRefusal is the reader-side COLUMN-ORDER-CHANGED.
func deferredOrderRefusal(qn, a, b string) error {
	return fmt.Errorf("mysql: cdc: %s: %w: the source declared %q before %q at this binlog event and %q before %q "+
		"at the last shape this stream knew. A RENAME COLUMN swap or rotation (the names move, the values stay) and a "+
		"column reorder (the values move with the names) look identical, and only the swap leaves the target's earlier "+
		"rows under the wrong names; %s",
		qn, ir.ErrColumnOrderChanged, b, a, a, b, deferredWhy)
}

// deferredTypeRefusal refuses a column whose type changed under a deferred
// shape: its rows would reach the target under the target's old type,
// which may hold them only by changing them.
func deferredTypeRefusal(qn, col string, was, now ir.Type) error {
	return fmt.Errorf("mysql: cdc: %s: %w: column %q is %v at this binlog event and was %v at the last shape this "+
		"stream knew. Its rows would be applied before the target's column changes type, and a target type that "+
		"cannot hold them exactly may round or truncate them without an error; %s",
		qn, ir.ErrDeferredSchemaChange, col, now, was, deferredWhy)
}

// deferredNoBasisRefusal refuses a deferred shape of a table the stream
// has no typed shape for (on a warm resume: no retained schema history at
// or before the resume position, typically a table that had no row or
// schema change between its copy and the stop).
func deferredNoBasisRefusal(qn string) error {
	return fmt.Errorf("mysql: cdc: %s: %w: the stream holds no typed shape of this table from before this event to "+
		"compare it with; %s", qn, ir.ErrDeferredSchemaChange, deferredWhy)
}

// SetDecodedShapeBaseline hands the reader each table's typed source shape
// at the position it is about to stream from — the schema the cold start
// copied from, the retained schema history at the warm-resume position, a
// backup chain's parent manifest — as the basis [CDCReader.holdDecodedOrder]
// holds the table's first deferred shape to. Once called (even with no
// tables) the reader is strict: a table absent from the baseline refuses
// its first deferred shape. It arms nothing else. A table with no Schema
// takes the reader's bound database. Must be called before
// [StreamChanges]; replaces any earlier baseline.
func (r *CDCReader) SetDecodedShapeBaseline(tables []*ir.Table) {
	r.decodedShapeBaselineSet = true
	r.decodedShapes = make(map[string]decodedShape, len(tables))
	for _, t := range tables {
		if t == nil {
			continue
		}
		schema := t.Schema
		if schema == "" {
			schema = r.schema
		}
		s := decodedShape{names: ir.ColumnNames(t), types: make(map[string]ir.Type, len(t.Columns))}
		for _, c := range t.Columns {
			if c != nil && c.Type != nil {
				s.types[c.Name] = c.Type
			}
		}
		r.decodedShapes[qualifiedName(schema, t.Name)] = s
	}
}
