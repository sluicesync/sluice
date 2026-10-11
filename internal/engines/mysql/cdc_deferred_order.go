// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// BUG301-FULL-DEFERRED-SWAP — a column-order change no boundary will carry.
//
// Under binlog_row_metadata=FULL a reader behind the binlog head decodes
// each event by its own column names (cdc_recorded_shape.go). When one of
// those columns has since been dropped or retyped, the shape is typed in
// part from the TABLE_MAP alone, and such a shape is never a schema
// boundary: the change it records reaches the pipeline only folded into
// the next boundary built from read definitions. A RENAME swap folded that
// way vanishes — `RENAME b TO c, c TO b` then `DROP COLUMN b` arrives as
// `(id,b,c) → (id,c)`, a drop with the surviving name in place, and the
// target keeps its pre-swap values under `c` at exit 0 (measured, MySQL
// 8.0, 8.4, MariaDB 11.4). The pipeline's column-order doors
// (internal/pipeline/column_order_boundary.go) never see it, so the order
// is held here instead: a shape that will not be a boundary must keep
// every pair of the names it shares with the table's last decoded shape in
// the same relative order, or the stream refuses before decoding a row.
//
// What else a deferred shape folds away, measured on MySQL 8.4 (FULL, warm
// resume) — each is caught downstream, so this file refuses only the order:
//
//   - a rename (`RENAME b TO x`, then a DROP of x or of another column):
//     the rows carry a column the target lacks and the apply refuses
//     ("schema drift: the target is missing a column"); a later rename to
//     a retyped x is RESUME-SCHEMA-DIVERGENCE at its boundary;
//   - an ADD followed by a DROP of another column: the same apply refusal;
//   - a swap followed by an ADD: not deferred (every recorded column has a
//     read definition), so the swap is its own boundary and the pipeline
//     refuses it;
//   - a type change folded into a later DROP: the later boundary still
//     shows the retype, but the rows before it apply under the target's
//     old type, so a value that type cannot hold is refused by the target
//     (Error 1264 out of range, DECIMAL-SCALE-EXCEEDED) — and one it
//     coerces silently would not be. That loudness is the target's, not
//     sluice's (BUG301-DEFERRED-RETYPE, filed). When the retyped column is
//     the one dropped, the source no longer has it.
//
// The basis is the shape at the previous decoded event, never the decode
// cache: behind the head the cache holds the catalog as of the read — the
// future — which may already lack the swapped column (priorSig is
// refreshed from it, so it cannot serve). Before the first event the
// basis is the baseline the pipeline hands over
// ([CDCReader.SetDecodedOrderBaseline]): the copied schema on a cold
// start, the stream's column-order records (else its retained history) on
// a warm resume, a backup chain's parent manifest. Not the streamer's
// [CDCReader.SetSchemaSeed]: on a warm resume that is the target's zone
// witness, projected to temporal columns and in the TARGET's order. A
// table with no baseline whose first event this run is a deferred swap is
// the residual: nothing older than the event exists to compare with
// (today: the multi-database and foreign-lineage readers, which take no
// baseline).

// holdDecodedOrder records the order tbl decodes events under and, when
// tbl will not be a schema boundary, refuses a column-order change against
// the previous one.
func (r *CDCReader) holdDecodedOrder(qn string, tbl *tableSchema) error {
	names := make([]string, 0, len(tbl.Columns))
	for _, c := range tbl.Columns {
		names = append(names, c.Name)
	}
	if prev, ok := r.decodedOrder[qn]; ok && tbl.synthesized {
		if a, b, flipped := firstOrderFlip(prev, names); flipped {
			return deferredOrderRefusal(qn, a, b)
		}
	}
	if r.decodedOrder == nil {
		r.decodedOrder = map[string][]string{}
	}
	r.decodedOrder[qn] = names
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

// deferredOrderRefusal is the reader-side COLUMN-ORDER-CHANGED: the same
// class as the pipeline's, with the re-copy remedy only — the operator
// cannot apply the change to the target and resume, because the boundary
// that would be judged against the target is never emitted.
func deferredOrderRefusal(qn, a, b string) error {
	return fmt.Errorf("mysql: cdc: %s: %w: the source declared %q before %q at this binlog event and %q before %q "+
		"at the last one this stream decoded. A RENAME COLUMN swap or rotation (the names move, the values stay) and a "+
		"column reorder (the values move with the names) look identical, and only the swap leaves the target's earlier "+
		"rows under the wrong names; this event's shape cannot be checked against the target, because a column of it "+
		"was dropped or retyped later in the binlog and its definition is not readable. Refusing before any row of it "+
		"is decoded. Re-copy the stream: `sluice sync start --reset-target-data --yes` with the stream's other flags "+
		"unchanged for a sync, or a fresh `backup full` for a backup chain",
		qn, ir.ErrColumnOrderChanged, b, a, a, b)
}

// SetDecodedOrderBaseline hands the reader each table's source column
// order at the position it is about to stream from — the stream's
// column-order records on a warm resume, the copied schema on a cold
// start, a backup chain's parent manifest — as the basis
// [CDCReader.holdDecodedOrder] holds the table's first deferred shape to.
// It arms nothing else. A record with no Schema takes the reader's bound
// database. Must be called before [StreamChanges]; replaces any earlier
// baseline.
func (r *CDCReader) SetDecodedOrderBaseline(records []ir.ColumnOrderRecord) {
	r.decodedOrder = make(map[string][]string, len(records))
	for _, rec := range records {
		if rec.SourceColumns == nil {
			continue
		}
		schema := rec.Schema
		if schema == "" {
			schema = r.schema
		}
		r.decodedOrder[qualifiedName(schema, rec.Table)] = append([]string(nil), rec.SourceColumns...)
	}
}
