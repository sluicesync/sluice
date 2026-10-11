// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
)

// decodedOrderBaselineSetter is the optional CDC-reader surface that gives
// a reader each table's source column order at the position it streams
// from (BUG301-FULL-DEFERRED-SWAP). A binlog reader behind the head under
// binlog_row_metadata=FULL can decode an event under a shape that will
// never be a schema boundary, so a RENAME swap carried only by that shape
// reaches no door here; the reader holds the order itself, against the
// previous event it decoded — and, for a table's first event, against
// this baseline. Without one, a stream whose first event of a table is
// such a shape has nothing to compare it with.
//
// Implemented by the MySQL-family binlog reader. The other lanes have no
// deferred shape: Postgres names every column in each Relation message,
// and the VStream lanes were not changed by Bug 301
// (BUG301-VSTREAM-PREMISE).
type decodedOrderBaselineSetter interface {
	SetDecodedOrderBaseline(records []ir.ColumnOrderRecord)
}

// wireDecodedOrderBaseline hands records to r when r holds the order and
// there is a baseline to give.
func wireDecodedOrderBaseline(r ir.CDCReader, records []ir.ColumnOrderRecord) {
	if setter, ok := r.(decodedOrderBaselineSetter); ok && len(records) > 0 {
		setter.SetDecodedOrderBaseline(records)
	}
}

// schemaOrderBaseline is the baseline a schema known to hold at the
// stream's start position gives: a cold start's copied source schema, a
// backup chain's parent manifest, a warm resume's retained history.
func schemaOrderBaseline(tables []*ir.Table) []ir.ColumnOrderRecord {
	out := make([]ir.ColumnOrderRecord, 0, len(tables))
	for _, t := range tables {
		if t != nil {
			out = append(out, ir.ColumnOrderRecord{Schema: t.Schema, Table: t.Name, SourceColumns: ir.ColumnNames(t)})
		}
	}
	return out
}

// schemaTables lists s's tables, or none for a nil schema.
func schemaTables(s *ir.Schema) []*ir.Table {
	if s == nil {
		return nil
	}
	return s.Tables
}

// warmResumeOrderBaseline is a warm resume's baseline: the stream's
// column-order records (written at the cold start and at every accepted
// boundary, so they hold the source order the target was last judged
// against), with the retained schema history at the resume position for
// a table that has no record — a stream started before the records
// existed. A records read that fails yields none: the same read runs
// again in [Streamer.phaseLoadColumnOrders] before any change is applied,
// and is loud there.
func warmResumeOrderBaseline(ctx context.Context, applier ir.ChangeApplier, streamID string, history []*ir.Table) []ir.ColumnOrderRecord {
	var records []ir.ColumnOrderRecord
	if store, ok := applier.(ir.ColumnOrderStore); ok {
		if read, err := store.ReadColumnOrders(ctx, streamID); err == nil {
			records = read
		}
	}
	held := make(map[string]bool, len(records))
	for _, rec := range records {
		held[rec.Table] = true
	}
	for _, rec := range schemaOrderBaseline(history) {
		if !held[rec.Table] {
			records = append(records, rec)
		}
	}
	return records
}
