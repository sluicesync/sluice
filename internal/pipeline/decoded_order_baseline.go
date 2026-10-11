// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import "sluicesync.dev/sluice/internal/ir"

// decodedShapeBaselineSetter is the optional CDC-reader surface that gives
// a reader each table's typed source shape at the position it streams from
// (BUG301-FULL-DEFERRED-SWAP, BUG301-DEFERRED-RETYPE). A binlog reader
// behind the head under binlog_row_metadata=FULL can decode an event under
// a shape that will never be a schema boundary, so a RENAME swap or a type
// change carried only by that shape reaches no door here before its rows
// do; the reader holds the shape itself, against the previous event it
// decoded — and, for a table's first event, against this baseline. Once
// handed one, the reader refuses a table's first deferred shape when the
// baseline lacks the table.
//
// Implemented by the MySQL-family binlog reader. The other lanes have no
// deferred shape: Postgres names and types every column in each Relation
// message, and the VStream lanes were not changed by Bug 301
// (BUG301-VSTREAM-PREMISE).
type decodedShapeBaselineSetter interface {
	SetDecodedShapeBaseline(tables []*ir.Table)
}

// wireDecodedShapeBaseline hands tables to r when r holds deferred shapes.
// A nil baseline means none is known for this open site and leaves the
// reader unarmed; an empty, non-nil one arms it with no table known.
func wireDecodedShapeBaseline(r ir.CDCReader, tables []*ir.Table) {
	if setter, ok := r.(decodedShapeBaselineSetter); ok && tables != nil {
		setter.SetDecodedShapeBaseline(tables)
	}
}

// schemaTables lists s's tables, or nil for a nil schema (a manifest that
// records none: the reader is left unarmed rather than strict over nothing).
func schemaTables(s *ir.Schema) []*ir.Table {
	if s == nil {
		return nil
	}
	return append([]*ir.Table{}, s.Tables...)
}

// warmResumeShapeBaseline is a warm resume's baseline: the retained schema
// history resolved at the resume position — typed, and the source's own
// shape as the reader emitted it. Never nil, so the reader is armed: a
// table with no retained version (typically one with no row or schema
// change between its copy and the stop) has no typed shape to hold a
// deferred event to, and refuses it with the re-copy remedy.
func warmResumeShapeBaseline(history []*ir.Table) []*ir.Table {
	return append([]*ir.Table{}, history...)
}
