// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "sluicesync.dev/sluice/internal/ir"

// The first-touch schema boundary, binlog lane (GC-44 D3).
//
// # The gap
//
// The binlog reader emits an ir.SchemaSnapshot only when a DDL is pending
// (pendingDDLActive). Postgres sends a RelationMessage at each table's first
// row of every stream and VStream a FIELD event, so both of those lanes hand
// the pipeline a boundary per table per resume, and the pipeline's
// first-boundary witness (pipeline/schema_forward_witness.go) checks it
// against the target. The binlog lane handed it nothing unless the resumed
// stream REPLAYED a DDL — so a DDL the stream never replays was never
// checked: one whose position was persisted past before its forward ran
// (an out-of-scope transaction committing after it, then a restart). The
// target kept the pre-DDL column and every following value landed in it.
//
// # The boundary
//
// Armed by the streamer ([CDCReader.ArmFirstTouchSchemaBoundaries], from
// pipeline.Streamer.wireSchemaDeltaArming at every reader-open site), each
// in-scope table's first row of the stream runs maybeSnapshotSchemaB1 as if
// a DDL were pending. The reader's snapshotSig is empty at that point, so
// the true-delta gate passes and the boundary is emitted — anchored at the
// stream's START position, the point the shape held from: any DDL after it
// is replayed and anchors its own boundary.
//
// It is NOT folded into owedSchemaBoundary (GC-43 (r) F1). That set answers
// "may a position past this be persisted without losing a forward"; a first
// touch owes nothing — a restart before it re-arms it on the next stream.
// Unarmed (every non-streamer construction: backup capture, tests, tools),
// the reader behaves exactly as before.

// firstTouchBoundaries is the per-stream first-touch state. The zero value
// is unarmed.
type firstTouchBoundaries struct {
	// armed is set by [CDCReader.ArmFirstTouchSchemaBoundaries] before
	// StreamChanges.
	armed bool

	// anchor is the stream's start position, set when StreamChanges
	// starts the binlog stream.
	anchor ir.Position

	// touched holds every table whose first row this stream has seen.
	touched map[string]struct{}
}

// ArmFirstTouchSchemaBoundaries makes each in-scope table's first row of the
// next stream emit a schema boundary (see the file comment). Must be called
// before [CDCReader.StreamChanges]. Implements
// pipeline.firstTouchBoundaryArmer.
func (r *CDCReader) ArmFirstTouchSchemaBoundaries() {
	r.firstTouch.armed = true
}

// start records the stream's start position and clears the touched set.
// Called once per StreamChanges, after the start position resolves.
func (f *firstTouchBoundaries) start(anchor ir.Position) {
	f.anchor = anchor
	f.touched = map[string]struct{}{}
}

// take reports whether qn's row is its first of an armed stream, and marks
// it touched either way. An out-of-scope table is marked without a
// boundary: nothing it emits reaches the target.
func (f *firstTouchBoundaries) take(qn string, inScope bool) bool {
	if !f.armed || f.touched == nil {
		return false
	}
	if _, seen := f.touched[qn]; seen {
		return false
	}
	f.touched[qn] = struct{}{}
	return inScope
}
