// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// schemaSnapshots returns the SchemaSnapshots among changes, in order, and
// the index of the first non-snapshot change.
func schemaSnapshots(changes []ir.Change) (snaps []ir.SchemaSnapshot, firstRow int) {
	firstRow = -1
	for i, c := range changes {
		if s, ok := c.(ir.SchemaSnapshot); ok {
			snaps = append(snaps, s)
			continue
		}
		if _, ok := c.(ir.Insert); ok && firstRow < 0 {
			firstRow = i
		}
	}
	return snaps, firstRow
}

// startArmedStream arms r and records its start the way StreamChanges does.
func startArmedStream(t *testing.T, r *CDCReader) ir.Position {
	t.Helper()
	r.ArmFirstTouchSchemaBoundaries()
	anchor, err := r.positionAt("", 0)
	if err != nil {
		t.Fatalf("anchor: %v", err)
	}
	r.firstTouch.start(anchor)
	return anchor
}

// TestFirstTouchBoundary_ArmedStreamEmitsOncePerTable pins GC-44 D3: an
// armed stream's first row of a table emits a SchemaSnapshot — ahead of the
// row, anchored at the stream's start — with no DDL pending, and only once
// (mutation M4 — disarm — fails this test).
func TestFirstTouchBoundary_ArmedStreamEmitsOncePerTable(t *testing.T) {
	r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")
	anchor := startArmedStream(t, r)
	got := dispatchAll(
		t, r,
		mysqlGTIDEvent(t, 6), queryEvent("BEGIN"), insertRowEvent(1), xidEvent(),
		mysqlGTIDEvent(t, 7), queryEvent("BEGIN"), insertRowEvent(2), xidEvent(),
	)
	snaps, firstRow := schemaSnapshots(got)
	if len(snaps) != 1 {
		t.Fatalf("emitted %d schema boundaries over two rows of one table, want exactly 1: %#v", len(snaps), got)
	}
	if snaps[0].Table != "users" || snaps[0].IR == nil || len(snaps[0].IR.Columns) != 1 {
		t.Errorf("boundary = %+v, want the users table's projection", snaps[0])
	}
	if snaps[0].Position != anchor {
		t.Errorf("boundary anchored at %v, want the stream's start %v", snaps[0].Position, anchor)
	}
	for i, c := range got {
		if _, ok := c.(ir.SchemaSnapshot); ok && firstRow >= 0 && i > firstRow {
			t.Errorf("the boundary arrived after the table's first row (index %d > %d)", i, firstRow)
		}
	}
}

// TestFirstTouchBoundary_UnarmedStreamEmitsNothing: the zero value — every
// non-streamer construction — behaves exactly as before.
func TestFirstTouchBoundary_UnarmedStreamEmitsNothing(t *testing.T) {
	r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")
	got := dispatchAll(t, r, mysqlGTIDEvent(t, 6), queryEvent("BEGIN"), insertRowEvent(1), xidEvent())
	if snaps, _ := schemaSnapshots(got); len(snaps) != 0 {
		t.Fatalf("an unarmed stream emitted %d schema boundaries with no DDL, want none", len(snaps))
	}
}

// TestFirstTouchBoundary_DDLKeepsItsOwnAnchor: a DDL replayed before the
// table's first row anchors the boundary at the DDL, as it always did; the
// first touch is consumed by that same boundary, not emitted twice.
func TestFirstTouchBoundary_DDLKeepsItsOwnAnchor(t *testing.T) {
	r := newStagingReader(t, FlavorVanilla, stagingUUID+":1-5")
	anchor := startArmedStream(t, r)
	got := dispatchAll(
		t, r,
		mysqlGTIDEvent(t, 6), queryEvent("ALTER TABLE users MODIFY id BIGINT"),
		mysqlGTIDEvent(t, 7), queryEvent("BEGIN"), insertRowEvent(1), xidEvent(),
	)
	snaps, _ := schemaSnapshots(got)
	if len(snaps) != 1 {
		t.Fatalf("emitted %d schema boundaries, want 1", len(snaps))
	}
	if snaps[0].Position == anchor {
		t.Error("the DDL's boundary took the stream-start anchor; it must keep the DDL's own position")
	}
}

// TestFirstTouchBoundary_OutOfScopeTakesNoBoundary: a table outside the
// scope predicate is marked touched without a boundary.
func TestFirstTouchBoundary_OutOfScopeTakesNoBoundary(t *testing.T) {
	var f firstTouchBoundaries
	f.armed = true
	f.start(ir.Position{Engine: "mysql", Token: "t"})
	if f.take("app.hidden", false) {
		t.Error("an out-of-scope table's first touch produced a boundary")
	}
	if f.take("app.hidden", true) {
		t.Error("a table's second touch produced a boundary")
	}
	if !f.take("app.shown", true) || f.take("app.shown", true) {
		t.Error("an in-scope table's first touch must produce exactly one boundary")
	}
}
