// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Unit pins for the v0.76.0 lease GC sweep (task #21). The matrix
// covers the two-condition safety semantics:
//
//   - GC of a row where ALL streams are past the anchor → deleted.
//   - GC of a row where ONE stream is still behind the anchor → NOT deleted.
//   - GC of a HELD (not yet applied) row → never deleted regardless of position.
//   - Legacy rows without an anchor (HasAnchor=false) → defensively retained.
//   - Empty lease table → no-op, no error.
//   - No streams → conservatively skips (per loud-failure tenet).
//
// The fakes here are scoped to GC — they don't drive the lease state
// machine, only model the post-Apply read paths the sweeper consumes.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// fakeGCLister enumerates a fixed set of rows.
type fakeGCLister struct {
	rows []ir.ShardConsolidationLeaseRow
	err  error
}

func (f *fakeGCLister) ListLeases(_ context.Context) ([]ir.ShardConsolidationLeaseRow, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make([]ir.ShardConsolidationLeaseRow, len(f.rows))
	copy(out, f.rows)
	return out, nil
}

// fakeGCDeleter records every DeleteLease call.
type fakeGCDeleter struct {
	mu      sync.Mutex
	deleted []string
	failOn  map[string]error
}

func (f *fakeGCDeleter) DeleteLease(_ context.Context, tableName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.failOn[tableName]; ok {
		return err
	}
	f.deleted = append(f.deleted, tableName)
	return nil
}

func (f *fakeGCDeleter) deletedSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.deleted))
	copy(out, f.deleted)
	return out
}

// fakeGCPosReader implements just enough of ir.ChangeApplier to satisfy
// the LeaseGCDeps.PosReader path — only ListStreams is consulted. Other
// methods panic loudly so a bug that calls them surfaces immediately.
type fakeGCPosReader struct {
	streams []ir.StreamStatus
	err     error
}

func (f *fakeGCPosReader) Apply(context.Context, string, <-chan ir.Change) error {
	panic("fakeGCPosReader.Apply: not used in GC tests")
}
func (f *fakeGCPosReader) EnsureControlTable(context.Context) error { return nil }
func (f *fakeGCPosReader) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (f *fakeGCPosReader) ListStreams(_ context.Context) ([]ir.StreamStatus, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.streams, nil
}
func (f *fakeGCPosReader) RequestStop(context.Context, string) error        { return nil }
func (f *fakeGCPosReader) ClearStopRequested(context.Context, string) error { return nil }

// numericOrderer is a trivial PositionOrderer for tests: positions
// encode an integer that increases monotonically. PositionAtOrAfter(p,
// anchor) is true iff p >= anchor. Concrete engines use opaque tokens
// (PG LSN, MySQL GTID); this synthetic shape exercises the comparison
// logic without dragging in either engine.
type numericOrderer struct{}

func (numericOrderer) PositionAtOrAfter(p, anchor ir.Position) (bool, error) {
	if p.Engine != "test" || anchor.Engine != "test" {
		return false, errors.New("test orderer: non-test engine in position")
	}
	pn, err := strconv.Atoi(p.Token)
	if err != nil {
		return false, fmt.Errorf("test orderer: parse p: %w", err)
	}
	an, err := strconv.Atoi(anchor.Token)
	if err != nil {
		return false, fmt.Errorf("test orderer: parse anchor: %w", err)
	}
	return pn >= an, nil
}

func testPos(n int) ir.Position {
	return ir.Position{Engine: "test", Token: strconv.Itoa(n)}
}

func appliedLease(table string, anchor int) ir.ShardConsolidationLeaseRow {
	return ir.ShardConsolidationLeaseRow{
		TargetTableFullName:  table,
		LeaseHolderStreamID:  "stream-a",
		LeaseExpiresAt:       time.Now(),
		HasLeaseExpiresAt:    true,
		DDLText:              "ALTER",
		DDLChecksum:          "deadbeef",
		AppliedSchemaVersion: 1,
		AppliedAt:            time.Now(),
		HasAppliedAt:         true,
		AnchorPosition:       testPos(anchor),
		HasAnchor:            true,
	}
}

func heldLease(table string) ir.ShardConsolidationLeaseRow {
	row := appliedLease(table, 0)
	row.HasAppliedAt = false
	row.AppliedAt = time.Time{}
	row.AnchorPosition = ir.Position{}
	row.HasAnchor = false
	return row
}

func legacyAppliedLease(table string) ir.ShardConsolidationLeaseRow {
	row := appliedLease(table, 0)
	row.AnchorPosition = ir.Position{}
	row.HasAnchor = false
	return row
}

func TestSweepConsolidationLeases_AllStreamsPastAnchor_Deleted(t *testing.T) {
	t.Parallel()
	lister := &fakeGCLister{rows: []ir.ShardConsolidationLeaseRow{
		appliedLease("public.users", 100),
	}}
	deleter := &fakeGCDeleter{}
	pos := &fakeGCPosReader{streams: []ir.StreamStatus{
		{StreamID: "shard-a", Position: testPos(150)},
		{StreamID: "shard-b", Position: testPos(200)},
		{StreamID: "shard-c", Position: testPos(100)}, // exactly at anchor → at-or-after = true
	}}
	deleted, err := SweepConsolidationLeases(context.Background(), LeaseGCDeps{
		Lister: lister, Deleter: deleter, PosReader: pos, Orderer: numericOrderer{},
	})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if deleted != 1 {
		t.Errorf("deleted = %d, want 1", deleted)
	}
	got := deleter.deletedSnapshot()
	if len(got) != 1 || got[0] != "public.users" {
		t.Errorf("deletedSnapshot = %v, want [public.users]", got)
	}
}

func TestSweepConsolidationLeases_OneStreamBehind_NotDeleted(t *testing.T) {
	t.Parallel()
	lister := &fakeGCLister{rows: []ir.ShardConsolidationLeaseRow{
		appliedLease("public.users", 100),
	}}
	deleter := &fakeGCDeleter{}
	pos := &fakeGCPosReader{streams: []ir.StreamStatus{
		{StreamID: "shard-a", Position: testPos(150)},
		{StreamID: "shard-b", Position: testPos(50)}, // behind anchor
	}}
	deleted, err := SweepConsolidationLeases(context.Background(), LeaseGCDeps{
		Lister: lister, Deleter: deleter, PosReader: pos, Orderer: numericOrderer{},
	})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if deleted != 0 {
		t.Errorf("deleted = %d, want 0", deleted)
	}
	if got := deleter.deletedSnapshot(); len(got) != 0 {
		t.Errorf("deletedSnapshot = %v, want empty", got)
	}
}

func TestSweepConsolidationLeases_HeldRow_NeverDeleted(t *testing.T) {
	t.Parallel()
	lister := &fakeGCLister{rows: []ir.ShardConsolidationLeaseRow{
		heldLease("public.users"),
	}}
	deleter := &fakeGCDeleter{}
	pos := &fakeGCPosReader{streams: []ir.StreamStatus{
		{StreamID: "shard-a", Position: testPos(99999)},
	}}
	deleted, err := SweepConsolidationLeases(context.Background(), LeaseGCDeps{
		Lister: lister, Deleter: deleter, PosReader: pos, Orderer: numericOrderer{},
	})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if deleted != 0 {
		t.Errorf("deleted = %d, want 0 (HELD never GC'd)", deleted)
	}
}

func TestSweepConsolidationLeases_LegacyRowNoAnchor_Retained(t *testing.T) {
	t.Parallel()
	lister := &fakeGCLister{rows: []ir.ShardConsolidationLeaseRow{
		legacyAppliedLease("public.users"),
	}}
	deleter := &fakeGCDeleter{}
	pos := &fakeGCPosReader{streams: []ir.StreamStatus{
		{StreamID: "shard-a", Position: testPos(99999)},
	}}
	deleted, err := SweepConsolidationLeases(context.Background(), LeaseGCDeps{
		Lister: lister, Deleter: deleter, PosReader: pos, Orderer: numericOrderer{},
	})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if deleted != 0 {
		t.Errorf("deleted = %d, want 0 (legacy NULL-anchor row retained)", deleted)
	}
}

func TestSweepConsolidationLeases_EmptyLeaseTable_NoOp(t *testing.T) {
	t.Parallel()
	lister := &fakeGCLister{rows: nil}
	deleter := &fakeGCDeleter{}
	pos := &fakeGCPosReader{streams: []ir.StreamStatus{
		{StreamID: "shard-a", Position: testPos(100)},
	}}
	deleted, err := SweepConsolidationLeases(context.Background(), LeaseGCDeps{
		Lister: lister, Deleter: deleter, PosReader: pos, Orderer: numericOrderer{},
	})
	if err != nil {
		t.Fatalf("Sweep on empty table: %v", err)
	}
	if deleted != 0 {
		t.Errorf("deleted = %d, want 0", deleted)
	}
}

func TestSweepConsolidationLeases_NoStreams_Conservative(t *testing.T) {
	t.Parallel()
	lister := &fakeGCLister{rows: []ir.ShardConsolidationLeaseRow{
		appliedLease("public.users", 100),
	}}
	deleter := &fakeGCDeleter{}
	pos := &fakeGCPosReader{streams: nil}
	deleted, err := SweepConsolidationLeases(context.Background(), LeaseGCDeps{
		Lister: lister, Deleter: deleter, PosReader: pos, Orderer: numericOrderer{},
	})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if deleted != 0 {
		t.Errorf("deleted = %d, want 0 (no-streams = conservative skip)", deleted)
	}
}

func TestSweepConsolidationLeases_MixedFleet(t *testing.T) {
	t.Parallel()
	// Three rows: one safe to GC (all streams past), one with a stream
	// behind, one HELD. Sweep should delete exactly the first.
	lister := &fakeGCLister{rows: []ir.ShardConsolidationLeaseRow{
		appliedLease("public.safe_to_gc", 50),
		appliedLease("public.has_stream_behind", 200),
		heldLease("public.still_held"),
	}}
	deleter := &fakeGCDeleter{}
	pos := &fakeGCPosReader{streams: []ir.StreamStatus{
		{StreamID: "shard-a", Position: testPos(150)},
		{StreamID: "shard-b", Position: testPos(100)}, // behind 200, ahead of 50
	}}
	deleted, err := SweepConsolidationLeases(context.Background(), LeaseGCDeps{
		Lister: lister, Deleter: deleter, PosReader: pos, Orderer: numericOrderer{},
	})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if deleted != 1 {
		t.Errorf("deleted = %d, want 1", deleted)
	}
	got := deleter.deletedSnapshot()
	if len(got) != 1 || got[0] != "public.safe_to_gc" {
		t.Errorf("deletedSnapshot = %v, want [public.safe_to_gc]", got)
	}
}

func TestSweepConsolidationLeases_DeleterError_AccumulatesContinues(t *testing.T) {
	t.Parallel()
	// Two GC-eligible rows; the deleter fails on the first one.
	// The sweep should attempt both, accumulate the error from the
	// first, and successfully delete the second.
	lister := &fakeGCLister{rows: []ir.ShardConsolidationLeaseRow{
		appliedLease("public.fails", 50),
		appliedLease("public.succeeds", 60),
	}}
	deleter := &fakeGCDeleter{
		failOn: map[string]error{
			"public.fails": errors.New("synthetic delete failure"),
		},
	}
	pos := &fakeGCPosReader{streams: []ir.StreamStatus{
		{StreamID: "shard-a", Position: testPos(200)},
	}}
	deleted, err := SweepConsolidationLeases(context.Background(), LeaseGCDeps{
		Lister: lister, Deleter: deleter, PosReader: pos, Orderer: numericOrderer{},
	})
	if err == nil {
		t.Fatal("expected error from sweep (one delete failed)")
	}
	if deleted != 1 {
		t.Errorf("deleted = %d, want 1 (second row should still succeed)", deleted)
	}
	got := deleter.deletedSnapshot()
	if len(got) != 1 || got[0] != "public.succeeds" {
		t.Errorf("deletedSnapshot = %v, want [public.succeeds]", got)
	}
}

func TestSweepConsolidationLeases_NoDeleter_NoOp(t *testing.T) {
	t.Parallel()
	// Engine doesn't implement the deleter surface — sweep is a no-op
	// (rows accumulate; no error).
	lister := &fakeGCLister{rows: []ir.ShardConsolidationLeaseRow{
		appliedLease("public.users", 100),
	}}
	pos := &fakeGCPosReader{streams: []ir.StreamStatus{
		{StreamID: "shard-a", Position: testPos(200)},
	}}
	deleted, err := SweepConsolidationLeases(context.Background(), LeaseGCDeps{
		Lister: lister, Deleter: nil, PosReader: pos, Orderer: numericOrderer{},
	})
	if err != nil {
		t.Fatalf("Sweep (no deleter): %v", err)
	}
	if deleted != 0 {
		t.Errorf("deleted = %d, want 0", deleted)
	}
}

func TestSweepConsolidationLeases_OrdererError_RetainsRow(t *testing.T) {
	t.Parallel()
	lister := &fakeGCLister{rows: []ir.ShardConsolidationLeaseRow{
		appliedLease("public.users", 100),
	}}
	deleter := &fakeGCDeleter{}
	// Stream position uses a non-test engine → numericOrderer rejects it.
	pos := &fakeGCPosReader{streams: []ir.StreamStatus{
		{StreamID: "shard-a", Position: ir.Position{Engine: "other", Token: "150"}},
	}}
	deleted, err := SweepConsolidationLeases(context.Background(), LeaseGCDeps{
		Lister: lister, Deleter: deleter, PosReader: pos, Orderer: numericOrderer{},
	})
	if err != nil {
		t.Fatalf("Sweep should accumulate orderer error as per-row retain, not propagate: %v", err)
	}
	if deleted != 0 {
		t.Errorf("deleted = %d, want 0 (orderer error retains the row)", deleted)
	}
}
