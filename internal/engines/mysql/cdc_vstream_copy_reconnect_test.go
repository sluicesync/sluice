// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtgateservice"

	"sluicesync.dev/sluice/internal/ir"
)

// scriptedStream is a Recv-only fake stream that replays scripted
// (response, error) steps in order. A non-nil error step is returned
// once (e.g. a retriable gRPC status to drive the Phase C reconnect);
// after the script drains it blocks on ctx like a real idle stream.
type scriptedStream struct {
	grpc.ClientStream

	ctx   context.Context
	mu    sync.Mutex
	next  int
	steps []scriptStep
}

type scriptStep struct {
	resp *vtgate.VStreamResponse
	err  error
}

func (s *scriptedStream) Recv() (*vtgate.VStreamResponse, error) {
	s.mu.Lock()
	if s.next < len(s.steps) {
		step := s.steps[s.next]
		s.next++
		s.mu.Unlock()
		return step.resp, step.err
	}
	s.mu.Unlock()
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

// reconnectFakeClient hands out a fresh scriptedStream per VStream call
// and records the requests it was opened with — so a test can assert the
// resume request carried the TablePKs cursor (ADR-0072 Phase C).
type reconnectFakeClient struct {
	vtgateservice.VitessClient // embedded nil: every other method panics

	ctx      context.Context
	mu       sync.Mutex
	streams  [][]scriptStep
	openIdx  int
	requests []*vtgate.VStreamRequest
}

func (c *reconnectFakeClient) VStream(_ context.Context, in *vtgate.VStreamRequest, _ ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, in)
	idx := c.openIdx
	if idx >= len(c.streams) {
		idx = len(c.streams) - 1
	}
	c.openIdx++
	return &scriptedStream{ctx: c.ctx, steps: c.streams[idx]}, nil
}

// TestVStreamSnapshot_CopyReconnectResumesFromCursor pins ADR-0072
// Phase C: a retriable mid-COPY Recv error reconnects the VStream IN
// PLACE from the last-observed cursor (currentVgtid, carrying TablePKs),
// the COPY then completes, and the reconnect request carries the cursor
// back to vtgate (so the scan resumes from the last-copied PK rather
// than restarting at row 0).
func TestVStreamSnapshot_CopyReconnectResumesFromCursor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A VGTID whose shard carries a TablePKs cursor — the position the
	// pump captures just before the stream drops.
	cursorVgtid := &binlogdata.VEvent{
		Type: binlogdata.VEventType_VGTID,
		Vgtid: &binlogdata.VGtid{ShardGtids: []*binlogdata.ShardGtid{{
			Keyspace: "main", Shard: "-", Gtid: "MySQL56/abc:1-50",
			TablePKs: []*binlogdata.TableLastPK{makeTableLastPK(t, "t", "id", 2)},
		}}},
	}

	// Stream 1: FIELD, two rows, the cursor VGTID, then a retriable drop.
	stream1 := []scriptStep{
		{resp: oneEvent(snapFieldEvent("-", &query.Field{Name: "id", Type: query.Type_INT64}))},
		{resp: oneEvent(snapRowEvent("-", "1"))},
		{resp: oneEvent(snapRowEvent("-", "2"))},
		{resp: oneEvent(cursorVgtid)},
		{err: status.Error(codes.Unavailable, "connector reset by peer")},
	}
	// Stream 2 (the reconnect): FIELD re-emit, the remaining row, final
	// VGTID, COPY_COMPLETED.
	stream2 := []scriptStep{
		{resp: oneEvent(snapFieldEvent("-", &query.Field{Name: "id", Type: query.Type_INT64}))},
		{resp: oneEvent(snapRowEvent("-", "3"))},
		{resp: oneEvent(snapVgtidEvent("MySQL56/abc:1-99"))},
		{resp: oneEvent(globalCopyCompleted())},
	}

	// openIdx starts at 1: streams[0] is installed directly as the
	// initial grpcStream (below); the first VStream reconnect call must
	// hand out streams[1].
	client := &reconnectFakeClient{ctx: ctx, streams: [][]scriptStep{stream1, stream2}, openIdx: 1}

	s := newTestSnapshotStream()
	s.client = client
	s.shards = []string{"-"}
	s.reconnectMax = defaultCopyReconnectMax
	s.reconnectBackoffBase = time.Millisecond // fast test
	s.reconnectBackoffCap = 2 * time.Millisecond
	s.copyDone = make(chan struct{})
	s.grpcStream = &scriptedStream{ctx: ctx, steps: stream1}

	streamObj := &ir.SnapshotStream{
		Rows:    &vstreamSnapshotRows{snap: s},
		Changes: &vstreamSnapshotChanges{snap: s},
	}
	go s.copyPump(ctx, cancel, streamObj)

	// Drain table "t" — should see all three rows (2 before the drop, 1
	// after the in-place reconnect): zero loss.
	tbl := &ir.Table{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	ch, err := (&vstreamSnapshotRows{snap: s}).ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var ids []int64
	for row := range ch {
		if id, ok := row["id"].(int64); ok {
			ids = append(ids, id)
		}
	}
	if err := (&vstreamSnapshotRows{snap: s}).Err(); err != nil {
		t.Fatalf("Err after drain (reconnect should have absorbed the drop): %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("consumed %d rows after reconnect; want 3 (zero loss): %v", len(ids), ids)
	}

	// The reconnect must have opened exactly one new stream, and its
	// request must carry the captured TablePKs cursor.
	client.mu.Lock()
	reqs := client.requests
	client.mu.Unlock()
	if len(reqs) != 1 {
		t.Fatalf("reconnect opened %d streams; want exactly 1", len(reqs))
	}
	got := reqs[0].GetVgtid().GetShardGtids()
	if len(got) != 1 {
		t.Fatalf("reconnect request had %d shards; want 1", len(got))
	}
	if got[0].GetGtid() != "MySQL56/abc:1-50" {
		t.Errorf("reconnect resumed from Gtid %q; want the captured cursor MySQL56/abc:1-50", got[0].GetGtid())
	}
	if len(got[0].GetTablePKs()) != 1 || got[0].GetTablePKs()[0].GetTableName() != "t" {
		t.Errorf("reconnect request dropped the TablePKs cursor: %+v", got[0].GetTablePKs())
	}

	// Final position is the post-reconnect VGTID.
	if streamObj.Position.Token == "" {
		t.Error("final snapshot Position not recorded after reconnect + COPY_COMPLETED")
	}
}

// TestVStreamSnapshot_CopyReconnectBudgetExhausted confirms that once the
// in-place reconnect budget is spent, the COPY fails (surfacing to the
// outer runWithRetry backstop) rather than looping forever.
func TestVStreamSnapshot_CopyReconnectBudgetExhausted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Every stream drops immediately with a retriable error.
	dropStream := []scriptStep{{err: status.Error(codes.Unavailable, "draining")}}
	streams := make([][]scriptStep, 0, 8)
	for i := 0; i < 8; i++ {
		streams = append(streams, dropStream)
	}
	client := &reconnectFakeClient{ctx: ctx, streams: streams}

	s := newTestSnapshotStream()
	s.client = client
	s.shards = []string{"-"}
	s.reconnectMax = 3
	s.reconnectBackoffBase = time.Millisecond
	s.reconnectBackoffCap = 2 * time.Millisecond
	s.copyDone = make(chan struct{})
	s.grpcStream = &scriptedStream{ctx: ctx, steps: dropStream}

	streamObj := &ir.SnapshotStream{Rows: &vstreamSnapshotRows{snap: s}, Changes: &vstreamSnapshotChanges{snap: s}}
	go s.copyPump(ctx, cancel, streamObj)

	// Wait for the pump to give up (copyDone closes).
	select {
	case <-s.copyDone:
	case <-ctx.Done():
		t.Fatal("pump did not terminate after exhausting the reconnect budget")
	}
	if s.Err() == nil {
		t.Error("expected a terminal copy error after budget exhaustion; got nil")
	}
	// reconnectMax reconnect attempts → reconnectMax extra streams opened.
	client.mu.Lock()
	n := len(client.requests)
	client.mu.Unlock()
	if n != s.reconnectMax {
		t.Errorf("opened %d reconnect streams; want reconnectMax=%d", n, s.reconnectMax)
	}
}
