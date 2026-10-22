// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
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
// once (e.g. a retriable gRPC status); after the script drains it blocks
// on ctx like a real idle stream.
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

// reconnectFakeClient hands out a fresh scriptedStream per VStream call,
// in call order, and records the requests it was opened with (the reshard
// reopen tests assert their resume positions with it).
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

// tableScriptedClient serves a scripted stream keyed by the Match table of
// the request (the concurrent pump opens its K streams in no fixed order)
// and counts the opens per table.
type tableScriptedClient struct {
	vtgateservice.VitessClient

	ctx     context.Context
	mu      sync.Mutex
	byTable map[string][]scriptStep
	opens   map[string]int
}

func (c *tableScriptedClient) VStream(_ context.Context, in *vtgate.VStreamRequest, _ ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	match := ""
	if rules := in.GetFilter().GetRules(); len(rules) == 1 {
		match = rules[0].GetMatch()
	}
	c.opens[match]++
	return &scriptedStream{ctx: c.ctx, steps: c.byTable[match]}, nil
}

func (c *tableScriptedClient) opened(table string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.opens[table]
}

func copyFieldEvent(table string) *binlogdata.VEvent {
	return &binlogdata.VEvent{Type: binlogdata.VEventType_FIELD, FieldEvent: &binlogdata.FieldEvent{
		TableName: table, Keyspace: "main", Shard: "-",
		Fields: []*query.Field{{Name: "id", Type: query.Type_INT64}},
	}}
}

func copyRowEvent(table, id string) *binlogdata.VEvent {
	return &binlogdata.VEvent{Type: binlogdata.VEventType_ROW, RowEvent: &binlogdata.RowEvent{
		TableName: table, Keyspace: "main", Shard: "-",
		RowChanges: []*binlogdata.RowChange{{After: makeRow([]string{id})}},
	}}
}

// copyCursorEvent is a mid-COPY VGTID carrying table's TablePKs cursor:
// exactly the position an in-place reconnect used to resume from.
func copyCursorEvent(t *testing.T, table string) *binlogdata.VEvent {
	t.Helper()
	return &binlogdata.VEvent{Type: binlogdata.VEventType_VGTID, Vgtid: &binlogdata.VGtid{ShardGtids: []*binlogdata.ShardGtid{{
		Keyspace: "main", Shard: "-", Gtid: "MySQL56/abc:1-50",
		TablePKs: []*binlogdata.TableLastPK{makeTableLastPK(t, table, "id", 2)},
	}}}}
}

// faultedCopyScript is a COPY that delivers two rows and its cursor, then
// fails Recv with fault.
func faultedCopyScript(t *testing.T, table string, fault error) []scriptStep {
	t.Helper()
	return []scriptStep{
		{resp: oneEvent(copyFieldEvent(table))},
		{resp: oneEvent(copyRowEvent(table, "1"))},
		{resp: oneEvent(copyRowEvent(table, "2"))},
		{resp: oneEvent(copyCursorEvent(t, table))},
		{err: fault},
	}
}

// completeCopyScript is a whole single-table COPY.
func completeCopyScript(table string) []scriptStep {
	return []scriptStep{
		{resp: oneEvent(copyFieldEvent(table))},
		{resp: oneEvent(copyRowEvent(table, "1"))},
		{resp: oneEvent(snapVgtidEvent("MySQL56/abc:1-40"))},
		{resp: oneEvent(globalCopyCompleted())},
	}
}

// TestVStreamSnapshot_MidCopyRecvErrorEndsTheCopy pins the withdrawal of
// the ADR-0072 Phase C in-place reconnect (VSTREAM-COPY-RECONNECT-STALE-ROWS)
// on EVERY COPY pump: a Recv error mid-copy ends the copy with a terminal
// snapshot error and re-opens nothing on the faulted table. A re-open from
// the cursor is what kept rows the source deleted during the reconnect
// window on the target: vtgate replays them as catch-up DELETEs inside the
// copy phase, which bufferCopyRow drops, and the CDC tail starts past them.
//
// The matrix is pump × error class. The pumps are the three Recv loops
// that held a reconnect: copyPump (the single-stream opt-out),
// pumpOneTableCopy (auto-shard, the default, and a one-table scope runs it
// too when it is the sequence's only table) and copyStream.pumpTable (the
// concurrent copy, K > 1). A retriable error must stay retriable, because
// the Streamer's runWithRetry is now the recovery (a first cold start
// restarts from scratch, clearing the target); a non-retriable one must
// stay terminal.
func TestVStreamSnapshot_MidCopyRecvErrorEndsTheCopy(t *testing.T) {
	faults := []struct {
		name          string
		err           error
		wantRetriable bool
	}{
		{"retriable", status.Error(codes.Unavailable, "connection reset by peer"), true},
		{"non-retriable", status.Error(codes.InvalidArgument, "bad filter"), false},
	}
	pumps := []struct {
		name string
		// run starts the pump over a COPY whose table "t" faults mid-copy
		// and returns the snapshot plus how many streams were opened on "t"
		// after the initial one.
		run func(t *testing.T, ctx context.Context, cancel context.CancelFunc, fault error) (*vstreamSnapshotStream, func() int)
	}{
		{"single-stream copyPump", func(t *testing.T, ctx context.Context, cancel context.CancelFunc, fault error) (*vstreamSnapshotStream, func() int) {
			client := &reconnectFakeClient{ctx: ctx, streams: [][]scriptStep{completeCopyScript("t")}}
			s := newTestSnapshotStream()
			s.client = client
			s.shards = []string{"-"}
			s.copyTables = []string{"t"}
			s.copyDone = make(chan struct{})
			s.grpcStream = &scriptedStream{ctx: ctx, steps: faultedCopyScript(t, "t", fault)}
			stream := &ir.SnapshotStream{Rows: &vstreamSnapshotRows{snap: s}, Changes: &vstreamSnapshotChanges{snap: s}}
			go s.copyPump(ctx, cancel, stream)
			return s, func() int {
				client.mu.Lock()
				defer client.mu.Unlock()
				return len(client.requests)
			}
		}},
		{"auto-shard pumpOneTableCopy", func(t *testing.T, ctx context.Context, cancel context.CancelFunc, fault error) (*vstreamSnapshotStream, func() int) {
			client := &tableScriptedClient{ctx: ctx, opens: map[string]int{}, byTable: map[string][]scriptStep{
				"t": completeCopyScript("t"), "u": completeCopyScript("u"),
			}}
			s := newTestSnapshotStream()
			s.client = client
			s.shards = []string{"-"}
			s.copyTablesSeq = []string{"t", "u"}
			s.copyTables = []string{"t"}
			s.tableCopyComplete = make(map[string]bool)
			s.copyDone = make(chan struct{})
			// The constructor opens the first table's stream; install it.
			s.grpcStream = &scriptedStream{ctx: ctx, steps: faultedCopyScript(t, "t", fault)}
			stream := &ir.SnapshotStream{Rows: &vstreamSnapshotRows{snap: s}, Changes: &vstreamSnapshotChanges{snap: s}}
			go s.copyPumpAutoShard(ctx, cancel, stream)
			return s, func() int { return client.opened("t") }
		}},
		{"concurrent copyStream.pumpTable", func(t *testing.T, ctx context.Context, cancel context.CancelFunc, fault error) (*vstreamSnapshotStream, func() int) {
			client := &tableScriptedClient{ctx: ctx, opens: map[string]int{}, byTable: map[string][]scriptStep{
				"t": faultedCopyScript(t, "t", fault), "u": completeCopyScript("u"),
			}}
			s := newTestSnapshotStream()
			s.client = client
			s.shards = []string{"-"}
			s.copyTablesSeq = []string{"t", "u"}
			s.copyTables = []string{"t"}
			s.tableCopyComplete = make(map[string]bool)
			s.copyDone = make(chan struct{})
			groups := partitionTablesForStreams(s.copyTablesSeq, 2, nil)
			s.concurrentCopy = true
			s.concurrentGroups = groups
			stream := &ir.SnapshotStream{Rows: &vstreamSnapshotRows{snap: s}, Changes: &vstreamSnapshotChanges{snap: s}}
			go s.copyPumpAutoShardConcurrent(ctx, cancel, stream, groups)
			// The concurrent pump opens "t" itself; count the opens after it.
			return s, func() int { return client.opened("t") - 1 }
		}},
	}

	for _, p := range pumps {
		for _, f := range faults {
			t.Run(p.name+"/"+f.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				s, reopened := p.run(t, ctx, cancel, f.err)

				select {
				case <-s.copyDone:
				case <-time.After(5 * time.Second):
					t.Fatal("the COPY pump did not end after a mid-copy Recv error")
				}
				err := s.Err()
				if err == nil {
					t.Fatal("a mid-copy Recv error left no terminal snapshot error")
				}
				if errors.Is(err, context.Canceled) {
					t.Fatalf("the snapshot error is the cancellation, not the Recv error: %v", err)
				}
				var re ir.RetriableError
				gotRetriable := errors.As(err, &re) && re.Retriable()
				if gotRetriable != f.wantRetriable {
					t.Errorf("snapshot error retriable = %v, want %v (runWithRetry's restart keys on it): %v", gotRetriable, f.wantRetriable, err)
				}
				if n := reopened(); n != 0 {
					t.Errorf("the pump re-opened table t %d time(s) after the Recv error; a mid-copy fault must not resume in place (VSTREAM-COPY-RECONNECT-STALE-ROWS)", n)
				}
			})
		}
	}
}
