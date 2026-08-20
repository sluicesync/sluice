// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtgate"

	"sluicesync.dev/sluice/internal/ir"
)

// infiniteRowStream is a Recv-only fake that delivers one FIELD event, then the
// same ROW event forever, so the post-COPY CDC pump keeps producing row
// changes. recvCount lets a test detect when the pump has PARKED on an unread
// send: once the pump blocks sending to a full out channel it stops calling
// Recv, so recvCount goes stable. Recv honours the stream ctx (bound to
// grpcCancel), like a real stream.
type infiniteRowStream struct {
	grpc.ClientStream

	ctx       context.Context
	field     *vtgate.VStreamResponse
	row       *vtgate.VStreamResponse
	sentField bool
	recvCount atomic.Int64
}

func (s *infiniteRowStream) Recv() (*vtgate.VStreamResponse, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	default:
	}
	s.recvCount.Add(1)
	if !s.sentField {
		s.sentField = true
		return s.field, nil
	}
	return s.row, nil
}

// waitRecvStable blocks until the stream's Recv count has stopped climbing for
// ~two samples — i.e. the pump has parked on an unread out send and can make no
// further progress. Fails the test if it never stabilizes.
func waitRecvStable(t *testing.T, s *infiniteRowStream) {
	t.Helper()
	prev := int64(-1)
	stable := 0
	for i := 0; i < 200; i++ { // ~2s ceiling
		n := s.recvCount.Load()
		if n == prev && n > 0 {
			stable++
			if stable >= 3 {
				return
			}
		} else {
			stable = 0
			prev = n
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("CDC pump never parked on a send (recvCount kept moving at %d) — the fake/dispatch shape is wrong, not a join test", prev)
}

// TestVStreamSnapshot_CloseJoinsSendBlockedCDCPump pins the audit-2026-08-19
// pump-join roster closure (the 4th implementor): close() must JOIN the
// post-COPY CDC pump even when it is parked on a SEND to a full out channel —
// the case grpcCancel alone cannot free, because the CDC pump's send() selects
// on the pump's own (caller-derived) context, not on grpcCancel's streamCtx.
// The out channel is UNBUFFERED and never read, so once the pump reaches its
// first send it is deterministically send-parked (recvCount goes stable) before
// close() runs. Mutation-verified in both directions: removing s.joinCDCPump()
// from close() leaves cdcPumpDone never closed (a leak); suppressing the
// cancel() inside joinCDCPump makes close() hang on the send-parked pump.
func TestVStreamSnapshot_CloseJoinsSendBlockedCDCPump(t *testing.T) {
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	field := &binlogdata.VEvent{
		Type: binlogdata.VEventType_FIELD,
		FieldEvent: &binlogdata.FieldEvent{
			TableName: "t", Keyspace: "main", Shard: "-",
			Fields: []*query.Field{{Name: "id", Type: query.Type_INT64}},
		},
	}
	row := &binlogdata.VEvent{
		Type: binlogdata.VEventType_ROW,
		RowEvent: &binlogdata.RowEvent{
			TableName: "t", Keyspace: "main", Shard: "-",
			RowChanges: []*binlogdata.RowChange{{After: makeRow([]string{"1"})}},
		},
	}
	stream := &infiniteRowStream{ctx: streamCtx, field: oneEvent(field), row: oneEvent(row)}

	s := newTestSnapshotStream()
	s.shards = []string{"-"}
	s.grpcStream = stream
	s.grpcCancel = streamCancel
	// s.conn stays nil — close() skips conn.Close and returns nil.

	out := make(chan ir.Change) // UNBUFFERED, never read: the first send parks the pump
	s.launchCDCPump(context.Background(), out)

	waitRecvStable(t, stream) // the pump is now blocked on an unread send

	done := make(chan error, 1)
	go func() { done <- s.close() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("close() did not return — the send-blocked CDC pump was not joined (cdcPumpCancel must free a send park; grpcCancel cannot)")
	}
	select {
	case <-s.cdcPumpDone:
	default:
		t.Error("cdcPumpDone not closed after close() returned — the CDC pump was not JOINED")
	}
}

// TestVStreamSnapshot_JoinCDCPumpFreesSendBlockedPump pins joinCDCPump's own
// contract deterministically, without the pump's Recv/dispatch machinery: given
// a goroutine parked in send() on the tracked derived context, joinCDCPump must
// CANCEL that context (freeing the send) and then WAIT for the goroutine to
// exit. Mutation: suppressing the cancel() leaves send() blocked forever, so
// joinCDCPump hangs — caught by the timeout.
func TestVStreamSnapshot_JoinCDCPumpFreesSendBlockedPump(t *testing.T) {
	s := newTestSnapshotStream()
	pumpCtx, pumpCancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	s.cdcPumpDone = done
	s.cdcPumpCancel = pumpCancel

	unread := make(chan ir.Change) // no reader — send() blocks until its ctx cancels
	go func() {
		defer close(done)
		_ = send(pumpCtx, unread, ir.Insert{Table: "t"})
	}()

	joined := make(chan struct{})
	go func() { s.joinCDCPump(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("joinCDCPump did not return — it must cancel cdcPumpCancel to free a send-blocked pump, then wait for it")
	}
}

// TestVStreamSnapshot_CloseJoinsRecvBlockedCDCPump is the sibling park: an idle
// CDC pump blocked in Recv (no events) must also be joined. grpcCancel unblocks
// the Recv; the join then waits for the exit.
func TestVStreamSnapshot_CloseJoinsRecvBlockedCDCPump(t *testing.T) {
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	stream := &scriptedStream{ctx: streamCtx} // no steps: Recv blocks on streamCtx

	s := newTestSnapshotStream()
	s.shards = []string{"-"}
	s.grpcStream = stream
	s.grpcCancel = streamCancel

	out := make(chan ir.Change, 1)
	s.launchCDCPump(context.Background(), out)

	done := make(chan error, 1)
	go func() { done <- s.close() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("close() did not return — the Recv-blocked CDC pump was not joined")
	}
	select {
	case <-s.cdcPumpDone:
	default:
		t.Error("cdcPumpDone not closed after close() returned")
	}
}
