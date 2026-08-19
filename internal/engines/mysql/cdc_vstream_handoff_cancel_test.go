// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtgateservice"
)

// ctxCapturingVStreamClient records the context reopenForCDC opens the CDC
// tail with, so the handoff's cancel-wiring can be asserted directly.
type ctxCapturingVStreamClient struct {
	vtgateservice.VitessClient // embedded nil: every other method panics

	mu  sync.Mutex
	ctx context.Context
}

func (c *ctxCapturingVStreamClient) VStream(ctx context.Context, _ *vtgate.VStreamRequest, _ ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
	c.mu.Lock()
	c.ctx = ctx
	c.mu.Unlock()
	return &scriptedStream{ctx: ctx}, nil
}

// TestVStreamSnapshot_ReopenForCDC_InstallsCancellableStreamCtx pins the
// audit-2026-08-19 fix: the auto-shard COPY→CDC handoff must open the tail on a
// FRESH Background-derived cancel context stored as s.grpcCancel, so the
// liveness watchdog's cancelGRPCStream (and CloseFn) actually unwind the tail.
//
// Before the fix the tail rode the caller's ctx while s.grpcCancel still
// pointed at the finished COPY stream: a fired watchdog cancelled a dead
// context and the tail's Recv parked forever — a silent sync freeze on the
// default auto-shard cold-start after a tablet failover. The two directions of
// that bug (tail bound to the caller ctx; s.grpcCancel left on the old stream)
// are each asserted below so the mutation fails both ways.
func TestVStreamSnapshot_ReopenForCDC_InstallsCancellableStreamCtx(t *testing.T) {
	callerCtx, callerCancel := context.WithCancel(context.Background())
	defer callerCancel()

	client := &ctxCapturingVStreamClient{}
	s := newTestSnapshotStream()
	s.client = client
	s.currentVgtid = []shardGtid{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/abc:1-50"}}

	// A pre-existing cancel standing in for the finished COPY stream's
	// context: the handoff must free it and must NOT leave s.grpcCancel
	// pointed at it.
	oldCtx, oldCancel := context.WithCancel(context.Background())
	s.grpcCancel = oldCancel

	if err := s.reopenForCDC(callerCtx); err != nil {
		t.Fatalf("reopenForCDC: %v", err)
	}

	client.mu.Lock()
	tailCtx := client.ctx
	client.mu.Unlock()
	if tailCtx == nil {
		t.Fatal("reopenForCDC never opened the CDC tail (VStream not called)")
	}
	// The tail must NOT ride the caller ctx — the watchdog cancels via
	// s.grpcCancel, and a tail bound to the caller ctx would ignore it and
	// park forever (the wedge).
	if tailCtx == callerCtx {
		t.Fatal("CDC tail bound to the caller ctx — s.grpcCancel cannot cancel it")
	}
	// The previous (finished COPY) stream's context was freed.
	select {
	case <-oldCtx.Done():
	default:
		t.Error("reopenForCDC did not cancel the previous (finished COPY) stream context")
	}
	// s.grpcCancel now cancels THE CDC TAIL, so the watchdog/CloseFn unwind it.
	if s.grpcCancel == nil {
		t.Fatal("s.grpcCancel is nil after handoff — watchdog/Close would be a no-op")
	}
	s.grpcCancel()
	select {
	case <-tailCtx.Done():
	default:
		t.Error("s.grpcCancel() did not cancel the CDC tail's context — the audit-2026-08-19 wedge")
	}
}
