// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins the VStream SNAPSHOT stream's join of its COPY pumps — the last
// instance of the "Close tears down state the pump still owns" class the
// 2026-08-08 sibling sweep filed as an openFinding (the CDC readers were
// fixed in that sweep; this one was deferred as a chunk of its own because
// the COPY pumps coordinate through a sync.Cond + copyComplete flag, so the
// join had to be placed inside that protocol without deadlocking a
// backpressured pump).
//
// Two unit pins, deterministic and Docker-free, one per hazard:
//
//   - JoinsCopyPump: close() must not return while a COPY pump it launched
//     is still running — otherwise it closes+nils s.conn under a straggler
//     still Recv'ing on the stream / reconnecting on the client.
//   - WakesBackpressuredCopyPump: the pump backpressure waits block in
//     s.cond.Wait(), which does NOT observe ctx-cancel — so close() must
//     flip the terminal state (err+copyComplete) they break out on, not
//     merely Broadcast, or the join hangs forever. This is the deadlock the
//     join would otherwise expose.
//
// The real-cluster end-to-end pin (mid-copy close in a loop, no panic, no
// deadlock) is TestVStream_SnapshotCloseJoinsCopyPumpsMidCopy, gated behind
// `integration && vstream`.

package mysql

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestSnapshotStreamClose_JoinsCopyPump pins that close() joins a COPY pump
// launched via goPump before it returns. A stand-in pump parks until
// released and, on the way out, writes shared state close() is about to tear
// down — the straggler the join must order after.
func TestSnapshotStreamClose_JoinsCopyPump(t *testing.T) {
	s := &vstreamSnapshotStream{keyspace: "ks"}
	s.cond = sync.NewCond(&s.mu)

	ctx, cancel := context.WithCancel(context.Background())
	s.grpcCancel = cancel

	parked := make(chan struct{})
	release := make(chan struct{})
	straggled := make(chan struct{})
	s.goPump(func() {
		<-ctx.Done() // close() cancels grpcCancel
		close(parked)
		<-release
		// The straggling write a non-joining close would race: shared state
		// under mu, exactly what a real COPY pump touches on its way out.
		s.mu.Lock()
		s.currentVgtid = []shardGtid{{Keyspace: "ks", Shard: "0", Gtid: "straggler"}}
		s.mu.Unlock()
		close(straggled)
	})

	closeReturned := make(chan error, 1)
	go func() { closeReturned <- s.close() }()

	<-parked
	select {
	case <-closeReturned:
		t.Fatal("close returned while the COPY pump was still running: it must join the pump before " +
			"closing s.conn, or a straggling Recv/reconnect races the connection teardown")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("close did not return after the pump exited")
	}

	select {
	case <-straggled:
	default:
		t.Fatal("close returned before the pump's straggling write completed: the join is not ordering the pump's exit")
	}

	// "Safe to call multiple times" is part of close's contract.
	if err := s.close(); err != nil {
		t.Errorf("second close: %v", err)
	}
}

// TestSnapshotStreamClose_WakesBackpressuredCopyPump pins the deadlock half:
// a pump parked in enqueue-backpressure cond.Wait() only exits when the
// terminal state (err/copyComplete) is set, because cond.Wait does not
// observe ctx-cancel. close() must flip that state (cancelCopyForShutdown),
// not merely Broadcast — otherwise the woken pump re-checks the same over-cap
// condition and re-parks, and the join hangs.
func TestSnapshotStreamClose_WakesBackpressuredCopyPump(t *testing.T) {
	s := &vstreamSnapshotStream{keyspace: "ks"}
	s.cond = sync.NewCond(&s.mu)

	_, cancel := context.WithCancel(context.Background())
	s.grpcCancel = cancel

	s.goPump(func() {
		// Mirror enqueueRowLocked's backpressure park: loop on a condition
		// only a terminal state clears, blocking in cond.Wait (ctx-blind).
		s.mu.Lock()
		for s.err == nil && !s.copyComplete {
			s.cond.Wait()
		}
		s.mu.Unlock()
	})

	done := make(chan error, 1)
	go func() { done <- s.close() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("close hung: a COPY pump parked in cond.Wait backpressure was not woken. close must flip the " +
			"terminal state (err+copyComplete) a bare Broadcast can't — otherwise the pump re-parks and the join deadlocks")
	}

	// The terminal state the wake set must be observable (a mid-copy close is
	// a loud cancellation, never a silent clean finish).
	s.mu.Lock()
	gotErr, gotComplete := s.err, s.copyComplete
	s.mu.Unlock()
	if gotErr == nil {
		t.Error("cancelCopyForShutdown left s.err nil on a mid-copy close; a concurrent consumer would see a clean finish, not a cancellation")
	}
	if !gotComplete {
		t.Error("cancelCopyForShutdown left copyComplete false; the backpressure wait never breaks out")
	}
}

// TestSnapshotStreamClose_NoOpTerminalStateAfterCleanCopy pins that a close()
// AFTER a healthy COPY_COMPLETED does not overwrite the clean terminal state
// or the finished position — cancelCopyForShutdown is guarded on
// !copyComplete precisely so teardown of a successful copy stays clean.
func TestSnapshotStreamClose_NoOpTerminalStateAfterCleanCopy(t *testing.T) {
	s := &vstreamSnapshotStream{keyspace: "ks"}
	s.cond = sync.NewCond(&s.mu)
	// Simulate a completed, error-free COPY.
	s.copyComplete = true

	if err := s.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		t.Errorf("close overwrote the clean terminal state after a healthy copy: s.err = %v (want nil)", s.err)
	}
}
