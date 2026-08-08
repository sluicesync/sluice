// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins the VStream reader's join of its event pump — the third instance of the
// "Close tears down state the pump still owns" class (PG CDC reader,
// sqlite-trigger, and now this), found by the 2026-08-08 sibling sweep after
// the second one shipped claiming the class.
//
// This engine's symptom is NOT a nil dereference: the pump reads `stream`, not
// r.conn, so closing the connection under it is benign. What is not benign is
// that the pump writes r.currentVgtid (dispatch's VGTID arm) and r.fields (the
// FIELD arm) with no lock, while the caller goroutine reads and REPLACES both.
// Close made that an unsynchronised access; applyReshardState made it a lost
// update — it cancelled the old pump and immediately overwrote r.shards and
// r.currentVgtid with the post-reshard layout, so a straggling VGTID from the
// old stream could write the OLD position back over the new one and Reopen
// would build its request from that.
//
// So there are two tests, not one: the second is the one with the data
// consequence, and a pin on Close alone would have read as covering it.

package mysql

import (
	"context"
	"testing"
	"time"
)

// blockingPump installs a stand-in pump on r that parks until released, and
// returns the two channels the test drives it with. It mirrors what the real
// pump does on the way out: one more write to the reader state the caller is
// about to replace.
func blockingPump(r *vstreamCDCReader) (parked, release chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	r.streamerCancel = cancel
	r.pumpDone = done

	parked = make(chan struct{})
	release = make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		close(parked)
		<-release
		// The straggling write. Under the pre-fix code this landed AFTER
		// applyReshardState had installed the new layout.
		r.currentVgtid = []shardGtid{{Keyspace: "ks", Shard: "-80", Gtid: "stale-from-the-old-pump"}}
	}()
	return parked, release
}

func TestVStreamClose_JoinsThePump(t *testing.T) {
	r := &vstreamCDCReader{keyspace: "ks"}
	parked, release := blockingPump(r)

	closeReturned := make(chan error, 1)
	go func() { closeReturned <- r.Close() }()

	<-parked

	select {
	case <-closeReturned:
		t.Fatal("Close returned while the event pump was still running: it must join the pump before " +
			"tearing down the connection, or the pump's writes to currentVgtid/fields race the caller")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return after the pump exited")
	}

	if r.pumpDone != nil {
		t.Error("Close left pumpDone set; a second Close would block on an already-closed stream")
	}
	// "Safe to call multiple times" is part of Close's contract.
	if err := r.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestApplyReshardState_JoinsThePumpBeforeReplacingTheLayout is the half with
// the data consequence. The assertion is on the VALUE that survives: the
// post-reshard position must be the journal's, not the old pump's straggler.
func TestApplyReshardState_JoinsThePumpBeforeReplacingTheLayout(t *testing.T) {
	r := &vstreamCDCReader{
		keyspace: "ks",
		shards:   []string{"0"},
	}
	parked, release := blockingPump(r)

	resh := &ShardLayoutChangedError{
		NewShards: []shardGtid{
			{Keyspace: "ks", Shard: "-80", Gtid: "post-reshard"},
			{Keyspace: "ks", Shard: "80-", Gtid: "post-reshard"},
		},
	}

	returned := make(chan error, 1)
	go func() { returned <- r.applyReshardState(resh) }()

	<-parked
	select {
	case <-returned:
		t.Fatal("applyReshardState returned while the old pump was still running: it replaces r.shards " +
			"and r.currentVgtid, and a straggling VGTID event writes the OLD layout's position over the new one")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-returned:
		if err != nil {
			t.Fatalf("applyReshardState: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("applyReshardState did not return after the old pump exited")
	}

	if len(r.currentVgtid) != 2 {
		t.Fatalf("currentVgtid has %d entries after the reshard; want 2 (the journal's new layout). "+
			"Got %v — one entry means the old pump's straggling write won", len(r.currentVgtid), r.currentVgtid)
	}
	for _, g := range r.currentVgtid {
		if g.Gtid != "post-reshard" {
			t.Fatalf("currentVgtid carries %q; want every shard at the journal's post-reshard GTID. "+
				"The old pump's position survived the layout swap — the stream would resume at the "+
				"PRE-reshard coordinate on the NEW shard set", g.Gtid)
		}
	}
}
