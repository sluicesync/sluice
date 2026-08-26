// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"

	"sluicesync.dev/sluice/internal/ir"
)

// These tests are the per-PR, image-independent executor for the item-72(b)
// reshard-window ride-out (audit 2026-08-26 T-1/C-2). The only end-to-end
// executor of [reopenReshardWindow] — TestVitessReshard_RelaxSkewReshardMidStream
// — is quarantined on a test-image residual, which left the loop's budget
// arithmetic, err-clear, and pump relaunch executing in NO running test. Here a
// scripted fake VStream client (the same [reconnectFakeClient]/[scriptedStream]
// harness the ADR-0072 Phase C reconnect pins use) injects the exact
// post-SwitchTraffic window shape (codes.NotFound + "tablet … is either down or
// nonexistent") and drives the Streamer's reopen loop the way
// [pipeline.Streamer] does: drain the closed channel → ReopenAfterReshard →
// repeat.
//
// Both reopen implementors are covered (the sibling-sweep enumeration):
//   - [vstreamSnapshotChanges.ReopenAfterReshard] (cold-start lane) — has the
//     in-process PRIMARY-pinned ride-out; tested for ride-out-and-deliver AND
//     for loud budget exhaustion below.
//   - [vstreamCDCReader.ReopenAfterReshard] (warm/steady-state lane) — is
//     single-shot BY DESIGN TODAY; its window behavior is pinned honestly in
//     TestVStreamCDCReader_ReshardWindow_SingleShotFallsToOuterRetry, which
//     names the C-2 divergence rather than papering over it.

// reshardWindowErr is the exact wire shape vtgate answers with while a
// freshly-resharded shard's PRIMARY is not yet routable (item 72(b) ground
// truth, extended-suites run 32804472095): codes.NotFound with the
// tablet-health wording [isReshardPrimaryUnroutableError] carves out.
func reshardWindowErr() error {
	return status.Error(codes.NotFound,
		"failed to get tablet connection to zone1-0000000201: target: main.-80.primary: tablet uid:201 is either down or nonexistent")
}

// reshardJournalEvent is a 1→2 reshard JOURNAL for keyspace "main": shard "-"
// retires in favor of "-80"/"80-", each stamped with the seam GTID the reopen
// must seed from.
func reshardJournalEvent() *binlogdata.VEvent {
	return &binlogdata.VEvent{
		Type: binlogdata.VEventType_JOURNAL,
		Journal: &binlogdata.Journal{
			Id:            1,
			MigrationType: binlogdata.MigrationType_SHARDS,
			Participants:  []*binlogdata.KeyspaceShard{{Keyspace: "main", Shard: "-"}},
			ShardGtids: []*binlogdata.ShardGtid{
				{Keyspace: "main", Shard: "-80", Gtid: "MySQL56/" + uuidA + ":1-100"},
				{Keyspace: "main", Shard: "80-", Gtid: "MySQL56/" + uuidA + ":1-100"},
			},
		},
	}
}

// newReshardWindowHarness builds a snapshot stream whose COPY phase runs a
// minimal one-table script ending in a reshard JOURNAL on the CDC tail, wired
// to a scripted client that serves the reopen streams. Mirrors the
// newStreamingHarness/reconnect-test construction.
func newReshardWindowHarness(ctx context.Context, cancel context.CancelFunc, client *reconnectFakeClient) (*vstreamSnapshotStream, *ir.SnapshotStream) {
	initial := []scriptStep{
		{resp: oneEvent(snapFieldEvent("-", &query.Field{Name: "id", Type: query.Type_INT64}))},
		{resp: oneEvent(snapRowEvent("-", "1"))},
		{resp: oneEvent(snapVgtidEvent("MySQL56/" + uuidA + ":1-50"))},
		{resp: oneEvent(globalCopyCompleted())},
		// The CDC tail's first Recv (same stream, resumed in place) surfaces
		// the reshard journal.
		{resp: oneEvent(reshardJournalEvent())},
	}

	s := newTestSnapshotStream()
	s.client = client
	s.shards = []string{"-"}
	s.copyDone = make(chan struct{})
	s.grpcStream = &scriptedStream{ctx: ctx, steps: initial}

	stream := &ir.SnapshotStream{
		Rows:    &vstreamSnapshotRows{snap: s},
		Changes: &vstreamSnapshotChanges{snap: s},
	}
	go s.copyPump(ctx, cancel, stream)
	return s, stream
}

// drainCopyThenHitJournal runs the harness through COPY and into the CDC
// phase until the reshard journal closes the change channel, returning the
// CDC half ready for the ReopenAfterReshard loop.
func drainCopyThenHitJournal(ctx context.Context, t *testing.T, stream *ir.SnapshotStream) *vstreamSnapshotChanges {
	t.Helper()
	tbl := &ir.Table{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	rows, err := stream.Rows.ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	n := 0
	for range rows {
		n++
	}
	if n != 1 {
		t.Fatalf("COPY rows = %d; want 1", n)
	}

	changes := stream.Changes.(*vstreamSnapshotChanges)
	out, err := changes.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	awaitChannelClose(t, out, "CDC tail after the JOURNAL")
	if !errors.Is(changes.Err(), ErrShardLayoutChanged) {
		t.Fatalf("Err() after JOURNAL = %v; want ShardLayoutChangedError", changes.Err())
	}
	return changes
}

// awaitChannelClose drains ch until it closes, failing the test if any change
// arrives or the close never lands.
func awaitChannelClose(t *testing.T, ch <-chan ir.Change, what string) {
	t.Helper()
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				return
			}
			t.Fatalf("%s delivered an unexpected change: %#v", what, c)
		case <-time.After(10 * time.Second):
			t.Fatalf("%s never closed its channel", what)
		}
	}
}

// TestVStreamSnapshot_ReshardWindowRideOut_RidesOutAndDelivers is the T-1
// running pin for [reopenReshardWindow]'s happy path: the reshard-follow
// reopen's first Recv races the post-SwitchTraffic window twice, the ride-out
// reopens IN PLACE on PRIMARY within budget (attempt arithmetic + err-clear +
// pump relaunch all execute), the settled stream delivers a row, and the
// serving event resets the window budget to a full tank.
func TestVStreamSnapshot_ReshardWindowRideOut_RidesOutAndDelivers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	healthy := []scriptStep{
		{resp: oneEvent(snapFieldEvent("-80", &query.Field{Name: "id", Type: query.Type_INT64}))},
		{resp: oneEvent(snapRowEvent("-80", "42"))},
	}
	client := &reconnectFakeClient{ctx: ctx, streams: [][]scriptStep{
		{{err: reshardWindowErr()}}, // reshard-follow reopen: first Recv races the window
		{{err: reshardWindowErr()}}, // ride-out attempt 1: still inside the window
		healthy,                     // ride-out attempt 2: the primary settled
	}}

	s, stream := newReshardWindowHarness(ctx, cancel, client)
	changes := drainCopyThenHitJournal(ctx, t, stream)

	// Streamer-loop turn 1: the journal → reopenAfterReshard. The reopened
	// tail's first Recv hits the window error.
	out, wasReshard, err := changes.ReopenAfterReshard(ctx)
	if err != nil || !wasReshard {
		t.Fatalf("reshard-follow reopen: wasReshard=%v err=%v; want true, nil", wasReshard, err)
	}
	awaitChannelClose(t, out, "reopened tail racing the window")
	if !isReshardPrimaryWindowError(changes.Err()) {
		t.Fatalf("Err() after the raced reopen = %v; want the retriable primary-window shape", changes.Err())
	}

	// Turn 2: the window recovery's first in-place PRIMARY reopen — still
	// inside the window.
	out, wasReshard, err = changes.ReopenAfterReshard(ctx)
	if err != nil || !wasReshard {
		t.Fatalf("ride-out attempt 1: wasReshard=%v err=%v; want true, nil", wasReshard, err)
	}
	awaitChannelClose(t, out, "ride-out attempt 1")
	if !isReshardPrimaryWindowError(changes.Err()) {
		t.Fatalf("Err() after ride-out attempt 1 = %v; want the retriable primary-window shape", changes.Err())
	}

	// Turn 3: the window is over; the ride-out reopen lands on the settled
	// PRIMARY and the stream delivers.
	out, wasReshard, err = changes.ReopenAfterReshard(ctx)
	if err != nil || !wasReshard {
		t.Fatalf("ride-out attempt 2: wasReshard=%v err=%v; want true, nil", wasReshard, err)
	}
	select {
	case c, ok := <-out:
		if !ok {
			t.Fatalf("settled stream closed without delivering; Err()=%v", changes.Err())
		}
		ins, isInsert := c.(ir.Insert)
		if !isInsert || ins.Table != "t" {
			t.Fatalf("settled stream delivered %#v; want an Insert on table t", c)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("settled stream never delivered the row — the ride-out did not recover the tail")
	}
	if err := changes.Err(); err != nil {
		t.Fatalf("Err() after the settled delivery = %v; want nil (the ride-out cleared the window error)", err)
	}

	// The serving event must reset the window budget so a much-later blip on
	// this long-lived stream starts from a full tank (the [pump] reset arm).
	s.mu.Lock()
	retries := s.reshardWindowRetries
	s.mu.Unlock()
	if retries != 0 {
		t.Errorf("reshardWindowRetries after a serving event = %d; want 0 (reset-on-serving)", retries)
	}

	// Every reopen in the recovery — the reshard-follow AND both ride-out
	// attempts — must pin PRIMARY (a REPLICA fallback is the non-converging
	// loop item 72(b) closed), and the reshard-follow must seed from the
	// journal-stamped new layout.
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.requests) != 3 {
		t.Fatalf("VStream reopen calls = %d; want 3", len(client.requests))
	}
	for i, req := range client.requests {
		if req.GetTabletType() != topodata.TabletType_PRIMARY {
			t.Errorf("reopen request %d TabletType = %v; want PRIMARY", i, req.GetTabletType())
		}
	}
	seed := client.requests[0].GetVgtid().GetShardGtids()
	if len(seed) != 2 || seed[0].GetShard() != "-80" || seed[1].GetShard() != "80-" {
		t.Errorf("reshard-follow reopen seeded %v; want the journal's -80/80- layout", seed)
	}
}

// TestVStreamSnapshot_ReshardWindowRideOut_BudgetExhaustsLoudly is the T-1
// bounded-budget pin: a window that never ends exhausts
// [maxReshardWindowRetries] and falls back to the normal settle path with the
// retriable error still cached — LOUD, never a silent spin and never a
// swallowed error. Exhaustion must also open no further stream.
func TestVStreamSnapshot_ReshardWindowRideOut_BudgetExhaustsLoudly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := &reconnectFakeClient{ctx: ctx, streams: [][]scriptStep{
		{{err: reshardWindowErr()}}, // reshard-follow reopen races the window
		{{err: reshardWindowErr()}}, // ride-out attempt maxReshardWindowRetries: still the window
	}}

	s, stream := newReshardWindowHarness(ctx, cancel, client)
	changes := drainCopyThenHitJournal(ctx, t, stream)

	out, wasReshard, err := changes.ReopenAfterReshard(ctx)
	if err != nil || !wasReshard {
		t.Fatalf("reshard-follow reopen: wasReshard=%v err=%v; want true, nil", wasReshard, err)
	}
	awaitChannelClose(t, out, "reopened tail racing the window")

	// Fast-forward the budget to its last attempt rather than sleeping
	// through 15 real backoffs (~14s): the increment→check boundary
	// arithmetic still executes on both sides below — attempt
	// maxReshardWindowRetries rides out, attempt maxReshardWindowRetries+1
	// exhausts.
	s.mu.Lock()
	s.reshardWindowRetries = maxReshardWindowRetries - 1
	s.mu.Unlock()

	out, wasReshard, err = changes.ReopenAfterReshard(ctx)
	if err != nil || !wasReshard {
		t.Fatalf("ride-out attempt at the budget boundary: wasReshard=%v err=%v; want true, nil", wasReshard, err)
	}
	awaitChannelClose(t, out, "ride-out attempt at the budget boundary")

	out, wasReshard, err = changes.ReopenAfterReshard(ctx)
	if err != nil {
		t.Fatalf("exhausted ride-out returned err=%v; want nil (settle path)", err)
	}
	if wasReshard || out != nil {
		t.Fatalf("exhausted ride-out: wasReshard=%v out=%v; want false, nil (fall back to the normal settle → warm-resume path)", wasReshard, out)
	}

	// The still-cached window error is what the Streamer settles LOUDLY: it
	// must remain retriable (ADR-0038 takes over) and never be cleared by the
	// failed recovery.
	var re ir.RetriableError
	if !errors.As(changes.Err(), &re) || !isReshardPrimaryWindowError(changes.Err()) {
		t.Fatalf("Err() after exhaustion = %v; want the cached retriable primary-window error", changes.Err())
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.requests) != 2 {
		t.Errorf("VStream reopen calls = %d; want 2 (exhaustion must not open another stream)", len(client.requests))
	}
}

// TestVStreamCDCReader_ReshardWindow_SingleShotFallsToOuterRetry pins the
// STANDALONE (warm/steady-state) reader's ACTUAL behavior under the identical
// scripted window the snapshot lane rides out above — audit 2026-08-26 C-2.
//
// CONFIRMED DIVERGENCE, pinned deliberately rather than "fixed" here: unlike
// [vstreamSnapshotChanges.ReopenAfterReshard], the standalone
// [vstreamCDCReader.ReopenAfterReshard] has NO in-process window ride-out. Its
// reshard Reopen pins PRIMARY (item 72(b), TestBuildReshardReopenRequest_
// PinsPrimary) but is SINGLE-SHOT: when the reopened tail's first Recv races
// the window, the reader caches the retriable primary-window error and the
// next ReopenAfterReshard reports wasReshard=false — handing recovery to the
// OUTER ADR-0038 warm-resume, which constructs a FRESH standalone reader on
// the ADR-0072 REPLICA default. Whether that converges depends on the retry
// budget outlasting the window plus the new replicas settling; reconciling the
// two lanes onto one ride-out mechanism is a design decision left open (C-2).
// This test pins the current contract — the reader surfaces a retriable error
// the Streamer WILL retry, loudly and boundedly — so any change to either half
// (the single-shot shape, or the retriable classification) fails here rather
// than shipping silently.
func TestVStreamCDCReader_ReshardWindow_SingleShotFallsToOuterRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := &reconnectFakeClient{ctx: ctx, streams: [][]scriptStep{
		{{resp: oneEvent(reshardJournalEvent())}}, // the steady-state tail hits the journal
		{{err: reshardWindowErr()}},               // the PRIMARY reopen races the window
	}}
	r := &vstreamCDCReader{
		keyspace: "main",
		shards:   []string{"-"},
		fields:   make(map[string][]*query.Field),
		client:   client,
	}
	defer r.Close() //nolint:errcheck // fake client; nothing to close but the pump join

	out, err := r.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	awaitChannelClose(t, out, "steady-state tail after the JOURNAL")
	if !errors.Is(r.Err(), ErrShardLayoutChanged) {
		t.Fatalf("Err() after JOURNAL = %v; want ShardLayoutChangedError", r.Err())
	}

	// Turn 1: the reshard-follow Reopen — PRIMARY-pinned, seeded from the
	// journal — whose first Recv races the window.
	out, wasReshard, err := r.ReopenAfterReshard(ctx)
	if err != nil || !wasReshard {
		t.Fatalf("reshard-follow reopen: wasReshard=%v err=%v; want true, nil", wasReshard, err)
	}
	awaitChannelClose(t, out, "reopened tail racing the window")

	// Turn 2: the divergence. The snapshot lane would now ride the window out
	// in place on PRIMARY; the standalone lane reports "not a reshard" and
	// leaves the retriable window error for the outer streamer retry.
	out, wasReshard, err = r.ReopenAfterReshard(ctx)
	if err != nil {
		t.Fatalf("second ReopenAfterReshard returned err=%v; want nil", err)
	}
	if wasReshard || out != nil {
		t.Fatalf("second ReopenAfterReshard: wasReshard=%v out=%v; want false, nil (single-shot: the window is settled by the OUTER retry, not in-process)", wasReshard, out)
	}
	var re ir.RetriableError
	if !errors.As(r.Err(), &re) || !isReshardPrimaryWindowError(r.Err()) {
		t.Fatalf("Err() after the raced reopen = %v; want the retriable primary-window shape (the outer ADR-0038 retry's signal)", r.Err())
	}

	// The reopen itself must have pinned PRIMARY and seeded the journal
	// layout; and no third stream may have been opened in-process.
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.requests) != 2 {
		t.Fatalf("VStream calls = %d; want 2 (initial tail + single-shot reopen)", len(client.requests))
	}
	if got := client.requests[1].GetTabletType(); got != topodata.TabletType_PRIMARY {
		t.Errorf("reshard reopen TabletType = %v; want PRIMARY", got)
	}
	seed := client.requests[1].GetVgtid().GetShardGtids()
	if len(seed) != 2 || seed[0].GetShard() != "-80" || seed[1].GetShard() != "80-" {
		t.Errorf("reshard reopen seeded %v; want the journal's -80/80- layout", seed)
	}
}
