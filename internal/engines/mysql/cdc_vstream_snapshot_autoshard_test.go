// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtgateservice"

	"sluicesync.dev/sluice/internal/ir"
)

// fakeVitessClient is a Recv-only client-level fake (ADR-0095 auto-shard
// tests). The auto-shard COPY driver reopens the stream once per table
// (and once for the CDC handoff) via client.VStream(...), so a test that
// exercises the per-table loop needs N+1 scripted streams, not the single
// grpcStream the streaming harness injects. Each VStream() call pops the
// next scripted response set and returns a fresh fakeVStreamClient over
// it; it also records the Filter rules of each request so the test can
// assert the per-table scoping and the keyspace-wide CDC handoff.
//
// Embeds the VitessClient interface so only VStream needs a body; any
// other method would panic (the snapshot path never calls them).
type fakeVitessClient struct {
	vtgateservice.VitessClient

	ctx context.Context

	mu       sync.Mutex
	next     int
	scripts  [][]*vtgate.VStreamResponse
	requests []*vtgate.VStreamRequest
}

func (c *fakeVitessClient) VStream(_ context.Context, req *vtgate.VStreamRequest, _ ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, req)
	idx := c.next
	c.next++
	var resp []*vtgate.VStreamResponse
	if idx < len(c.scripts) {
		resp = c.scripts[idx]
	}
	return &fakeVStreamClient{ctx: c.ctx, resp: resp}, nil
}

// requestFilterTables returns the unqualified Match table of each
// recorded VStream request (or "/.*/"" for the keyspace-wide CDC handoff).
func (c *fakeVitessClient) requestMatches() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.requests))
	for _, r := range c.requests {
		rules := r.GetFilter().GetRules()
		if len(rules) == 1 {
			out = append(out, rules[0].GetMatch())
		} else {
			out = append(out, "<multi>")
		}
	}
	return out
}

// perTableCopyScript builds the scripted COPY responses for ONE
// single-table VStream: a FIELD event, n ROW events, the table's snapshot
// VGTID, and the global COPY_COMPLETED (a single-table COPY emits exactly
// one global terminator). shard is "-" (unsharded). The field/row helpers
// hardcode TableName "t"; here we override TableName so each table's
// FIELD/ROW carry the right name (the dispatcher keys the field cache and
// rowBuffer by table name).
func perTableCopyScript(table, gtid string, n int) []*vtgate.VStreamResponse {
	field := &binlogdata.VEvent{
		Type: binlogdata.VEventType_FIELD,
		FieldEvent: &binlogdata.FieldEvent{
			TableName: table,
			Keyspace:  "main",
			Shard:     "-",
			Fields:    []*query.Field{{Name: "id", Type: query.Type_INT64}},
		},
	}
	out := []*vtgate.VStreamResponse{oneEvent(field)}
	for i := 0; i < n; i++ {
		row := &binlogdata.VEvent{
			Type: binlogdata.VEventType_ROW,
			RowEvent: &binlogdata.RowEvent{
				TableName:  table,
				Keyspace:   "main",
				Shard:      "-",
				RowChanges: []*binlogdata.RowChange{{After: makeRow([]string{itoa(i)})}},
			},
		}
		out = append(out, oneEvent(row))
	}
	out = append(out, oneEvent(snapVgtidEvent(gtid)), oneEvent(globalCopyCompleted()))
	return out
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}

// newAutoShardHarness wires an auto-shard snapshot stream around a
// client-level fake that serves one scripted stream per table plus one
// for the CDC handoff. It constructs the stream the way
// openVStreamSnapshotStreamFrom does in auto-shard mode (first stream
// scoped to table[0], copyTablesSeq set), then spawns the auto-shard
// pump. Returns the snap, the SnapshotStream, the fake client (for
// request assertions), and a cancel.
func newAutoShardHarness(t *testing.T, tables []string, scripts [][]*vtgate.VStreamResponse, capBytes int64) (*vstreamSnapshotStream, *ir.SnapshotStream, *fakeVitessClient, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	client := &fakeVitessClient{ctx: ctx, scripts: scripts}
	// The constructor would have consumed the FIRST script for table[0]'s
	// initial stream via client.VStream; mirror that here so the pump's
	// per-table reopen pops script[1], script[2], ... and the CDC handoff
	// pops the last.
	first, _ := client.VStream(ctx, &vtgate.VStreamRequest{
		Filter: &binlogdata.Filter{Rules: vstreamCopyFilterRules([]string{tables[0]})},
	})

	s := newTestSnapshotStream()
	if capBytes > 0 {
		s.maxBufferBytes = capBytes
	}
	s.client = client
	s.shards = []string{"-"}
	s.copyTablesSeq = tables
	s.copyTables = []string{tables[0]}
	s.tableCopyComplete = make(map[string]bool)
	s.copyDone = make(chan struct{})
	s.grpcStream = first

	stream := &ir.SnapshotStream{
		Rows:    &vstreamSnapshotRows{snap: s},
		Changes: &vstreamSnapshotChanges{snap: s},
	}
	// Mirror OpenSnapshotStream: wire the COPY-completion barrier the
	// cold-start handoff joins (via stream.WaitCopyComplete) before it
	// reads stream.Position. Without this the harness can't exercise the
	// fix path.
	stream.WaitCopyCompleteFn = s.waitCopyComplete
	go s.copyPumpAutoShard(ctx, cancel, stream)
	return s, stream, client, cancel
}

// TestVStreamSnapshot_AutoShard_PerTableThenStitch is the ADR-0095
// end-to-end unit pin: a two-table keyspace copies each table as its own
// single-table COPY (constant memory, no interleave), each ReadRows
// drains its table fully, the captured handoff Position is the per-shard
// GTID-set MINIMUM of the two per-table snapshots, and the CDC handoff
// opens a fresh KEYSPACE-WIDE stream from that stitched position.
func TestVStreamSnapshot_AutoShard_PerTableThenStitch(t *testing.T) {
	tables := []string{"users", "orders"}
	// users finishes COPY at :1-100, orders at :1-300 (later). The stitch
	// must pick :1-100 (the min) as the CDC resume position.
	usersGTID := "MySQL56/" + uuidA + ":1-100"
	ordersGTID := "MySQL56/" + uuidA + ":1-300"
	scripts := [][]*vtgate.VStreamResponse{
		perTableCopyScript("users", usersGTID, 3),
		perTableCopyScript("orders", ordersGTID, 2),
		// CDC handoff stream: no events, just parks (drained → blocks).
		{},
	}

	s, stream, client, cancel := newAutoShardHarness(t, tables, scripts, 0)
	defer cancel()

	ctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ccancel()

	// Drain table 1 (users) — the orchestrator's first ReadRows.
	usersTbl := &ir.Table{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	ch, err := stream.Rows.ReadRows(ctx, usersTbl)
	if err != nil {
		t.Fatalf("ReadRows(users): %v", err)
	}
	got := 0
	for range ch {
		got++
	}
	if got != 3 {
		t.Fatalf("users rows = %d; want 3", got)
	}
	if err := stream.Rows.Err(); err != nil {
		t.Fatalf("Err after users drain: %v", err)
	}

	// Drain table 2 (orders) — the auto-shard pump advances to it only
	// after users completes, so this exercises the per-table reopen.
	ordersTbl := &ir.Table{Name: "orders", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	ch2, err := stream.Rows.ReadRows(ctx, ordersTbl)
	if err != nil {
		t.Fatalf("ReadRows(orders): %v", err)
	}
	got2 := 0
	for range ch2 {
		got2++
	}
	if got2 != 2 {
		t.Fatalf("orders rows = %d; want 2", got2)
	}
	if err := stream.Rows.Err(); err != nil {
		t.Fatalf("Err after orders drain: %v", err)
	}

	// Wait for the auto-shard pump to finish (copyDone closed by the
	// driver) so the stitched Position is recorded.
	select {
	case <-s.copyDone:
	case <-time.After(5 * time.Second):
		t.Fatal("copy pump did not finish")
	}
	if err := s.Err(); err != nil {
		t.Fatalf("pump error: %v", err)
	}

	// The captured handoff Position must be the per-shard MIN (users,
	// :1-100), NOT the max (orders, :1-300). Selecting the max would skip
	// (P_users, P_orders] for users — silent loss.
	wantPos, err := encodeVStreamPos([]shardGtid{{Keyspace: "main", Shard: "-", Gtid: usersGTID}})
	if err != nil {
		t.Fatalf("encodeVStreamPos: %v", err)
	}
	if stream.Position.Token != wantPos.Token {
		t.Fatalf("stitched Position token = %q; want the MIN %q (not the max %q)",
			stream.Position.Token, wantPos.Token, ordersGTID)
	}

	// CDC handoff: StreamChanges must open a fresh KEYSPACE-WIDE stream
	// from the stitched position.
	if _, err := stream.Changes.StreamChanges(ctx, stream.Position); err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// Request audit: stream 0 = users (Match users), stream 1 = orders
	// (Match orders), stream 2 = CDC handoff (keyspace-wide "/.*/").
	matches := client.requestMatches()
	if len(matches) != 3 {
		t.Fatalf("VStream call count = %d (%v); want 3 (users COPY, orders COPY, CDC handoff)", len(matches), matches)
	}
	if matches[0] != "users" {
		t.Errorf("stream[0] Match = %q; want %q (per-table COPY)", matches[0], "users")
	}
	if matches[1] != "orders" {
		t.Errorf("stream[1] Match = %q; want %q (per-table COPY)", matches[1], "orders")
	}
	if matches[2] != "/.*/" {
		t.Errorf("stream[2] Match = %q; want %q (keyspace-wide CDC handoff)", matches[2], "/.*/")
	}
}

// TestVStreamSnapshot_AutoShard_WaitCopyCompleteOrdersPosition pins the
// fix for the Position/copyDone handoff race (ADR-0095 / ADR-0099): on
// the auto-shard path each table's ReadRows channel closes on a PER-TABLE
// signal (tableCopyComplete), so the LAST ReadRows returning does NOT
// guarantee the producer goroutine has stitched and written the
// CDC-resume Position. The cold-start handoff MUST therefore join the
// producer's completion barrier (ir.SnapshotStream.WaitCopyComplete —
// copyDone, closed only after finishCopyAutoShard writes Position under
// mu) BEFORE it reads stream.Position. Without the join the handoff races
// the write and can read an EMPTY Position — the wrong CDC start
// position, a potential silent gap (the bug a K=2 -race resume test
// surfaced as a DATA RACE + "resumed handoff Position empty").
//
// This test asserts the post-WaitCopyComplete contract: after the
// barrier returns, stream.Position is non-empty and equals the stitched
// per-shard MIN. The accompanying CI -race job is what proves the data
// race itself is gone; this pin guards the empty/wrong-Position
// correctness regression and that the barrier hook is wired.
func TestVStreamSnapshot_AutoShard_WaitCopyCompleteOrdersPosition(t *testing.T) {
	tables := []string{"users", "orders"}
	usersGTID := "MySQL56/" + uuidA + ":1-100"
	ordersGTID := "MySQL56/" + uuidA + ":1-300"
	scripts := [][]*vtgate.VStreamResponse{
		perTableCopyScript("users", usersGTID, 3),
		perTableCopyScript("orders", ordersGTID, 2),
		{}, // CDC handoff stream (unused here).
	}

	s, stream, _, cancel := newAutoShardHarness(t, tables, scripts, 0)
	defer cancel()

	ctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ccancel()

	// The handoff barrier MUST be wired by the constructor (mirrored in
	// the harness): a nil hook would silently degrade the join to a no-op
	// and re-expose the race (the zero-value-default trap).
	if stream.WaitCopyCompleteFn == nil {
		t.Fatal("WaitCopyCompleteFn is nil; the auto-shard handoff barrier is not wired — the Position/copyDone race is unguarded")
	}

	// Drain every table's ReadRows to EOF — exactly what runBulkCopy does
	// before the handoff. On the auto-shard path this can complete BEFORE
	// the producer writes Position (per-table close signal).
	for _, name := range tables {
		tbl := &ir.Table{Name: name, Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
		ch, err := stream.Rows.ReadRows(ctx, tbl)
		if err != nil {
			t.Fatalf("ReadRows(%s): %v", name, err)
		}
		for range ch { //nolint:revive // drain to EOF; row count asserted elsewhere
		}
		if err := stream.Rows.Err(); err != nil {
			t.Fatalf("Err after %s drain: %v", name, err)
		}
	}

	// Join the producer's completion barrier — the load-bearing edge the
	// handoff relies on. After this returns, Position is established.
	if err := stream.WaitCopyComplete(ctx); err != nil {
		t.Fatalf("WaitCopyComplete: %v", err)
	}
	if err := s.Err(); err != nil {
		t.Fatalf("pump error: %v", err)
	}

	// Post-barrier: Position must be non-empty AND the stitched per-shard
	// MIN (users :1-100, not orders :1-300). An empty token here is the
	// exact bug — the handoff would persist a zero anchor and start CDC
	// from the wrong position.
	if stream.Position.Token == "" {
		t.Fatal("stream.Position is EMPTY after WaitCopyComplete; the handoff would start CDC from a zero anchor (silent-gap bug)")
	}
	wantPos, err := encodeVStreamPos([]shardGtid{{Keyspace: "main", Shard: "-", Gtid: usersGTID}})
	if err != nil {
		t.Fatalf("encodeVStreamPos: %v", err)
	}
	if stream.Position.Token != wantPos.Token {
		t.Fatalf("Position token after WaitCopyComplete = %q; want the stitched MIN %q", stream.Position.Token, wantPos.Token)
	}
}

// TestVStreamSnapshot_AutoShard_BoundedMemory pins that auto-shard copies
// stay bounded even when the TOTAL row volume across tables dwarfs the
// byte cap: because exactly one table is in flight, the buffer never
// exceeds the cap by more than one in-flight row, and crucially the
// not-yet-consumed table NEVER triggers the ADR-0071 multi-table-
// interleave loud refusal (the bug this ADR fixes).
func TestVStreamSnapshot_AutoShard_BoundedMemory(t *testing.T) {
	const (
		nPerTable = 500
		capBytes  = 4 * 1024
	)
	tables := []string{"a", "b", "c"}
	scripts := [][]*vtgate.VStreamResponse{
		perTableCopyScript("a", "MySQL56/"+uuidA+":1-10", nPerTable),
		perTableCopyScript("b", "MySQL56/"+uuidA+":1-20", nPerTable),
		perTableCopyScript("c", "MySQL56/"+uuidA+":1-30", nPerTable),
		{},
	}

	s, stream, _, cancel := newAutoShardHarness(t, tables, scripts, capBytes)
	defer cancel()

	ctx, ccancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer ccancel()

	var peak int64
	for _, name := range tables {
		tbl := &ir.Table{Name: name, Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
		ch, err := stream.Rows.ReadRows(ctx, tbl)
		if err != nil {
			t.Fatalf("ReadRows(%s): %v", name, err)
		}
		got := 0
		for range ch {
			s.mu.Lock()
			if s.bufferedBytes > peak {
				peak = s.bufferedBytes
			}
			s.mu.Unlock()
			got++
		}
		if err := stream.Rows.Err(); err != nil {
			// The ADR-0071 multi-table-interleave refusal would surface
			// here — its ABSENCE is the point of this test.
			t.Fatalf("Err after %s drain (auto-shard must NOT loud-refuse): %v", name, err)
		}
		if got != nPerTable {
			t.Fatalf("%s rows = %d; want %d", name, got, nPerTable)
		}
	}

	// Bounded: never more than the cap + one row's slack.
	if peak > int64(capBytes)*4 {
		t.Errorf("peak buffered bytes = %d; want bounded near cap %d — auto-shard memory not bounded", peak, capBytes)
	}
}
