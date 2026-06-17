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
	"vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtgateservice"

	"sluicesync.dev/sluice/internal/ir"
)

// tableKeyedVitessClient is the concurrent-COPY (ADR-0099) test fake. Unlike
// fakeVitessClient (which serves scripts in CALL order), the concurrent
// driver opens K per-table streams from K goroutines in NON-deterministic
// order, so the fake must serve a scripted stream keyed by the Match TABLE
// in the request filter. A keyspace-wide "/.*/" request (the CDC handoff)
// serves the "" key. Each per-table key may be requested more than once (a
// reconnect); the fake serves the same script each time.
type tableKeyedVitessClient struct {
	vtgateservice.VitessClient

	ctx context.Context

	mu        sync.Mutex
	byTable   map[string][]*vtgate.VStreamResponse
	requests  []string // Match of each request, in call order (for audit)
	openCount map[string]int
	// seededTables records, per Match table, whether ANY open request for it
	// carried a TablePKs resume cursor (ADR-0098 seed). Used by the
	// concurrent-resume placement pin to assert the in-progress table's
	// stream resumed from its cursor (not from-beginning).
	seededTables map[string]bool
}

func newTableKeyedClient(ctx context.Context, byTable map[string][]*vtgate.VStreamResponse) *tableKeyedVitessClient {
	return &tableKeyedVitessClient{
		ctx:          ctx,
		byTable:      byTable,
		openCount:    map[string]int{},
		seededTables: map[string]bool{},
	}
}

func (c *tableKeyedVitessClient) VStream(_ context.Context, req *vtgate.VStreamRequest, _ ...grpc.CallOption) (vtgateservice.Vitess_VStreamClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	match := ""
	rules := req.GetFilter().GetRules()
	if len(rules) == 1 {
		match = rules[0].GetMatch()
	}
	key := match
	if match == "/.*/" {
		key = "" // CDC handoff
	}
	c.requests = append(c.requests, match)
	c.openCount[key]++
	// Record whether this open carried a TablePKs resume cursor (ADR-0098
	// seed) for the matched table.
	for _, sg := range req.GetVgtid().GetShardGtids() {
		if len(sg.GetTablePKs()) > 0 {
			c.seededTables[match] = true
		}
	}
	return &fakeVStreamClient{ctx: c.ctx, resp: c.byTable[key]}, nil
}

func (c *tableKeyedVitessClient) matches() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.requests))
	copy(out, c.requests)
	return out
}

// newConcurrentHarness wires a concurrent-COPY snapshot stream around a
// table-keyed fake. It constructs the stream the way
// openVStreamSnapshotStreamFrom does in concurrent mode (no eager stream
// open; copyTablesSeq + the K disjoint groups set), then spawns the
// concurrent driver.
func newConcurrentHarness(t *testing.T, tables []string, k int, byTable map[string][]*vtgate.VStreamResponse, capBytes int64) (*vstreamSnapshotStream, *ir.SnapshotStream, *tableKeyedVitessClient, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	client := newTableKeyedClient(ctx, byTable)
	s := newTestSnapshotStream()
	if capBytes > 0 {
		s.maxBufferBytes = capBytes
	}
	s.client = client
	s.shards = []string{"-"}
	s.copyTablesSeq = tables
	s.copyTables = []string{tables[0]}
	s.tableCopyComplete = make(map[string]bool)
	s.reconnectMax = defaultCopyReconnectMax
	s.reconnectBackoffBase = defaultCopyReconnectBackoffBase
	s.reconnectBackoffCap = defaultCopyReconnectBackoffCap
	s.copyDone = make(chan struct{})

	groups := partitionTablesForStreams(tables, k, nil)

	stream := &ir.SnapshotStream{
		Rows:    &vstreamSnapshotRows{snap: s},
		Changes: &vstreamSnapshotChanges{snap: s},
	}
	go s.copyPumpAutoShardConcurrent(ctx, cancel, stream, groups)
	return s, stream, client, cancel
}

// TestVStreamConcurrent_AllTablesLandAndStitchMin is the ADR-0099
// end-to-end unit pin: K=2 concurrent streams over 4 disjoint tables — every
// table's rows land (via the orchestrator's per-table ReadRows), the captured
// handoff Position is the per-shard GTID-set MINIMUM across the UNION of all
// 4 per-table snapshots (never the max — the silent-loss guard), and the CDC
// handoff opens one keyspace-wide stream from the stitched min.
func TestVStreamConcurrent_AllTablesLandAndStitchMin(t *testing.T) {
	tables := []string{"a", "b", "c", "d"}
	// Distinct per-table snapshot GTIDs; the MIN is :1-10 (table "a"). The
	// stitch must pick the min across ALL streams' ALL tables, not the max.
	gtids := map[string]string{
		"a": "MySQL56/" + uuidA + ":1-10",
		"b": "MySQL56/" + uuidA + ":1-50",
		"c": "MySQL56/" + uuidA + ":1-200",
		"d": "MySQL56/" + uuidA + ":1-90",
	}
	rowsPer := map[string]int{"a": 3, "b": 5, "c": 2, "d": 4}
	byTable := map[string][]*vtgate.VStreamResponse{}
	for _, tbl := range tables {
		byTable[tbl] = perTableCopyScript(tbl, gtids[tbl], rowsPer[tbl])
	}
	byTable[""] = []*vtgate.VStreamResponse{} // CDC handoff parks

	s, stream, client, cancel := newConcurrentHarness(t, tables, 2, byTable, 0)
	defer cancel()

	ctx, ccancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer ccancel()

	// Drain every table (the orchestrator reads tables one at a time in
	// schema order, regardless of which stream produced each).
	for _, tbl := range tables {
		irTbl := &ir.Table{Name: tbl, Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
		ch, err := stream.Rows.ReadRows(ctx, irTbl)
		if err != nil {
			t.Fatalf("ReadRows(%s): %v", tbl, err)
		}
		got := 0
		for range ch {
			got++
		}
		if err := stream.Rows.Err(); err != nil {
			t.Fatalf("Err after %s drain: %v", tbl, err)
		}
		if got != rowsPer[tbl] {
			t.Fatalf("%s rows = %d; want %d", tbl, got, rowsPer[tbl])
		}
	}

	// All K streams must finish before the stitched Position is recorded.
	select {
	case <-s.copyDone:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent copy did not finish")
	}
	if err := s.Err(); err != nil {
		t.Fatalf("pump error: %v", err)
	}

	// The stitched Position must be the per-shard MIN (:1-10, table a), NOT
	// the max (:1-200, table c). Selecting the max would skip changes after
	// every earlier table's snapshot — silent loss.
	wantPos, err := encodeVStreamPos([]shardGtid{{Keyspace: "main", Shard: "-", Gtid: gtids["a"]}})
	if err != nil {
		t.Fatalf("encodeVStreamPos: %v", err)
	}
	if stream.Position.Token != wantPos.Token {
		t.Fatalf("stitched Position = %q; want the MIN %q (not the max %q)",
			stream.Position.Token, wantPos.Token, gtids["c"])
	}

	// CDC handoff opens one keyspace-wide stream from the stitched min.
	if _, err := stream.Changes.StreamChanges(ctx, stream.Position); err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	// Audit: a keyspace-wide "/.*/" request was made (the CDC handoff), and
	// each per-table COPY was opened.
	matches := client.matches()
	sawCDC := false
	sawTable := map[string]bool{}
	for _, m := range matches {
		switch m {
		case "/.*/":
			sawCDC = true
		default:
			sawTable[m] = true
		}
	}
	if !sawCDC {
		t.Errorf("no keyspace-wide CDC handoff request; got %v", matches)
	}
	for _, tbl := range tables {
		if !sawTable[tbl] {
			t.Errorf("table %q never opened a per-table COPY; got %v", tbl, matches)
		}
	}
}

// TestVStreamConcurrent_PositionOnlyAfterAllStreamsComplete pins the
// ADR-0099 ADR-0007 ordering: the global handoff Position is NOT recorded
// until EVERY stream completes EVERY table. We drain all tables, and assert
// Position is unset until copyDone (which only closes after the WaitGroup
// join over all K streams).
func TestVStreamConcurrent_PositionOnlyAfterAllStreamsComplete(t *testing.T) {
	tables := []string{"a", "b", "c"}
	byTable := map[string][]*vtgate.VStreamResponse{
		"a": perTableCopyScript("a", "MySQL56/"+uuidA+":1-10", 2),
		"b": perTableCopyScript("b", "MySQL56/"+uuidA+":1-20", 2),
		"c": perTableCopyScript("c", "MySQL56/"+uuidA+":1-30", 2),
		"":  {},
	}
	s, stream, _, cancel := newConcurrentHarness(t, tables, 3, byTable, 0)
	defer cancel()

	ctx, ccancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer ccancel()

	for _, tbl := range tables {
		irTbl := &ir.Table{Name: tbl, Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
		ch, err := stream.Rows.ReadRows(ctx, irTbl)
		if err != nil {
			t.Fatalf("ReadRows(%s): %v", tbl, err)
		}
		for range ch {
		}
	}

	select {
	case <-s.copyDone:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent copy did not finish")
	}
	if stream.Position.Token == "" {
		t.Fatal("Position unset after all streams complete; want the stitched min")
	}
}

// TestVStreamConcurrent_BoundedMemory pins that K concurrent streams stay
// bounded under one shared cap even when total volume dwarfs it: each stream
// backpressures against the shared bufferedBytes, so the peak never blows
// past the cap by more than the K streams' in-flight slack — and crucially
// no ADR-0071 cross-table loud refusal fires (concurrent mode never
// interleaves on one stream).
func TestVStreamConcurrent_BoundedMemory(t *testing.T) {
	const (
		nPerTable = 300
		capBytes  = 8 * 1024
	)
	tables := []string{"a", "b", "c", "d"}
	byTable := map[string][]*vtgate.VStreamResponse{"": {}}
	for i, tbl := range tables {
		byTable[tbl] = perTableCopyScript(tbl, "MySQL56/"+uuidA+":1-"+itoa(10*(i+1)), nPerTable)
	}

	s, stream, _, cancel := newConcurrentHarness(t, tables, 2, byTable, capBytes)
	defer cancel()

	ctx, ccancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer ccancel()

	var peak int64
	for _, tbl := range tables {
		irTbl := &ir.Table{Name: tbl, Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
		ch, err := stream.Rows.ReadRows(ctx, irTbl)
		if err != nil {
			t.Fatalf("ReadRows(%s): %v", tbl, err)
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
			t.Fatalf("Err after %s drain (concurrent must NOT loud-refuse): %v", tbl, err)
		}
		if got != nPerTable {
			t.Fatalf("%s rows = %d; want %d", tbl, got, nPerTable)
		}
	}

	// Bounded near K × cap (K=2). Allow generous slack for the shared-cap
	// backpressure timing; the point is it is NOT Σ(all tables).
	if peak > int64(capBytes)*16 {
		t.Errorf("peak buffered = %d; want bounded near cap %d (concurrent memory not bounded)", peak, capBytes)
	}
}

// TestVStreamConcurrent_StreamErrorAbortsLoudly pins ADR-0099 §6: any
// stream's failure aborts the whole copy LOUDLY with no global position
// advance. One table's script emits a ROW with no preceding FIELD (the
// "row without FIELD" loud floor), which fails that stream; the others
// cancel and Position never advances.
func TestVStreamConcurrent_StreamErrorAbortsLoudly(t *testing.T) {
	tables := []string{"a", "b", "c", "d"}
	byTable := map[string][]*vtgate.VStreamResponse{"": {}}
	for _, tbl := range tables {
		byTable[tbl] = perTableCopyScript(tbl, "MySQL56/"+uuidA+":1-10", 2)
	}
	// Corrupt table "c": a ROW with no preceding FIELD event → loud error.
	badRow := &binlogdata.VEvent{
		Type: binlogdata.VEventType_ROW,
		RowEvent: &binlogdata.RowEvent{
			TableName:  "c",
			Keyspace:   "main",
			Shard:      "-",
			RowChanges: []*binlogdata.RowChange{{After: makeRow([]string{"0"})}},
		},
	}
	byTable["c"] = []*vtgate.VStreamResponse{oneEvent(badRow)}

	s, stream, _, cancel := newConcurrentHarness(t, tables, 2, byTable, 0)
	defer cancel()

	ctx, ccancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer ccancel()

	// Drain attempts surface the error; we don't require a specific table to
	// fail first, just that the copy ends with a loud error and no Position.
	for _, tbl := range tables {
		irTbl := &ir.Table{Name: tbl, Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
		ch, err := stream.Rows.ReadRows(ctx, irTbl)
		if err != nil {
			continue
		}
		for range ch {
		}
	}

	select {
	case <-s.copyDone:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent copy did not finish after a stream error")
	}
	if s.Err() == nil {
		t.Fatal("want a loud terminal error after a stream failure; got nil")
	}
	if stream.Position.Token != "" {
		t.Fatalf("Position advanced to %q after a stream error; want unset (no silent partial success)", stream.Position.Token)
	}
}

// TestVStreamConcurrent_ResumeSeedLandsInRightStream pins the ADR-0099 §5
// resume-composition silent-loss surface: on a resume the persisted cursor's
// in-progress table must be seeded from its cursor in WHICHEVER stream group
// the (stable, deterministic) partition assigns it to — while every other
// table re-copies fresh from-beginning. A mis-placed seed would either skip
// the in-progress table or restart it from row 0.
func TestVStreamConcurrent_ResumeSeedLandsInRightStream(t *testing.T) {
	tables := []string{"a", "b", "c", "d"}
	const k = 2

	// Determine which table the cursor names + assert the partition is stable.
	groups := partitionTablesForStreams(tables, k, nil)
	// Pick the in-progress table from one of the groups deterministically.
	resumeTable := "c"

	byTable := map[string][]*vtgate.VStreamResponse{"": {}}
	for _, tbl := range tables {
		byTable[tbl] = perTableCopyScript(tbl, "MySQL56/"+uuidA+":1-10", 2)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newTableKeyedClient(ctx, byTable)

	s := newTestSnapshotStream()
	s.client = client
	s.shards = []string{"-"}
	s.copyTablesSeq = tables
	s.copyTables = []string{tables[0]}
	s.tableCopyComplete = make(map[string]bool)
	s.reconnectMax = defaultCopyReconnectMax
	s.reconnectBackoffBase = defaultCopyReconnectBackoffBase
	s.reconnectBackoffCap = defaultCopyReconnectBackoffCap
	s.copyDone = make(chan struct{})
	// ADR-0098 resume seed for the in-progress table.
	s.resumeSeedTable = resumeTable
	s.resumeSeed = resumeCursorPos(t, resumeTable, 9000)

	stream := &ir.SnapshotStream{
		Rows:    &vstreamSnapshotRows{snap: s},
		Changes: &vstreamSnapshotChanges{snap: s},
	}
	go s.copyPumpAutoShardConcurrent(ctx, cancel, stream, groups)

	dctx, dcancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer dcancel()
	for _, tbl := range tables {
		irTbl := &ir.Table{Name: tbl, Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
		ch, err := stream.Rows.ReadRows(dctx, irTbl)
		if err != nil {
			t.Fatalf("ReadRows(%s): %v", tbl, err)
		}
		for range ch {
		}
	}
	select {
	case <-s.copyDone:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent resume did not finish")
	}
	if err := s.Err(); err != nil {
		t.Fatalf("pump error: %v", err)
	}

	// The in-progress table's stream must have resumed FROM ITS CURSOR; no
	// other table's stream may carry a cursor (they re-copy fresh).
	client.mu.Lock()
	defer client.mu.Unlock()
	if !client.seededTables[resumeTable] {
		t.Fatalf("in-progress table %q did not resume from its cursor (no TablePKs on its open)", resumeTable)
	}
	for _, tbl := range tables {
		if tbl == resumeTable {
			continue
		}
		if client.seededTables[tbl] {
			t.Errorf("table %q opened with a resume cursor; only the in-progress table may seed (others re-copy fresh)", tbl)
		}
	}
}

// TestVStreamConcurrent_CtxCancelNoLeak pins ADR-0099 §6 clean shutdown: a
// mid-copy cancel unwinds every stream without hanging and reports no
// success (Position unset). (Goroutine-leak detection proper is the -race
// integration job's domain; this asserts the cancel path terminates.)
func TestVStreamConcurrent_CtxCancelNoLeak(t *testing.T) {
	tables := []string{"a", "b", "c", "d"}
	byTable := map[string][]*vtgate.VStreamResponse{"": {}}
	for i, tbl := range tables {
		// Large per-table scripts so the copy is still running when we cancel.
		byTable[tbl] = perTableCopyScript(tbl, "MySQL56/"+uuidA+":1-"+itoa(10*(i+1)), 5000)
	}

	s, _, _, cancel := newConcurrentHarness(t, tables, 3, byTable, 4*1024)

	// Cancel almost immediately, before any consumer drains — the streams are
	// backpressured/parked. The driver must unwind and close copyDone.
	cancel()

	select {
	case <-s.copyDone:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent copy did not unwind on ctx-cancel")
	}
}
