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
	// Mirror the constructor (ADR-0100): the concurrent path sets these so
	// ReadRows skips the sequential-only activeTable bookkeeping and the
	// reader can surface the partition to the pipeline.
	s.concurrentCopy = len(groups) > 1
	s.concurrentGroups = groups

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

	// Let every stream fill its tiny per-stream sub-cap and PARK in the
	// backpressure wait (no consumer drains). This deterministically reaches
	// the all-streams-parked state — the one the ctx-cancel waker fixes:
	// previously, with every producer parked in s.cond.Wait() (which does not
	// observe ctx), a bare cancel had no goroutine to broadcast, so wg.Wait()
	// hung (the flake under load was exactly this race — sometimes a stream
	// hadn't parked yet and tripped failCopy itself). The brief settle removes
	// that timing dependency: pre-fix this hangs to the timeout; post-fix the
	// waker trips failCopy on cancel and every parked producer unwinds at once.
	time.Sleep(300 * time.Millisecond)

	// Cancel with all streams backpressured/parked. The driver must unwind
	// every stream and close copyDone.
	cancel()

	select {
	case <-s.copyDone:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent copy did not unwind on ctx-cancel (all-streams-parked)")
	}
}

// bigRowTableScript is perTableCopyScript with a SINGLE row whose payload is
// large enough to fill a tight shared byte cap by itself. Used by the
// liveness-deadlock pin to make one look-ahead table monopolize the cap.
func bigRowTableScript(table, gtid string, payloadBytes int) []*vtgate.VStreamResponse {
	field := &binlogdata.VEvent{
		Type: binlogdata.VEventType_FIELD,
		FieldEvent: &binlogdata.FieldEvent{
			TableName: table,
			Keyspace:  "main",
			Shard:     "-",
			Fields:    []*query.Field{{Name: "id", Type: query.Type_INT64}},
		},
	}
	big := make([]byte, payloadBytes)
	for i := range big {
		big[i] = 'x'
	}
	row := &binlogdata.VEvent{
		Type: binlogdata.VEventType_ROW,
		RowEvent: &binlogdata.RowEvent{
			TableName:  table,
			Keyspace:   "main",
			Shard:      "-",
			RowChanges: []*binlogdata.RowChange{{After: makeRow([]string{string(big)})}},
		},
	}
	return []*vtgate.VStreamResponse{
		oneEvent(field), oneEvent(row),
		oneEvent(snapVgtidEvent(gtid)), oneEvent(globalCopyCompleted()),
	}
}

// TestVStreamConcurrent_PerStreamCapNoDeadlock is the ADR-0099 §2
// liveness-deadlock pin (the BLOCKER the value-fidelity review found). It
// reproduces the exact wedge: K=2 over groups [a,c]/[b,d] under a tight
// SHARED cap, where a LOOK-AHEAD table (c, stream 0's second table) fills the
// whole cap before the stream that owns the consumer's NEXT table (b, stream
// 1) can enqueue its first row.
//
// Sequence on the OLD shared-cap code:
//   - stream 0 copies "a" (small, the consumer drains it), advances to "c",
//     and enqueues c's single OVERSIZED row — admitted because the shared
//     buffer was momentarily empty (the single-row-exceeds-cap fallback), so
//     bufferedBytes is now > cap;
//   - stream 1 tries to enqueue "b"'s first row but bufferedBytes is already
//     over cap (held by c, a look-ahead table) → it PARKS;
//   - the consumer finishes "a", moves to "b" — but b has NO rows (stream 1
//     is parked) → the consumer BLOCKS;
//   - c's row never drains (the consumer won't reach table c until it
//     finishes b) → WEDGE. With the watchdog disabled in the unit harness
//     this hangs forever; in production the ~10-minute progress watchdog
//     aborts LOUDLY.
//
// With the per-stream sub-budget (ADR-0099 §2) c counts only against stream
// 0's own sub-cap, so stream 1's sub-cap is free: it enqueues b, the consumer
// drains b then c, and the copy COMPLETES. This test FAILS (times out) on the
// shared-cap code and PASSES on the per-stream-cap fix.
func TestVStreamConcurrent_PerStreamCapNoDeadlock(t *testing.T) {
	tables := []string{"a", "b", "c", "d"}
	const k = 2 // round-robin over sorted ⇒ group0=[a,c], group1=[b,d].

	// A tight cap. "c" carries one row larger than the WHOLE cap, so on the
	// old shared-cap code that single look-ahead row monopolizes the buffer.
	const capBytes = 4096
	byTable := map[string][]*vtgate.VStreamResponse{"": {}}
	byTable["a"] = perTableCopyScript("a", "MySQL56/"+uuidA+":1-10", 2)
	byTable["b"] = perTableCopyScript("b", "MySQL56/"+uuidA+":1-20", 3)
	byTable["c"] = bigRowTableScript("c", "MySQL56/"+uuidA+":1-30", capBytes*2)
	byTable["d"] = perTableCopyScript("d", "MySQL56/"+uuidA+":1-40", 2)

	s, stream, _, cancel := newConcurrentHarness(t, tables, k, byTable, capBytes)
	defer cancel()

	ctx, ccancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer ccancel()

	// Drain in schema order [a, b, c, d] — exactly the order that strands the
	// look-ahead table "c" against the cap while the consumer waits on "b".
	rowsPer := map[string]int{"a": 2, "b": 3, "c": 1, "d": 2}
	done := make(chan error, 1)
	go func() {
		for _, tbl := range tables {
			irTbl := &ir.Table{Name: tbl, Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
			ch, err := stream.Rows.ReadRows(ctx, irTbl)
			if err != nil {
				done <- err
				return
			}
			got := 0
			for range ch {
				got++
			}
			if e := stream.Rows.Err(); e != nil {
				done <- e
				return
			}
			if got != rowsPer[tbl] {
				done <- &countMismatch{tbl: tbl, got: got, want: rowsPer[tbl]}
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("concurrent copy drain: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent copy WEDGED: a look-ahead table monopolized the shared cap and the active table starved " +
			"(this is the shared-cap deadlock the per-stream sub-budget fixes)")
	}

	select {
	case <-s.copyDone:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent copy did not finish after drain")
	}
	if err := s.Err(); err != nil {
		t.Fatalf("pump error: %v", err)
	}
}

type countMismatch struct {
	tbl       string
	got, want int
}

func (c *countMismatch) Error() string {
	return c.tbl + ": rows = " + itoa(c.got) + "; want " + itoa(c.want)
}

// perTableTwoShardScript is perTableCopyScript for a 2-shard keyspace: it
// emits FIELD+ROWs for BOTH shards ("-80" and "80-") and one VGTID carrying a
// per-shard GTID for each, before the global COPY_COMPLETED. gtidLo/gtidHi are
// the two shards' snapshot GTIDs for this table. Used by the multi-shard
// set-min pin to ground-truth that the stitch selects each shard's min
// INDEPENDENTLY across the K streams.
func perTableTwoShardScript(table, gtidLo, gtidHi string, nPerShard int) []*vtgate.VStreamResponse {
	out := []*vtgate.VStreamResponse{}
	for _, shard := range []string{"-80", "80-"} {
		field := &binlogdata.VEvent{
			Type: binlogdata.VEventType_FIELD,
			FieldEvent: &binlogdata.FieldEvent{
				TableName: table,
				Keyspace:  "main",
				Shard:     shard,
				Fields:    []*query.Field{{Name: "id", Type: query.Type_INT64}},
			},
		}
		out = append(out, oneEvent(field))
		for i := 0; i < nPerShard; i++ {
			row := &binlogdata.VEvent{
				Type: binlogdata.VEventType_ROW,
				RowEvent: &binlogdata.RowEvent{
					TableName:  table,
					Keyspace:   "main",
					Shard:      shard,
					RowChanges: []*binlogdata.RowChange{{After: makeRow([]string{itoa(i)})}},
				},
			}
			out = append(out, oneEvent(row))
		}
	}
	vgtid := &binlogdata.VEvent{
		Type: binlogdata.VEventType_VGTID,
		Vgtid: &binlogdata.VGtid{
			ShardGtids: []*binlogdata.ShardGtid{
				{Keyspace: "main", Shard: "-80", Gtid: gtidLo},
				{Keyspace: "main", Shard: "80-", Gtid: gtidHi},
			},
		},
	}
	out = append(out, oneEvent(vgtid), oneEvent(globalCopyCompleted()))
	return out
}

// TestVStreamConcurrent_MultiShardSetMinAcrossStreams pins the per-shard
// set-min selection across K>1 streams on a >=2-shard keyspace (the existing
// concurrent pins are all single-shard). Each of K=2 streams copies its
// disjoint tables; each table finishes both shards at distinct per-shard
// GTIDs. The stitched handoff position must be, FOR EACH SHARD, the min across
// the UNION of every stream's every per-table snapshot — and the two shards'
// minima come from DIFFERENT tables (so a per-shard bug that picked one
// shard's min globally would be caught).
func TestVStreamConcurrent_MultiShardSetMinAcrossStreams(t *testing.T) {
	tables := []string{"a", "b", "c", "d"} // group0=[a,c], group1=[b,d] at K=2.
	const k = 2

	// Per-shard GTIDs chosen so:
	//   shard -80 min  = :1-10  (table "a", stream 0)
	//   shard 80- min  = :1-15  (table "b", stream 1)
	// i.e. the two shards' minima are produced by tables in DIFFERENT streams.
	loGTID := map[string]string{
		"a": "MySQL56/" + uuidA + ":1-10",
		"b": "MySQL56/" + uuidA + ":1-40",
		"c": "MySQL56/" + uuidA + ":1-90",
		"d": "MySQL56/" + uuidA + ":1-70",
	}
	hiGTID := map[string]string{
		"a": "MySQL56/" + uuidB + ":1-50",
		"b": "MySQL56/" + uuidB + ":1-15",
		"c": "MySQL56/" + uuidB + ":1-80",
		"d": "MySQL56/" + uuidB + ":1-60",
	}

	byTable := map[string][]*vtgate.VStreamResponse{"": {}}
	for _, tbl := range tables {
		byTable[tbl] = perTableTwoShardScript(tbl, loGTID[tbl], hiGTID[tbl], 2)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newTableKeyedClient(ctx, byTable)

	s := newTestSnapshotStream()
	s.client = client
	s.shards = []string{"-80", "80-"}
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

	dctx, dcancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer dcancel()
	for _, tbl := range tables {
		irTbl := &ir.Table{Name: tbl, Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
		ch, err := stream.Rows.ReadRows(dctx, irTbl)
		if err != nil {
			t.Fatalf("ReadRows(%s): %v", tbl, err)
		}
		got := 0
		for range ch {
			got++
		}
		if got != 4 { // 2 per shard × 2 shards
			t.Fatalf("%s rows = %d; want 4 (2 shards × 2)", tbl, got)
		}
	}

	select {
	case <-s.copyDone:
	case <-time.After(10 * time.Second):
		t.Fatal("multi-shard concurrent copy did not finish")
	}
	if err := s.Err(); err != nil {
		t.Fatalf("pump error: %v", err)
	}

	// The stitched position must carry BOTH shards, each at its own per-shard
	// MIN across the union of all 4 tables (across both streams):
	//   -80 → :1-10 (a),  80- → :1-15 (b).
	wantPos, err := encodeVStreamPos([]shardGtid{
		{Keyspace: "main", Shard: "-80", Gtid: loGTID["a"]},
		{Keyspace: "main", Shard: "80-", Gtid: hiGTID["b"]},
	})
	if err != nil {
		t.Fatalf("encodeVStreamPos: %v", err)
	}
	if stream.Position.Token != wantPos.Token {
		t.Fatalf("stitched Position = %q; want per-shard min across streams %q",
			stream.Position.Token, wantPos.Token)
	}
}

// TestVStreamConcurrent_ResumeSeedTableNotGroupFirst pins the resume edge the
// review flagged: the in-progress (seed) table is NOT its group's first sorted
// table. With group0=[a,c] (K=2) and the cursor naming "c" (group0's SECOND
// table), the earlier-in-group table "a" must re-copy FRESH (opened
// from-beginning, no cursor) while "c" opens SEEDED from its cursor — and the
// copy completes with every table contributing a snapshot. A bug that seeded
// the group's first table, or skipped the earlier-in-group table, is caught.
func TestVStreamConcurrent_ResumeSeedTableNotGroupFirst(t *testing.T) {
	tables := []string{"a", "b", "c", "d"}
	const k = 2

	groups := partitionTablesForStreams(tables, k, nil)
	// Sanity: confirm "c" is group0's SECOND table (the not-first precondition).
	var cGroup []string
	for _, g := range groups {
		for _, tbl := range g {
			if tbl == "c" {
				cGroup = g
			}
		}
	}
	if len(cGroup) < 2 || cGroup[0] == "c" {
		t.Fatalf("precondition: want 'c' as a NON-first table in its group; group=%v", cGroup)
	}
	earlierInGroup := cGroup[0] // "a" — must re-copy fresh.
	const resumeTable = "c"

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
		got := 0
		for range ch {
			got++
		}
		if got != 2 {
			t.Fatalf("%s rows = %d; want 2 (re-copied fresh / resumed)", tbl, got)
		}
	}

	select {
	case <-s.copyDone:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent resume (non-first seed table) did not finish")
	}
	if err := s.Err(); err != nil {
		t.Fatalf("pump error: %v", err)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	// The in-progress table (group's SECOND) seeds from its cursor.
	if !client.seededTables[resumeTable] {
		t.Fatalf("in-progress table %q (group's second) did not seed from its cursor", resumeTable)
	}
	// The EARLIER-in-group table must re-copy FRESH (opened, never seeded).
	if client.seededTables[earlierInGroup] {
		t.Errorf("earlier-in-group table %q opened seeded; it must re-copy fresh from-beginning", earlierInGroup)
	}
	if client.openCount[earlierInGroup] < 1 {
		t.Errorf("earlier-in-group table %q never opened a COPY; it must re-copy fresh", earlierInGroup)
	}
	// No other table seeds.
	for _, tbl := range tables {
		if tbl == resumeTable {
			continue
		}
		if client.seededTables[tbl] {
			t.Errorf("table %q seeded; only the in-progress table %q may seed", tbl, resumeTable)
		}
	}
}
