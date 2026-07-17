// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtgate"

	"sluicesync.dev/sluice/internal/ir"
)

// fakeVStreamClient is a deterministic stand-in for
// [vtgateservice.Vitess_VStreamClient]. The COPY/CDC pumps only ever
// call Recv, so embedding a nil grpc.ClientStream and overriding Recv
// is enough — any other method would panic, which is the desired
// "this fake is Recv-only" contract. Recv replays scripted responses
// in order; once drained it blocks until ctx cancels (mirrors a real
// stream that's gone idle waiting for the server), so a pump that has
// consumed the COPY phase parks rather than spinning.
type fakeVStreamClient struct {
	grpc.ClientStream

	ctx  context.Context
	mu   sync.Mutex
	next int
	resp []*vtgate.VStreamResponse
}

func (f *fakeVStreamClient) Recv() (*vtgate.VStreamResponse, error) {
	f.mu.Lock()
	if f.next < len(f.resp) {
		r := f.resp[f.next]
		f.next++
		f.mu.Unlock()
		return r, nil
	}
	f.mu.Unlock()
	// Drained: block until cancelled so the pump parks like a real
	// idle stream rather than EOF-ing the COPY phase prematurely.
	<-f.ctx.Done()
	return nil, f.ctx.Err()
}

// oneEvent wraps a single VEvent in its own VStreamResponse so the
// pump's per-response, per-event loops both get exercised.
func oneEvent(ev *binlogdata.VEvent) *vtgate.VStreamResponse {
	return &vtgate.VStreamResponse{Events: []*binlogdata.VEvent{ev}}
}

func snapFieldEvent(shard string, fields ...*query.Field) *binlogdata.VEvent {
	return &binlogdata.VEvent{
		Type: binlogdata.VEventType_FIELD,
		FieldEvent: &binlogdata.FieldEvent{
			TableName: "t",
			Keyspace:  "main",
			Shard:     shard,
			Fields:    fields,
		},
	}
}

func snapRowEvent(shard string, vals ...string) *binlogdata.VEvent {
	return &binlogdata.VEvent{
		Type: binlogdata.VEventType_ROW,
		RowEvent: &binlogdata.RowEvent{
			TableName:  "t",
			Keyspace:   "main",
			Shard:      shard,
			RowChanges: []*binlogdata.RowChange{{After: makeRow(vals)}},
		},
	}
}

func snapVgtidEvent(gtid string) *binlogdata.VEvent {
	return &binlogdata.VEvent{
		Type: binlogdata.VEventType_VGTID,
		Vgtid: &binlogdata.VGtid{
			ShardGtids: []*binlogdata.ShardGtid{
				{Keyspace: "main", Shard: "-", Gtid: gtid},
			},
		},
	}
}

func globalCopyCompleted() *binlogdata.VEvent {
	return &binlogdata.VEvent{Type: binlogdata.VEventType_COPY_COMPLETED}
}

// newStreamingHarness wires a snapshot stream around a scripted fake
// gRPC client and spawns the COPY pump exactly as the constructor
// does. Returns the snap, the SnapshotStream the pump records Position
// onto, and a cancel that tears the pump down.
func newStreamingHarness(t *testing.T, resp []*vtgate.VStreamResponse) (*vstreamSnapshotStream, *ir.SnapshotStream, context.CancelFunc) {
	t.Helper()
	return newStreamingHarnessCapped(t, resp, 0)
}

// newStreamingHarnessCapped is [newStreamingHarness] with the byte cap
// applied BEFORE the copy pump goroutine starts. A test that needs a
// small cap must NOT set s.maxBufferBytes after the harness returns:
// the pump is already running and buffers at the 64 MiB default
// ([defaultSnapshotMaxBufferBytes]) until the lower cap lands, so the
// high-water mark overshoots by however many rows the pump enqueued in
// that window. Under -race that window is wide enough to blow a tight
// bound — the v0.99.x CI flake in TestVStreamSnapshot_StreamingBounded-
// Memory (peak 53064 vs cap 8192). Applying the cap before the pump
// starts makes the bound exact and the assertion non-flaky.
//
// capBytes <= 0 keeps the constructor default.
func newStreamingHarnessCapped(t *testing.T, resp []*vtgate.VStreamResponse, capBytes int64) (*vstreamSnapshotStream, *ir.SnapshotStream, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	s := newTestSnapshotStream()
	if capBytes > 0 {
		s.maxBufferBytes = capBytes
	}
	s.copyDone = make(chan struct{})
	s.grpcStream = &fakeVStreamClient{ctx: ctx, resp: resp}

	stream := &ir.SnapshotStream{
		Rows:    &vstreamSnapshotRows{snap: s},
		Changes: &vstreamSnapshotChanges{snap: s},
	}
	go s.copyPump(ctx, cancel, stream)
	return s, stream, cancel
}

// snapPKField is an INT64 field carrying the PRI_KEY_FLAG so the dedup
// tracker treats it as the table's primary key.
func snapPKField(name string) *query.Field {
	return &query.Field{
		Name:  name,
		Type:  query.Type_INT64,
		Flags: uint32(query.MySqlFlag_PRI_KEY_FLAG),
	}
}

// TestVStreamSnapshot_StreamingBoundedMemory is the ADR-0071 pin: a
// single table whose total row volume dwarfs the byte cap streams
// through ReadRows in CONSTANT memory. Under the pre-streaming reader
// every row buffered before ReadRows ran (the OOM class); here we
// assert buffered + in-flight bytes never exceed the cap by more than
// one row while the consumed-row count climbs to the full total.
func TestVStreamSnapshot_StreamingBoundedMemory(t *testing.T) {
	const (
		nRows    = 4000
		rowSize  = 256 // each row's payload bytes (approx)
		capBytes = 8 * 1024
	)
	// Script: FIELD, then nRows ROWs, a VGTID, and the global
	// COPY_COMPLETED. No PK flag → dedup is a no-op, every row kept.
	resp := make([]*vtgate.VStreamResponse, 0, nRows+3)
	resp = append(resp, oneEvent(snapFieldEvent(
		"-",
		&query.Field{Name: "id", Type: query.Type_INT64},
		&query.Field{Name: "blob", Type: query.Type_VARCHAR},
	)))
	payload := strings.Repeat("x", rowSize)
	for i := 0; i < nRows; i++ {
		resp = append(resp, oneEvent(snapRowEvent("-", fmt.Sprintf("%d", i), payload)))
	}
	resp = append(
		resp,
		oneEvent(snapVgtidEvent("MySQL56/abc:1-100")),
		oneEvent(globalCopyCompleted()),
	)

	// Apply the cap BEFORE the pump starts (newStreamingHarnessCapped),
	// not after — otherwise the pump buffers at the 64 MiB default until
	// the cap lands and the high-water mark overshoots (the v0.99.x
	// -race flake). With the cap set up front the bound is exact.
	s, _, cancel := newStreamingHarnessCapped(t, resp, capBytes)
	defer cancel()

	// Sample the buffered-bytes high-water mark from a watchdog while
	// the consumer drains, so we observe the pump's steady state rather
	// than only the post-drain zero.
	var peak int64
	stopWatch := make(chan struct{})
	var watchWG sync.WaitGroup
	watchWG.Add(1)
	go func() {
		defer watchWG.Done()
		tick := time.NewTicker(50 * time.Microsecond)
		defer tick.Stop()
		for {
			select {
			case <-stopWatch:
				return
			case <-tick.C:
				s.mu.Lock()
				if s.bufferedBytes > peak {
					peak = s.bufferedBytes
				}
				s.mu.Unlock()
			}
		}
	}()

	ctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer ccancel()
	tbl := &ir.Table{Name: "t", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "blob", Type: ir.Varchar{Length: 4096}},
	}}
	ch, err := (&vstreamSnapshotRows{snap: s}).ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}

	consumed := 0
	var maxObservedBuffered int64
	for range ch {
		// Inspect the queue depth on the consumer side too — a slow
		// consumer (this loop) must keep the pump bounded.
		s.mu.Lock()
		if s.bufferedBytes > maxObservedBuffered {
			maxObservedBuffered = s.bufferedBytes
		}
		s.mu.Unlock()
		consumed++
	}
	close(stopWatch)
	watchWG.Wait()

	if err := (&vstreamSnapshotRows{snap: s}).Err(); err != nil {
		t.Fatalf("Err after drain: %v", err)
	}
	if consumed != nRows {
		t.Fatalf("consumed %d rows; want %d (streaming must surface every row)", consumed, nRows)
	}

	// The cap is a soft target: the pump may overshoot by at most one
	// in-flight row (the row whose append crossed the cap before the
	// consumer debited). Allow one row's slack over the cap.
	limit := int64(capBytes) + rowSize + 64
	if peak > limit {
		t.Errorf("watchdog peak buffered bytes = %d; want <= %d (cap %d + 1 row slack) — memory not bounded", peak, limit, capBytes)
	}
	if maxObservedBuffered > limit {
		t.Errorf("consumer-observed peak buffered bytes = %d; want <= %d — memory not bounded", maxObservedBuffered, limit)
	}
	// Sanity: the total volume really did exceed the cap many times
	// over, so a passing bound is meaningful (not a too-small input).
	if int64(nRows)*rowSize < int64(capBytes)*10 {
		t.Fatalf("test input too small to prove bounding: total %d, cap %d", int64(nRows)*rowSize, capBytes)
	}
}

// TestVStreamSnapshot_StreamingMultiShardFanIn confirms that under the
// streaming path rows for one logical table arriving from TWO shards
// all surface through a single ReadRows call (the fan-in invariant the
// pre-streaming reader had, preserved by keying the queue on the
// unqualified table name).
func TestVStreamSnapshot_StreamingMultiShardFanIn(t *testing.T) {
	resp := []*vtgate.VStreamResponse{
		oneEvent(snapFieldEvent("-80", &query.Field{Name: "id", Type: query.Type_INT64})),
		oneEvent(snapFieldEvent("80-", &query.Field{Name: "id", Type: query.Type_INT64})),
		oneEvent(snapRowEvent("-80", "1")),
		oneEvent(snapRowEvent("80-", "2")),
		oneEvent(snapRowEvent("-80", "3")),
		oneEvent(snapRowEvent("80-", "4")),
		oneEvent(snapVgtidEvent("MySQL56/abc:1-100")),
		oneEvent(globalCopyCompleted()),
	}
	s, _, cancel := newStreamingHarness(t, resp)
	defer cancel()

	ctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ccancel()
	tbl := &ir.Table{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	ch, err := (&vstreamSnapshotRows{snap: s}).ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := map[int64]bool{}
	for r := range ch {
		id, _ := r["id"].(int64)
		got[id] = true
	}
	if err := (&vstreamSnapshotRows{snap: s}).Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	for _, want := range []int64{1, 2, 3, 4} {
		if !got[want] {
			t.Errorf("row id=%d from shard fan-in missing; got=%v", want, got)
		}
	}
	if len(got) != 4 {
		t.Errorf("got %d distinct rows; want 4 (both shards merged)", len(got))
	}
}

// TestVStreamSnapshot_StreamingKeepsEveryRow is the Bug 125 pin: the
// COPY pump no longer drops behind-the-scan re-emissions. The deleted
// copyDedupTracker assumed Vitess's COPY scan emits in PK-ascending
// order of the PRI_KEY_FLAG column and dropped any row with PK <=
// max-seen — but that assumption is false when Vitess orders the scan
// by a cheaper unique key, so legitimate forward rows were silently
// dropped (the 13.5M-of-19M incident). Every decoded ROW now reaches
// the consumer verbatim; the idempotent COPY writer absorbs the
// re-emissions downstream without any ordering assumption.
func TestVStreamSnapshot_StreamingKeepsEveryRow(t *testing.T) {
	resp := []*vtgate.VStreamResponse{
		oneEvent(snapFieldEvent("-", snapPKField("id"))),
		oneEvent(snapRowEvent("-", "1")),
		oneEvent(snapRowEvent("-", "2")),
		oneEvent(snapRowEvent("-", "3")),
		// Behind-the-scan re-emissions (PK <= max seen). Pre-Bug-125
		// these were dropped; now they flow through and the idempotent
		// writer upserts them harmlessly.
		oneEvent(snapRowEvent("-", "2")),
		oneEvent(snapRowEvent("-", "1")),
		// Forward again.
		oneEvent(snapRowEvent("-", "4")),
		oneEvent(snapVgtidEvent("MySQL56/abc:1-100")),
		oneEvent(globalCopyCompleted()),
	}
	s, _, cancel := newStreamingHarness(t, resp)
	defer cancel()

	ctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ccancel()
	tbl := &ir.Table{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	ch, err := (&vstreamSnapshotRows{snap: s}).ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var ids []int64
	for r := range ch {
		id, _ := r["id"].(int64)
		ids = append(ids, id)
	}
	if err := (&vstreamSnapshotRows{snap: s}).Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	// Every emitted ROW is delivered in arrival order — zero drops.
	want := []int64{1, 2, 3, 2, 1, 4}
	if len(ids) != len(want) {
		t.Fatalf("got ids %v; want %v (zero drops — Bug 125)", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Errorf("ids[%d] = %d; want %d", i, ids[i], want[i])
		}
	}
}

// TestVStreamSnapshot_CopyNeedsIdempotentWriter pins the marker the
// orchestrator reads to route the cold-start bulk copy through the
// upsert writer (Bug 125). The VStream snapshot reader must declare it.
func TestVStreamSnapshot_CopyNeedsIdempotentWriter(t *testing.T) {
	var rr ir.RowReader = &vstreamSnapshotRows{snap: newTestSnapshotStream()}
	icr, ok := rr.(ir.IdempotentCopyReader)
	if !ok {
		t.Fatal("vstreamSnapshotRows must implement ir.IdempotentCopyReader")
	}
	if !icr.CopyNeedsIdempotentWriter() {
		t.Error("CopyNeedsIdempotentWriter() = false; want true (VStream COPY re-emits rows)")
	}
}

// TestVStreamSnapshot_StreamingPositionAfterDrain is the race-clean
// pump↔consumer handoff pin: a concurrent COPY pump fills the stream
// while a consumer drains via ReadRows, and AFTER the drain completes
// the orchestrator-style read of stream.Position observes the final
// VGTID the pump recorded at COPY_COMPLETED. The happens-before edge is
// the ReadRows channel close (the pump writes Position + sets
// copyComplete under mu before the consumer can observe copyComplete
// and close its channel). Run with -race to exercise the edge.
func TestVStreamSnapshot_StreamingPositionAfterDrain(t *testing.T) {
	const finalGtid = "MySQL56/abc:1-12345"
	resp := []*vtgate.VStreamResponse{
		oneEvent(snapFieldEvent("-", &query.Field{Name: "id", Type: query.Type_INT64})),
		oneEvent(snapVgtidEvent("MySQL56/abc:1-1")), // intermediate
		oneEvent(snapRowEvent("-", "1")),
		oneEvent(snapRowEvent("-", "2")),
		oneEvent(snapVgtidEvent(finalGtid)), // snapshot-consistent position
		oneEvent(globalCopyCompleted()),
	}
	s, stream, cancel := newStreamingHarness(t, resp)
	defer cancel()

	ctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ccancel()
	tbl := &ir.Table{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	ch, err := (&vstreamSnapshotRows{snap: s}).ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	count := 0
	for range ch {
		count++
	}
	if count != 2 {
		t.Fatalf("consumed %d rows; want 2", count)
	}
	if err := (&vstreamSnapshotRows{snap: s}).Err(); err != nil {
		t.Fatalf("Err after drain: %v", err)
	}

	// Post-drain read, exactly as the cold-start orchestrator does
	// after runBulkCopy. The channel-close edge orders this read after
	// the pump's Position write.
	want, err := encodeVStreamPos([]shardGtid{{Keyspace: "main", Shard: "-", Gtid: finalGtid}})
	if err != nil {
		t.Fatalf("encodeVStreamPos: %v", err)
	}
	if stream.Position.Token != want.Token || stream.Position.Engine != want.Engine {
		t.Fatalf("stream.Position = %+v after drain; want final VGTID %+v", stream.Position, want)
	}

	// StreamChanges with the captured position must validate (no
	// mismatch) now that the pump has joined.
	if _, err := stream.Changes.StreamChanges(ctx, stream.Position); err != nil {
		t.Fatalf("StreamChanges with captured position: %v", err)
	}
}

// TestVStreamSnapshot_StreamingMultiTableInterleaveRefuse pins the
// Phase 1 loud refusal (ADR-0071): rows for a not-yet-consumed table
// accumulating past the cap WHILE a different table is being drained is
// a loud error, not an OOM or a deadlock. We drive table "a" rows
// while a consumer holds table "b" active; once "a" crosses the cap the
// pump records a refusal that Err surfaces.
func TestVStreamSnapshot_StreamingMultiTableInterleaveRefuse(t *testing.T) {
	field := func(table string) *binlogdata.VEvent {
		return &binlogdata.VEvent{
			Type: binlogdata.VEventType_FIELD,
			FieldEvent: &binlogdata.FieldEvent{
				TableName: table, Keyspace: "main", Shard: "-",
				Fields: []*query.Field{
					{Name: "id", Type: query.Type_INT64},
					{Name: "blob", Type: query.Type_VARCHAR},
				},
			},
		}
	}
	row := func(table, id, blob string) *binlogdata.VEvent {
		return &binlogdata.VEvent{
			Type: binlogdata.VEventType_ROW,
			RowEvent: &binlogdata.RowEvent{
				TableName: table, Keyspace: "main", Shard: "-",
				RowChanges: []*binlogdata.RowChange{{After: makeRow([]string{id, blob})}},
			},
		}
	}
	const capBytes = 4 * 1024
	payload := strings.Repeat("y", 512)
	resp := []*vtgate.VStreamResponse{
		oneEvent(field("b")),
		oneEvent(row("b", "1", "small")), // gives consumer of "b" one row
		oneEvent(field("a")),
	}
	// Pile "a" rows well past the cap.
	for i := 0; i < 200; i++ {
		resp = append(resp, oneEvent(row("a", fmt.Sprintf("%d", i), payload)))
	}
	resp = append(resp, oneEvent(globalCopyCompleted()))

	// Cap applied before the pump starts (see newStreamingHarnessCapped)
	// so the interleaving refusal fires deterministically rather than
	// racing the default-cap buffering window.
	s, _, cancel := newStreamingHarnessCapped(t, resp, capBytes)
	defer cancel()

	ctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ccancel()

	// Consume table "b" but DO NOT consume "a". Holding "b" active
	// means "a"'s over-cap growth has no drain → loud refuse.
	tblB := &ir.Table{Name: "b", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "blob", Type: ir.Varchar{Length: 4096}},
	}}
	ch, err := (&vstreamSnapshotRows{snap: s}).ReadRows(ctx, tblB)
	if err != nil {
		t.Fatalf("ReadRows(b): %v", err)
	}
	// Drain whatever "b" yields; its channel closes when copy completes
	// or the pump errors (which also flips copyComplete).
	for range ch {
	}

	gotErr := (&vstreamSnapshotRows{snap: s}).Err()
	if gotErr == nil {
		t.Fatal("expected a loud refusal Err for the multi-table interleave over-cap case; got nil")
	}
	if !strings.Contains(gotErr.Error(), "max-buffer-bytes") {
		t.Errorf("refusal Err = %q; want it to name --max-buffer-bytes", gotErr.Error())
	}
}

// TestVStreamSnapshot_ActiveTableToggles asserts the sequential COPY
// path's activeTable bookkeeping: ReadRows marks its table active
// synchronously (so the pump backpressures rather than refuses on that
// table) and the drain goroutine clears it once the table's COPY
// completes and its channel closes.
//
// The two invariants are pinned SEPARATELY because they require opposite
// pump states, and conflating them is what made this a -race flake (it
// toggled three times across releases, always "activeTable = \"\" right
// after ReadRows"). activeTable is SET synchronously by ReadRows (under
// mu, before the goroutine spawns) but CLEARED asynchronously by the
// drain goroutine's deferred cleanup. With a single tiny script (one row
// + COPY_COMPLETED) that goroutine can run to completion — push the row
// into the 64-deep buffered out-channel (rowChanBuffer), observe
// copyComplete, return, and run the deferred clear — all BEFORE the test
// acquires mu to read the set. Every access is mutex-guarded, so this is
// not a data race the detector reports; it is a logical ordering flake
// that -race's aggressive scheduler surfaces by widening that window.
//
// The fix synchronizes on the pump state each invariant actually needs —
// no sleep, no retry:
//
//   - SET: a script that never sends COPY_COMPLETED, so after the row the
//     drain goroutine parks in cond.Wait and CANNOT clear activeTable —
//     the synchronous set is then race-free to observe. cancel() (via
//     failCopy's broadcast) unwedges the parked goroutine at teardown.
//   - CLEAR: the full script; the deferred clear runs under mu and is
//     sequenced-before close(out) (defer LIFO), so the post-range read is
//     ordered after the clear by the channel-close happens-before edge.
func TestVStreamSnapshot_ActiveTableToggles(t *testing.T) {
	tbl := &ir.Table{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}

	t.Run("set synchronously on ReadRows", func(t *testing.T) {
		resp := []*vtgate.VStreamResponse{
			oneEvent(snapFieldEvent("-", &query.Field{Name: "id", Type: query.Type_INT64})),
			oneEvent(snapRowEvent("-", "1")),
			// Deliberately NO VGTID / COPY_COMPLETED: the pump enqueues the
			// row then parks on the drained fake stream, and the drain
			// goroutine parks in cond.Wait — so activeTable cannot be
			// cleared and the synchronous set is deterministic to observe.
		}
		s, _, cancel := newStreamingHarness(t, resp)
		defer cancel()

		ctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		if _, err := (&vstreamSnapshotRows{snap: s}).ReadRows(ctx, tbl); err != nil {
			t.Fatalf("ReadRows: %v", err)
		}
		s.mu.Lock()
		setNow := s.activeTable
		s.mu.Unlock()
		if setNow != "t" {
			t.Errorf("activeTable = %q right after ReadRows; want %q", setNow, "t")
		}
	})

	t.Run("cleared after channel closes", func(t *testing.T) {
		resp := []*vtgate.VStreamResponse{
			oneEvent(snapFieldEvent("-", &query.Field{Name: "id", Type: query.Type_INT64})),
			oneEvent(snapRowEvent("-", "1")),
			oneEvent(snapVgtidEvent("MySQL56/abc:1-1")),
			oneEvent(globalCopyCompleted()),
		}
		s, _, cancel := newStreamingHarness(t, resp)
		defer cancel()

		ctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		ch, err := (&vstreamSnapshotRows{snap: s}).ReadRows(ctx, tbl)
		if err != nil {
			t.Fatalf("ReadRows: %v", err)
		}
		for range ch {
		}

		// close(out) is ordered after the deferred clear (defer LIFO), so
		// this read — sequenced after the range loop drains that close — is
		// race-free.
		s.mu.Lock()
		got := s.activeTable
		s.mu.Unlock()
		if got != "" {
			t.Errorf("activeTable = %q after drain; want cleared", got)
		}
	})
}
