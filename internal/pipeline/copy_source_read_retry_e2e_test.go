// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// These tests drive the FULL migrate per-table copy (bulkCopyOneTable +
// the ADR-0109 source-read retry) against an in-memory source table whose
// reader DROPS the connection mid-table on a scripted attempt and recovers
// on the next, then ground-truth the target row-set == the source row-set
// with ZERO duplicates and ZERO drops. They pin the value-fidelity core of
// ADR-0109: a reconnect must never lose or duplicate a row.

// fakeSourceStore is the SHARED backing of a scriptable source: the
// id=1..maxID rows and the one-shot drop latch. Multiple fakeSource reader
// HANDLES (one per OpenRowReader / per chunk) read from it concurrently —
// modelling production, where every chunk/table copy opens its OWN
// connection-backed reader against the same source database.
//
// dropBeforeID scripts EXACTLY ONE mid-table connection drop, keyed on the
// PK value rather than a call counter so it fires DETERMINISTICALLY
// regardless of which chunk reads first under within-table parallelism.
// The latch (dropFired) lives here so the drop is one-shot ACROSS all
// reader handles; the RETRIABLE sticky error it raises is set on the
// individual handle that hit the drop, never shared (see fakeSource.err).
type fakeSourceStore struct {
	mu           sync.Mutex
	maxID        int64 // backing rows are id = 1..maxID
	dropBeforeID int64 // PK at which the ONE drop fires (0 = never)
	dropFired    bool  // the drop is one-shot for the whole run
}

// fakeSource is one reader HANDLE over a fakeSourceStore. It serves rows
// from the shared store, supporting the whole-table (ReadRows) and
// cursor/chunk (ReadRowsBatch / ReadRowsBatchBounded) read shapes plus the
// chunk-eligibility surfaces (RangeBounds / RowCounter / RowCountEstimator).
//
// CRITICAL fidelity (the v34/ADR-0109 -race-only e2e failure): err is
// PER-HANDLE, never shared across reader handles. In production each chunk
// opens its own *RowReader with its own sticky err (row_reader.go), set at
// the start of each ReadRowsBatch and read by the caller after the page
// drains — sequentially, on that ONE reader. An EARLIER cut of this fake
// shared a single instance (and thus a single err) across concurrent chunk
// readers, so a sibling chunk's emit could reset err=nil between the
// dropping chunk's drop and its readerStreamErr check — masking the
// retriable drop, which then read as a clean short-page end-of-chunk and
// SILENTLY dropped the unread rows. That was a HARNESS modelling defect,
// not a production bug: per-handle err reproduces the real isolation.
type fakeSource struct {
	store *fakeSourceStore
	mu    sync.Mutex
	err   error // sticky error from the most recent read on THIS handle
}

// newFakeSource builds a store and one reader handle over it. Additional
// handles (one per chunk / per OpenRowReader) come from openHandle, sharing
// the store but each carrying its own sticky err.
func newFakeSource(maxID, dropBeforeID int64) *fakeSource {
	return &fakeSource{store: &fakeSourceStore{maxID: maxID, dropBeforeID: dropBeforeID}}
}

// openHandle returns a fresh reader handle over the same store — the fake's
// analog of source.OpenRowReader minting a new connection-backed reader.
func (s *fakeSource) openHandle() *fakeSource {
	return &fakeSource{store: s.store}
}

func (s *fakeSource) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// emit serves rows with id in (after, upTo] (upTo<=0 meaning no upper
// bound), up to limit (limit<=0 meaning unbounded). The first time any
// handle's emit reaches id == dropBeforeID it injects the one-shot drop
// (closes the channel having sent the rows before it, sets a retriable
// sticky error on THIS handle). The err reset and the drop's err are both
// on this handle (s.mu / s.err); the one-shot latch is on the shared store
// (s.store.mu / dropFired).
func (s *fakeSource) emit(ctx context.Context, after, upTo int64, limit int) <-chan ir.Row {
	s.mu.Lock()
	s.err = nil
	s.mu.Unlock()

	out := make(chan ir.Row, rowChanBuffer)
	go func() {
		defer close(out)
		sent := 0
		for id := after + 1; id <= s.store.maxID; id++ {
			if upTo > 0 && id > upTo {
				return
			}
			if limit > 0 && sent >= limit {
				return
			}
			s.store.mu.Lock()
			if s.store.dropBeforeID != 0 && !s.store.dropFired && id == s.store.dropBeforeID {
				// Inject the one-shot mid-table connection drop: stop emitting
				// at this PK and surface a RETRIABLE sticky error on THIS
				// handle (the connection-drop class). Deterministic across
				// concurrent chunks because it is keyed on the PK value, not a
				// call count; one-shot because the latch is on the store.
				s.store.dropFired = true
				s.store.mu.Unlock()
				s.mu.Lock()
				s.err = fakeRetriableErr{msg: "mysql: rows iteration: invalid connection"}
				s.mu.Unlock()
				return
			}
			s.store.mu.Unlock()
			select {
			case <-ctx.Done():
				s.mu.Lock()
				s.err = ctx.Err()
				s.mu.Unlock()
				return
			case out <- ir.Row{"id": id, "v": id * 10}:
				sent++
			}
		}
	}()
	return out
}

func (s *fakeSource) ReadRows(ctx context.Context, _ *ir.Table) (<-chan ir.Row, error) {
	return s.emit(ctx, 0, 0, 0), nil
}

func (s *fakeSource) ReadRowsBatch(ctx context.Context, _ *ir.Table, after []any, limit int) (<-chan ir.Row, error) {
	return s.emit(ctx, afterID(after), 0, limit), nil
}

func (s *fakeSource) ReadRowsBatchBounded(ctx context.Context, _ *ir.Table, after, upTo []any, limit int) (<-chan ir.Row, error) {
	return s.emit(ctx, afterID(after), afterID(upTo), limit), nil
}

func (s *fakeSource) RangeBounds(_ context.Context, _ *ir.Table, _ string) (minVal, maxVal any, err error) {
	return int64(1), s.store.maxID, nil
}

func (s *fakeSource) CountRows(context.Context, *ir.Table) (int64, error) { return s.store.maxID, nil }

func (s *fakeSource) EstimateRowCount(context.Context, *ir.Table) (int64, error) {
	return s.store.maxID, nil
}

func afterID(pk []any) int64 {
	if len(pk) == 0 || pk[0] == nil {
		return 0
	}
	switch v := pk[0].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	default:
		return 0
	}
}

// fakeTarget records every row written, keyed by PK, distinguishing the
// idempotent (upsert) and plain (INSERT) write calls, and supports
// TRUNCATE (the truncate-restart strategy). plainInserts counts how many
// times a PK was plain-INSERTed so a test can assert "zero duplicate plain
// inserts after a truncate-restart".
type fakeTarget struct {
	mu           sync.Mutex
	rows         map[int64]int64 // id -> v
	plainInserts int             // total plain-INSERT row applications
	upserts      int             // total upsert row applications
	truncations  int
}

func newFakeTarget() *fakeTarget { return &fakeTarget{rows: map[int64]int64{}} }

func (w *fakeTarget) WriteRows(_ context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	for row := range rows {
		id := row["id"].(int64)
		w.mu.Lock()
		w.rows[id] = row["v"].(int64)
		w.plainInserts++
		w.mu.Unlock()
	}
	return nil
}

func (w *fakeTarget) WriteRowsIdempotent(_ context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	for row := range rows {
		id := row["id"].(int64)
		w.mu.Lock()
		w.rows[id] = row["v"].(int64) // UPSERT: overwrite absorbs overlap
		w.upserts++
		w.mu.Unlock()
	}
	return nil
}

func (w *fakeTarget) HandlesNoPKIdempotentCopy() bool { return true }

func (w *fakeTarget) TruncateTable(_ context.Context, _ *ir.Table) error {
	w.mu.Lock()
	w.rows = map[int64]int64{}
	w.truncations++
	w.mu.Unlock()
	return nil
}

func (w *fakeTarget) snapshotIDs() []int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	ids := make([]int64, 0, len(w.rows))
	for id := range w.rows {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// fakeTargetEngine hands fakeTarget back from OpenRowWriter (the chunk /
// table-pair connection factory) and a FRESH fakeSource HANDLE from
// OpenRowReader (the per-chunk reader factory + the ADR-0109 fresh-reader
// factory). The SAME *fakeTarget instance is returned every time so writes
// from chunk-0's primary writer and a freshly-opened chunk/table writer all
// land in one row map. OpenRowReader, by contrast, returns a NEW handle
// each call (sharing the backing store) — modelling production, where each
// chunk opens its own connection-backed reader with its own sticky Err()
// (the fidelity that makes the e2e test exercise the real isolation rather
// than a shared-err artifact; see fakeSource).
type fakeTargetEngine struct {
	stubEngine
	target *fakeTarget
	source *fakeSource
}

func (e *fakeTargetEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return e.target, nil
}

func (e *fakeTargetEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return e.source.openHandle(), nil
}

// expectedIDs returns 1..n.
func expectedIDs(n int64) []int64 {
	out := make([]int64, n)
	for i := int64(0); i < n; i++ {
		out[i] = i + 1
	}
	return out
}

func intPKTable(name string) *ir.Table {
	return &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 8}},
			{Name: "v", Type: ir.Integer{Width: 8}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

func noPKTable(name string) *ir.Table {
	return &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 8}},
			{Name: "v", Type: ir.Integer{Width: 8}},
		},
	}
}

// chunkDeps builds parallelBulkCopyDeps wired to the shared source/target
// so the within-table chunked path (and the ADR-0109 fresh-reader factory)
// engage. minRows=1 so even a small table chunks.
func chunkDeps(eng *fakeTargetEngine) *parallelBulkCopyDeps {
	return &parallelBulkCopyDeps{
		source:      eng,
		target:      eng,
		parallelism: 2,
		minRows:     1,
	}
}

// runOneTable drives bulkCopyOneTable with a disabled state store (every
// writeTableProgress / markFailed is a no-op), an in-memory state map, and
// the supplied deps/reader/writer.
func runOneTable(t *testing.T, table *ir.Table, rows ir.RowReader, rw ir.RowWriter, deps *parallelBulkCopyDeps, resuming bool) error {
	t.Helper()
	state := &ir.MigrationState{TableProgress: map[string]ir.TableProgress{}}
	var mu sync.Mutex
	return bulkCopyOneTable(context.Background(), resumeContext{}, state, &mu, rows, rw, table,
		resuming, 7 /* small batch so the chunk loop pages */, deps, nil, ShardColumnSpec{})
}

func assertConverged(t *testing.T, tgt *fakeTarget, n int64) {
	t.Helper()
	got := tgt.snapshotIDs()
	want := expectedIDs(n)
	if len(got) != len(want) {
		t.Fatalf("target has %d distinct rows, want %d (a drop or dup); ids=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("target ids diverge at %d: got %d want %d (full %v)", i, got[i], want[i], got)
		}
	}
	// Every PK present exactly once is guaranteed by the map; verify values too.
	tgt.mu.Lock()
	defer tgt.mu.Unlock()
	for id, v := range tgt.rows {
		if v != id*10 {
			t.Fatalf("row id=%d has v=%d, want %d (value corruption)", id, v, id*10)
		}
	}
}

// TestSourceReadRetryE2E_KeysetChunked_ResumesFromLastPK pins ADR-0109
// case 1: a keyset/integer-PK CHUNKED table whose source read drops
// mid-chunk reconnects and RESUMES from the chunk's persisted LastPK
// (WHERE pk > LastPK) via the idempotent path — src row-set == dst, zero
// dups, zero drops.
func TestSourceReadRetryE2E_KeysetChunked_ResumesFromLastPK(t *testing.T) {
	captureSlog(t)
	withFastSourceReadBackoff(t)

	const n = 40
	src := newFakeSource(n, 8) // drop mid chunk-0 (ids 1..20), at id=8
	tgt := newFakeTarget()
	eng := &fakeTargetEngine{target: tgt, source: src}

	if err := runOneTable(t, intPKTable("documents"), src, tgt, chunkDeps(eng), false); err != nil {
		t.Fatalf("bulkCopyOneTable: %v", err)
	}
	assertConverged(t, tgt, n)
	if tgt.truncations != 0 {
		t.Errorf("chunked resume must NOT truncate (it resumes from LastPK); truncations=%d", tgt.truncations)
	}
}

// TestSourceReadRetryE2E_GrowGate_QuiescesAndConverges pins the ADR-0110
// coordinated-pause integration on the SOURCE-READ path end-to-end (no
// Docker): a real growGate (shrunk envelope) is wired into the run; a
// mid-chunk source drop TRIPS the gate (so siblings would quiesce) and each
// retry Awaits it, and the copy still converges byte-identically to the
// no-gate case — proving the gate changes only WHEN a read attempt runs,
// never WHAT lands (zero dup, zero drop). The cross-lane "siblings issue no
// new flushes while closed" property is pinned mechanically by the FSM
// unit tests (grow_gate_test.go: reopen-broadcast + ctx-cancel-unwind +
// coalescing); here we pin that the integrated path preserves correctness.
func TestSourceReadRetryE2E_GrowGate_QuiescesAndConverges(t *testing.T) {
	captureSlog(t)
	withFastSourceReadBackoff(t)
	withFastGrowGate(t)

	const n = 40
	src := newFakeSource(n, 8) // drop mid chunk-0 at id=8
	tgt := newFakeTarget()
	eng := &fakeTargetEngine{target: tgt, source: src}

	deps := chunkDeps(eng)
	gate := migcore.NewGrowGate(context.Background(), nil)
	var tripped, awaited int64
	obs := &observingGate{inner: gate, trips: &tripped, awaits: &awaited}
	deps.growGate = obs

	if err := runOneTable(t, intPKTable("documents"), src, tgt, deps, false); err != nil {
		t.Fatalf("bulkCopyOneTable: %v", err)
	}
	assertConverged(t, tgt, n)
	if tripped == 0 {
		t.Error("the mid-chunk source drop should have TRIPPED the coordinated grow-gate; trips=0")
	}
	if awaited == 0 {
		t.Error("the source-read path should have AWAITED the gate at least once; awaits=0")
	}
}

// observingGate wraps a real growGate to count Await/Trip calls while
// preserving the real FSM behaviour (so the e2e exercises the actual
// coordinator, not a no-op stub).
type observingGate struct {
	inner  *migcore.GrowGate
	trips  *int64
	awaits *int64
}

func (g *observingGate) Await(ctx context.Context) error {
	atomic.AddInt64(g.awaits, 1)
	return g.inner.Await(ctx)
}

func (g *observingGate) Trip(reason string) {
	atomic.AddInt64(g.trips, 1)
	g.inner.Trip(reason)
}

// TestSourceReadRetryE2E_Idempotent_AbsorbsOverlap pins ADR-0109 case 2:
// an integer-PK table copied through the idempotent (upsert) path whose
// read drops mid-table converges — the re-read overlap between the drop
// point and the next page is absorbed by the upsert (no dup). Same
// chunked dispatch, but here we assert the upsert path actually ran (the
// overlap is absorbed, not plain-inserted twice).
func TestSourceReadRetryE2E_Idempotent_AbsorbsOverlap(t *testing.T) {
	captureSlog(t)
	withFastSourceReadBackoff(t)

	const n = 30
	src := newFakeSource(n, 6) // drop mid chunk-0 (ids 1..15), at id=6
	tgt := newFakeTarget()
	eng := &fakeTargetEngine{target: tgt, source: src}

	if err := runOneTable(t, intPKTable("connections"), src, tgt, chunkDeps(eng), false); err != nil {
		t.Fatalf("bulkCopyOneTable: %v", err)
	}
	assertConverged(t, tgt, n)
	if tgt.upserts == 0 {
		t.Error("expected the idempotent (upsert) path to run on the chunked resume; upserts=0")
	}
}

// TestSourceReadRetryE2E_PlainNonChunkable_TruncateRestart pins ADR-0109
// case 3: a NO-PK table (no safe mid-table cursor) whose plain read drops
// mid-table is recovered by TRUNCATE + restart from a fresh reader — src
// row-set == dst, zero dups. After the truncate the final copy is the only
// surviving write, so every PK appears exactly once.
func TestSourceReadRetryE2E_PlainNonChunkable_TruncateRestart(t *testing.T) {
	captureSlog(t)
	withFastSourceReadBackoff(t)

	const n = 25
	src := newFakeSource(n, 11) // drop the whole-table scan partway, at id=11
	tgt := newFakeTarget()
	eng := &fakeTargetEngine{target: tgt, source: src}

	if err := runOneTable(t, noPKTable("logs"), src, tgt, chunkDeps(eng), false); err != nil {
		t.Fatalf("bulkCopyOneTable: %v", err)
	}
	assertConverged(t, tgt, n)
	if tgt.truncations < 1 {
		t.Errorf("non-chunkable resume must TRUNCATE before restart; truncations=%d", tgt.truncations)
	}
	// The plain path applied the 10 pre-drop rows AND the n clean-restart
	// rows, but the TRUNCATE wiped the partial first attempt — so the final
	// distinct row-set is exactly 1..n (assertConverged above) with no
	// duplicate PKs surviving, even though more inserts were issued in total.
	if tgt.plainInserts <= n {
		t.Errorf("plain inserts issued = %d; expected > %d (a partial pre-drop attempt + the clean restart)", tgt.plainInserts, n)
	}
}

// TestSourceReadRetryE2E_NonRetriableDecodeIsTerminal pins that a NON-
// retriable read error (a decode fault) on a non-chunkable table stays
// terminal: no retry, no truncate, the error surfaces.
func TestSourceReadRetryE2E_NonRetriableDecodeIsTerminal(t *testing.T) {
	captureSlog(t)
	withFastSourceReadBackoff(t)

	const n = 20
	src := &terminalDropSource{fakeSource: newFakeSource(n, 0)}
	tgt := newFakeTarget()
	eng := &fakeTargetEngine{target: tgt, source: src.fakeSource}

	err := runOneTable(t, noPKTable("bad"), src, tgt, chunkDeps(eng), false)
	if err == nil {
		t.Fatal("expected a terminal error for a non-retriable decode fault, got nil")
	}
	if tgt.truncations != 0 {
		t.Errorf("non-retriable error must NOT truncate; truncations=%d", tgt.truncations)
	}
}

// terminalDropSource wraps fakeSource but injects a NON-retriable (decode-
// class) sticky error on the first whole-table read instead of the
// retriable connection-drop. It exercises the terminal path through the
// real bulkCopyOneTable copy.
type terminalDropSource struct{ *fakeSource }

func (s *terminalDropSource) ReadRows(ctx context.Context, _ *ir.Table) (<-chan ir.Row, error) {
	out := make(chan ir.Row, rowChanBuffer)
	go func() {
		defer close(out)
		for id := int64(1); id <= 3; id++ {
			select {
			case <-ctx.Done():
				return
			case out <- ir.Row{"id": id, "v": id * 10}:
			}
		}
		s.mu.Lock()
		s.err = errPlainDecode // a plain (non-retriable) error
		s.mu.Unlock()
	}()
	return out, nil
}

var errPlainDecode = &decodeFaultErr{}

type decodeFaultErr struct{}

func (*decodeFaultErr) Error() string { return "mysql: column \"v\": decode failed (non-retriable)" }

// TestSourceReadRetryE2E_SiblingUnaffected pins that a transient on ONE
// table does not abort a SIBLING table's copy: two tables run through the
// cross-table pool; one drops-and-recovers, the other copies clean, and
// BOTH converge. (The retry is contained in bulkCopyOneTable, so a
// recovered transient never returns to the pool's errgroup.)
func TestSourceReadRetryE2E_SiblingUnaffected(t *testing.T) {
	captureSlog(t)
	withFastSourceReadBackoff(t)

	const n = 30
	// The dropping table and its target.
	dropSrc := newFakeSource(n, 5)
	dropTgt := newFakeTarget()
	dropEng := &fakeTargetEngine{target: dropTgt, source: dropSrc}

	// The clean sibling table and its target.
	cleanSrc := newFakeSource(n, 0)
	cleanTgt := newFakeTarget()
	cleanEng := &fakeTargetEngine{target: cleanTgt, source: cleanSrc}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = runOneTable(t, intPKTable("dropping"), dropSrc, dropTgt, chunkDeps(dropEng), false)
	}()
	go func() {
		defer wg.Done()
		errs[1] = runOneTable(t, intPKTable("clean"), cleanSrc, cleanTgt, chunkDeps(cleanEng), false)
	}()
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("table %d copy failed: %v", i, err)
		}
	}
	assertConverged(t, dropTgt, n)
	assertConverged(t, cleanTgt, n)
}
