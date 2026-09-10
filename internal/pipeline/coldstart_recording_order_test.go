// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// orderingStateStore is a [ir.MigrationStateStore] double that holds the FIRST
// WriteTableProgress open until a SECOND write arrives (or a short grace
// elapses), and records every entry in the order the store actually saw it.
//
// It exists because the defect it pins is invisible to `-race`: the recorder's
// in-memory arithmetic is correctly locked, and only the durable writes race
// (see [tableProgressRecorder.persistMu]). The only way to observe it is to
// hold one write open and see whether a newer one can go past.
//
// The grace is what makes the fixture WORK IN BOTH DIRECTIONS rather than
// deadlocking on the fixed code. Unfixed, the second write reaches the store
// while the first is held, releases it, and lands FIRST — the defect. Fixed,
// the second write never reaches the store at all (it is waiting on the
// table's write lock), so nothing releases the first; the grace expires, the
// first lands, the lock frees, and the second lands after it.
type orderingStateStore struct {
	mu      sync.Mutex
	entries []ir.TableProgress

	firstEntered  chan struct{} // closed when the first write arrives
	secondEntered chan struct{} // closed when a second write reaches the store
	seen          int
}

// orderingGrace bounds how long the held first write waits for a second. It
// only elapses on the CORRECT code path, so it is a bounded test cost, not a
// race window.
const orderingGrace = 250 * time.Millisecond

func newOrderingStateStore() *orderingStateStore {
	return &orderingStateStore{
		firstEntered:  make(chan struct{}),
		secondEntered: make(chan struct{}),
	}
}

func (s *orderingStateStore) EnsureControlTable(context.Context) error     { return nil }
func (s *orderingStateStore) Close() error                                 { return nil }
func (s *orderingStateStore) ClearMigration(context.Context, string) error { return nil }

func (s *orderingStateStore) Read(context.Context, string) (ir.MigrationState, bool, error) {
	return ir.MigrationState{}, false, nil
}

func (s *orderingStateStore) Write(context.Context, ir.MigrationState) error { return nil }

func (s *orderingStateStore) WriteTableProgress(_ context.Context, _, _ string, entry ir.TableProgress) error {
	s.mu.Lock()
	s.seen++
	nth := s.seen
	s.mu.Unlock()
	switch nth {
	case 1:
		close(s.firstEntered)
		select {
		case <-s.secondEntered:
		case <-time.After(orderingGrace):
		}
	case 2:
		close(s.secondEntered)
	}
	s.mu.Lock()
	s.entries = append(s.entries, entry)
	s.mu.Unlock()
	return nil
}

func (s *orderingStateStore) last() (entry ir.TableProgress, writes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 {
		return ir.TableProgress{}, 0
	}
	return s.entries[len(s.entries)-1], len(s.entries)
}

// Two work-stealing pipelines finishing chunks of the SAME table must not be
// able to leave the DURABLE row behind the truth. The recorder snapshots under
// its own lock and writes outside it, so without per-table write ordering the
// slower snapshot lands LAST and overwrites `complete` with `in_progress` —
// permanently, since nothing writes that table again.
//
// Mutation-run: removing the per-table lock in
// [tableProgressRecorder.persist] leaves the final entry at {100,
// in_progress}, which is the defect (audit VF0910-F2).
func TestTableProgressRecorderPersistsInOrderUnderConcurrentChunks(t *testing.T) {
	t.Parallel()
	store := newOrderingStateStore()
	// nil throttle: this pin grades ORDERING, so every write must reach the
	// store rather than being coalesced.
	rc := resumeContext{store: store, migrationID: "sync-order", enabled: true, noResume: true}
	rec := newTableProgressRecorder(rc, "")
	table := &ir.Table{Name: "big"}
	rec.expectItems(table, 2)

	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rec.completed(ctx, table, 100) // chunk 1: {100, in_progress}; blocks in the store
	}()

	<-store.firstEntered // chunk 1 is inside the store write, holding it open

	wg.Add(1)
	go func() {
		defer wg.Done()
		rec.completed(ctx, table, 100) // chunk 2: {200, complete} — the LAST chunk
	}()

	// Chunk 2 must not be able to overtake chunk 1's in-flight write.
	wg.Wait()

	last, n := store.last()
	if n != 2 {
		t.Fatalf("store saw %d writes, want 2 — the fixture is not exercising both chunks", n)
	}
	if last.State != ir.TableProgressComplete || last.RowsCopied != 200 {
		t.Fatalf("the DURABLE row ended at {rows:%d, state:%q}; want {200, %q}.\n"+
			"A stale snapshot overwrote a newer one: the table finished, and the row that survives says it "+
			"did not. `sync status` would show it in_progress forever, and a resume gated on these rows would "+
			"decline a copy that actually landed.",
			last.RowsCopied, last.State, ir.TableProgressComplete)
	}
}

// The mechanism the ordering rests on, pinned directly: a snapshot whose
// sequence is behind what already landed is DROPPED, not written.
func TestTableProgressRecorderDropsAStaleSnapshot(t *testing.T) {
	t.Parallel()
	store := newFakeStateStore()
	rc := resumeContext{store: store, migrationID: "sync-stale", enabled: true, noResume: true}
	rec := newTableProgressRecorder(rc, "")

	ctx := context.Background()
	rec.persist(ctx, "big", ir.TableProgress{RowsCopied: 200, State: ir.TableProgressComplete}, 2)
	rec.persist(ctx, "big", ir.TableProgress{RowsCopied: 100, State: ir.TableProgressInProgress}, 1)

	state, _ := store.get("sync-stale")
	entry := state.TableProgress["big"]
	if entry.State != ir.TableProgressComplete || entry.RowsCopied != 200 {
		t.Fatalf("a stale snapshot (seq 1) overwrote a newer one (seq 2): row is {rows:%d, state:%q}, "+
			"want {200, %q}", entry.RowsCopied, entry.State, ir.TableProgressComplete)
	}

	// And a NEWER snapshot still lands — the drop rule must not wedge the row.
	rec.persist(ctx, "big", ir.TableProgress{RowsCopied: 300, State: ir.TableProgressComplete}, 3)
	state, _ = store.get("sync-stale")
	if got := state.TableProgress["big"].RowsCopied; got != 300 {
		t.Fatalf("a newer snapshot (seq 3) was dropped: row still says %d rows, want 300", got)
	}
}
