// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestLiveAddedFilter_ContainsAndSet pins the load-bearing surface of
// liveAddedFilter — concurrent reads + writes are correct (no torn
// state, no map corruption). The atomic-pointer-to-immutable-map
// shape (ADR-0034) is the reason the dispatch hot path can read
// lock-free.
func TestLiveAddedFilter_ContainsAndSet(t *testing.T) {
	var f liveAddedFilter

	// Empty filter contains nothing.
	if f.Contains("orders") {
		t.Errorf("empty filter Contains(orders) = true; want false")
	}

	// Set a few tables → Contains reports membership.
	f.Set([]string{"orders", "events"})
	if !f.Contains("orders") {
		t.Errorf("after Set([orders, events]), Contains(orders) = false; want true")
	}
	if !f.Contains("events") {
		t.Errorf("after Set([orders, events]), Contains(events) = false; want true")
	}
	if f.Contains("audit_log") {
		t.Errorf("after Set([orders, events]), Contains(audit_log) = true; want false")
	}

	// Re-Set replaces (not merges); the new set is authoritative.
	f.Set([]string{"audit_log"})
	if f.Contains("orders") {
		t.Errorf("after re-Set([audit_log]), Contains(orders) = true; want false")
	}
	if !f.Contains("audit_log") {
		t.Errorf("after re-Set([audit_log]), Contains(audit_log) = false; want true")
	}

	// Empty re-Set clears.
	f.Set(nil)
	if f.Contains("audit_log") {
		t.Errorf("after Set(nil), Contains(audit_log) = true; want false")
	}
}

// TestLiveAddedFilter_NilSafe pins the nil-receiver shape — a nil
// *liveAddedFilter must report Contains=false rather than panicking.
// The streamer's filterChanges hot path passes a *liveAddedFilter that
// MAY be nil (legacy / engine-without-the-surface), and the dispatch
// check does not nil-guard at the call site.
func TestLiveAddedFilter_NilSafe(t *testing.T) {
	var f *liveAddedFilter // nil
	if f.Contains("orders") {
		t.Errorf("nil-receiver Contains = true; want false")
	}
}

// TestLiveAddedFilter_Snapshot pins the deterministic-sort property
// for log output.
func TestLiveAddedFilter_Snapshot(t *testing.T) {
	var f liveAddedFilter
	f.Set([]string{"zeta", "alpha", "  ", "mu"})
	got := f.Snapshot()
	want := []string{"alpha", "mu", "zeta"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Snapshot() = %v; want %v (sorted, whitespace-trimmed)", got, want)
	}
}

// TestChangeAllowedWithLiveAdd_OrSemantics pins the additive merge
// rule (ADR-0034 § "Mechanism"): a change passes if EITHER the base
// filter allows it OR the live-added set contains its unqualified
// name.
func TestChangeAllowedWithLiveAdd_OrSemantics(t *testing.T) {
	cases := []struct {
		name    string
		base    TableFilter
		live    []string
		change  ir.Change
		allowed bool
	}{
		{
			name:    "empty base + empty live → all pass",
			base:    TableFilter{},
			live:    nil,
			change:  ir.Insert{Schema: "s", Table: "users"},
			allowed: true,
		},
		{
			name:    "include allows the table → pass",
			base:    TableFilter{Include: []string{"users"}},
			live:    nil,
			change:  ir.Insert{Schema: "s", Table: "users"},
			allowed: true,
		},
		{
			name:    "include drops the table; live empty → drop",
			base:    TableFilter{Include: []string{"users"}},
			live:    nil,
			change:  ir.Insert{Schema: "s", Table: "orders"},
			allowed: false,
		},
		{
			name:    "include drops the table; live admits it → pass (additive)",
			base:    TableFilter{Include: []string{"users"}},
			live:    []string{"orders"},
			change:  ir.Insert{Schema: "s", Table: "orders"},
			allowed: true,
		},
		{
			name:    "include drops the table; live empty (other tables) → drop",
			base:    TableFilter{Include: []string{"users"}},
			live:    []string{"events"},
			change:  ir.Insert{Schema: "s", Table: "orders"},
			allowed: false,
		},
		{
			name:    "exclude drops the table; live admits it → pass (additive override)",
			base:    TableFilter{Exclude: []string{"audit_*"}},
			live:    []string{"audit_special"},
			change:  ir.Insert{Schema: "s", Table: "audit_special"},
			allowed: true,
		},
		{
			name:    "exclude drops the table; live empty → drop",
			base:    TableFilter{Exclude: []string{"audit_*"}},
			live:    nil,
			change:  ir.Insert{Schema: "s", Table: "audit_log"},
			allowed: false,
		},
		{
			name:    "tx-begin bypasses both filters",
			base:    TableFilter{Include: []string{"users"}},
			live:    nil,
			change:  ir.TxBegin{},
			allowed: true,
		},
		{
			name:    "tx-commit bypasses both filters",
			base:    TableFilter{Include: []string{"users"}},
			live:    nil,
			change:  ir.TxCommit{},
			allowed: true,
		},
		{
			name:    "schema-prefixed name strips schema before lookup",
			base:    TableFilter{Include: []string{"users"}},
			live:    []string{"orders"},
			change:  ir.Insert{Schema: "public", Table: "orders"},
			allowed: true,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			live := &liveAddedFilter{}
			if len(c.live) > 0 {
				live.Set(c.live)
			}
			got := changeAllowedWithLiveAdd(c.change, c.base, live)
			if got != c.allowed {
				t.Errorf("changeAllowedWithLiveAdd(%v) = %v; want %v", c.change.QualifiedName(), got, c.allowed)
			}
		})
	}
}

// TestFilterChangesWithLiveAdd_PassThroughEmpty confirms the zero-
// allocation fast path: empty base AND nil liveAddedFilter returns
// the input channel verbatim with no goroutine.
func TestFilterChangesWithLiveAdd_PassThroughEmpty(t *testing.T) {
	in := make(chan ir.Change)
	got := filterChangesWithLiveAdd(context.Background(), in, TableFilter{}, nil)
	if got != in {
		t.Errorf("empty filter + nil live: filterChangesWithLiveAdd returned a wrapped channel; want the same channel pointer (zero-overhead fast path)")
	}
}

// TestFilterChangesWithLiveAdd_LiveAdmitsExcluded pins the wrapped
// path: a base filter that excludes a table + a live-added set that
// includes it must allow the change through. This is the load-bearing
// shape of the ADR-0034 add-table flow.
func TestFilterChangesWithLiveAdd_LiveAdmitsExcluded(t *testing.T) {
	base := TableFilter{Include: []string{"users"}}
	live := &liveAddedFilter{}
	live.Set([]string{"orders"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan ir.Change, 4)
	out := filterChangesWithLiveAdd(ctx, in, base, live)

	// 1. users insert → passes (base filter).
	// 2. orders insert → passes (live-added).
	// 3. audit_log insert → drops.
	// 4. users insert again → passes.
	in <- ir.Insert{Schema: "s", Table: "users", Row: ir.Row{"id": int64(1)}}
	in <- ir.Insert{Schema: "s", Table: "orders", Row: ir.Row{"id": int64(2)}}
	in <- ir.Insert{Schema: "s", Table: "audit_log", Row: ir.Row{"id": int64(3)}}
	in <- ir.Insert{Schema: "s", Table: "users", Row: ir.Row{"id": int64(4)}}
	close(in)

	deadline := time.After(2 * time.Second)
	allowed := []string{}
	for {
		select {
		case c, ok := <-out:
			if !ok {
				goto done
			}
			ins := c.(ir.Insert)
			allowed = append(allowed, ins.Table)
		case <-deadline:
			t.Fatalf("timed out draining filtered channel; allowed so far: %v", allowed)
		}
	}
done:
	want := []string{"users", "orders", "users"}
	if !reflect.DeepEqual(allowed, want) {
		t.Errorf("allowed events = %v; want %v (audit_log must drop)", allowed, want)
	}
}

// TestPollLiveAddedTables_DetectsChange pins the poll-then-set shape:
// when the reader returns a non-empty set, the goroutine writes it
// into the target liveAddedFilter, and the dispatch hot path observes
// the change.
func TestPollLiveAddedTables_DetectsChange(t *testing.T) {
	setPollIntervalForTest(t, 5*time.Millisecond)

	reader := &fakeLiveAddedReader{}
	target := &liveAddedFilter{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pollLiveAddedTables(ctx, reader, "stream-a", target)

	// Initially the target is empty.
	if target.Contains("orders") {
		t.Fatal("target reported orders before any poll tick")
	}

	// Simulate the orchestrator's column write.
	reader.Set([]string{"orders"})

	// Poll cadence is 5ms; allow a few ticks for the merge to land.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if target.Contains("orders") {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Errorf("poll never observed live-added orders within 500ms")
}

// fakeLiveAddedReader is a thread-safe in-memory liveAddedTablesReader
// for the poll-loop unit test. The mutex is load-bearing — the test
// goroutine writes via Set while the poller goroutine reads via
// ReadLiveAddedTables, and CI's -race detector flagged the unsynced
// access (the docstring claimed thread-safety but the implementation
// was actually unsafe; v0.27.0 first-tag CI surfaced the race).
type fakeLiveAddedReader struct {
	mu     sync.Mutex
	tables []string
}

func (f *fakeLiveAddedReader) ReadLiveAddedTables(_ context.Context, _ string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.tables))
	copy(out, f.tables)
	return out, nil
}

func (f *fakeLiveAddedReader) Set(t []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tables = append(f.tables[:0], t...)
}
