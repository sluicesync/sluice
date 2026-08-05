// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestLaneForRoute_ScopeDecidesTheHash is the item-131 unit pin at the single
// point where a [Route] becomes a lane. Three properties, all load-bearing:
//
//   - RouteScopeKey spreads distinct keys of a table across lanes (the fast
//     path is not lost);
//   - RouteScopeTable collapses every key of a table onto ONE lane, and that
//     lane still differs between tables (cross-table concurrency survives);
//   - the ZERO VALUE behaves as RouteScopeTable — the fail-closed default a
//     future implementor inherits without reading a word of this file.
func TestLaneForRoute_ScopeDecidesTheHash(t *testing.T) {
	const lanes = 8
	r := NewRouter(lanes)

	keyLanes := map[int]bool{}
	tableLanes := map[int]bool{}
	zeroLanes := map[int]bool{}
	for i := int64(1); i <= 64; i++ {
		keyLanes[r.LaneForRoute(Route{Qualified: "ks.t", PKVals: []any{i}, Scope: RouteScopeKey})] = true
		tableLanes[r.LaneForRoute(Route{Qualified: "ks.t", PKVals: []any{i}, Scope: RouteScopeTable})] = true
		// Deliberately NOT setting Scope: this is the Route a future
		// implementor writes before anyone reviews it.
		zeroLanes[r.LaneForRoute(Route{Qualified: "ks.t", PKVals: []any{i}})] = true
	}
	if len(keyLanes) < 2 {
		t.Errorf("RouteScopeKey collapsed 64 keys onto %d lane(s); the per-key fast path is gone", len(keyLanes))
	}
	if len(tableLanes) != 1 {
		t.Errorf("RouteScopeTable spread 64 keys over %d lanes; want 1 (item 131 ordering)", len(tableLanes))
	}
	if len(zeroLanes) != 1 {
		t.Errorf("the zero-value scope spread 64 keys over %d lanes; the zero value MUST be the safe whole-table one", len(zeroLanes))
	}

	// Cross-table concurrency survives whole-table scoping: a spread of table
	// names must not all land on one lane.
	acrossTables := map[int]bool{}
	for _, tbl := range []string{"ks.a", "ks.b", "ks.c", "ks.d", "ks.e", "ks.f", "ks.g", "ks.h", "ks.i", "ks.j"} {
		acrossTables[r.LaneForRoute(Route{Qualified: tbl})] = true
	}
	if len(acrossTables) < 2 {
		t.Errorf("whole-table scoping put 10 tables on %d lane(s); cross-table concurrency must survive", len(acrossTables))
	}
}

// scopedSeam is a no-DB [LaneApplier] that routes by a per-change Route the
// test supplies and records which lane each batch arrived on plus the order
// changes were applied in, so an orchestrator-level pin can assert routeRow
// honours the scope end to end. applyDelay makes a lane's commit slow enough
// that a missing barrier drain would be observable.
type scopedSeam struct {
	route      func(ir.Change) (Route, bool)
	applyDelay time.Duration

	mu                    sync.Mutex
	lanes                 map[int]bool
	order                 []string
	applied               int
	appliedAtFirstBarrier int
	barriers              int
}

func (s *scopedSeam) RouteForChange(_ context.Context, c ir.Change) (Route, bool, error) {
	r, ok := s.route(c)
	return r, ok, nil
}

func (s *scopedSeam) ApplyLaneBatch(_ context.Context, lane int, batch []ir.Change) (int, error) {
	if s.applyDelay > 0 {
		time.Sleep(s.applyDelay)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lanes[lane] = true
	s.applied += len(batch)
	for _, c := range batch {
		s.order = append(s.order, c.Pos().Token)
	}
	return len(batch), nil
}

func (s *scopedSeam) ClassifyError(err error) error                             { return err }
func (s *scopedSeam) WriteCheckpoint(context.Context, ir.Position, int64) error { return nil }

func (s *scopedSeam) ApplyBarrierChange(context.Context, ir.Change) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.barriers == 0 {
		s.appliedAtFirstBarrier = s.applied
	}
	s.barriers++
	return nil
}

// TestRouteRow_HonoursTableScope drives the whole orchestrator with a seam
// that reports RouteScopeTable and asserts every change landed on ONE lane, in
// source order. The router pin above proves LaneForRoute hashes correctly; this
// proves [Orchestrator.routeRow] passes the scope through rather than
// re-deriving a per-key lane, which is where item 131 lived.
func TestRouteRow_HonoursTableScope(t *testing.T) {
	seam := &scopedSeam{
		lanes: map[int]bool{},
		route: func(c ir.Change) (Route, bool) {
			ins, ok := c.(ir.Insert)
			if !ok {
				return Route{}, false
			}
			// Whole-table scope, with a genuinely varying PK: a routeRow that
			// ignored Scope would spread these across the lanes.
			return Route{Qualified: "ks.t", PKVals: []any{ins.Row["id"]}}, true
		},
	}
	o := NewOrchestrator(Config{Lanes: 8, MaxBatchSize: 4}, seam)

	changes := make(chan ir.Change, 64)
	for i := int64(1); i <= 64; i++ {
		changes <- ir.Insert{
			Position: ir.Position{Engine: "test", Token: "t-" + strconv.FormatInt(i, 10)},
			Schema:   "ks", Table: "t", Row: ir.Row{"id": i},
		}
	}
	close(changes)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := o.Run(ctx, changes); err != nil {
		t.Fatalf("Run: %v", err)
	}

	seam.mu.Lock()
	defer seam.mu.Unlock()
	if len(seam.lanes) != 1 {
		t.Errorf("whole-table-scoped changes reached %d lanes; want 1 (item 131)", len(seam.lanes))
	}
	if len(seam.order) != 64 {
		t.Fatalf("applied %d changes; want 64", len(seam.order))
	}
	for i := range seam.order {
		if want := "t-" + strconv.FormatInt(int64(i+1), 10); seam.order[i] != want {
			t.Fatalf("change %d applied as %q; want %q (one lane must apply in source order)", i, seam.order[i], want)
		}
	}
}

// TestBarrier_DrainsAllLanesBeforeApply is the PREMISE check for the engines'
// schema-boundary cache invalidation: both appliers drop nonPKUniqueCache on a
// schema event, which re-routes a table from per-key lanes to a single lane,
// and their comments argue that is safe because the barrier has already
// drained every lane. That argument is only as good as the drain. Here 40
// key-scoped changes (deliberately slow to commit, spread over 8 lanes) are
// followed by a barrier event; every one of them must be applied BEFORE
// ApplyBarrierChange runs.
func TestBarrier_DrainsAllLanesBeforeApply(t *testing.T) {
	const n = 40
	seam := &scopedSeam{
		lanes:      map[int]bool{},
		applyDelay: 5 * time.Millisecond,
		route: func(c ir.Change) (Route, bool) {
			ins, ok := c.(ir.Insert)
			if !ok {
				return Route{}, false // Truncate → barrier
			}
			return Route{Qualified: "ks.t", PKVals: []any{ins.Row["id"]}, Scope: RouteScopeKey}, true
		},
	}
	o := NewOrchestrator(Config{Lanes: 8, MaxBatchSize: 4}, seam)

	changes := make(chan ir.Change, n+1)
	for i := int64(1); i <= n; i++ {
		changes <- ir.Insert{
			Position: ir.Position{Engine: "test", Token: "t-" + strconv.FormatInt(i, 10)},
			Schema:   "ks", Table: "t", Row: ir.Row{"id": i},
		}
	}
	changes <- ir.Truncate{Position: ir.Position{Engine: "test", Token: "t-barrier"}, Schema: "ks", Table: "t"}
	close(changes)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := o.Run(ctx, changes); err != nil {
		t.Fatalf("Run: %v", err)
	}

	seam.mu.Lock()
	defer seam.mu.Unlock()
	if seam.barriers != 1 {
		t.Fatalf("barriers applied = %d; want 1 (the pin did not exercise the drain)", seam.barriers)
	}
	if len(seam.lanes) < 2 {
		t.Fatalf("lane changes reached %d lane(s); the drain is untested unless several lanes were in flight", len(seam.lanes))
	}
	if seam.appliedAtFirstBarrier != n {
		t.Errorf("%d of %d lane changes were durable when the barrier applied; the barrier must drain EVERY lane first",
			seam.appliedAtFirstBarrier, n)
	}
}
