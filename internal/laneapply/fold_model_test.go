// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// An external test package: these pins drive the orchestrator against the
// REAL lane-side fence (applymarks.LaneFence), which the laneapply package
// itself cannot import (applymarks imports laneapply).
package laneapply_test

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// modelSeam is a no-database [laneapply.LaneApplier] that models what
// amendment D must preserve, on the real [applymarks.LaneFence]:
//
//   - tables named m* are a marked class (a secondary-unique table: table
//     scope, one lane each), table "p" is PK-only (key scope);
//   - a change to a table in routeBlind is marked at APPLY time but not at
//     ROUTE time — the caches-moved disagreement the lane-side check exists
//     for;
//   - the persisted position is whatever the last committed position write
//     (a checkpoint, or a fold batch) wrote.
//
// It records a violation whenever (a) a position write goes backwards, (b) a
// position is persisted ahead of data (a transaction at or below it has a row
// not yet committed), or (c) a transaction's marks become durable while the
// persisted position is not that transaction's start — amendment B's
// invariant, "marks only for the first transaction after the persisted
// position", checked at the instant it could break.
type modelSeam struct {
	fence      applymarks.LaneFence
	routeBlind map[string]bool
	// delay returns how long a lane batch takes before it "commits".
	delay func(lane int, fold *laneapply.FoldTicket) time.Duration

	mu         sync.Mutex
	persisted  int // tx number the persisted position names (0 = run start)
	committed  map[string]bool
	rowsOfTx   map[int][]string
	violations []string
	admitted   map[string]bool // change id → its marks were written
	dropped    map[string]bool // change id → its marks were refused
	folds      int
	started    map[string]time.Time // change id → when its batch began
	foldDone   map[string]time.Time // tx → when its fold committed
}

func newModelSeam() *modelSeam {
	return &modelSeam{
		routeBlind: map[string]bool{},
		delay:      func(int, *laneapply.FoldTicket) time.Duration { return 0 },
		committed:  map[string]bool{},
		rowsOfTx:   map[int][]string{},
		admitted:   map[string]bool{},
		dropped:    map[string]bool{},
		started:    map[string]time.Time{},
		foldDone:   map[string]time.Time{},
	}
}

// modelTxNum is the transaction number a commit token (c0007) or a TxID
// (tx0007) names.
func modelTxNum(token string) int {
	n, err := strconv.Atoi(strings.TrimLeft(token, "ctx"))
	if err != nil {
		panic(token)
	}
	return n
}

func (s *modelSeam) RouteForChange(_ context.Context, c ir.Change) (laneapply.Route, bool, error) {
	ins := c.(ir.Insert)
	if ins.Table == "p" {
		return laneapply.Route{Qualified: "ks.p", PKVals: []any{ins.Row["id"]}, Scope: laneapply.RouteScopeKey}, true, nil
	}
	return laneapply.Route{Qualified: "ks." + ins.Table, PKVals: []any{ins.Row["id"]}}, true, nil
}

func (s *modelSeam) marked(c ir.Change) bool {
	return strings.HasPrefix(c.(ir.Insert).Table, "m")
}

func (s *modelSeam) ApplyMarkTx(_ context.Context, c ir.Change) string {
	if s.marked(c) && !s.routeBlind[c.(ir.Insert).Table] {
		return ir.ApplyIDOf(c).TxID
	}
	return ""
}

func (s *modelSeam) ApplyMarksFenced(txID string, anchored bool) { s.fence.Open(txID, anchored) }

func (s *modelSeam) ApplyLaneBatch(_ context.Context, lane int, batch []ir.Change, fold *laneapply.FoldTicket) (int, error) {
	start := time.Now()
	foldTx := ""
	if fold != nil {
		foldTx = fold.Tx
	}
	// Apply time: the lane's own verdict, through the real fence.
	type marked struct{ id, tx string }
	var marks []marked
	for _, c := range batch {
		id := fmt.Sprint(c.(ir.Insert).Row["id"])
		s.mu.Lock()
		s.started[id] = start
		s.mu.Unlock()
		if !s.marked(c) {
			continue
		}
		tx := ir.ApplyIDOf(c).TxID
		if s.fence.Admitted([]applymarks.Mark{{TxID: tx, Table: "m"}}, foldTx) != nil {
			marks = append(marks, marked{id, tx})
		} else {
			s.mu.Lock()
			s.dropped[id] = true
			s.mu.Unlock()
		}
	}
	time.Sleep(s.delay(lane, fold))
	// The commit: rows, marks and a fold's position become durable together.
	s.mu.Lock()
	for _, c := range batch {
		s.committed[fmt.Sprint(c.(ir.Insert).Row["id"])] = true
	}
	if fold != nil {
		s.folds++
		s.persistLocked("fold", fold.Pos.Token)
		s.foldDone[fold.Tx] = time.Now()
	}
	for _, m := range marks {
		s.admitted[m.id] = true
		if want := modelTxNum(m.tx) - 1; s.persisted != want {
			s.violations = append(s.violations, fmt.Sprintf("the marks of %s (row %s) became durable with the position at "+
				"transaction %d's commit, not at the transaction's start (%d): marks for two transactions at once", m.tx, m.id, s.persisted, want))
		}
	}
	s.mu.Unlock()
	if fold != nil {
		s.fence.Anchor(fold.Tx)
	}
	return len(batch), nil
}

func (s *modelSeam) persistLocked(writer, token string) {
	k := modelTxNum(token)
	if k < s.persisted {
		s.violations = append(s.violations, fmt.Sprintf("%s wrote position %s after %d: the persisted position went backwards", writer, token, s.persisted))
	}
	for tx := 1; tx <= k; tx++ {
		for _, id := range s.rowsOfTx[tx] {
			if !s.committed[id] {
				s.violations = append(s.violations, fmt.Sprintf("%s wrote position %s with row %s of transaction %d not yet committed: "+
					"the position led the data", writer, token, id, tx))
			}
		}
	}
	s.persisted = k
}

func (s *modelSeam) WriteCheckpoint(_ context.Context, pos ir.Position, _ int64, _ []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persistLocked("checkpoint", pos.Token)
	return nil
}

func (s *modelSeam) ClassifyError(err error) error                       { return err }
func (s *modelSeam) ApplyBarrierChange(context.Context, ir.Change) error { return nil }
func (s *modelSeam) SkipsRowChange(context.Context, ir.Change) bool      { return false }

// modelTx builds transaction n (committing at token c<n>) with one row per
// table, ids "<table>-<n>-<i>".
func modelTx(s *modelSeam, n int, tables ...string) []ir.Change {
	tok := func(k int) ir.Position { return ir.Position{Engine: "test", Token: fmt.Sprintf("c%04d", k)} }
	tx := fmt.Sprintf("tx%04d", n)
	out := []ir.Change{ir.TxBegin{Position: tok(n - 1)}}
	for i, table := range tables {
		id := fmt.Sprintf("%s-%d-%d", table, n, i)
		s.rowsOfTx[n] = append(s.rowsOfTx[n], id)
		out = append(out, ir.Insert{
			Position: tok(n - 1), Schema: "ks", Table: table, Row: ir.Row{"id": id},
			ApplyID: ir.ApplyID{TxID: tx, Seq: uint64(i + 1)},
		})
	}
	return append(out, ir.TxCommit{Position: tok(n)})
}

func runModel(t *testing.T, s *modelSeam, lanes int, stream []ir.Change) {
	t.Helper()
	o := laneapply.NewOrchestrator(laneapply.Config{Lanes: lanes, MaxBatchSize: 4, ExactlyOnceLanes: true}, s)
	ch := make(chan ir.Change, len(stream))
	for _, c := range stream {
		ch <- c
	}
	close(ch)
	if err := o.Run(context.Background(), ch); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// distinctLaneTables returns n table names m* that route to n different
// lanes of lanes (table scope), so a test can place a transaction's marked
// changes on lanes of its choosing.
func distinctLaneTables(t *testing.T, lanes, n int) []string {
	t.Helper()
	r := laneapply.NewRouter(lanes)
	seen := map[int]bool{}
	var out []string
	for i := 0; i < 200 && len(out) < n; i++ {
		name := "m" + strconv.FormatInt(int64(i)*2654435761, 36) // varied: FNV-1a mod a non-power-of-2 lane count clusters near-identical names
		if l := r.LaneForRoute(laneapply.Route{Qualified: "ks." + name}); !seen[l] {
			seen[l] = true
			out = append(out, name)
		}
	}
	if len(out) < n {
		t.Fatalf("found %d tables on distinct lanes of %d; want %d", len(out), lanes, n)
	}
	return out
}

// TestOrchestrator_AnchoredRule pins amendment D's anchored rule, both
// halves, with the fold's lane held for 150 ms. Transaction 2 sends three
// marked changes, each to its own lane: the first carries the fold ticket;
// the second is marked only at apply time (a route-time verdict the caches
// overturned), so the coordinator does not wait for it, and the lane-side
// fence must refuse its marks while the fold is in flight; the third must
// WAIT for the fold (the coordinator's step 8, operator decision D-Q1) — and
// so be written WITH its marks, not without. The model records a
// violation if any of transaction 2's marks become durable before its
// position does.
func TestOrchestrator_AnchoredRule(t *testing.T) {
	const lanes = 3
	tbl := distinctLaneTables(t, lanes, 3)
	fold, second, blind := tbl[0], tbl[1], tbl[2]
	s := newModelSeam()
	s.routeBlind[blind] = true
	s.delay = func(_ int, f *laneapply.FoldTicket) time.Duration {
		if f != nil {
			return 150 * time.Millisecond
		}
		return 0
	}
	stream := append(modelTx(s, 1, "p"), modelTx(s, 2, fold, blind, second)...)
	runModel(t, s, lanes, stream)

	for _, v := range s.violations {
		t.Error(v)
	}
	if s.folds != 1 {
		t.Fatalf("%d folds; want 1 (transaction 2 is not the run's first, so its anchor rides a ticket)", s.folds)
	}
	blindID, secondID := blind+"-2-1", second+"-2-2"
	if !s.admitted[secondID] {
		t.Errorf("the second lane's marked change of the fenced transaction was written WITHOUT its marks: the coordinator "+
			"must wait for the fold before routing it to another lane (D-Q1), not let the lane drop them (dropped=%v)", s.dropped[secondID])
	}
	if s.started[secondID].Before(s.foldDone["tx0002"]) {
		t.Errorf("the second lane's batch began %v before the fold committed: the coordinator did not wait (step 8)",
			s.foldDone["tx0002"].Sub(s.started[secondID]))
	}
	if !s.dropped[blindID] || !s.started[blindID].Before(s.foldDone["tx0002"]) {
		t.Errorf("the route-blind change did not exercise the lane-side check (dropped=%v, began before the fold committed=%v) — "+
			"the anchored rule's apply-time half went unexercised", s.dropped[blindID], s.started[blindID].Before(s.foldDone["tx0002"]))
	}
}

// TestOrchestrator_PositionWritesTotallyOrdered drives random marked and
// unmarked transactions across lanes with random per-lane delays, and holds
// the two position writers — the coordinator's checkpoint and the lanes'
// folds — to one order: every persisted position, of either writer, is
// non-decreasing, never ahead of the data, and every transaction's marks
// become durable only with the position at that transaction's start.
func TestOrchestrator_PositionWritesTotallyOrdered(t *testing.T) {
	for _, lanes := range []int{2, 4} {
		for seed := int64(1); seed <= 6; seed++ {
			t.Run(fmt.Sprintf("lanes=%d/seed=%d", lanes, seed), func(t *testing.T) {
				r := rand.New(rand.NewSource(seed))
				s := newModelSeam()
				tbl := distinctLaneTables(t, lanes, 2)
				s.routeBlind["mb"] = true
				var delayMu sync.Mutex
				s.delay = func(int, *laneapply.FoldTicket) time.Duration {
					delayMu.Lock()
					defer delayMu.Unlock()
					return time.Duration(r.Intn(3)) * time.Millisecond
				}
				pool := []string{"p", "p", tbl[0], tbl[1], "mb"}
				var stream []ir.Change
				for n := 1; n <= 40; n++ {
					k := 1 + r.Intn(4)
					tables := make([]string, k)
					for i := range tables {
						tables[i] = pool[r.Intn(len(pool))]
					}
					stream = append(stream, modelTx(s, n, tables...)...)
				}
				runModel(t, s, lanes, stream)
				for _, v := range s.violations {
					t.Error(v)
				}
				if s.folds == 0 {
					t.Fatal("no fold ran: the stream did not exercise the second position writer")
				}
			})
		}
	}
}
