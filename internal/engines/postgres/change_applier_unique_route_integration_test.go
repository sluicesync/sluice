//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Gate G-1 for roadmap item 131 (audit 2026-08-05 A-1) on the Postgres
// target. The concurrent key-hash apply routed lanes by (table, PRIMARY KEY)
// only, so two changes with DIFFERENT primary keys that collide on a SECONDARY
// UNIQUE index landed on different lanes with unconstrained commit order. PG
// manifests that LOUDLY rather than silently — the INSERT that commits ahead of
// the DELETE it depends on raises 23505 and takes the run down — but a sync
// that dies on an ordinary unique-value reassignment is still broken, and the
// routing defect is the same one MySQL loses a row to.
//
// Same two pins as the MySQL sibling (change_applier_unique_route_integration_test.go):
// the stall-ordering pin and the routing roster, both directions.
//
// NOTE: this is a CONCURRENCY chunk — the -race Integration job on CI is the
// authoritative gate (this box is CGO=0 so -race can't run locally).

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/laneapply"
)

// uniqueRouteStallBudget bounds how long the stall-ordering pin holds the
// DELETE's lane waiting for the INSERT's lane. Before the fix the INSERT
// reaches the target within milliseconds; after it, both changes share one lane
// so the budget is paid in full exactly once. See the MySQL sibling's note.
const uniqueRouteStallBudget = 5 * time.Second

// uniqueRoutePacerGap is the delay inserted between the pacer change and the
// rest of the stream. A lane batches greedily for its idle-flush grace
// ([defaultIdleFlushPeriod], 100ms), so a same-lane successor arriving inside
// that window joins the pacer's TRANSACTION and the stall would hold the
// DELETE too — defeating the pin.
const uniqueRoutePacerGap = 400 * time.Millisecond

// uniqueRouteBatchSize is the lane batch size the pin runs at. It must be > 1:
// ApplyBatch routes maxBatchSize <= 1 to the SERIAL per-change path, which has
// no lanes and never reaches the lane commit hook (a size-1 pin looks green and
// exercises nothing).
const uniqueRouteBatchSize = 8

// pumpUniqueRoutePaced feeds events[0] (the pacer), waits
// [uniqueRoutePacerGap] so the pacer flushes as a lane batch of its own, then
// feeds the rest as fast as the coordinator takes them.
func pumpUniqueRoutePaced(t *testing.T, ctx context.Context, applier ir.ChangeApplier, streamID string, events []ir.Change) {
	t.Helper()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	batched, ok := applier.(ir.BatchedChangeApplier)
	if !ok {
		t.Fatalf("applier does not implement BatchedChangeApplier")
	}
	ch := make(chan ir.Change, len(events))
	go func() {
		defer close(ch)
		for i, e := range events {
			if i == 1 {
				time.Sleep(uniqueRoutePacerGap)
			}
			ch <- e
		}
	}()
	if err := batched.ApplyBatch(ctx, streamID, ch, uniqueRouteBatchSize); err != nil {
		t.Fatalf("ApplyBatch (stream %s): %v", streamID, err)
	}
}

// uniqueRouteLaneSplit picks three primary keys for the stall-ordering pin:
// `pacer` and `victim` on the SAME per-key lane, `arrival` on a different one —
// the hazard geometry, derived from the live router so the pin cannot silently
// stop exercising it.
func uniqueRouteLaneSplit(t *testing.T, qualified string, lanes int) (pacer, victim, arrival int64) {
	t.Helper()
	r := laneapply.NewRouter(lanes)
	lane := func(id int64) int { return r.LaneFor(qualified, []any{id}) }
	victim = 1
	home := lane(victim)
	for i := int64(2); i <= 500; i++ {
		if pacer == 0 && lane(i) == home {
			pacer = i
			continue
		}
		if arrival == 0 && lane(i) != home {
			arrival = i
		}
		if pacer != 0 && arrival != 0 {
			return pacer, victim, arrival
		}
	}
	t.Fatalf("no same-lane/different-lane key pair found for %q over %d lanes", qualified, lanes)
	return 0, 0, 0
}

// TestConcurrentApply_SecondaryUniqueReassignmentIsOrdered is the item-131
// stall-ordering pin on PG. Source order is DELETE(pk=victim, uq='x') then
// INSERT(pk=arrival, uq='x'); the DELETE's lane is held (by a pacer change
// occupying it) while the INSERT's lane runs. The run must SUCCEED and the
// final target state must equal the source state.
func TestConcurrentApply_SecondaryUniqueReassignmentIsOrdered(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE uniq_reassign (
			id BIGINT PRIMARY KEY,
			uq TEXT NOT NULL UNIQUE,
			v  TEXT NOT NULL
		);`)

	pacer, victim, arrival := uniqueRouteLaneSplit(t, "public.uniq_reassign", concurrentLanesW)
	applyPGApplier(t, dsn, fmt.Sprintf(
		"INSERT INTO uniq_reassign (id, uq, v) VALUES (%d, 'x', 'old');", victim,
	))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := openConcurrentApplier(t, ctx, dsn, concurrentLanesW)
	defer func() { _ = a.Close() }()

	// Independent connection: the stall's release condition is observed OUTSIDE
	// the applier's own pools, so the ordering evidence does not ride the path
	// under test.
	probe, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open probe: %v", err)
	}
	defer func() { _ = probe.Close() }()

	var stalled, sawArrival atomic.Bool
	laneCommitHookForTest = func(buf []laneChange) error {
		if len(buf) != 1 {
			return nil
		}
		ins, isIns := buf[0].change.(ir.Insert)
		if !isIns || ins.Row["id"] != pacer {
			return nil
		}
		stalled.Store(true)
		deadline := time.Now().Add(uniqueRouteStallBudget)
		for time.Now().Before(deadline) {
			var n int
			if err := probe.QueryRowContext(ctx, "SELECT COUNT(*) FROM uniq_reassign WHERE v = 'new'").Scan(&n); err == nil && n > 0 {
				sawArrival.Store(true)
				return nil
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(20 * time.Millisecond):
			}
		}
		return nil
	}
	t.Cleanup(func() { laneCommitHookForTest = nil })

	tokN := 0
	tok := func() string { tokN++; return fmt.Sprintf("uq-%06d", tokN) }
	events := []ir.Change{
		ir.Insert{Position: cpos(tok()), Schema: "public", Table: "uniq_reassign", Row: ir.Row{"id": pacer, "uq": "pacer", "v": "pace"}},
		ir.Delete{Position: cpos(tok()), Schema: "public", Table: "uniq_reassign", Before: ir.Row{"id": victim, "uq": "x", "v": "old"}},
		ir.Insert{Position: cpos(tok()), Schema: "public", Table: "uniq_reassign", Row: ir.Row{"id": arrival, "uq": "x", "v": "new"}},
	}
	pumpUniqueRoutePaced(t, ctx, a, testStreamID, events)

	if !stalled.Load() {
		t.Fatal("the pacer lane never stalled — the pin did not exercise the ordering hazard (vacuous green)")
	}
	t.Logf("arrival INSERT committed ahead of the DELETE: %v (false once both share a lane)", sawArrival.Load())

	if got := countAllRows(t, dsn, "uniq_reassign"); got != 2 {
		t.Errorf("rows = %d; want 2 (pacer + the reassigned uq='x' row)", got)
	}
	gotID, ok := pgScalarString(t, dsn, "SELECT id::text FROM uniq_reassign WHERE uq = 'x'")
	if !ok {
		t.Fatalf("no row carries uq='x'; source state has one (pk=%d)", arrival)
	}
	if gotID != fmt.Sprint(arrival) {
		t.Errorf("uq='x' is held by id=%s; want id=%d", gotID, arrival)
	}
	if v, ok := pgScalarString(t, dsn, fmt.Sprintf("SELECT v FROM uniq_reassign WHERE id = %d", victim)); ok {
		t.Errorf("id=%d still present with v=%q; the DELETE must have removed it", victim, v)
	}
}

// laneRouteCase is one row of the item-131 routing roster: a table shape, a
// change builder that varies only the primary key, and the expected routing
// verdict. `single` means every key of that table must resolve to ONE lane;
// `!single` means the per-key fast path must survive; `barrier` means the
// change must not be lane-routed at all.
type laneRouteCase struct {
	name    string
	ddl     string
	change  func(i int64) ir.Change
	single  bool
	barrier bool
	why     string
}

// TestLaneRouteRoster_SecondaryUniqueTablesAreSingleLane is the item-131 roster
// half of gate G-1 on PG, asserted against a REAL catalog in both directions.
// PG's roster is wider than MySQL's because PG has more ways to spell "this
// table refuses two rows with the same value": a UNIQUE constraint, a bare
// unique index, a PARTIAL unique index, an EXPRESSION unique index, and an
// EXCLUSION constraint. All of them create the cross-key ordering hazard.
func TestLaneRouteRoster_SecondaryUniqueTablesAreSingleLane(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	row := func(name string) func(int64) ir.Change {
		return func(i int64) ir.Change {
			return ir.Insert{Schema: "public", Table: name, Row: ir.Row{"id": i, "v": fmt.Sprintf("v%d", i)}}
		}
	}
	cases := []laneRouteCase{
		{
			name:   "pk_only",
			ddl:    "CREATE TABLE pk_only (id BIGINT PRIMARY KEY, v TEXT NOT NULL);",
			change: row("pk_only"),
			why:    "no non-PK unique index — the per-key fast path must survive",
		},
		{
			name:   "pk_plus_nonunique_index",
			ddl:    "CREATE TABLE pk_plus_nonunique_index (id BIGINT PRIMARY KEY, v TEXT NOT NULL); CREATE INDEX ON pk_plus_nonunique_index (v);",
			change: row("pk_plus_nonunique_index"),
			why:    "a plain secondary index enforces nothing; it must not collapse the table",
		},
		{
			name: "composite_pk_only",
			ddl:  "CREATE TABLE composite_pk_only (a BIGINT NOT NULL, b BIGINT NOT NULL, v TEXT, PRIMARY KEY (a, b));",
			change: func(i int64) ir.Change {
				return ir.Insert{Schema: "public", Table: "composite_pk_only", Row: ir.Row{"a": i, "b": int64(7), "v": "x"}}
			},
			why: "a composite PK is still just a PK",
		},
		{
			name:   "pk_plus_unique_constraint",
			ddl:    "CREATE TABLE pk_plus_unique_constraint (id BIGINT PRIMARY KEY, v TEXT NOT NULL UNIQUE);",
			change: row("pk_plus_unique_constraint"),
			single: true,
			why:    "a UNIQUE constraint refuses two rows with the same value across primary keys",
		},
		{
			name:   "pk_plus_unique_index",
			ddl:    "CREATE TABLE pk_plus_unique_index (id BIGINT PRIMARY KEY, v TEXT NOT NULL); CREATE UNIQUE INDEX ON pk_plus_unique_index (v);",
			change: row("pk_plus_unique_index"),
			single: true,
			why:    "a bare unique index enforces the same thing without a constraint",
		},
		{
			name:   "pk_plus_unique_partial",
			ddl:    "CREATE TABLE pk_plus_unique_partial (id BIGINT PRIMARY KEY, v TEXT); CREATE UNIQUE INDEX ON pk_plus_unique_partial (v) WHERE v IS NOT NULL;",
			change: row("pk_plus_unique_partial"),
			single: true,
			why:    "a partial unique index still refuses a duplicate inside its predicate",
		},
		{
			name:   "pk_plus_unique_expression",
			ddl:    "CREATE TABLE pk_plus_unique_expression (id BIGINT PRIMARY KEY, v TEXT NOT NULL); CREATE UNIQUE INDEX ON pk_plus_unique_expression (lower(v));",
			change: row("pk_plus_unique_expression"),
			single: true,
			why:    "an expression unique index conflicts on values the row images do not literally carry",
		},
		{
			name: "pk_plus_exclude",
			ddl:  "CREATE TABLE pk_plus_exclude (id BIGINT PRIMARY KEY, r tsrange NOT NULL, EXCLUDE USING gist (r WITH &&));",
			change: func(i int64) ir.Change {
				return ir.Insert{Schema: "public", Table: "pk_plus_exclude", Row: ir.Row{"id": i, "r": "[2020-01-01,2020-01-02)"}}
			},
			single: true,
			why:    "an EXCLUSION constraint is the same cross-key conflict with a different operator",
		},
		{
			name: "keyless",
			ddl:  "CREATE TABLE keyless (id BIGINT, v TEXT);",
			change: func(i int64) ir.Change {
				return ir.Insert{Schema: "public", Table: "keyless", Row: ir.Row{"id": i, "v": "x"}}
			},
			barrier: true,
			why:     "no PK — the ADR-0089 barrier guard must be unchanged",
		},
		{
			name: "pk_changing_update",
			ddl:  "CREATE TABLE pk_changing_update (id BIGINT PRIMARY KEY, v TEXT NOT NULL);",
			change: func(i int64) ir.Change {
				return ir.Update{
					Schema: "public", Table: "pk_changing_update",
					Before: ir.Row{"id": i, "v": "a"},
					After:  ir.Row{"id": i + 1000, "v": "a"},
				}
			},
			barrier: true,
			why:     "a key migration must stay globally ordered — barrier, unchanged",
		},
	}

	for _, c := range cases {
		applyPGApplier(t, dsn, c.ddl)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := openConcurrentApplier(t, ctx, dsn, concurrentLanesW)
	defer func() { _ = a.Close() }()
	la := &laneApplierAdapter{a: a}
	router := laneapply.NewRouter(concurrentLanesW)

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			lanes := map[int]bool{}
			for i := int64(1); i <= 64; i++ {
				lane, routed := routeLaneForTest(t, ctx, la, router, c.change(i))
				if c.barrier {
					if routed {
						t.Fatalf("%s: change routed to a lane; want the barrier path (%s)", c.name, c.why)
					}
					continue
				}
				if !routed {
					t.Fatalf("%s: change took the barrier path; want a lane (%s)", c.name, c.why)
				}
				lanes[lane] = true
			}
			if c.barrier {
				return
			}
			switch {
			case c.single && len(lanes) != 1:
				t.Errorf("%s: 64 keys spread over %d lanes; want exactly 1 (%s)", c.name, len(lanes), c.why)
			case !c.single && len(lanes) < 2:
				t.Errorf("%s: 64 keys collapsed onto %d lane(s); want >1 (%s)", c.name, len(lanes), c.why)
			}
		})
	}
}

// routeLaneForTest resolves one change through the engine's routing seam and
// the shared router, returning the lane it lands on (routed=false means the
// change takes the barrier path). Single point of contact with the seam so the
// roster reads the same on both engines.
func routeLaneForTest(t *testing.T, ctx context.Context, la *laneApplierAdapter, router *laneapply.Router, c ir.Change) (lane int, routed bool) {
	t.Helper()
	route, ok, err := la.RouteForChange(ctx, c)
	if err != nil {
		t.Fatalf("RouteForChange: %v", err)
	}
	if !ok {
		return 0, false
	}
	return router.LaneForRoute(route), true
}

// pgScalarString returns a single string column for a query that may match no
// rows (ok=false), unlike the package's queryScalarString which fails the test
// on ErrNoRows — the item-131 pins assert BOTH presence and absence.
func pgScalarString(t *testing.T, dsn, query string) (string, bool) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var s sql.NullString
	err = db.QueryRowContext(ctx, query).Scan(&s)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return s.String, true
}
