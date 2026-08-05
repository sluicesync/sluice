//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Gate G-1 for roadmap item 131 (audit 2026-08-05 A-1): the concurrent
// key-hash apply routed lanes by (table, PRIMARY KEY) only, so two changes
// with DIFFERENT primary keys that collide on a SECONDARY UNIQUE index landed
// on different lanes with unconstrained commit order. On a MySQL-family target
// every lane INSERT carries ON DUPLICATE KEY UPDATE, which fires on ANY unique
// index and excludes the PK from its SET list — so an INSERT that commits
// ahead of the DELETE it depends on MUTATES the surviving row in place instead
// of inserting the new one, and the DELETE then removes it. Zero rows where
// the source has one, both lanes green, checkpoint advanced past the loss.
//
// Two pins:
//
//   - the STALL-ORDERING pin ([TestConcurrentApply_SecondaryUniqueReassignmentIsOrdered]):
//     the DELETE's lane is held before its DML while the INSERT's lane runs;
//     the final target state must equal the source state.
//   - the ROUTING ROSTER ([TestLaneRouteRoster_SecondaryUniqueTablesAreSingleLane]):
//     every table shape carrying a non-PK unique index must resolve to ONE
//     lane, and every shape without one must keep spreading across lanes
//     (the anti-vacuity half — a fix that collapsed everything would pass a
//     one-directional gate).
//
// NOTE: this is a CONCURRENCY chunk — the -race Integration job on CI is the
// authoritative gate (this box is CGO=0 so -race can't run locally).

package mysql

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
// DELETE's lane waiting for the INSERT's lane to commit. Before the fix the
// INSERT commits within milliseconds and the budget never binds; AFTER the fix
// both changes share one lane, so the INSERT can never commit while the stall
// is held and the pin deliberately pays the whole budget once. Small enough to
// keep the green run cheap, large enough that a slow container cannot make the
// RED case flap green.
const uniqueRouteStallBudget = 5 * time.Second

// uniqueRoutePacerGap is the delay inserted between the pacer change and the
// rest of the stream. A lane batches greedily for its idle-flush grace
// ([defaultIdleFlushPeriod], 100ms), so a same-lane successor arriving inside
// that window joins the pacer's TRANSACTION and the stall would hold the
// DELETE's DML too — defeating the pin. Feeding the DELETE a comfortable
// multiple of the grace later guarantees the pacer flushes alone.
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
// `pacer` and `victim`, which PER-KEY routing places on the SAME lane, and
// `arrival`, which it places on a DIFFERENT one. That is exactly the hazard
// geometry: the DELETE of `victim` and the INSERT of `arrival` are unordered
// against each other under per-key routing even though they collide on the
// table's secondary unique index. The keys are derived from the live router so
// the pin cannot silently stop exercising the hazard if the hash changes.
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
// stall-ordering pin. Source order is DELETE(pk=victim, uq='x') then
// INSERT(pk=arrival, uq='x') — a unique-value REASSIGNMENT, which is what a
// natural-key update looks like on the wire. The DELETE's lane is held (by a
// pacer change occupying it) until the INSERT's lane has committed, then
// released. The final target state must equal the source state: one row
// carrying uq='x', and it must be pk=arrival.
//
// Holding the DELETE's OWN commit is NOT sufficient to reproduce the hazard —
// by then its DML has run and InnoDB's row lock on (uq='x') serializes the
// INSERT behind it, hiding the defect. The stall must land BEFORE the DELETE's
// DML, which is what the pacer change (same lane, different key, no unique
// collision) achieves.
func TestConcurrentApply_SecondaryUniqueReassignmentIsOrdered(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	const tbl = "uniq_reassign"
	applyMySQLApplier(t, dsn, fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT      NOT NULL,
			uq VARCHAR(64) NOT NULL,
			v  VARCHAR(64) NOT NULL,
			PRIMARY KEY (id),
			UNIQUE KEY uq_reassign (uq)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`, quoteIdent(tbl)))

	qualified := "target_db." + tbl
	pacer, victim, arrival := uniqueRouteLaneSplit(t, qualified, concurrentLanesW)
	applyMySQLApplier(t, dsn, fmt.Sprintf(
		"INSERT INTO %s (id, uq, v) VALUES (%d, 'x', 'old');", quoteIdent(tbl), victim,
	))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := openConcurrentApplier(t, ctx, dsn, concurrentLanesW)
	defer closeApplier(a)

	// Independent connection: the stall's release condition must be observed
	// OUTSIDE the applier's own pools, so the pin's ordering evidence does not
	// ride the path under test.
	probe, err := sql.Open("mysql", dsn)
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
		// Hold this lane (and therefore the DELETE queued behind it) until the
		// INSERT of `arrival` is durably visible to an independent session, or
		// the budget expires.
		deadline := time.Now().Add(uniqueRouteStallBudget)
		for time.Now().Before(deadline) {
			var n int
			q := "SELECT COUNT(*) FROM target_db." + quoteIdent(tbl) + " WHERE v = 'new'"
			if err := probe.QueryRowContext(ctx, q).Scan(&n); err == nil && n > 0 {
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
		// The pacer occupies the victim's lane so the DELETE has not yet run
		// its DML when the arrival INSERT commits.
		ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: tbl, Row: ir.Row{"id": pacer, "uq": "pacer", "v": "pace"}},
		ir.Delete{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: tbl, Before: ir.Row{"id": victim, "uq": "x", "v": "old"}},
		ir.Insert{Position: ir.Position{Engine: engineNameMySQL, Token: tok()}, Schema: "target_db", Table: tbl, Row: ir.Row{"id": arrival, "uq": "x", "v": "new"}},
	}
	pumpUniqueRoutePaced(t, ctx, a, testStreamID, events)

	if !stalled.Load() {
		t.Fatal("the pacer lane never stalled — the pin did not exercise the ordering hazard (vacuous green)")
	}
	t.Logf("arrival INSERT committed ahead of the DELETE: %v (false once both share a lane)", sawArrival.Load())

	if got := countAllRows(t, dsn, "target_db", quoteIdent(tbl)); got != 2 {
		t.Errorf("rows = %d; want 2 (pacer + the reassigned uq='x' row) — a lost row is item 131's silent loss", got)
	}
	gotID, ok := queryScalarString(t, dsn, "SELECT id FROM target_db."+quoteIdent(tbl)+" WHERE uq = 'x'")
	if !ok {
		t.Fatalf("no row carries uq='x'; source state has one (pk=%d) — the row was silently lost", arrival)
	}
	if gotID != fmt.Sprint(arrival) {
		t.Errorf("uq='x' is held by id=%s; want id=%d (ON DUPLICATE KEY UPDATE mutated the old row in place)", gotID, arrival)
	}
	if v, ok := queryScalarString(t, dsn, "SELECT v FROM target_db."+quoteIdent(tbl)+" WHERE id = ?", victim); ok {
		t.Errorf("id=%d still present with v=%q; the DELETE must have removed it", victim, v)
	}
}

// laneRouteCase is one row of the item-131 routing roster: a table shape, a
// change builder that varies only the primary key, and the expected routing
// verdict. `single` means every key of that table must resolve to ONE lane
// (the table carries a non-PK unique index, so its changes must be ordered
// against each other); `!single` means the per-key fast path must survive.
// `barrier` means the change must not be lane-routed at all.
type laneRouteCase struct {
	name    string
	ddl     string
	change  func(i int64) ir.Change
	single  bool
	barrier bool
	why     string
}

// TestLaneRouteRoster_SecondaryUniqueTablesAreSingleLane is the item-131
// roster half of gate G-1. It enumerates the table shapes the routing decision
// must distinguish and asserts the verdict for each against a REAL MySQL
// catalog — both directions, so neither "always per-key" (the defect) nor
// "always one lane" (a fix that threw away all concurrency) can pass.
func TestLaneRouteRoster_SecondaryUniqueTablesAreSingleLane(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	row := func(name string) func(int64) ir.Change {
		return func(i int64) ir.Change {
			return ir.Insert{Schema: "target_db", Table: name, Row: ir.Row{"id": i, "v": fmt.Sprintf("v%d", i)}}
		}
	}
	cases := []laneRouteCase{
		{
			name:   "pk_only",
			ddl:    "id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id)",
			change: row("pk_only"),
			why:    "no non-PK unique index — the per-key fast path must survive",
		},
		{
			name:   "pk_plus_nonunique_index",
			ddl:    "id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id), KEY v_idx (v)",
			change: row("pk_plus_nonunique_index"),
			why:    "a plain secondary index enforces nothing; it must not collapse the table",
		},
		{
			name: "composite_pk_only",
			ddl:  "a BIGINT NOT NULL, b BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (a, b)",
			change: func(i int64) ir.Change {
				return ir.Insert{Schema: "target_db", Table: "composite_pk_only", Row: ir.Row{"a": i, "b": int64(7), "v": "x"}}
			},
			why: "a composite PK is still just a PK",
		},
		{
			name:   "pk_plus_unique",
			ddl:    "id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id), UNIQUE KEY v_uq (v)",
			change: row("pk_plus_unique"),
			single: true,
			why:    "ON DUPLICATE KEY UPDATE fires on this index across primary keys",
		},
		{
			name:   "pk_plus_unique_nullable",
			ddl:    "id BIGINT NOT NULL, v VARCHAR(64) NULL, PRIMARY KEY (id), UNIQUE KEY v_uq (v)",
			change: row("pk_plus_unique_nullable"),
			single: true,
			why:    "MySQL allows repeated NULLs but a non-NULL duplicate still conflicts",
		},
		{
			name:   "pk_plus_unique_composite",
			ddl:    "id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, w BIGINT NOT NULL, PRIMARY KEY (id), UNIQUE KEY vw_uq (v, w)",
			change: row("pk_plus_unique_composite"),
			single: true,
			why:    "a multi-column unique index is the same hazard",
		},
		{
			name:   "pk_plus_unique_prefix",
			ddl:    "id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id), UNIQUE KEY v_uq (v(8))",
			change: row("pk_plus_unique_prefix"),
			single: true,
			why:    "a prefix unique index enforces uniqueness on the prefix",
		},
		{
			name: "keyless",
			ddl:  "id BIGINT NOT NULL, v VARCHAR(64) NOT NULL",
			change: func(i int64) ir.Change {
				return ir.Insert{Schema: "target_db", Table: "keyless", Row: ir.Row{"id": i, "v": "x"}}
			},
			barrier: true,
			why:     "no PK — the ADR-0089 barrier guard must be unchanged",
		},
		{
			name: "pk_changing_update",
			ddl:  "id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id)",
			change: func(i int64) ir.Change {
				return ir.Update{
					Schema: "target_db", Table: "pk_changing_update",
					Before: ir.Row{"id": i, "v": "a"},
					After:  ir.Row{"id": i + 1000, "v": "a"},
				}
			},
			barrier: true,
			why:     "a key migration must stay globally ordered — barrier, unchanged",
		},
	}

	for _, c := range cases {
		applyMySQLApplier(t, dsn, fmt.Sprintf("CREATE TABLE %s (%s) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			quoteIdent(c.name), c.ddl))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := openConcurrentApplier(t, ctx, dsn, concurrentLanesW)
	defer closeApplier(a)
	la := &laneApplierAdapter{a: a.(*ChangeApplier)}
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
