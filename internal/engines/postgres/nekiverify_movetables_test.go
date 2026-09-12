//go:build nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Tier-2 coverage item #3: NK213 against a REAL MoveTables cutover.
//
// # What premise this defends
//
// `SLUICE-E-TARGET-TABLE-BLOCKED-BY-WORKFLOW` exists because a MoveTables
// write switch BLOCKS the table on the database sluice is connected to, on
// every shard primary, rather than redirecting the connection — a Postgres
// client picks its database at connect time, so a read switch structurally
// cannot reach it and a write switch has nowhere to send it. sluice classifies
// the resulting SQLSTATE NK213 as TERMINAL (not retriable) and annotates it
// with the three `__neki` calls an operator needs.
//
// Every part of that rests on the platform still doing what it did when it was
// measured by hand on 2026-09-10. The dangerous direction is not the block
// disappearing — that would make sluice merely over-cautious — but the block
// arriving as something OTHER than NK213, because then sluice's terminal-code
// shield does not recognise it and a stream that should halt keeps running
// against a table it can no longer write.
//
// # Cost, which is why this is worth reading before deciding where it runs
//
// MoveTables moves tables between DATABASES, and on Neki a second database is
// `CREATE DATABASE` through the router — NOT a second provisioned cluster. So
// this arm costs extra WALL CLOCK on the database the suite already
// provisions, not extra infrastructure. The per-phase timing table logged at
// the end exists so that cost is a measured number rather than a guess: if it
// turns out to dominate the run, moving this one arm to a dispatch-only input
// is a one-line change.
//
// # UNVERIFIED UNTIL ITS FIRST LIVE RUN
//
// The call sequence mirrors the one recorded in docs/dev/neki-readiness.md
// from the manual 2026-09-10 run, and the topology arguments are read back
// from `__neki.get_data_topology()` rather than constructed. If the signature
// has moved, the first failure names the function and what the router said,
// which is the useful outcome — not a silent skip.
func nekiMoveTablesBlocksWithNK213(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()

	t.Run("PREMISE: a MoveTables write switch blocks the table with NK213", func(t *testing.T) {
		const (
			workflow = "nekiverify_mvw"
			moveTgt  = "nekiverify_mvtgt"
			table    = "mv_src"
		)
		timings := map[string]time.Duration{}
		phase := func(name string, fn func() error) error {
			start := time.Now()
			err := fn()
			timings[name] = time.Since(start)
			return err
		}
		defer func() {
			var b strings.Builder
			total := time.Duration(0)
			for _, k := range []string{
				"create_target_database", "create_and_enrol_table",
				"move_tables_create", "switch_reads", "switch_writes",
				"observe_block", "reverse_traffic", "cleanup",
			} {
				if d, ok := timings[k]; ok {
					fmt.Fprintf(&b, "\n  %-24s %6.1fs", k, d.Seconds())
					total += d
				}
			}
			fmt.Fprintf(&b, "\n  %-24s %6.1fs", "TOTAL", total.Seconds())
			t.Logf("MoveTables arm timing (this is the number that decides whether it belongs on the "+
				"weekly schedule or a dispatch input):%s", b.String())
		}()

		// The table must exist and be ENROLLED IN THE TOPOLOGY. Enrolment is
		// the part that surprised the manual run: `CREATE TABLE` through the
		// router does NOT add a table to the data topology, and
		// move_tables_create refuses an unenrolled table with NK604 "not found
		// in the populated database topology" even though it is fully routable
		// and holding rows. The topology is an operator-DECLARED document, not
		// a derived inventory.
		if err := phase("create_and_enrol_table", func() error {
			if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS `+table); err != nil {
				return fmt.Errorf("drop: %w", err)
			}
			if _, err := db.ExecContext(ctx, `
				CREATE TABLE `+table+` (
					tenant_id BIGINT NOT NULL,
					id        BIGINT NOT NULL,
					payload   TEXT,
					PRIMARY KEY (tenant_id, id)
				)`); err != nil {
				return fmt.Errorf("create: %w", err)
			}
			if _, err := db.ExecContext(ctx,
				`INSERT INTO `+table+` (tenant_id, id, payload) VALUES (1,1,'before-move')`); err != nil {
				return fmt.Errorf("seed: %w", err)
			}
			return enrolTableInTopology(ctx, db, table)
		}); err != nil {
			t.Fatalf("preparing the move source: %v", err)
		}

		if err := phase("create_target_database", func() error {
			_, err := db.ExecContext(ctx, `CREATE DATABASE `+moveTgt)
			return err
		}); err != nil {
			t.Fatalf("create the move-target database (on Neki this is a logical database on the same "+
				"cluster, not a second provisioned one): %v", err)
		}
		t.Cleanup(func() {
			cctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			_, _ = db.ExecContext(cctx, `DROP DATABASE IF EXISTS `+moveTgt)
		})

		srcTopo, err := currentTopologyDoc(ctx, db)
		if err != nil {
			t.Fatalf("read the source topology to pass to move_tables_create: %v", err)
		}

		// Phase 1 — create. Measured transparent: the per-shard copy+stream
		// runs alongside anything else reading the table.
		if err := phase("move_tables_create", func() error {
			_, err := db.ExecContext(ctx,
				`SELECT __neki.move_tables_create($1,'postgres',$2::jsonb,$3,$2::jsonb,
				                                  ARRAY['public.`+table+`'],ARRAY['public.`+table+`'],'{}')`,
				workflow, srcTopo, moveTgt)
			return err
		}); err != nil {
			t.Fatalf("move_tables_create: %v\n"+
				"If this is an NK604 'not found in the populated database topology', the enrolment step "+
				"above no longer does what it did on 2026-09-10. If it is a signature error, the metafunc "+
				"has changed — run `SELECT * FROM __neki.list_metafuncs()` and update this call; do NOT "+
				"relax the test, because the arm it guards is a terminal refusal", err)
		}
		t.Cleanup(func() {
			cctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			// Best-effort: leave no workflow holding a block on a database the
			// next test in this run still has to write to.
			_, _ = db.ExecContext(cctx, `SELECT __neki.move_tables_reverse_traffic($1)`, workflow)
		})

		// The table must still be writable — if it were not, the NK213 below
		// would prove nothing about the WRITE SWITCH specifically.
		if _, err := db.ExecContext(ctx,
			`INSERT INTO `+table+` (tenant_id, id, payload) VALUES (1,2,'during-create')`); err != nil {
			t.Fatalf("the table stopped accepting writes at move_tables_create, which the 2026-09-10 "+
				"measurement recorded as TRANSPARENT — the premise moved: %v", err)
		}

		// Phase 2 — switch reads. Measured transparent for a Postgres client.
		if err := phase("switch_reads", func() error {
			_, err := db.ExecContext(ctx, `SELECT * FROM __neki.move_tables_switch_reads($1)`, workflow)
			return err
		}); err != nil {
			t.Fatalf("move_tables_switch_reads: %v", err)
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO `+table+` (tenant_id, id, payload) VALUES (1,3,'during-read-switch')`); err != nil {
			t.Errorf("the table stopped accepting writes at the READ switch, which was measured "+
				"transparent (a PG client picks its database at connect time, so a read switch cannot "+
				"reach it): %v", err)
		}

		// Phase 3 — switch writes. THIS is the one that blocks.
		if err := phase("switch_writes", func() error {
			_, err := db.ExecContext(ctx,
				`SELECT * FROM __neki.move_tables_switch_writes($1,'{"enable_reverse_replication":true}')`,
				workflow)
			return err
		}); err != nil {
			t.Fatalf("move_tables_switch_writes: %v", err)
		}

		// The measurement itself: what does the NEXT statement get, and does
		// sluice recognise it?
		var blockErr error
		if err := phase("observe_block", func() error {
			_, blockErr = db.ExecContext(ctx,
				`INSERT INTO `+table+` (tenant_id, id, payload) VALUES (1,4,'after-write-switch')`)
			return nil
		}); err != nil {
			t.Fatalf("observe: %v", err)
		}

		if blockErr == nil {
			t.Fatal("the write SUCCEEDED after move_tables_switch_writes. The premise behind " +
				"SLUICE-E-TARGET-TABLE-BLOCKED-BY-WORKFLOW is that the write switch blocks the table on " +
				"the source database; if it no longer does, that refusal is over-cautious — safe, but " +
				"re-derive it before trusting the operator guidance that goes with it")
		}
		if !isNekiTableBlocked(blockErr) {
			t.Fatalf("the write was refused, but NOT as NK213, so sluice's terminal-code shield does not "+
				"recognise it: a stream that should HALT would keep running against a table it can no "+
				"longer write. Got: %v", blockErr)
		}
		annotated := annotateNekiBlockedTable(blockErr)
		if annotated == nil {
			t.Fatal("annotateNekiBlockedTable returned nil for a genuine NK213")
		}
		for _, want := range []string{"list_blocked_tables", "move_tables_status", "move_tables_reverse_traffic"} {
			if !strings.Contains(annotated.Error(), want) && !strings.Contains(fmt.Sprint(annotated), want) {
				t.Errorf("the annotated refusal does not name %q — the remedy an operator reads mid-incident "+
					"is incomplete: %v", want, annotated)
			}
		}
		t.Logf("premise holds: the write switch blocked the table and sluice classified it — %v", blockErr)

		// Phase 4 — reverse. This is the half nekiRemedyFunctionsExist cannot
		// check: that the function sluice's remedy names actually HANDS THE
		// TABLE BACK, not merely that it exists.
		if err := phase("reverse_traffic", func() error {
			_, err := db.ExecContext(ctx, `SELECT __neki.move_tables_reverse_traffic($1)`, workflow)
			return err
		}); err != nil {
			t.Fatalf("move_tables_reverse_traffic — sluice's own remedy for this refusal: %v", err)
		}
		if err := phase("cleanup", func() error {
			_, err := db.ExecContext(ctx,
				`INSERT INTO `+table+` (tenant_id, id, payload) VALUES (1,5,'after-reverse')`)
			return err
		}); err != nil {
			t.Errorf("after move_tables_reverse_traffic the table is STILL blocked, so the remedy sluice "+
				"prints does not recover the situation it is printed for: %v", err)
		}
	})
}

// enrolTableInTopology adds a table to the operator-declared data topology,
// which is what makes it visible to a workflow. Reads the current document,
// appends the table if absent, and writes it back — rather than constructing a
// document, so the fixture's existing shard groups are preserved.
func enrolTableInTopology(ctx context.Context, db *sql.DB, table string) error {
	doc, err := currentTopologyDoc(ctx, db)
	if err != nil {
		return err
	}
	if strings.Contains(doc, `"`+table+`"`) {
		return nil
	}
	// The topology's tables live under databases.<db>.schemas.public.tables.
	// jsonb_set through the router keeps the document's own shape rather than
	// this test inventing one.
	_, err = db.ExecContext(ctx, `
		SELECT __neki.set_data_topology(
			jsonb_set(
				$1::jsonb,
				ARRAY['databases','postgres','schemas','public','tables','`+table+`'],
				'{}'::jsonb,
				true
			)::text
		)`, doc)
	if err != nil {
		return fmt.Errorf("enrol %q in the data topology (CREATE TABLE does not do this — the topology is "+
			"declared, not derived; an unenrolled table is refused by move_tables_create with NK604): %w",
			table, err)
	}
	return nil
}

// currentTopologyDoc returns the cluster's declared data topology as text.
func currentTopologyDoc(ctx context.Context, db *sql.DB) (string, error) {
	var doc string
	if err := db.QueryRowContext(ctx, `SELECT __neki.get_data_topology()::text`).Scan(&doc); err != nil {
		return "", fmt.Errorf("get_data_topology: %w", err)
	}
	if strings.TrimSpace(doc) == "" {
		return "", fmt.Errorf("get_data_topology returned an empty document")
	}
	return doc, nil
}
