//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// TRIGGER-DDL-REMEDY-RUNS (third review, R1): the recovery a postgres-trigger
// stream's DDL refusals name must run, and converge, on a populated target.
// The refusals said to land the change "with `sluice migrate`", which refuses
// a populated target (COLD-START-TARGET-NOT-EMPTY) and never ALTERs. This
// measures the remedies the refusals now name, on a real populated target
// after a real halt, with a row deleted on the source while the stream was
// stopped — the row a merging re-copy would leave behind:
//
//   - unsharded: `sync start --restart-from-scratch` alone. An operator
//     restart drops and recreates the in-scope target tables from the
//     source's CURRENT schema (freshCopyOperatorRestart), so the new column
//     lands without any manual ALTER, and the re-copy carries no stale row.
//     Measured on a Postgres and a MySQL target.
//   - unsharded, the heavier alternative: --reset-target-data.
//   - --inject-shard-column: the consolidated table is shared, so the restart
//     does not drop it. The operator applies the DDL to it and deletes this
//     shard's rows, then restarts from scratch; another shard's rows, one
//     sharing an id this shard deleted, survive untouched.
//
// The independent expected value is the source's own rows, read by id and
// value, compared with the target's after the remedy (and, sharded, the other
// shard's rows as they were written).

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines/pgtrigger"
)

// trRows reads t's rows matching where as "id:v:extra" in id order.
func trRows(t *testing.T, db twfbDB, where string) string {
	t.Helper()
	if db.engine == "mysql" {
		return db.scalar(t, "SELECT GROUP_CONCAT(CONCAT(id, ':', v, ':', COALESCE(CAST(extra AS CHAR), 'null')) ORDER BY id SEPARATOR ',') FROM t WHERE "+where)
	}
	return db.scalar(t, "SELECT string_agg(id::text || ':' || v || ':' || COALESCE(extra::text, 'null'), ',' ORDER BY id) FROM t WHERE "+where)
}

// trCell is one remedy on one target.
type trCell struct {
	name, target, remedy string
	sharded              bool
}

func TestTriggerDDLRecovery_RemedyConvergesOnAPopulatedTarget(t *testing.T) {
	for _, c := range []trCell{
		{name: "restart-from-scratch", target: "postgres", remedy: "restart-from-scratch"},
		{name: "reset-target-data", target: "postgres", remedy: "reset-target-data"},
		// Fourth review: the drop-and-recreate on a MySQL target, from the
		// Postgres source's current schema.
		{name: "mysql-target/restart-from-scratch", target: "mysql", remedy: "restart-from-scratch"},
		{name: "sharded-alter-delete-shard-then-restart-from-scratch", target: "postgres", remedy: "restart-from-scratch", sharded: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			srcDSN, tgtDSN, cleanup := startPostgres(t)
			defer cleanup()
			src, tgt := twfbDB{engine: "postgres", dsn: srcDSN}, twfbDB{engine: "postgres", dsn: tgtDSN}
			if c.target == "mysql" {
				_, myRoot, myCleanup := startMySQL(t)
				defer myCleanup()
				twfbDB{engine: "mysql", dsn: myRoot}.exec(t, "CREATE DATABASE trr_t")
				myDSN, err := buildMySQLDSN(myRoot, "trr_t")
				if err != nil {
					t.Fatal(err)
				}
				tgt = twfbDB{engine: "mysql", dsn: myDSN}
			}
			src.exec(t, "CREATE TABLE t (id int PRIMARY KEY, v text NOT NULL); INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
			if _, err := pgtrigger.Setup(context.Background(), srcDSN, pgtrigger.SetupOptions{Tables: []string{"t"}, Schema: "public"}); err != nil {
				t.Fatalf("pgtrigger.Setup: %v", err)
			}
			streamer := func() *Streamer {
				s := kttcStreamer(t, src, tgt, "trr")
				if c.sharded {
					s.InjectShardColumn = ShardColumnSpec{Name: "shard", Value: "s1"}
					s.NoCoordinateLiveDDL = true
				}
				return s
			}
			cell := twfbCell{src: src, tgt: tgt, streamID: "trr"}
			run := startTWFBRun(streamer())
			if !tgt.waitRow(t, "t", 3, run, 180*time.Second) {
				t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
			}
			cell.waitStreaming(t, run)
			// Fourth review: the consolidated table is shared, so another
			// shard's rows — one with an id this shard deletes below — must
			// survive this shard's remedy untouched.
			const otherShard, otherRows = "shard = 's2'", "3:z:null,10:y:null"
			if c.sharded {
				tgt.exec(t, "INSERT INTO t (shard, id, v) VALUES ('s2', 3, 'z'), ('s2', 10, 'y')")
			}

			// The halt: a DDL on the captured table, then a row written under it.
			src.exec(t, "ALTER TABLE t ADD COLUMN extra int")
			src.exec(t, "INSERT INTO t VALUES (4,'d',7)")
			deadline := time.Now().Add(60 * time.Second)
			for time.Now().Before(deadline) && !run.exited() {
				time.Sleep(250 * time.Millisecond)
			}
			if !run.exited() || !strings.Contains(errString(run.err), "observed source-side DDL") {
				_ = run.stop(t)
				t.Fatalf("premise: the stream did not halt at the DDL marker (%v)", run.err)
			}
			// While the stream is stopped: a delete the remedy must not lose.
			src.exec(t, "DELETE FROM t WHERE id = 3")
			src.exec(t, "INSERT INTO t VALUES (5,'e',9)")

			s := streamer()
			switch c.remedy {
			case "restart-from-scratch":
				s.RestartFromScratch = true
			case "reset-target-data":
				s.ResetTargetData = true
			}
			mine := "true"
			if c.sharded {
				tgt.exec(t, "ALTER TABLE t ADD COLUMN extra int; DELETE FROM t WHERE shard = 's1'")
				mine = "shard = 's1'"
			}
			run = startTWFBRun(s)
			want := trRows(t, src, "true")
			deadline = time.Now().Add(120 * time.Second)
			for time.Now().Before(deadline) && !run.exited() && trRows(t, tgt, mine) != want {
				time.Sleep(500 * time.Millisecond)
			}
			got := trRows(t, tgt, mine)
			if run.exited() {
				t.Errorf("the remedy's run stopped: %v", run.err)
			} else if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("the remedy's run stopped with %v", err)
			}
			t.Logf("MEASURED %s: source %s target %s", c.name, want, got)
			if got != want {
				t.Errorf("after %s the target holds %s where the source holds %s", c.name, got, want)
			}
			if c.sharded {
				if other := trRows(t, tgt, otherShard); other != otherRows {
					t.Errorf("after %s the other shard's rows are %s; want %s, untouched", c.name, other, otherRows)
				}
			}
		})
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
