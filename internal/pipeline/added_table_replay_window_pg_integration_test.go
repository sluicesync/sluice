//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 336 on a Postgres source: the drained `schema add-table` replay window.
//
// The add runs ALTER PUBLICATION … ADD TABLE and then creates a temporary
// slot whose exported snapshot it copies from. The restarted stream decodes
// the table from the publication add on, so every change committed between
// the two is both in the copy and replayed onto it. pgoutput's before-image
// under REPLICA IDENTITY DEFAULT is the key alone, so the replay was
// key-addressed: `UPDATE id=-X WHERE id=X; UPDATE id=X WHERE id=W; DELETE
// id=-X` moved the copy's row X away and deleted it, at exit 0 (measured 1 row
// per sequence, PG→PG and PG→MySQL, sluice-testing workspace/v16310/nd.sh),
// and a key change followed by an INSERT reusing the old key wedged the
// restart on 23505.
//
// Every cell commits its changes in that span deterministically (a hook runs
// them right after the add's ALTER PUBLICATION returns, before the snapshot
// slot exists), drives the real Streamer and AddTable, and grades the target
// against the SOURCE's own rows. The TOAST cells pin the partial-after-image
// rule: pgoutput omits an unchanged TOASTed value from an UPDATE's new tuple,
// so a key-preserving such update replays as a key-addressed UPDATE of what it
// carries (exact, the large value included) and a key-changing one refuses
// ADDED-TABLE-REPLAY-IMAGE-INCOMPLETE.
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	go test -tags=integration -count=1 -timeout=60m -v \
//	  -run 'TestStreamer_AddedTableReplayWindow_Postgres' ./internal/pipeline/

package pipeline

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// pgrwCell is one drained add on the Postgres source.
type pgrwCell struct {
	name string
	ops  string
	// toast gives the table a large TEXT column, out of line on every row the
	// ops touch, that no op writes.
	toast bool
	// wantRefusal: the restart must stop with this marker instead of
	// converging.
	wantRefusal string
}

var pgrwCells = []pgrwCell{
	{name: "Bug336InWindow", ops: "seq336"}, // the silent arm
	{name: "ReuseInWindow", ops: "reuse"},   // the 23505 wedge
	{name: "KeyChangeInWindow", ops: "kc"},
	{name: "DeleteReinsertInWindow", ops: "delreins"},
	{name: "NonKeyUpdatesInWindow", ops: "updupd"},
	{name: "ToastNonKeyUpdatesInWindow", ops: "updupd", toast: true},
	{name: "ToastKeyChangeInWindow", ops: "kc", toast: true, wantRefusal: AddedTableReplayImageIncompleteMarker},
}

// pgrwBig is an incompressible value far past TOAST_TUPLE_THRESHOLD, so
// Postgres stores it out of line and pgoutput sends 'u' for it when an
// UPDATE leaves it unchanged.
func pgrwBig(t *testing.T) string {
	t.Helper()
	b := make([]byte, 6000)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// pgrwBigDigests reads md5(big) per id; nil when the table is absent.
func pgrwBigDigests(t *testing.T, d ndwDB, table string) map[int64]string {
	t.Helper()
	db, err := sql.Open(d.driver, d.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rs, err := db.Query("SELECT id, md5(COALESCE(big, '')) FROM " + table)
	if err != nil {
		t.Fatalf("read %s digests: %v", table, err)
	}
	defer func() { _ = rs.Close() }()
	out := map[int64]string{}
	for rs.Next() {
		var id int64
		var dg string
		if err := rs.Scan(&id, &dg); err != nil {
			t.Fatal(err)
		}
		out[id] = dg
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func runPGReplayWindowCell(t *testing.T, src, tgt ndwDB, tgtEngine string, c pgrwCell, tag string) {
	t.Helper()
	logs := captureSlog(t)
	t.Cleanup(func() { rwDumpOnFailure(t, logs.String()) })
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	tEng, ok := engines.Get(tgtEngine)
	if !ok {
		t.Fatalf("%s engine not registered", tgtEngine)
	}
	base, newt := "rpb_"+tag, "rpn_"+tag
	streamID := "rp-" + tag
	src.exec(t, "CREATE TABLE "+base+" (id BIGINT NOT NULL, payload VARCHAR(64) NOT NULL, PRIMARY KEY (id))",
		"INSERT INTO "+base+" VALUES (1, 'b1')")
	rwSeed(t, src, newt)
	if c.toast {
		src.exec(t, "ALTER TABLE "+newt+" ADD COLUMN big TEXT NULL")
		for i := 1; i <= rwOps; i++ {
			x := int64(i*100 + 7)
			src.exec(t, fmt.Sprintf("UPDATE %s SET big = '%s' WHERE id IN (%d, %d)", newt, pgrwBig(t), x, x+50))
		}
	}
	filter, err := migcore.NewTableFilter([]string{base}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mk := func() *Streamer {
		return &Streamer{
			Source: pgEng, Target: tEng, SourceDSN: src.dsn, TargetDSN: tgt.dsn, StreamID: streamID, Filter: filter,
			ApplyRetryAttempts: 2, ApplyBatchSize: 1000, ApplyConcurrency: 4,
			// Each cell is its own stream over its own publication and slot.
			SlotName: "rp" + tag, PublicationName: "rp_pub_" + tag,
		}
	}
	defer func() {
		// The cell's slot holds WAL and a slot budget; drop it once done.
		db, err := sql.Open(src.driver, src.dsn)
		if err == nil {
			_, _ = db.Exec("SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1 AND NOT active",
				resolveSlotName("rp"+tag))
			_ = db.Close()
		}
	}()
	cellTgt := twfbDB{engine: "postgres", dsn: tgt.dsn}
	if tgtEngine != "postgres" {
		cellTgt.engine = "mysql"
	}
	run := startTWFBRun(mk())
	defer func() { _ = run.stop(t) }()
	twfbCell{tgt: cellTgt, streamID: streamID}.waitStreaming(t, run)
	src.exec(t, "INSERT INTO "+base+" VALUES (2, 'b2')")
	if !cellTgt.waitRow(t, base, 2, run, 120*time.Second) {
		t.Fatalf("RIG: the stream never applied a change before the stop (exited=%v err=%v)", run.exited(), run.err)
	}
	ndwStopWait(t, tEng, tgt.dsn, streamID, run)

	// The changes, committed after the ALTER PUBLICATION and before the
	// snapshot slot: the span the restart replays onto the copy.
	ops := rwOpsFor(c.ops, newt)
	ranInWindow := false
	var none func()
	around := func() func() {
		return func() {
			src.exec(t, ops...)
			ranInWindow = true
		}
	}
	add := &pgndwSource{Engine: pgEng.(postgres.Engine), onSnapshot: &none, onMidCopy: &none, aroundPubAdd: &around}
	if err := (&AddTable{Source: add, Target: tEng, SourceDSN: src.dsn, TargetDSN: tgt.dsn, StreamID: streamID, TableName: newt}).Run(context.Background()); err != nil {
		t.Fatalf("drained AddTable: %v", err)
	}
	if !ranInWindow {
		t.Fatal("RIG: the changes did not run between the publication add and the snapshot")
	}

	offset := logs.Len()
	run = startTWFBRun(mk())
	if c.wantRefusal != "" {
		deadline := time.Now().Add(120 * time.Second)
		for !run.exited() && time.Now().Before(deadline) {
			time.Sleep(250 * time.Millisecond)
		}
		if !run.exited() {
			t.Fatalf("the restart did not refuse within 120s; want %s", c.wantRefusal)
		}
		if run.err == nil || !strings.Contains(run.err.Error(), c.wantRefusal) {
			t.Fatalf("the restart exited with %v; want the %s refusal", run.err, c.wantRefusal)
		}
		t.Logf("%s: the restart refused loudly: %v", c.name, run.err)
		if errors.Is(run.err, context.Canceled) {
			t.Fatal("RIG: the run was cancelled, not refused")
		}
		return
	}
	src.exec(t, fmt.Sprintf("INSERT INTO %s VALUES (%d, 'marker', 'p')", newt, rwMarkerID), "INSERT INTO "+base+" VALUES (3, 'b3')")
	if !cellTgt.waitRow(t, newt, rwMarkerID, run, 120*time.Second) || !cellTgt.waitRow(t, base, 3, run, 60*time.Second) {
		t.Fatalf("WEDGED: the restarted stream never applied the post-add marker (exited=%v err=%v)", run.exited(), run.err)
	}
	srcRows, tgtRows := rwRows(t, src, newt), rwRows(t, tgt, newt)
	missing, extra, stale := rwVerdict(srcRows, tgtRows)
	t.Logf("%s: source %d rows, target %d: missing %d %v, extra %d %v, stale %d %v",
		c.name, len(srcRows), len(tgtRows), len(missing), rwHead(missing), len(extra), rwHead(extra), len(stale), rwHead(stale))
	if len(missing)+len(extra)+len(stale) > 0 {
		t.Errorf("DIVERGED after the drained add's restart: missing %d, extra %d, stale %d", len(missing), len(extra), len(stale))
	}
	if c.toast {
		sd, td := pgrwBigDigests(t, src, newt), pgrwBigDigests(t, tgt, newt)
		_, _, staleBig := rwVerdict(sd, td)
		t.Logf("%s: large values differing: %d %v", c.name, len(staleBig), rwHead(staleBig))
		if len(staleBig) > 0 {
			t.Errorf("DIVERGED: %d large (TOASTed) values differ from the source", len(staleBig))
		}
	}
	tail := logs.String()[offset:]
	if !strings.Contains(tail, AddedTableReplayWindowMarker+": the stream resumes from before this added table's copy") ||
		!strings.Contains(tail, AddedTableReplayWindowMarker+": the stream is past this added table's copy position") {
		t.Error("RIG: the replay window did not open and close; the cell proves nothing without it")
	}
}

func runPGReplayWindowMatrix(t *testing.T, src, tgt ndwDB, tgtEngine string) {
	t.Helper()
	for i, c := range pgrwCells {
		t.Run(c.name, func(t *testing.T) {
			runPGReplayWindowCell(t, src, tgt, tgtEngine, c, fmt.Sprintf("%d", i))
		})
	}
}

func TestStreamer_AddedTableReplayWindow_Postgres_PGToPG(t *testing.T) {
	src, tgt, cleanup := pgndwPGPG(t)
	defer cleanup()
	runPGReplayWindowMatrix(t, src, tgt, "postgres")
}

func TestStreamer_AddedTableReplayWindow_Postgres_PGToMySQL(t *testing.T) {
	src, tgt, cleanup := pgndwPGMySQL(t)
	defer cleanup()
	runPGReplayWindowMatrix(t, src, tgt, "mysql")
}
