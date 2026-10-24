//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 333 on binlog sources: the drained `schema add-table` replay window.
//
// The stream is stopped, the table is added (copied from a consistent
// snapshot), and the restart resumes from the stream's pre-add position, so
// it replays the table's changes the copy already holds. The binlog
// before-image is the primary key alone (Bug 88), so a replayed key change
// X→Y moves whatever row sits at X now. Before the window that wedged the
// stream on a duplicate key in two measured shapes (sluice-testing
// workspace/b333/cell.sh, v0.163.9):
//
//   - the table existed before the stream, and its old key was reused before
//     the copy read it: the replay moved the reuse row onto the copied new
//     key (MySQL→PG 23505, MySQL→MySQL 1062);
//   - the table was created and filled while the stream was stopped, and a
//     key change happened before the copy read the row: the replay
//     resurrected the seed row and then moved it onto the copied new key.
//
// Every cell runs the real Streamer and AddTable against real servers, over
// each binlog position family (MySQL GTID, MySQL file/pos, MariaDB GTID) and
// into a MySQL-family and a Postgres target, and grades the target against
// the SOURCE's own (id, v) set — never sluice's state. Each also requires the
// window to have opened (ADDED-TABLE-REPLAY-WINDOW) and closed, so a cell
// cannot pass because nothing was replayed.
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	go test -tags=integration -count=1 -timeout=60m -v \
//	  -run 'TestStreamer_AddedTableReplayWindow_Binlog' ./internal/pipeline/

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/mysql"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

const (
	rwSeedRows = 3000
	rwOps      = 20
	rwMarkerID = 9_000_001
)

// rwOpsFor renders one cell's source changes for each of the rwOps keys
// X = i*100+7: a key change followed by an INSERT reusing the old key
// ("reuse"), a key change alone ("kc"), a delete followed by a reinsert
// ("delreins"), or two non-key updates ("updupd").
func rwOpsFor(kind, table string) []string {
	var out []string
	for i := 1; i <= rwOps; i++ {
		x := int64(i*100 + 7)
		switch kind {
		case "reuse":
			out = append(out,
				fmt.Sprintf("UPDATE %s SET id = %d, v = 'kc-%d' WHERE id = %d", table, -x, x, x),
				fmt.Sprintf("INSERT INTO %s VALUES (%d, 'reuse-%d', 'p')", table, x, x))
		case "kc":
			out = append(out, fmt.Sprintf("UPDATE %s SET id = %d, v = 'kc-%d' WHERE id = %d", table, x+5_000_000, x, x))
		case "delreins":
			out = append(out,
				fmt.Sprintf("DELETE FROM %s WHERE id = %d", table, x),
				fmt.Sprintf("INSERT INTO %s VALUES (%d, 'again-%d', 'p')", table, x, x))
		case "seq336":
			// Bug 336: a key change vacates X, a second moves W into X, and the
			// vacating row is deleted. The source ends with X holding W's data.
			w := x + 50
			out = append(out,
				fmt.Sprintf("UPDATE %s SET id = %d WHERE id = %d", table, -x, x),
				fmt.Sprintf("UPDATE %s SET id = %d, v = 'm2-%d' WHERE id = %d", table, x, x, w),
				fmt.Sprintf("DELETE FROM %s WHERE id = %d", table, -x))
		case "updupd":
			out = append(out,
				fmt.Sprintf("UPDATE %s SET v = 'u1-%d' WHERE id = %d", table, x, x),
				fmt.Sprintf("UPDATE %s SET v = 'u2-%d' WHERE id = %d", table, x, x))
		default:
			panic("unknown op kind " + kind)
		}
	}
	return out
}

// rwSeed creates table (id BIGINT PK, v, pad) on the source and fills ids
// 1..rwSeedRows.
func rwSeed(t *testing.T, src ndwDB, table string) {
	t.Helper()
	src.exec(t, "CREATE TABLE "+table+" (id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, pad VARCHAR(64) NOT NULL, PRIMARY KEY (id))"+rwEngineClause(src))
	for lo := 1; lo <= rwSeedRows; lo += 1000 {
		var b strings.Builder
		b.WriteString("INSERT INTO " + table + " VALUES ")
		for id := lo; id < lo+1000 && id <= rwSeedRows; id++ {
			if id > lo {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "(%d,'v1-%d','padpadpadpadpadpadpadpadpadpadpad')", id, id)
		}
		src.exec(t, b.String())
	}
}

// rwRows reads every (id, v) of table; nil when the table is absent.
func rwRows(t *testing.T, d ndwDB, table string) map[int64]string {
	t.Helper()
	db, err := sql.Open(d.driver, d.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rs, err := db.Query("SELECT id, v FROM " + table)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "does not exist") {
			return nil
		}
		t.Fatalf("read %s: %v", table, err)
	}
	defer func() { _ = rs.Close() }()
	out := map[int64]string{}
	for rs.Next() {
		var id int64
		var v string
		if err := rs.Scan(&id, &v); err != nil {
			t.Fatal(err)
		}
		out[id] = v
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// rwVerdict compares the source's rows with the target's, by id and value.
func rwVerdict(src, tgt map[int64]string) (missing, extra, stale []int64) {
	for id, sv := range src {
		tv, ok := tgt[id]
		switch {
		case !ok:
			missing = append(missing, id)
		case tv != sv:
			stale = append(stale, id)
		}
	}
	for id := range tgt {
		if _, ok := src[id]; !ok {
			extra = append(extra, id)
		}
	}
	return missing, extra, stale
}

// rwCell is one drained add: where the added table comes from, which
// changes run, and when.
type rwCell struct {
	name string
	// late: the table is created and filled while the stream is stopped
	// (its CREATE and seed are replayed too); otherwise it exists, filled,
	// before the stream starts.
	late bool
	ops  string
	// midCopy runs the changes inside the add's copy (after its snapshot
	// opened, so outside the copy and past the window); otherwise they run
	// while the stream is stopped, before the add.
	midCopy bool
}

// rwCells are the measured shapes and their controls.
var rwCells = []rwCell{
	{name: "ReuseBeforeCopy", ops: "reuse"},                  // the binlog order-1 wedge
	{name: "LateKeyChangeBeforeCopy", late: true, ops: "kc"}, // the filed wedge
	{name: "LateReuseBeforeCopy", late: true, ops: "reuse"},
	{name: "KeyChangeNoReuse", ops: "kc"},
	{name: "DeleteThenReinsert", ops: "delreins"},
	{name: "NonKeyUpdates", ops: "updupd"},
	{name: "Bug336BeforeCopy", ops: "seq336"}, // the silent binlog arm (Bug 336)
	{name: "ReuseDuringCopy", ops: "reuse", midCopy: true},
}

// runReplayWindowCell drives one cell: a stream on base, stopped; the cell's
// table and changes; the drained add; the restart; and the exact verdict.
func runReplayWindowCell(t *testing.T, srcEngine, tgtEngine string, src, tgt ndwDB, c rwCell, tag string) {
	t.Helper()
	logs := captureSlog(t)
	t.Cleanup(func() { rwDumpOnFailure(t, logs.String()) })
	sEng, ok := engines.Get(srcEngine)
	if !ok {
		t.Fatalf("%s engine not registered", srcEngine)
	}
	tEng, ok := engines.Get(tgtEngine)
	if !ok {
		t.Fatalf("%s engine not registered", tgtEngine)
	}
	base, newt := "rwb_"+tag, "rwn_"+tag
	streamID := "rw-" + tag
	src.exec(t, "CREATE TABLE "+base+" (id BIGINT NOT NULL, payload VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB",
		"INSERT INTO "+base+" VALUES (1, 'b1')")
	if !c.late {
		rwSeed(t, src, newt)
	}
	filter, err := migcore.NewTableFilter([]string{base}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mk := func() *Streamer {
		// The binary's defaults: batched apply over four key-hash lanes, the
		// path a split key change (a delete and an upsert on two keys, so
		// two lanes) actually takes.
		return &Streamer{
			Source: sEng, Target: tEng, SourceDSN: src.dsn, TargetDSN: tgt.dsn, StreamID: streamID, Filter: filter,
			ApplyRetryAttempts: 2, ApplyBatchSize: 1000, ApplyConcurrency: 4,
		}
	}
	cellTgt := twfbDB{engine: "mysql", dsn: tgt.dsn}
	if tgtEngine == "postgres" {
		cellTgt.engine = "postgres"
	}
	run := startTWFBRun(mk())
	defer func() { _ = run.stop(t) }()
	twfbCell{tgt: cellTgt, streamID: streamID}.waitStreaming(t, run)
	src.exec(t, "INSERT INTO "+base+" VALUES (2, 'b2')")
	if !cellTgt.waitRow(t, base, 2, run, 120*time.Second) {
		t.Fatalf("RIG: the stream never applied a change before the stop (exited=%v err=%v)", run.exited(), run.err)
	}
	ndwStopWait(t, tEng, tgt.dsn, streamID, run)

	if c.late {
		rwSeed(t, src, newt)
	}
	ops := rwOpsFor(c.ops, newt)
	add := &ndwSource{Engine: sEng.(mysql.Engine)}
	if c.midCopy {
		add.onSnapshot = func() { src.exec(t, ops...) }
	} else {
		src.exec(t, ops...)
	}
	offset := logs.Len()
	if err := (&AddTable{Source: add, Target: tEng, SourceDSN: src.dsn, TargetDSN: tgt.dsn, StreamID: streamID, TableName: newt}).Run(context.Background()); err != nil {
		t.Fatalf("drained AddTable: %v", err)
	}

	run = startTWFBRun(mk())
	src.exec(t, fmt.Sprintf("INSERT INTO %s VALUES (%d, 'marker', 'p')", newt, rwMarkerID), "INSERT INTO "+base+" VALUES (3, 'b3')")
	landed := cellTgt.waitRow(t, newt, rwMarkerID, run, 120*time.Second) && cellTgt.waitRow(t, base, 3, run, 60*time.Second)
	if !landed {
		t.Fatalf("WEDGED: the restarted stream never applied the post-add marker (exited=%v err=%v)", run.exited(), run.err)
	}

	srcRows, tgtRows := rwRows(t, src, newt), rwRows(t, tgt, newt)
	missing, extra, stale := rwVerdict(srcRows, tgtRows)
	t.Logf("%s: source %d rows, target %d: missing %d %v, extra %d %v, stale %d %v",
		c.name, len(srcRows), len(tgtRows), len(missing), rwHead(missing), len(extra), rwHead(extra), len(stale), rwHead(stale))
	if len(missing)+len(extra)+len(stale) > 0 {
		t.Errorf("DIVERGED after the drained add's restart: missing %d, extra %d, stale %d", len(missing), len(extra), len(stale))
	}
	if b1, b2 := ndwDB.tableRows(src, t, base), ndwDB.tableRows(tgt, t, base); len(b1) != len(b2) {
		t.Errorf("the established table diverged: source %d rows, target %d", len(b1), len(b2))
	}
	tail := logs.String()[offset:]
	opened := strings.Contains(tail, AddedTableReplayWindowMarker+": the stream resumes from before this added table's copy")
	closed := strings.Contains(tail, AddedTableReplayWindowMarker+": the stream is past this added table's copy position")
	switch {
	case c.midCopy && !opened:
		// Changes after the snapshot are past the copy; with nothing
		// committed between the stop and the snapshot (a target on another
		// server) the copy position equals the stop position and no window
		// opens, which is right. The cell then pins the normal apply past it.
	case !opened || !closed:
		t.Errorf("RIG: the replay window did not open and close (opened=%v closed=%v); the cell proves nothing without it", opened, closed)
	}
}

func rwHead(ids []int64) []int64 { return ids[:min(5, len(ids))] }

// rwEngineClause is the storage-engine clause a CREATE TABLE takes on src.
func rwEngineClause(src ndwDB) string {
	if src.driver == "pgx" {
		return ""
	}
	return " ENGINE=InnoDB"
}

// rwDumpOnFailure logs a failed cell's WARN/ERROR lines and window lines.
func rwDumpOnFailure(t *testing.T, logs string) {
	if !t.Failed() {
		return
	}
	shown := 0
	lines := strings.Split(logs, "\n")
	for _, line := range lines {
		if strings.Contains(line, "level=WARN") || strings.Contains(line, "level=ERROR") || strings.Contains(line, "ADDED-TABLE-") {
			if shown++; shown > 40 {
				break
			}
			t.Logf("  log: %s", line[:min(len(line), 600)])
		}
	}
	for _, line := range lines[max(0, len(lines)-25):] {
		t.Logf("  tail: %s", line[:min(len(line), 400)])
	}
}

// runReplayWindowMatrix runs every cell against one source/target pair, each
// with its own tables and stream id.
func runReplayWindowMatrix(t *testing.T, srcEngine, tgtEngine string, src, tgt ndwDB) {
	t.Helper()
	for i, c := range rwCells {
		t.Run(c.name, func(t *testing.T) {
			runReplayWindowCell(t, srcEngine, tgtEngine, src, tgt, c, fmt.Sprintf("%d", i))
		})
	}
}

func TestStreamer_AddedTableReplayWindow_Binlog_MySQLGTIDToMySQL(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLGTID(t)
	defer cleanup()
	runReplayWindowMatrix(t, "mysql", "mysql", ndwDB{"mysql", srcDSN}, ndwDB{"mysql", tgtDSN})
}

func TestStreamer_AddedTableReplayWindow_Binlog_MySQLGTIDToPostgres(t *testing.T) {
	srcDSN, _, cleanup := startMySQLGTID(t)
	defer cleanup()
	_, pgDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()
	runReplayWindowMatrix(t, "mysql", "postgres", ndwDB{"mysql", srcDSN}, ndwDB{"pgx", pgDSN})
}

func TestStreamer_AddedTableReplayWindow_Binlog_MySQLFilePosToMySQL(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()
	runReplayWindowMatrix(t, "mysql", "mysql", ndwDB{"mysql", srcDSN}, ndwDB{"mysql", tgtDSN})
}

func TestStreamer_AddedTableReplayWindow_Binlog_MySQLFilePosToPostgres(t *testing.T) {
	srcDSN, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	_, pgDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()
	runReplayWindowMatrix(t, "mysql", "postgres", ndwDB{"mysql", srcDSN}, ndwDB{"pgx", pgDSN})
}

func TestStreamer_AddedTableReplayWindow_Binlog_MariaDBToMariaDB(t *testing.T) {
	srcDSN, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	ndwDB{"mysql", srcDSN}.exec(t, "CREATE DATABASE target_db")
	tgtDSN := strings.Replace(srcDSN, "/source_db?", "/target_db?", 1)
	runReplayWindowMatrix(t, "mariadb", "mariadb", ndwDB{"mysql", srcDSN}, ndwDB{"mysql", tgtDSN})
}
