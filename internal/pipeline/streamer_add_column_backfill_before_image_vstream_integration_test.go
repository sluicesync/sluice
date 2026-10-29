//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_AddColumnBackfill_StaleBeforeImage_VStream is the Bug 339 pin
// (sluice-testing BUG-CATALOG, CRITICAL silent).
//
// # The defect
//
// The forwarded ADD COLUMN backfill reads the source's CURRENT value of the
// added column. A row updated between the ALTER and that read gets its new
// value from the backfill, ahead of the stream; the stream's own UPDATE of the
// row then arrives with a before-image holding the ALTER's DEFAULT. The VStream
// readers pass the full row image through, and the appliers used to compare
// every column of it in the WHERE, so the UPDATE matched no row and was
// dropped — every other column it changed stayed stale — and so was every
// later UPDATE and DELETE of the row, whose before-images carried values the
// target never received. Row counts stayed equal until a lost DELETE made the
// target hold a row the source had deleted. Measured before the fix: Postgres
// targets lost every such row on both lanes and both apply modes; MySQL lost
// them on the per-change path (Shape A always takes it: its rows are routed
// before the shard column is stamped, so the lanes send them as barriers) and
// was exact only on the batch path, which applies a keyed UPDATE as an upsert
// by primary key.
//
// # The matrix
//
// target {Postgres, MySQL} × lane {single stream, Shape A} × apply mode
// {per-change, the CLI default (batched, auto-tuned, concurrent lanes)}, each
// forwarding ONE ALTER that adds a column of every family the lane carries —
// NULL default, NOT NULL default, expression defaults, integer, decimal,
// double, datetime, boolean, binary, enum — and an UPDATE that sets every one
// of them (one of them to NULL) together with a pre-existing column. The ALTER
// and the UPDATEs run while the stream is stopped, so the backfill always
// reads after them: the race the filing measured, made deterministic.
//
// # The independent expected value
//
// The source's own rows, read back with SELECT, every column in canonical text
// form per family — including which ids exist, so a lost DELETE fails the
// grade rather than hiding behind an equal count.
func TestStreamer_AddColumnBackfill_StaleBeforeImage_VStream(t *testing.T) {
	type cell struct {
		name  string
		tgt   fdDialect
		shard bool
		batch bool
	}
	var cells []cell
	for _, tgt := range []fdDialect{fdPG, fdMySQL} {
		for _, shard := range []bool{false, true} {
			for _, batch := range []bool{false, true} {
				cells = append(cells, cell{tgt: tgt, shard: shard, batch: batch})
			}
		}
	}
	keyspaces := make([]string, len(cells))
	for i := range cells {
		keyspaces[i] = fmt.Sprintf("bi%d", i)
	}
	mysqlDSN, grpcEndpoint, _, cleanupSrc := startVTTestServerKeyspaces(t, keyspaces, 1)
	defer cleanupSrc()
	pgDSN, cleanupPG := startPGTarget(t)
	defer cleanupPG()
	myDSN, cleanupMy := startMySQLTarget(t)
	defer cleanupMy()

	for i, c := range cells {
		name := map[fdDialect]string{fdPG: "postgres", fdMySQL: "mysql"}[c.tgt]
		label := fmt.Sprintf("to-%s/shapeA=%v/batched=%v", name, c.shard, c.batch)
		t.Run(label, func(t *testing.T) {
			src, err := buildMySQLDSN(mysqlDSN, keyspaces[i])
			if err != nil {
				t.Fatal(err)
			}
			tgtBase := pgDSN
			if c.tgt == fdMySQL {
				tgtBase = myDSN
			}
			lane := fdLane{
				name: label, sourceEngine: "planetscale", targetEngine: name,
				sourceDSN: src, targetDSN: fdFreshDB(t, c.tgt, tgtBase, fmt.Sprintf("bi_t%d", i)),
				src: fdMySQL, tgt: c.tgt,
				streamParams: fmt.Sprintf("&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0", grpcEndpoint),
				settle:       time.Second,
			}
			if c.shard {
				lane.shardColumn = ShardColumnSpec{Name: "src_shard", Value: "lo"}
			}
			biRun(t, lane, c.batch)
		})
	}
}

// TestStreamer_AddColumnBackfill_StaleBeforeImage_Remedy_VStream measures the
// remedy the Bug 339 CHANGELOG entry gives for a target an affected release
// already damaged: stop the stream, then start it once with
// --restart-from-scratch (on an --inject-shard-column stream, whose
// consolidated table is shared and so is not dropped, first delete that
// shard's rows from it). The damage is written onto the target directly, in
// the two shapes the defect leaves — rows whose UPDATEs were dropped (stale
// non-key columns) and a row whose DELETE was dropped — and the grade is the
// source's own rows.
func TestStreamer_AddColumnBackfill_StaleBeforeImage_Remedy_VStream(t *testing.T) {
	type cell struct {
		tgt   fdDialect
		shard bool
	}
	cells := []cell{{fdPG, false}, {fdMySQL, false}, {fdPG, true}, {fdMySQL, true}}
	keyspaces := make([]string, len(cells))
	for i := range cells {
		keyspaces[i] = fmt.Sprintf("rm%d", i)
	}
	mysqlDSN, grpcEndpoint, _, cleanupSrc := startVTTestServerKeyspaces(t, keyspaces, 1)
	defer cleanupSrc()
	pgDSN, cleanupPG := startPGTarget(t)
	defer cleanupPG()
	myDSN, cleanupMy := startMySQLTarget(t)
	defer cleanupMy()

	for i, c := range cells {
		name := map[fdDialect]string{fdPG: "postgres", fdMySQL: "mysql"}[c.tgt]
		label := fmt.Sprintf("to-%s/shapeA=%v", name, c.shard)
		t.Run(label, func(t *testing.T) {
			src, err := buildMySQLDSN(mysqlDSN, keyspaces[i])
			if err != nil {
				t.Fatal(err)
			}
			tgtBase := pgDSN
			if c.tgt == fdMySQL {
				tgtBase = myDSN
			}
			lane := fdLane{
				name: label, sourceEngine: "planetscale", targetEngine: name,
				sourceDSN: src, targetDSN: fdFreshDB(t, c.tgt, tgtBase, fmt.Sprintf("rm_t%d", i)),
				src: fdMySQL, tgt: c.tgt,
				streamParams: fmt.Sprintf("&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0", grpcEndpoint),
				settle:       time.Second,
			}
			if c.shard {
				lane.shardColumn = ShardColumnSpec{Name: "src_shard", Value: "lo"}
			}
			const streamID = "test-bi-remedy"
			s := fdStartStream(t, lane, lane.sourceDSN, lane.targetDSN, streamID)
			defer s.cancel()
			s.waitRows(t, "cold start", fdSeedRows)
			s.stop(t)

			// The damage an affected release leaves: stale values on rows
			// whose UPDATEs were dropped, and a row whose DELETE was dropped.
			fdExec(t, lane.tgt, lane.targetDSN, "UPDATE w SET name = 'stale' WHERE id IN (1, 2)")
			fdExec(t, lane.src, lane.sourceDSN, "DELETE FROM w WHERE id = 3")
			if c.shard {
				// The remedy's first step on a consolidated table: this
				// shard's rows only.
				fdExec(t, lane.tgt, lane.targetDSN, "DELETE FROM w WHERE src_shard = 'lo'")
			}

			s = biRemedyStream(t, lane, streamID)
			defer s.cancel()
			fdExec(t, lane.src, lane.sourceDSN, "INSERT INTO w (id, name) VALUES (100, 'marker')")
			biWaitID(t, s, 100)
			biGrade(t, lane, nil, "after the --restart-from-scratch remedy")
			s.stop(t)
		})
	}
}

// biRemedyStream restarts the lane's stream once with RestartFromScratch
// (`sync start --restart-from-scratch`).
func biRemedyStream(t *testing.T, lane fdLane, streamID string) *fdStream {
	t.Helper()
	srcEng, _ := engines.Get(lane.sourceEngine)
	tgtEng, _ := engines.Get(lane.targetEngine)
	streamer := &Streamer{
		Source: srcEng, Target: tgtEng,
		SourceDSN: lane.sourceDSN + lane.streamParams, TargetDSN: lane.targetDSN,
		StreamID: streamID, SlotName: strings.ReplaceAll(streamID, "-", "_"),

		InjectShardColumn:  lane.shardColumn,
		RestartFromScratch: true,
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &fdStream{lane: lane, src: lane.sourceDSN, tgt: lane.targetDSN, runErr: make(chan error, 1), cancel: cancel}
	go func() { s.runErr <- streamer.Run(ctx) }()
	return s
}

// biColumn is one added column: its DDL, how both engines render it, and the
// value the post-ALTER UPDATE sets it to.
type biColumn struct {
	shape fdShape
	set   string
}

// biColumns is the added-column family matrix (MySQL DDL; every shape is one
// the VStream lanes forward cleanly to both targets — see
// fdMySQLShapes for the shapes that halt instead).
func biColumns() []biColumn {
	return []biColumn{
		{fdShape{"c_null", "VARCHAR(20) DEFAULT NULL", fdText}, "'set-from-null'"},
		{fdShape{"c_nn", "VARCHAR(20) NOT NULL DEFAULT 'nn'", fdText}, "'nn2'"},
		{fdShape{"c_expr", "INT DEFAULT (1 + 2)", fdText}, "9"},
		{fdShape{"c_textexpr", "TEXT DEFAULT ('txt')", fdText}, "'changed'"},
		{fdShape{"c_int", "INT DEFAULT 42", fdText}, "NULL"},
		{fdShape{"c_dec", "DECIMAL(10,3) DEFAULT 12.345", fdText}, "98.765"},
		{fdShape{"c_dbl", "DOUBLE DEFAULT -0.25", fdText}, "2.5"},
		{fdShape{"c_dt6", "DATETIME(6) DEFAULT '2024-01-02 03:04:05.123456'", fdDateTime}, "'2025-05-06 07:08:09.654321'"},
		{fdShape{"c_bool", "TINYINT(1) DEFAULT 1", fdBool}, "0"},
		{fdShape{"c_vbin", "VARBINARY(4) DEFAULT 0xAB00CD", fdBinary}, "0x0102"},
		{fdShape{"c_enum", "ENUM('a','b','c d') DEFAULT 'c d'", fdText}, "'b'"},
	}
}

// biRun seeds and cold-starts a stream, stops it, runs the ALTER and the
// UPDATEs, restarts it in the cell's apply mode, and grades the target twice:
// once the backfill and the UPDATEs have landed, and again after a later
// UPDATE and DELETE of rows the stale before-images used to strand.
func biRun(t *testing.T, lane fdLane, batched bool) {
	t.Helper()
	const streamID = "test-bi-backfill"
	s := fdStartStream(t, lane, lane.sourceDSN, lane.targetDSN, streamID)
	defer s.cancel()
	s.waitRows(t, "cold start", fdSeedRows)
	for id := fdSeedRows + 1; id <= 6; id++ {
		fdExec(t, lane.src, lane.sourceDSN, fmt.Sprintf("INSERT INTO w (id, name) VALUES (%d, 'seed%d')", id, id))
	}
	s.waitRows(t, "rows carried by CDC", 6)
	s.stop(t)

	cols := biColumns()
	adds := make([]string, len(cols))
	sets := make([]string, len(cols))
	for i, c := range cols {
		adds[i] = "ADD COLUMN " + c.shape.col + " " + c.shape.def
		sets[i] = c.shape.col + " = " + c.set
	}
	fdExec(t, lane.src, lane.sourceDSN, "ALTER TABLE w "+strings.Join(adds, ", "))
	time.Sleep(lane.settle)
	// Rows 1–3: every added column AND a pre-existing one — the shape whose
	// before-image carries the defaults the backfill has already overwritten.
	fdExec(t, lane.src, lane.sourceDSN, "UPDATE w SET "+strings.Join(sets, ", ")+", name = 'after-add' WHERE id IN (1, 2, 3)")
	// Row 4: a pre-existing column only (its before-image agrees with the
	// backfill, so it applied even before the fix — the control).
	fdExec(t, lane.src, lane.sourceDSN, "UPDATE w SET name = 'name-only' WHERE id = 4")

	s = biStartStream(t, lane, streamID, batched)
	defer s.cancel()
	fdExec(t, lane.src, lane.sourceDSN, "INSERT INTO w (id, name) VALUES (100, 'marker-1')")
	s.waitRows(t, "first marker", 7)
	biGrade(t, lane, cols, "after the backfill and the post-ALTER UPDATEs")

	// The rows a dropped UPDATE used to strand: each later change's
	// before-image carries values the target never received.
	fdExec(t, lane.src, lane.sourceDSN, "UPDATE w SET name = 'second' WHERE id = 1")
	fdExec(t, lane.src, lane.sourceDSN, "DELETE FROM w WHERE id = 2")
	fdExec(t, lane.src, lane.sourceDSN, "INSERT INTO w (id, name) VALUES (101, 'marker-2')")
	biWaitID(t, s, 101)
	biGrade(t, lane, cols, "after a later UPDATE and DELETE of the same rows")
	s.stop(t)
}

// biStartStream restarts the lane's stream (the source and target already
// hold it) in the cell's apply mode: per-change, or the CLI default's batched,
// auto-tuned apply with the default (auto) lane count.
func biStartStream(t *testing.T, lane fdLane, streamID string, batched bool) *fdStream {
	t.Helper()
	srcEng, ok := engines.Get(lane.sourceEngine)
	if !ok {
		t.Fatalf("%s engine not registered", lane.sourceEngine)
	}
	tgtEng, ok := engines.Get(lane.targetEngine)
	if !ok {
		t.Fatalf("%s engine not registered", lane.targetEngine)
	}
	streamer := &Streamer{
		Source:    srcEng,
		Target:    tgtEng,
		SourceDSN: lane.sourceDSN + lane.streamParams,
		TargetDSN: lane.targetDSN,
		StreamID:  streamID,
		SlotName:  strings.ReplaceAll(streamID, "-", "_"),

		InjectShardColumn: lane.shardColumn,
	}
	if batched {
		streamer.ApplyBatchSize = 100
		streamer.AutoTune = true
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &fdStream{lane: lane, src: lane.sourceDSN, tgt: lane.targetDSN, runErr: make(chan error, 1), cancel: cancel}
	go func() { s.runErr <- streamer.Run(ctx) }()
	return s
}

// biWaitID waits for row id to reach the target (a marker carried behind
// every change the grade reads).
func biWaitID(t *testing.T, s *fdStream, id int) {
	t.Helper()
	db, err := sql.Open(s.lane.tgt.driver(), s.tgt)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	deadline := time.Now().Add(90 * time.Second)
	for {
		var n int
		if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM w WHERE id = %d", id)).Scan(&n); err == nil && n == 1 {
			return
		}
		select {
		case err := <-s.runErr:
			t.Fatalf("%s: streamer halted waiting for id=%d: %v", s.lane.name, id, err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: id=%d never reached the target", s.lane.name, id)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// biGrade compares every row of `w`, column by column, target against
// source, and fails naming the rows that differ — including ids present on
// one side only.
func biGrade(t *testing.T, lane fdLane, cols []biColumn, phase string) {
	t.Helper()
	srcDB, err := sql.Open(lane.src.driver(), fdUTC(lane.src, lane.sourceDSN))
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	tgtDB, err := sql.Open(lane.tgt.driver(), fdUTC(lane.tgt, lane.targetDSN))
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	shapes := []fdShape{{"name", "pre-existing column", fdText}}
	for _, c := range cols {
		shapes = append(shapes, c.shape)
	}
	const below = 90 // the marker rows carry no updated values
	var bad []string
	for _, sh := range shapes {
		sv, err := fdColumnValues(ctx, srcDB, lane.src, sh, below)
		if err != nil {
			t.Fatalf("%s: read source %s: %v", lane.name, sh.col, err)
		}
		tv, err := fdColumnValues(ctx, tgtDB, lane.tgt, sh, below)
		if err != nil {
			t.Fatalf("%s: read target %s: %v", lane.name, sh.col, err)
		}
		ids := map[int]bool{}
		for id := range sv {
			ids[id] = true
		}
		for id := range tv {
			ids[id] = true
		}
		sorted := make([]int, 0, len(ids))
		for id := range ids {
			sorted = append(sorted, id)
		}
		sort.Ints(sorted)
		for _, id := range sorted {
			s, inSrc := sv[id]
			g, inTgt := tv[id]
			switch {
			case !inSrc:
				bad = append(bad, fmt.Sprintf("id=%d is on the target but not the source (a lost DELETE)", id))
			case !inTgt:
				bad = append(bad, fmt.Sprintf("id=%d is on the source but not the target", id))
			case s != g:
				bad = append(bad, fmt.Sprintf("id=%d %s: source [%s] target [%s]", id, sh.col, s, g))
			}
		}
	}
	if len(bad) > 0 {
		t.Errorf("%s: %s: the target diverged from the source in %d place(s) — a change whose before-image the "+
			"added-column backfill had already overtaken matched no target row and was dropped (Bug 339):\n  %s",
			lane.name, phase, len(bad), strings.Join(bad, "\n  "))
	}
}
