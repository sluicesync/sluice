//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCDCReader_BlockedConsumerPastNetWriteTimeout is the MySQL half of the
// perf-parity gap-37 question: does a consumer that stops reading for
// longer than the server's write timeout — a forwarded ADD COLUMN's
// backfill holds the consumer for a whole pass over a table — cost the
// binlog stream anything?
//
// Unlike Postgres (whose pump owed the walsender a standby status it could
// not send while blocked), the binlog protocol needs nothing from the
// client while the server streams: the dump thread only writes. What CAN
// happen is that the server's write blocks once every buffer between it and
// the stalled consumer is full (the reader's channel, go-mysql's
// EventCacheCount of 10,240 events, both sockets), and after
// net_write_timeout the dump thread gives up and closes the connection.
// go-mysql then re-dials inside the syncer (retrySync, from the GTID set of
// the last COMPLETED transaction), invisibly to sluice.
//
// So the grading is on the rows, against the source's own count: every row
// of one large transaction written while the consumer is stalled arrives
// (no loss), and the reader records no error. That rows arrive MORE than
// once is a pinned KNOWN-WRONG cell (below). Whether the dump thread was
// replaced is logged — it is the measurement, not the pass condition.
//
// Per-test GTID container (startMySQLGTIDForCDC), so SET GLOBAL touches no
// other test.
func TestCDCReader_BlockedConsumerPastNetWriteTimeout(t *testing.T) {
	dsn, cleanup := startMySQLGTIDForCDC(t)
	defer cleanup()

	const (
		netWriteTimeout = 2 * time.Second
		rows            = 150000
	)
	applyMySQL(t, dsn, `SET GLOBAL net_write_timeout = 2`)
	applyMySQL(t, dsn, `CREATE TABLE bc (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, pad VARCHAR(1000) NOT NULL) ENGINE=InnoDB`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rdrAny, err := Engine{}.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	rdr, ok := rdrAny.(*CDCReader)
	if !ok {
		t.Fatalf("OpenCDCReader returned %T, want *CDCReader", rdrAny)
	}
	defer func() { _ = rdr.Close() }()
	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	applyMySQL(t, dsn, `INSERT INTO bc (pad) VALUES ('first')`)
	for seen := false; !seen; {
		select {
		case c, ok := <-changes:
			if !ok {
				t.Fatalf("change channel closed before the first row: %v", rdr.Err())
			}
			if ins, isIns := c.(ir.Insert); isIns && ins.Table == "bc" {
				seen = true
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for the first row")
		}
	}
	dumpBefore := binlogDumpThreadIDs(t, dsn)

	// One transaction far larger than every buffer in the path.
	applyMySQL(t, dsn, `SET SESSION cte_max_recursion_depth = 200000;
		INSERT INTO bc (pad)
		WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 150000)
		SELECT REPEAT('x', 1000) FROM g`)

	time.Sleep(5 * netWriteTimeout)
	dumpDuring := binlogDumpThreadIDs(t, dsn)

	seen := make(map[int64]int, rows)
	for len(seen) < rows {
		select {
		case c, ok := <-changes:
			if !ok {
				t.Fatalf("change channel closed after %d of %d rows: %v", len(seen), rows, rdr.Err())
			}
			if ins, isIns := c.(ir.Insert); isIns && ins.Table == "bc" {
				id, _ := ins.Row["id"].(int64)
				seen[id]++
			}
		case <-ctx.Done():
			t.Fatalf("timed out with %d of %d rows delivered (reader err: %v)", len(seen), rows, rdr.Err())
		}
	}
	// Drain anything already buffered, so a re-sent transaction shows up.
	for drained := false; !drained; {
		select {
		case c := <-changes:
			if ins, isIns := c.(ir.Insert); isIns && ins.Table == "bc" {
				id, _ := ins.Row["id"].(int64)
				seen[id]++
			}
		case <-time.After(3 * time.Second):
			drained = true
		}
	}
	dumpAfter := binlogDumpThreadIDs(t, dsn)
	t.Logf("binlog dump thread ids: before %v, after the stall %v, after the drain %v", dumpBefore, dumpDuring, dumpAfter)

	dups := 0
	for _, n := range seen {
		if n > 1 {
			dups++
		}
	}
	// KNOWN-WRONG (perf-parity gap 37, MySQL arm — measured 2026-09-24 on
	// mysql 8.0 with GTID on: dump thread replaced, 101,936 of 150,000 rows
	// delivered twice). The server drops the dump thread mid-transaction,
	// go-mysql re-dials from the last COMPLETED transaction's GTID set, and
	// the in-flight transaction is re-sent from its start — its earlier rows
	// reach the applier twice (how the re-sent transaction's boundaries
	// interleave with the first, partial delivery is not measured). Row
	// images are absolute and replay in order, so
	// the expected outcome is convergence or a loud duplicate-key refusal,
	// NOT loss — but that is unmeasured end to end. This cell pins the
	// re-send so a fix (dedupe the re-sent transaction in the reader, or
	// keep the binlog socket drained while the consumer is blocked) flips it
	// deliberately rather than silently.
	if dups == 0 {
		t.Errorf("KNOWN-WRONG cell flipped: no row was re-delivered across a consumer stall past net_write_timeout " +
			"(the dump thread ids above show whether the server still dropped it). If the re-send is fixed, make this assert dups == 0 and update perf-parity gap 37")
	} else {
		t.Logf("KNOWN-WRONG (gap 37, MySQL arm): %d of %d rows re-delivered after go-mysql re-dialled mid-transaction", dups, rows)
	}
	if err := rdr.Err(); err != nil {
		t.Errorf("the reader recorded an error across the stall: %v", err)
	}
}

// binlogDumpThreadIDs lists the server's binlog dump threads.
func binlogDumpThreadIDs(t *testing.T, dsn string) []int64 {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	rs, err := db.Query(`SELECT ID FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%' ORDER BY ID`)
	if err != nil {
		t.Fatalf("processlist: %v", err)
	}
	defer func() { _ = rs.Close() }()
	var ids []int64
	for rs.Next() {
		var id int64
		if err := rs.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rs.Err(); err != nil {
		t.Fatalf("processlist rows: %v", err)
	}
	return ids
}
