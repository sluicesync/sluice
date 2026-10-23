//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for `schema add-table` on a MySQL binlog stream, and
// the helpers the other MySQL add-table tests share.
//
// This file used to hold the `--no-drain` (ADR-0034 filter-flip) happy path
// and its under-load variant. That path is refused now
// (NO-DRAIN-BINLOG-SOURCE): it lost every change made to the new table
// between the add's snapshot and the stream admitting it
// (add_table_no_drain_window_binlog_integration_test.go measured it and pins
// the refusal). What stays is the additive-scope pin, on the drained flow:
//
//   - TestStreamer_AddTable_MySQL_FilterAdditiveAfterDrainedAdd: the
//     operator's Include=[users] stays in scope; the added `orders` joins
//     through the live-added set on restart; an OUT-OF-SCOPE table
//     `audit_log` stays excluded.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// TestStreamer_AddTable_MySQL_FilterAdditiveAfterDrainedAdd pins the
// additive semantics: the added table extends the stream's scope; it does
// NOT replace the operator's filter. Tables outside both the base filter and
// the live-added set stay excluded. The stream is stopped for the add and
// restarted with its original --include-table, the documented drained flow.
func TestStreamer_AddTable_MySQL_FilterAdditiveAfterDrainedAdd(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE audit_log (
			id   BIGINT       NOT NULL AUTO_INCREMENT,
			what VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('alice@example.com');
		INSERT INTO audit_log (what) VALUES ('seed');
	`
	applyDDLMySQL(t, srcDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	const streamID = "test-add-mysql-additive"
	filter, err := migcore.NewTableFilter([]string{"users"}, nil)
	if err != nil {
		t.Fatalf("migcore.NewTableFilter: %v", err)
	}
	start := func() (context.CancelFunc, chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		runErr := make(chan error, 1)
		streamer := &Streamer{
			Source: mysqlEng, Target: mysqlEng, SourceDSN: srcDSN, TargetDSN: tgtDSN,
			StreamID: streamID, Filter: filter,
		}
		go func() { runErr <- streamer.Run(ctx) }()
		return cancel, runErr
	}
	stop := func(cancel context.CancelFunc, runErr chan error) {
		t.Helper()
		cancel()
		select {
		case err := <-runErr:
			if err != nil {
				t.Errorf("Streamer.Run returned err: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("Streamer.Run did not return after ctx cancel")
		}
	}

	cancel, runErr := start()
	// Bulk-copy + warmup. audit_log is filtered out — neither
	// bulk-copied nor CDC-dispatched.
	if !waitForRowCountMySQL(t, tgtDSN, "users", 1, 30*time.Second) {
		t.Fatalf("bulk copy did not deliver users seed row")
	}
	applyDDLMySQL(t, srcDSN, "INSERT INTO users (email) VALUES ('warmup@example.com');")
	if !waitForRowCountMySQL(t, tgtDSN, "users", 2, 30*time.Second) {
		t.Fatalf("CDC did not deliver post-snapshot warmup insert")
	}
	if mysqlTableExistsInTarget(t, tgtDSN, "audit_log") {
		t.Fatalf("audit_log table appeared on target — base filter is leaking BEFORE the add (pre-condition broken)")
	}
	stop(cancel, runErr)

	// Add `orders` (NOT audit_log) while the stream is stopped.
	applyDDLMySQL(t, srcDSN, `
		CREATE TABLE orders (
			id        BIGINT NOT NULL AUTO_INCREMENT,
			customer  BIGINT NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO orders (customer) VALUES (1);
	`)
	addCtx, addCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer addCancel()
	if err := (&AddTable{
		Source: mysqlEng, Target: mysqlEng, SourceDSN: srcDSN, TargetDSN: tgtDSN,
		StreamID: streamID, TableName: "orders",
	}).Run(addCtx); err != nil {
		t.Fatalf("AddTable: %v", err)
	}
	if !waitForRowCountMySQL(t, tgtDSN, "orders", 1, 30*time.Second) {
		t.Fatalf("the add did not deliver the orders snapshot row")
	}

	// Restart with the ORIGINAL --include-table, and insert on all three
	// tables: users and orders arrive; audit_log stays filtered.
	cancel, runErr = start()
	defer func() { stop(cancel, runErr) }()
	applyDDLMySQL(t, srcDSN, "INSERT INTO orders (customer) VALUES (2);")
	applyDDLMySQL(t, srcDSN, "INSERT INTO users (email) VALUES ('post-add-users@example.com');")
	applyDDLMySQL(t, srcDSN, "INSERT INTO audit_log (what) VALUES ('post-add-audit');")

	if !waitForRowCountMySQL(t, tgtDSN, "orders", 2, 30*time.Second) {
		t.Errorf("the added orders never received a CDC row after the restart (live-added scope regression)")
	}
	if !waitForRowCountMySQL(t, tgtDSN, "users", 3, 30*time.Second) {
		t.Errorf("base-filter users never received a CDC row after the restart (base filter regression)")
	}
	if mysqlTableExistsInTarget(t, tgtDSN, "audit_log") {
		t.Errorf("audit_log appeared on target after the add — the additive scope leaked beyond the live-added set")
	}
}

// waitForLiveAddPollObservation sleeps for slightly longer than the
// streamer's default 5s poll cadence, so a running streamer's poll
// goroutine has had a chance to observe a freshly-recorded
// live_added_tables column update and merge it into the dispatch filter.
// Changes to the table that reach the stream before then are dropped,
// which is why `--no-drain` on a binlog source is refused
// (NO-DRAIN-BINLOG-SOURCE); the tests that still add to a running stream
// wait it out before driving changes they grade.
//
// 15s is well past the default 5s poll cadence and gives plenty of
// margin for slow-CI scheduling.
func waitForLiveAddPollObservation(t *testing.T) {
	t.Helper()
	const wait = 15 * time.Second
	t.Logf("waiting %s for streamer's live-added-tables poll to observe cdc-state column update", wait)
	time.Sleep(wait)
}

// mysqlTableExistsInTarget reports whether the named table exists in
// the target's database.
func mysqlTableExistsInTarget(t *testing.T, dsn, table string) bool {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	const q = `
		SELECT COUNT(*)
		FROM   information_schema.TABLES
		WHERE  TABLE_SCHEMA = DATABASE()
		  AND  TABLE_NAME   = ?`
	if err := db.QueryRowContext(ctx, q, table).Scan(&n); err != nil {
		return false
	}
	return n > 0
}
