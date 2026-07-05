//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the mid-stream live add-table flow on MySQL
// (Phase 2, `--no-drain`, ADR-0034). The shape mirrors the PG-side
// integration tests in add_table_live_pg_integration_test.go but
// exercises the binlog filter-flip mechanism (cdc-state column
// `live_added_tables` + streamer poll) instead of PG's publication-add.
//
// Three scenarios:
//
//   - TestStreamer_AddTable_LiveMode_MySQL: happy path. Active stream with
//     Filter:Include=[users]; live-add `orders`; verify orders snapshot
//     rows + post-add CDC delivery.
//   - TestStreamer_AddTable_LiveMode_MySQL_UnderLoad: best-effort under load.
//     Sustained INSERTs on `orders` during the live add; pin snapshot
//     rows + post-flip CDC; in-flight gap logged as best-effort.
//   - TestStreamer_AddTable_LiveMode_MySQL_FilterRespectedAfterFlip: pins
//     additive semantics. Operator's existing Include=[users] stays in
//     scope; live-added `orders` joins; an OUT-OF-SCOPE table
//     `audit_log` stays excluded.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// TestStreamer_AddTable_LiveMode_MySQL is the load-bearing scenario for ADR-
// 0034: live add of a new table against an actively-running MySQL
// stream that started with --include-table=users. Verifies the
// existing table's CDC is unaffected and the new table is fully
// brought into the stream's scope (existing rows via snapshot + new
// rows via CDC after the filter-flip).
func TestStreamer_AddTable_LiveMode_MySQL(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('alice@example.com');
	`
	applyDDLMySQL(t, srcDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	const streamID = "test-live-add-mysql-happy"
	filter, err := migcore.NewTableFilter([]string{"users"}, nil)
	if err != nil {
		t.Fatalf("migcore.NewTableFilter: %v", err)
	}

	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: srcDSN,
		TargetDSN: tgtDSN,
		StreamID:  streamID,
		Filter:    filter,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, tgtDSN, "users", 1, 30*time.Second) {
		t.Fatalf("bulk copy did not deliver users seed row")
	}
	// Warm CDC + cdc-state row so the live-add's preflight finds the
	// stream entry.
	applyDDLMySQL(t, srcDSN, "INSERT INTO users (email) VALUES ('warmup@example.com');")
	if !waitForRowCountMySQL(t, tgtDSN, "users", 2, 30*time.Second) {
		t.Fatalf("CDC did not deliver post-snapshot warmup insert; cdc-state row may not be written yet")
	}

	// ---- Live add-table for a NEW source table that already has rows.
	const newTableDDL = `
		CREATE TABLE orders (
			id        BIGINT NOT NULL AUTO_INCREMENT,
			customer  BIGINT NOT NULL,
			amount    DECIMAL(10,2) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO orders (customer, amount) VALUES (1, 19.99), (2, 42.00);
	`
	applyDDLMySQL(t, srcDSN, newTableDDL)

	addCtx, addCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer addCancel()
	add := &AddTable{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: srcDSN,
		TargetDSN: tgtDSN,
		StreamID:  streamID,
		TableName: "orders",
		LiveMode:  true,
	}
	if err := add.Run(addCtx); err != nil {
		t.Fatalf("AddTable (MySQL live mode): %v", err)
	}

	// Snapshot rows landed on the target.
	if !waitForRowCountMySQL(t, tgtDSN, "orders", 2, 30*time.Second) {
		t.Fatalf("live add did not deliver orders snapshot rows")
	}

	// Wait for the streamer's poll to observe the cdc-state column
	// update before driving the post-add INSERT. This is the
	// best-effort window documented in ADR-0034 § "best-effort
	// caveat": events on the new table arriving at the streamer's
	// dispatch BEFORE its poll observes the column update are dropped.
	// 15s is well past the default 5s poll cadence; without this
	// wait the test races against the poll and intermittently hits
	// the in-flight-loss surface.
	waitForLiveAddPollObservation(t)

	// Insert a fresh row on the new table; with the filter flipped,
	// the streamer dispatches the event to the applier.
	applyDDLMySQL(t, srcDSN, "INSERT INTO orders (customer, amount) VALUES (1, 7.50);")
	if !waitForRowCountMySQL(t, tgtDSN, "orders", 3, 30*time.Second) {
		t.Fatalf("CDC did not deliver post-add insert on orders — filter-flip mechanism regressed")
	}

	// Sanity: original users CDC continued to function during the
	// live add (no wedge from the filter mutation).
	applyDDLMySQL(t, srcDSN, "INSERT INTO users (email) VALUES ('carol@example.com');")
	if !waitForRowCountMySQL(t, tgtDSN, "users", 3, 30*time.Second) {
		t.Fatalf("CDC on original users table wedged after live add")
	}

	streamCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Streamer.Run returned err: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_AddTable_LiveMode_MySQL_UnderLoad runs live add while a
// goroutine drives sustained INSERTs on the new table. Pin: snapshot
// rows + post-flip CDC are delivered (load-bearing). In-flight events
// during the add window are best-effort (ADR-0034 § "best-effort
// caveat") — logged but not asserted.
func TestStreamer_AddTable_LiveMode_MySQL_UnderLoad(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('alice@example.com');
	`
	applyDDLMySQL(t, srcDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	const streamID = "test-live-add-mysql-under-load"
	filter, err := migcore.NewTableFilter([]string{"users"}, nil)
	if err != nil {
		t.Fatalf("migcore.NewTableFilter: %v", err)
	}

	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: srcDSN,
		TargetDSN: tgtDSN,
		StreamID:  streamID,
		Filter:    filter,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, tgtDSN, "users", 1, 30*time.Second) {
		t.Fatalf("bulk copy did not deliver users seed row")
	}
	applyDDLMySQL(t, srcDSN, "INSERT INTO users (email) VALUES ('warmup@example.com');")
	if !waitForRowCountMySQL(t, tgtDSN, "users", 2, 30*time.Second) {
		t.Fatalf("CDC did not deliver post-snapshot warmup insert")
	}

	const newTableDDL = `
		CREATE TABLE events (
			id   BIGINT NOT NULL AUTO_INCREMENT,
			body VARCHAR(255),
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyDDLMySQL(t, srcDSN, newTableDDL)

	srcDB, err := sql.Open("mysql", srcDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()

	const seedRowCount = 50
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 30*time.Second)
	for i := 0; i < seedRowCount; i++ {
		if _, err := srcDB.ExecContext(seedCtx, "INSERT INTO events (body) VALUES (?)", fmt.Sprintf("seed-%d", i)); err != nil {
			seedCancel()
			t.Fatalf("seed insert %d: %v", i, err)
		}
	}
	seedCancel()

	// Driver goroutine — same shape as the PG under-load test:
	// counter increments AFTER successful commit (avoid off-by-one
	// under ctx-cancel races).
	loadCtx, loadCancel := context.WithCancel(context.Background())
	var inserted atomic.Int64
	var loadWG sync.WaitGroup
	loadWG.Add(1)
	go func() {
		defer loadWG.Done()
		var local int64
		for {
			select {
			case <-loadCtx.Done():
				inserted.Store(local)
				return
			default:
			}
			if _, err := srcDB.ExecContext(context.Background(), "INSERT INTO events (body) VALUES (?)", fmt.Sprintf("load-%d", local+1)); err != nil {
				inserted.Store(local)
				return
			}
			local++
			select {
			case <-loadCtx.Done():
				inserted.Store(local)
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()

	addCtx, addCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer addCancel()
	add := &AddTable{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: srcDSN,
		TargetDSN: tgtDSN,
		StreamID:  streamID,
		TableName: "events",
		LiveMode:  true,
	}
	if err := add.Run(addCtx); err != nil {
		loadCancel()
		loadWG.Wait()
		t.Fatalf("AddTable (MySQL live mode under load): %v", err)
	}

	loadCancel()
	loadWG.Wait()
	finalInserted := inserted.Load()

	// Wait for the streamer's poll to observe the cdc-state column
	// update before the sentinel INSERT. ADR-0034 § "best-effort
	// caveat" — without this wait, the sentinel races against the
	// poll and we'd intermittently fail the load-bearing pin below.
	waitForLiveAddPollObservation(t)

	// Drive a post-add INSERT to verify CDC for the new table is
	// healthy beyond the filter-flip boundary — same shape as the PG
	// under-load test's sentinel pin.
	if _, err := srcDB.ExecContext(context.Background(), "INSERT INTO events (body) VALUES (?)", "post-add-sentinel"); err != nil {
		t.Fatalf("post-add insert: %v", err)
	}

	minTotal := seedRowCount + 1
	maxTotal := int(int64(seedRowCount) + finalInserted + 1)
	if !waitForRowCountMySQL(t, tgtDSN, "events", minTotal, 60*time.Second) {
		got := pollRowCountMySQL(tgtDSN, "events")
		t.Fatalf("under-load events row count = %d; want at least %d (snapshot rows + post-add sentinel must all land)", got, minTotal)
	}

	if !sentinelDeliveredMySQL(t, tgtDSN, "events", "post-add-sentinel") {
		t.Errorf("post-add-sentinel NOT delivered to target — CDC delivery for new table after filter-flip is broken (the load-bearing ADR-0034 pin)")
	}

	got := pollRowCountMySQL(tgtDSN, "events")
	if got < maxTotal {
		t.Logf("under-load events row count = %d; ideal (zero-loss) = %d; gap = %d. ADR-0034 in-flight-loss best-effort caveat — load-* rows landing during filter-flip window may not be delivered. Snapshot rows + post-add CDC pinned above; this is a best-effort log only.",
			got, maxTotal, maxTotal-got)
	}

	// Pin: no duplicates. The idempotent applier (ON DUPLICATE KEY
	// UPDATE) must absorb the [snapshot-pos, filter-flip-observed]
	// overlap on the new table.
	if distinct := distinctRowCountMySQL(t, tgtDSN, "events", "id"); distinct != got {
		t.Errorf("under-load events DISTINCT id count = %d; total count = %d (duplicate ids implies idempotent applier regression)", distinct, got)
	}

	streamCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Streamer.Run returned err: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_AddTable_LiveMode_MySQL_FilterRespectedAfterFlip pins the
// additive semantics: the live-flip extends the stream's scope; it
// does NOT replace the operator's filter. Tables outside both the
// base filter and the live-added set stay excluded.
func TestStreamer_AddTable_LiveMode_MySQL_FilterRespectedAfterFlip(t *testing.T) {
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

	const streamID = "test-live-add-mysql-additive"
	filter, err := migcore.NewTableFilter([]string{"users"}, nil)
	if err != nil {
		t.Fatalf("migcore.NewTableFilter: %v", err)
	}

	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: srcDSN,
		TargetDSN: tgtDSN,
		StreamID:  streamID,
		Filter:    filter,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Bulk-copy + warmup. audit_log is filtered out — neither
	// bulk-copied nor CDC-dispatched.
	if !waitForRowCountMySQL(t, tgtDSN, "users", 1, 30*time.Second) {
		t.Fatalf("bulk copy did not deliver users seed row")
	}
	applyDDLMySQL(t, srcDSN, "INSERT INTO users (email) VALUES ('warmup@example.com');")
	if !waitForRowCountMySQL(t, tgtDSN, "users", 2, 30*time.Second) {
		t.Fatalf("CDC did not deliver post-snapshot warmup insert")
	}
	// audit_log should NOT have a target table.
	if mysqlTableExistsInTarget(t, tgtDSN, "audit_log") {
		t.Fatalf("audit_log table appeared on target — base filter is leaking BEFORE live-add (pre-condition broken)")
	}

	// Live-add `orders` (NOT audit_log).
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
	add := &AddTable{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: srcDSN,
		TargetDSN: tgtDSN,
		StreamID:  streamID,
		TableName: "orders",
		LiveMode:  true,
	}
	if err := add.Run(addCtx); err != nil {
		t.Fatalf("AddTable: %v", err)
	}

	if !waitForRowCountMySQL(t, tgtDSN, "orders", 1, 30*time.Second) {
		t.Fatalf("live add did not deliver orders snapshot row")
	}

	// Wait for the streamer's poll to observe the cdc-state column
	// update — see ADR-0034 § "best-effort caveat" / the
	// waitForLiveAddPollObservation helper for the timing details.
	waitForLiveAddPollObservation(t)

	// Insert on all three tables. users + orders should arrive;
	// audit_log should still be filtered.
	applyDDLMySQL(t, srcDSN, "INSERT INTO orders (customer) VALUES (2);")
	applyDDLMySQL(t, srcDSN, "INSERT INTO users (email) VALUES ('post-add-users@example.com');")
	applyDDLMySQL(t, srcDSN, "INSERT INTO audit_log (what) VALUES ('post-add-audit');")

	if !waitForRowCountMySQL(t, tgtDSN, "orders", 2, 30*time.Second) {
		t.Errorf("live-added orders never received post-add CDC row (filter-flip merge regression)")
	}
	if !waitForRowCountMySQL(t, tgtDSN, "users", 3, 30*time.Second) {
		t.Errorf("base-filter users never received post-add CDC row (base filter regression)")
	}

	// audit_log table must STILL not exist on the target — the live-
	// added set is additive, but it didn't include audit_log.
	if mysqlTableExistsInTarget(t, tgtDSN, "audit_log") {
		t.Errorf("audit_log appeared on target after live-add — additive merge leaked beyond the live-added set")
	}

	streamCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Streamer.Run returned err: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// waitForLiveAddPollObservation sleeps for slightly longer than the
// streamer's default 5s poll cadence, so the running streamer's poll
// goroutine has had a chance to observe a freshly-recorded
// live_added_tables column update and merge it into the dispatch
// filter. ADR-0034 § "best-effort caveat" describes the production
// equivalent: operators using `--no-drain` see the same brief lag
// between the orchestrator's column write and CDC delivery on the new
// table.
//
// 15s is well past the default 5s poll cadence and gives plenty of
// margin for slow-CI scheduling. The helper does not override
// pollIntervalForTest because it'd require importing test-internal
// state into MySQL flow tests; the production cadence is the right
// thing to exercise here. Tests that need a faster cadence pin live
// in the unit-test layer (streamer_filter_flip_test.go).
func waitForLiveAddPollObservation(t *testing.T) {
	t.Helper()
	const wait = 15 * time.Second
	t.Logf("waiting %s for streamer's live-added-tables poll to observe cdc-state column update", wait)
	time.Sleep(wait)
}

// ---- MySQL-flavored helpers (sibling to the PG-flavored
// sentinelDelivered / distinctRowCount in
// add_table_live_pg_integration_test.go).

func sentinelDeliveredMySQL(t *testing.T, dsn, table, sentinel string) bool {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var n int
		q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE body = ?", table)
		err = db.QueryRowContext(ctx, q, sentinel).Scan(&n)
		cancel()
		_ = db.Close()
		if err == nil && n > 0 {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

func distinctRowCountMySQL(t *testing.T, dsn, table, column string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	q := fmt.Sprintf("SELECT COUNT(DISTINCT %s) FROM %s", column, table)
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatalf("distinct-count: %v", err)
	}
	return n
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
