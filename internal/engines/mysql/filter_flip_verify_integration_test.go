//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Phase A verification for ADR-0034 (MySQL Phase 2 mid-stream live
// add-table). The chunk's filter-flip approach hangs on a single
// load-bearing claim:
//
//   When the orchestrator writes a new table name into the per-target
//   `sluice_cdc_state.live_added_tables` column, a *running* streamer
//   that started with `Filter: Include=[users]` will, on its next poll
//   tick, observe the column update and start dispatching binlog
//   events for the new table to the applier. Before the column write,
//   inserts on the new table are dropped by the streamer's filter;
//   after, they reach the applier.
//
// Phase A asks one falsifiable question: does the column-update +
// poll-merge mechanism actually deliver post-flip events on the new
// table? If H1 fails, ADR-0034's mechanism is broken at the load-
// bearing layer and the rest of the chunk's code is moot. If H1 holds,
// the orchestrator can rely on the mechanism for the user-facing
// add-table flow.
//
// Test shape:
//
//   1. Boot MySQL with binlog enabled.
//   2. Create `users` and `orders` on the source.
//   3. Cold-start a Streamer with `Filter: Include=[users]`. Wait for
//      bulk-copy of users.
//   4. Drive an INSERT on users. Wait for it to land on the target.
//      (Pin: the stream is healthy and dispatching CDC for the
//      filter-included table.)
//   5. Drive an INSERT on orders. The streamer's filter drops it.
//      Verify the row does NOT appear on the target.
//   6. Write `orders` to `sluice_cdc_state.live_added_tables` for the
//      stream's row.
//   7. Wait for the next poll tick (test override sets a fast cadence)
//      so the streamer merges the column into its dispatch filter.
//   8. Drive a fresh INSERT on orders. Verify it now arrives at the
//      target.
//
// The verdict line in the test logs surfaces in CI for any future
// regression.

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/pipeline"
)

// TestFilterFlip_Verify_PostFlipEventsDelivered is the load-bearing
// Phase A verification for ADR-0034. See file-level comment for the
// hypothesis and shape.
func TestFilterFlip_Verify_PostFlipEventsDelivered(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlogVerify(t)
	defer cleanup()

	// Seed: two source tables. `users` is in the streamer's Include
	// filter; `orders` is initially OUT of scope.
	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE orders (
			id        BIGINT NOT NULL AUTO_INCREMENT,
			customer  BIGINT NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('alice@example.com');
	`
	applyVerifyDDL(t, srcDSN, seedDDL)

	mysqlEng := Engine{Flavor: FlavorVanilla}
	const streamID = "test-filter-flip-verify"

	filter, err := pipeline.NewTableFilter([]string{"users"}, nil)
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}

	streamer := &pipeline.Streamer{
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

	// Step 4: bulk-copy users + post-snapshot insert reaches target.
	if !waitForRowCountVerify(t, tgtDSN, "users", 1, 30*time.Second) {
		t.Fatalf("verify: bulk copy never delivered users seed row")
	}
	applyVerifyDDL(t, srcDSN, "INSERT INTO users (email) VALUES ('bob@example.com');")
	if !waitForRowCountVerify(t, tgtDSN, "users", 2, 30*time.Second) {
		t.Fatalf("verify: CDC never delivered users insert (pre-flip stream is unhealthy)")
	}
	t.Logf("VERIFY_STEP_4: stream healthy, users CDC delivering events")

	// Step 5: insert on orders. Filter SHOULD drop it. Target's
	// orders table doesn't exist yet (filter excluded it from cold-
	// start), so an INSERT on the source orders won't even have a
	// target table to land in if the filter ever leaks. We assert
	// "orders table does not exist on target."
	applyVerifyDDL(t, srcDSN, "INSERT INTO orders (customer) VALUES (101);")
	// Give the streamer a few seconds to emit (or correctly drop) the
	// event before we assert.
	time.Sleep(2 * time.Second)
	if mysqlTableExists(t, tgtDSN, "orders") {
		t.Fatalf("verify: orders table appeared on target — filter is leaking BEFORE flip (pre-condition broken)")
	}
	t.Logf("VERIFY_STEP_5: pre-flip filter correctly drops orders events")

	// Step 6: simulate the orchestrator's filter-flip step. The
	// orchestrator's real path also bulk-copies orders' existing rows
	// to the target — for the verification test, we mimic that with a
	// CREATE TABLE + manual seed of the target so subsequent CDC has
	// a table to upsert into. The mechanism we're verifying is purely
	// "does the streamer's filter open up after the column write"; the
	// bulk-copy plumbing is exercised separately in the
	// add_table_live_mysql_integration_test.go end-to-end test.
	applyVerifyDDL(t, tgtDSN, `
		CREATE TABLE orders (
			id        BIGINT NOT NULL AUTO_INCREMENT,
			customer  BIGINT NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	// Open an applier directly against the target so we can call the
	// new RecordLiveAddedTable surface — this is the same call path
	// the orchestrator's add-table flow uses.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	applier, err := mysqlEng.OpenChangeApplier(ctx, tgtDSN)
	if err != nil {
		t.Fatalf("verify: open target applier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	rec, ok := applier.(*ChangeApplier)
	if !ok {
		t.Fatal("verify: expected *ChangeApplier from OpenChangeApplier")
	}
	if err := rec.RecordLiveAddedTable(ctx, streamID, "orders"); err != nil {
		t.Fatalf("verify: RecordLiveAddedTable: %v", err)
	}
	t.Logf("VERIFY_STEP_6: orders recorded on cdc-state.live_added_tables")

	// Step 7: wait for the streamer's poll to observe the column
	// update. Default poll cadence is 5s; we wait up to 15s for the
	// merge to land. This is the real-world best-effort caveat
	// documented in ADR-0034 § "best-effort caveat" — operators
	// running --no-drain see a similar few-second window between the
	// orchestrator's column write and CDC delivery on the new table.
	// Without the wait, the new INSERT below races against the poll
	// and the test sees the in-flight loss surface (correct behaviour,
	// but not what this verification is asking).
	//
	// The wait is intentionally NOT replaced with an override of
	// pipeline.pollIntervalForTest — that var is package-internal to
	// pipeline, and the verification test deliberately exercises the
	// production cadence to confirm the mechanism works under the
	// real timing operators will see.
	const pollObservationWait = 15 * time.Second
	t.Logf("VERIFY_STEP_7: waiting up to %s for streamer's poll to observe column update", pollObservationWait)
	time.Sleep(pollObservationWait)

	// Step 8: fresh INSERT on orders. With the filter flipped, the
	// streamer should now dispatch the event to the applier.
	applyVerifyDDL(t, srcDSN, "INSERT INTO orders (customer) VALUES (202);")
	if !waitForRowCountVerify(t, tgtDSN, "orders", 1, 30*time.Second) {
		got := pollRowCountVerify(tgtDSN, "orders")
		t.Fatalf("VERDICT_H1: FAILS — post-flip orders count = %d; want 1. Filter-flip mechanism did not deliver post-flip events. ADR-0034 is broken at the load-bearing layer.", got)
	}
	t.Logf("VERDICT_H1: HOLDS — filter-flip mechanism delivers post-flip events on the live-added table.")

	// Sanity: original users CDC continues to function after the
	// flip (no wedge from the filter mutation).
	applyVerifyDDL(t, srcDSN, "INSERT INTO users (email) VALUES ('carol@example.com');")
	if !waitForRowCountVerify(t, tgtDSN, "users", 3, 30*time.Second) {
		t.Errorf("verify: users CDC wedged after filter-flip (regression in dispatch)")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("verify: streamer did not return after ctx cancel")
	}
}

// ---- verification helpers (sibling to the broader filter_flip
// integration test in internal/pipeline/; lifted here so the engine-
// level test stays self-contained — it answers a structural-mechanism
// question, not a full end-to-end one).

// startMySQLBinlogVerify returns DSNs pointed at freshly-reset
// `source_db` and `target_db` databases on the shard's shared
// mysqld container (see shared_container_integration_test.go).
// Both schemas live on the same mysqld so cross-db binlog tests
// continue to work; reset semantics match the pre-refactor behaviour
// of booting a fresh container with both databases empty.
//
// The (dsn, dsn, cleanup) shape is preserved; cleanup is a no-op
// because TestMain owns container teardown.
func startMySQLBinlogVerify(t *testing.T) (sourceDSN, targetDSN string, cleanup func()) {
	t.Helper()
	src, _ := newSharedDB(t, "source_db")
	tgt, _ := newSharedDB(t, "target_db")
	return src, tgt, func() {}
}

func applyVerifyDDL(t *testing.T, dsn, sqlText string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("verify: open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, sqlText); err != nil {
		t.Fatalf("verify: apply sql: %v", err)
	}
}

func waitForRowCountVerify(t *testing.T, dsn, table string, n int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pollRowCountVerify(dsn, table) >= n {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func pollRowCountVerify(dsn, table string) int {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return 0
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		return 0
	}
	return n
}

// mysqlTableExists reports whether the named table exists in the DSN's
// default database. False on any error (treated as "not yet").
func mysqlTableExists(t *testing.T, dsn, table string) bool {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return false
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

// silence the unused-import / "ir not used" lint when the file
// compiles standalone. Used implicitly via Streamer.Filter and the
// applier interface assertions above.
var _ = ir.Position{}
