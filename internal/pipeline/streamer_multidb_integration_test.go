//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for multi-database MySQL `sync start` (ADR-0074
// Phase 1b.2): cold-start N source databases under ONE spanning
// consistent snapshot → N same-named target namespaces, then steady-state
// CDC routed per-change to the right namespace.
//
//	(a) MySQL → MySQL: two source DBs → two auto-created target DBs, then
//	    a post-cold-start INSERT/UPDATE/DELETE in EACH source DB streams to
//	    the right target DB. Per-DB counts correct, no cross-DB bleed.
//	(b) MySQL → PG: same shape, two source DBs → two PG schemas.
//	(c) Concurrent writes DURING cold-start: zero loss / zero dup after CDC
//	    catches up (the union of snapshot + CDC covers every row once) —
//	    the correctness gate for the single spanning snapshot.
//	(d) Single-database `sync start` back-compat unchanged (no DB flags).
//
// Each stream is stopped via ctx cancel (drain) before the final
// assertions.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// waitForRowCountMySQLDB polls a specific database.table for >= n rows.
func waitForRowCountMySQLDB(t *testing.T, serverDSNStr, database, table string, n int, timeout time.Duration) bool {
	t.Helper()
	dsn, err := buildMySQLDSN(serverDSNStr, database)
	if err != nil {
		t.Fatalf("build DSN %q: %v", database, err)
	}
	return waitForRowCountMySQL(t, dsn, table, n, timeout)
}

// mysqlDBRowCount returns the row count of database.table on the server.
func mysqlDBRowCount(t *testing.T, serverDSNStr, database, table string) int {
	t.Helper()
	dsn, err := buildMySQLDSN(serverDSNStr, database)
	if err != nil {
		t.Fatalf("build DSN %q: %v", database, err)
	}
	return countRowsMySQL(t, dsn, table)
}

// TestStreamer_MultiDatabase_MySQLToMySQL is scenario (a): cold-start two
// source databases under one spanning snapshot into two auto-created
// target databases, then verify a post-cold-start DML burst in each
// source database streams to the right target database with no bleed.
func TestStreamer_MultiDatabase_MySQLToMySQL(t *testing.T) {
	srcServer, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	tgtServer, tgtHomeDSN, tgtCleanup := startMySQLBinlog(t)
	defer tgtCleanup()

	// source_db already exists (startMySQLBinlog). Seed it + shop_db.
	applyDDLMySQL(t, srcServer, `
		CREATE TABLE widgets (
			id   BIGINT NOT NULL AUTO_INCREMENT,
			name VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO widgets (name) VALUES ('a-one'), ('a-two');
	`)
	shopDSN := mustCreateMySQLDatabase(t, srcServer, "shop_db")
	applyDDLMySQL(t, shopDSN, `
		CREATE TABLE widgets (
			id   BIGINT NOT NULL AUTO_INCREMENT,
			name VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO widgets (name) VALUES ('b-one'), ('b-two'), ('b-three');
	`)

	mysqlEng, _ := engines.Get("mysql")

	// Target DSN names the "home" database (target_db) that hosts the
	// sluice_cdc_state control table; user data routes to per-source-db
	// namespaces (source_db / shop_db), auto-created via EnsureDatabase.
	streamer := &Streamer{
		Source:         mysqlEng,
		Target:         mysqlEng,
		SourceDSN:      serverDSN(t, srcServer),
		TargetDSN:      tgtHomeDSN,
		StreamID:       "multidb-m2m",
		DatabaseFilter: DatabaseFilter{Include: []string{"source_db", "shop_db"}},
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()
	defer func() {
		streamCancel()
		select {
		case <-runErr:
		case <-time.After(20 * time.Second):
			t.Error("streamer did not return after ctx cancel")
		}
	}()

	// Cold-start lands the seed rows in both target databases.
	if !waitForRowCountMySQLDB(t, serverDSN(t, tgtServer), "source_db", "widgets", 2, 60*time.Second) {
		t.Fatalf("cold-start never delivered source_db.widgets")
	}
	if !waitForRowCountMySQLDB(t, serverDSN(t, tgtServer), "shop_db", "widgets", 3, 60*time.Second) {
		t.Fatalf("cold-start never delivered shop_db.widgets")
	}

	// Post-cold-start DML in EACH source database — must stream to its own
	// target database via per-change routing.
	applyDDLMySQL(t, srcServer, "INSERT INTO source_db.widgets (name) VALUES ('a-three');")
	applyDDLMySQL(t, shopDSN, "INSERT INTO widgets (name) VALUES ('b-four');")
	applyDDLMySQL(t, srcServer, "UPDATE source_db.widgets SET name='a-one-upd' WHERE name='a-one';")
	applyDDLMySQL(t, shopDSN, "DELETE FROM widgets WHERE name='b-two';")

	if !waitForRowCountMySQLDB(t, serverDSN(t, tgtServer), "source_db", "widgets", 3, 30*time.Second) {
		t.Fatalf("CDC never delivered source_db INSERT")
	}
	// shop_db: started at 3, +1 insert, -1 delete = 3.
	if !waitForRowCountMySQLDB(t, serverDSN(t, tgtServer), "shop_db", "widgets", 3, 30*time.Second) {
		t.Fatalf("CDC never settled shop_db")
	}

	// Give the UPDATE/DELETE a moment to land, then assert no cross-DB
	// bleed and the routed values.
	time.Sleep(2 * time.Second)

	if got := mysqlDBRowCount(t, serverDSN(t, tgtServer), "source_db", "widgets"); got != 3 {
		t.Errorf("target source_db.widgets = %d; want 3", got)
	}
	if got := mysqlDBRowCount(t, serverDSN(t, tgtServer), "shop_db", "widgets"); got != 3 {
		t.Errorf("target shop_db.widgets = %d; want 3", got)
	}

	// The UPDATE landed in source_db only (a-one -> a-one-upd), shop_db
	// untouched by it.
	srcUpd := queryStringMySQL(t, serverDSN(t, tgtServer), "source_db",
		"SELECT COUNT(*) FROM widgets WHERE name='a-one-upd'")
	if srcUpd != "1" {
		t.Errorf("source_db UPDATE not routed: a-one-upd count = %s; want 1", srcUpd)
	}
	// The DELETE landed in shop_db only (b-two gone), source_db untouched.
	shopDel := queryStringMySQL(t, serverDSN(t, tgtServer), "shop_db",
		"SELECT COUNT(*) FROM widgets WHERE name='b-two'")
	if shopDel != "0" {
		t.Errorf("shop_db DELETE not routed: b-two count = %s; want 0", shopDel)
	}
	// Cross-DB bleed guard: shop_db must NOT contain source_db's update.
	bleed := queryStringMySQL(t, serverDSN(t, tgtServer), "shop_db",
		"SELECT COUNT(*) FROM widgets WHERE name='a-one-upd'")
	if bleed != "0" {
		t.Errorf("cross-DB bleed: shop_db has source_db's a-one-upd (%s); want 0", bleed)
	}
}

// TestStreamer_MultiDatabase_MySQLToPostgres is scenario (b): two source
// databases → two same-named PG schemas, cold-start + CDC.
func TestStreamer_MultiDatabase_MySQLToPostgres(t *testing.T) {
	srcServer, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyDDLMySQL(t, srcServer, `
		CREATE TABLE widgets (
			id   BIGINT NOT NULL AUTO_INCREMENT,
			name VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO widgets (name) VALUES ('a-one'), ('a-two');
	`)
	shopDSN := mustCreateMySQLDatabase(t, srcServer, "shop_db")
	applyDDLMySQL(t, shopDSN, `
		CREATE TABLE widgets (
			id   BIGINT NOT NULL AUTO_INCREMENT,
			name VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO widgets (name) VALUES ('b-one'), ('b-two'), ('b-three');
	`)

	mysqlEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")

	streamer := &Streamer{
		Source:         mysqlEng,
		Target:         pgEng,
		SourceDSN:      serverDSN(t, srcServer),
		TargetDSN:      pgTarget,
		StreamID:       "multidb-m2pg",
		DatabaseFilter: DatabaseFilter{Include: []string{"source_db", "shop_db"}},
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()
	defer func() {
		streamCancel()
		select {
		case <-runErr:
		case <-time.After(20 * time.Second):
			t.Error("streamer did not return after ctx cancel")
		}
	}()

	if !waitForPGSchemaCount(t, pgTarget, "source_db", "widgets", 2, 60*time.Second) {
		t.Fatalf("cold-start never delivered source_db.widgets to PG")
	}
	if !waitForPGSchemaCount(t, pgTarget, "shop_db", "widgets", 3, 60*time.Second) {
		t.Fatalf("cold-start never delivered shop_db.widgets to PG")
	}

	applyDDLMySQL(t, srcServer, "INSERT INTO source_db.widgets (name) VALUES ('a-three');")
	applyDDLMySQL(t, shopDSN, "INSERT INTO widgets (name) VALUES ('b-four');")

	if !waitForPGSchemaCount(t, pgTarget, "source_db", "widgets", 3, 30*time.Second) {
		t.Fatalf("CDC never delivered source_db insert to PG")
	}
	if !waitForPGSchemaCount(t, pgTarget, "shop_db", "widgets", 4, 30*time.Second) {
		t.Fatalf("CDC never delivered shop_db insert to PG")
	}

	// No cross-schema bleed: source_db has the a-* names only.
	db, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = db.Close() }()
	var bleed int
	if err := db.QueryRow(`SELECT COUNT(*) FROM source_db.widgets WHERE name LIKE 'b-%'`).Scan(&bleed); err != nil {
		t.Fatalf("bleed query: %v", err)
	}
	if bleed != 0 {
		t.Errorf("cross-schema bleed: source_db has %d b-* rows; want 0", bleed)
	}
}

// TestStreamer_MultiDatabase_ConcurrentWritesDuringColdStart is scenario
// (c) — the correctness gate for the single spanning snapshot. While the
// cold-start bulk-copy runs, a concurrent writer INSERTs into BOTH source
// databases. After CDC catches up, every row must appear on the target
// exactly once (snapshot + CDC union = each row once; the single
// spanning snapshot + single position is what makes the handoff gapless).
func TestStreamer_MultiDatabase_ConcurrentWritesDuringColdStart(t *testing.T) {
	srcServer, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	tgtServer, tgtHomeDSN, tgtCleanup := startMySQLBinlog(t)
	defer tgtCleanup()

	// Seed each source database with a sizeable table so the bulk-copy
	// window is wide enough to overlap concurrent writes.
	const seedRows = 2000
	applyDDLMySQL(t, srcServer, `
		CREATE TABLE events (
			id   BIGINT NOT NULL AUTO_INCREMENT,
			body VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)
	shopDSN := mustCreateMySQLDatabase(t, srcServer, "shop_db")
	applyDDLMySQL(t, shopDSN, `
		CREATE TABLE events (
			id   BIGINT NOT NULL AUTO_INCREMENT,
			body VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)
	seedEvents(t, srcServer, "source_db", seedRows, "seed-a")
	seedEvents(t, srcServer, "shop_db", seedRows, "seed-b")

	mysqlEng, _ := engines.Get("mysql")
	streamer := &Streamer{
		Source:         mysqlEng,
		Target:         mysqlEng,
		SourceDSN:      serverDSN(t, srcServer),
		TargetDSN:      tgtHomeDSN,
		StreamID:       "multidb-concurrent",
		DatabaseFilter: DatabaseFilter{Include: []string{"source_db", "shop_db"}},
	}

	// Concurrent writer: while the stream cold-starts, fire CDC-window
	// inserts into both databases. These rows are written AFTER the
	// snapshot position for some, possibly DURING the bulk copy for
	// others — the union must still cover each exactly once.
	const concurrentRows = 300
	writerCtx, writerCancel := context.WithCancel(context.Background())
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		concurrentInsert(writerCtx, srcServer, "source_db", concurrentRows, "live-a")
		concurrentInsert(writerCtx, srcServer, "shop_db", concurrentRows, "live-b")
	}()

	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	wantA := seedRows + concurrentRows
	wantB := seedRows + concurrentRows

	// Let the concurrent writer finish before asserting catch-up.
	writerWG.Wait()
	writerCancel()

	// Catch-up ceiling. This is an async wait for the CDC tail to apply the
	// 600 concurrent rows; the EXACT-count assertion below is the real
	// zero-loss gate (a genuinely lost row never reaches the target and this
	// times out). The ceiling only needs to be generous enough to never
	// false-fail on slow apply under the CI `-race` build (~10x slower).
	//
	// History (do not relax this assertion): a `got 2299/2300` here was first
	// mistaken for a slow-apply flake and "fixed" by bumping the ceiling — but
	// the row never arrived no matter the ceiling. It was a real silent-loss
	// boundary gap in the snapshot capture: a commit landing between START
	// TRANSACTION WITH CONSISTENT SNAPSHOT and the binlog-position read fell
	// into neither the snapshot nor the CDC tail. The fix is FTWRL around the
	// capture (see openBinlogSnapshotStreamShared); this test is its pin.
	const catchUpCeiling = 180 * time.Second
	if !waitForRowCountMySQLDB(t, serverDSN(t, tgtServer), "source_db", "events", wantA, catchUpCeiling) {
		streamCancel()
		<-runErr
		t.Fatalf("source_db never reached %d rows (got %d)", wantA,
			mysqlDBRowCount(t, serverDSN(t, tgtServer), "source_db", "events"))
	}
	if !waitForRowCountMySQLDB(t, serverDSN(t, tgtServer), "shop_db", "events", wantB, catchUpCeiling) {
		streamCancel()
		<-runErr
		t.Fatalf("shop_db never reached %d rows (got %d)", wantB,
			mysqlDBRowCount(t, serverDSN(t, tgtServer), "shop_db", "events"))
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Error("streamer did not return after ctx cancel")
	}

	// Zero loss / zero dup: EXACTLY wantA / wantB rows (no duplicates from
	// the idempotent copy absorbing the CDC overlap, no losses from the
	// handoff). The source and target counts must match exactly.
	srcA := mysqlDBRowCount(t, serverDSN(t, srcServer), "source_db", "events")
	srcB := mysqlDBRowCount(t, serverDSN(t, srcServer), "shop_db", "events")
	tgtA := mysqlDBRowCount(t, serverDSN(t, tgtServer), "source_db", "events")
	tgtB := mysqlDBRowCount(t, serverDSN(t, tgtServer), "shop_db", "events")

	if srcA != wantA || tgtA != wantA {
		t.Errorf("source_db zero-loss/dup FAIL: src=%d tgt=%d want=%d", srcA, tgtA, wantA)
	}
	if srcB != wantB || tgtB != wantB {
		t.Errorf("shop_db zero-loss/dup FAIL: src=%d tgt=%d want=%d", srcB, tgtB, wantB)
	}
}

// TestStreamer_MultiDatabase_SingleDatabaseBackCompat is scenario (d):
// a normal single-database `sync start` (NO database flags) must be
// byte-identical — multiDatabaseMode() false, no scope/routing set. This
// reuses the canonical single-database MySQL→MySQL streamer test shape.
func TestStreamer_MultiDatabase_SingleDatabaseBackCompat(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyDDLMySQL(t, sourceDSN, `
		CREATE TABLE users (
			id    BIGINT NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('one@example.com');
	`)

	mysqlEng, _ := engines.Get("mysql")
	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN, // single-database DSN (has DBName)
		TargetDSN: targetDSN,
		StreamID:  "single-db-backcompat",
		// NO DatabaseFilter / AllDatabases — must take the single-database
		// path byte-identically.
	}
	if streamer.multiDatabaseMode() {
		t.Fatal("single-database streamer reported multiDatabaseMode()=true")
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()
	defer func() {
		streamCancel()
		select {
		case <-runErr:
		case <-time.After(20 * time.Second):
			t.Error("streamer did not return after ctx cancel")
		}
	}()

	if !waitForRowCountMySQL(t, targetDSN, "users", 1, 60*time.Second) {
		t.Fatalf("single-database cold-start never delivered seed row")
	}
	applyDDLMySQL(t, sourceDSN, "INSERT INTO users (email) VALUES ('two@example.com');")
	if !waitForRowCountMySQL(t, targetDSN, "users", 2, 30*time.Second) {
		t.Fatalf("single-database CDC never delivered second row")
	}
}

// --- helpers ---

// seedEvents bulk-inserts n rows into database.events on the server.
func seedEvents(t *testing.T, serverDSNStr, database string, n int, prefix string) {
	t.Helper()
	dsn, err := buildMySQLDSN(serverDSNStr, database)
	if err != nil {
		t.Fatalf("build DSN %q: %v", database, err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %q: %v", database, err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO events (body) VALUES (?)")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := stmt.ExecContext(ctx, fmt.Sprintf("%s-%d", prefix, i)); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// concurrentInsert fires n single-row inserts into database.events. Best-
// effort: stops on ctx cancellation; errors are tolerated (the test's
// final source/target count comparison is the authority).
func concurrentInsert(ctx context.Context, serverDSNStr, database string, n int, prefix string) {
	dsn, err := buildMySQLDSN(serverDSNStr, database)
	if err != nil {
		return
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()
	for i := 0; i < n; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_, _ = db.ExecContext(ctx, "INSERT INTO events (body) VALUES (?)", fmt.Sprintf("%s-%d", prefix, i))
	}
}

// queryStringMySQL runs a single-value query against database on the
// server and returns the scalar as a string.
func queryStringMySQL(t *testing.T, serverDSNStr, database, query string) string {
	t.Helper()
	dsn, err := buildMySQLDSN(serverDSNStr, database)
	if err != nil {
		t.Fatalf("build DSN %q: %v", database, err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %q: %v", database, err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var s string
	if err := db.QueryRowContext(ctx, query).Scan(&s); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return s
}

// waitForPGSchemaCount polls schema.table on the PG target for >= n rows.
func waitForPGSchemaCount(t *testing.T, dsn, schema, table string, n int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schema, table)
	for time.Now().Before(deadline) {
		if pgScalarCount(dsn, q) >= n {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// pgScalarCount returns the scalar count for q, tolerating errors (0) so
// a poll during the cold-start window (schema not yet created) doesn't
// fatal.
func pgScalarCount(dsn, q string) int {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return 0
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0
	}
	return n
}
