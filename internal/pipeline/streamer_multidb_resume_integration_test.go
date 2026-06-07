//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for multi-database MySQL `sync start` WARM-RESUME
// (ADR-0074 Phase 1b.3): cold-start a multi-database sync over two source
// databases, drain-stop, RESTART (warm-resume from the ONE persisted
// server-wide binlog position, re-scoped to the selected set, routing on —
// NOT a re-cold-start), write MORE to both databases, drain-stop again, and
// assert every row landed in the correct target namespace with ZERO loss
// and ZERO duplication ACROSS the stop/restart.
//
// The crux versus the Phase 1b.2 cold-start tests is the RESTART boundary:
// the resume must pick up the single server-wide position and re-scope to
// the selected database set WITHOUT re-snapshotting/re-copying — proving the
// warm-resume dispatch (streamer.go) + warmResumeMultiDatabase
// (streamer_multidb.go) wire SetCDCDatabaseScope + SetMultiDatabaseRouting
// from the persisted position.
//
//	(a) MySQL → MySQL: two source DBs → two auto-created target DBs.
//	(b) MySQL → PG: two source DBs → two same-named PG schemas.
//
// Each stream is drain-stopped via ctx cancel (the deterministic graceful
// drain the established multi-database + resume tests use) and the next Run
// returns before the restart begins, so the position is durably persisted
// before warm-resume reads it back.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// drainStopStreamer cancels the stream ctx and waits for Run to return,
// failing the test if it doesn't drain within the window. The graceful
// drain commits the in-flight batch (position + data atomic per ADR-0007),
// so the persisted position is durable by the time Run returns.
func drainStopStreamer(t *testing.T, cancel context.CancelFunc, runErr <-chan error, label string) {
	t.Helper()
	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("%s: streamer returned error on drain stop: %v", label, err)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("%s: streamer did not return after ctx cancel (drain stop)", label)
	}
}

// TestStreamer_MultiDatabase_StopRestart_MySQLToMySQL is scenario (a): the
// zero-loss-across-stop/restart pin for the multi-database warm-resume.
func TestStreamer_MultiDatabase_StopRestart_MySQLToMySQL(t *testing.T) {
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
	const streamID = "multidb-stop-restart-m2m"
	newStreamer := func() *Streamer {
		return &Streamer{
			Source:         mysqlEng,
			Target:         mysqlEng,
			SourceDSN:      serverDSN(t, srcServer),
			TargetDSN:      tgtHomeDSN,
			StreamID:       streamID,
			DatabaseFilter: DatabaseFilter{Include: []string{"source_db", "shop_db"}},
		}
	}

	// ---- Phase 1: cold-start the multi-database sync ----
	streamer1 := newStreamer()
	ctx1, cancel1 := context.WithCancel(context.Background())
	runErr1 := make(chan error, 1)
	go func() { runErr1 <- streamer1.Run(ctx1) }()

	tgtServerDSN := serverDSN(t, tgtServer)
	if !waitForRowCountMySQLDB(t, tgtServerDSN, "source_db", "widgets", 2, 60*time.Second) {
		cancel1()
		<-runErr1
		t.Fatalf("phase 1: cold-start never delivered source_db.widgets")
	}
	if !waitForRowCountMySQLDB(t, tgtServerDSN, "shop_db", "widgets", 3, 60*time.Second) {
		cancel1()
		<-runErr1
		t.Fatalf("phase 1: cold-start never delivered shop_db.widgets")
	}

	// Write to BOTH databases before the stop, and confirm they stream so
	// the persisted position is past the cold-start anchor (the restart must
	// resume from a genuinely-advanced server-wide position).
	applyDDLMySQL(t, srcServer, "INSERT INTO source_db.widgets (name) VALUES ('a-three');")
	applyDDLMySQL(t, shopDSN, "INSERT INTO widgets (name) VALUES ('b-four');")
	if !waitForRowCountMySQLDB(t, tgtServerDSN, "source_db", "widgets", 3, 30*time.Second) {
		cancel1()
		<-runErr1
		t.Fatalf("phase 1: CDC never delivered source_db pre-stop insert")
	}
	if !waitForRowCountMySQLDB(t, tgtServerDSN, "shop_db", "widgets", 4, 30*time.Second) {
		cancel1()
		<-runErr1
		t.Fatalf("phase 1: CDC never delivered shop_db pre-stop insert")
	}

	// ---- Phase 2: drain-stop ----
	drainStopStreamer(t, cancel1, runErr1, "phase 2")

	// The control table must carry a server-wide position so warm-resume has
	// something to resume from (NOT a re-cold-start).
	persistedToken := readPersistedPositionMySQL(t, tgtHomeDSN, streamID)
	if persistedToken == "" {
		t.Fatal("phase 2: sluice_cdc_state has no position for streamID — multi-database warm resume can't work")
	}
	t.Logf("phase 2: persisted server-wide position token = %q", persistedToken)

	// ---- Phase 3: WARM-RESUME restart (must NOT re-cold-start) ----
	streamer2 := newStreamer()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	runErr2 := make(chan error, 1)
	go func() { runErr2 <- streamer2.Run(ctx2) }()

	// Counts must stay stable for a few seconds after restart: a re-cold-start
	// would re-open a spanning snapshot and re-copy (the idempotent path would
	// absorb dups, but the warm-resume path must not snapshot at all). Stable
	// counts are the clean proxy for "warm-resumed, no re-copy".
	time.Sleep(3 * time.Second)
	if got := mysqlDBRowCount(t, tgtServerDSN, "source_db", "widgets"); got != 3 {
		cancel2()
		<-runErr2
		t.Fatalf("phase 3: source_db.widgets = %d after warm-resume; want 3 (no re-copy)", got)
	}
	if got := mysqlDBRowCount(t, tgtServerDSN, "shop_db", "widgets"); got != 4 {
		cancel2()
		<-runErr2
		t.Fatalf("phase 3: shop_db.widgets = %d after warm-resume; want 4 (no re-copy)", got)
	}

	// ---- Phase 4: write MORE to BOTH databases through the resumed stream ----
	applyDDLMySQL(t, srcServer, "INSERT INTO source_db.widgets (name) VALUES ('a-four');")
	applyDDLMySQL(t, shopDSN, "INSERT INTO widgets (name) VALUES ('b-five');")
	applyDDLMySQL(t, srcServer, "UPDATE source_db.widgets SET name='a-one-upd' WHERE name='a-one';")
	applyDDLMySQL(t, shopDSN, "DELETE FROM widgets WHERE name='b-two';")

	// source_db: 3 + 1 insert = 4. shop_db: 4 + 1 insert - 1 delete = 4.
	if !waitForRowCountMySQLDB(t, tgtServerDSN, "source_db", "widgets", 4, 30*time.Second) {
		cancel2()
		<-runErr2
		t.Fatalf("phase 4: resumed CDC never delivered source_db post-restart insert")
	}
	if !waitForRowCountMySQLDB(t, tgtServerDSN, "shop_db", "widgets", 4, 30*time.Second) {
		cancel2()
		<-runErr2
		t.Fatalf("phase 4: resumed CDC never settled shop_db post-restart")
	}
	// Let the UPDATE/DELETE settle before the bleed/value assertions.
	time.Sleep(2 * time.Second)

	// ---- Phase 5: drain-stop again ----
	drainStopStreamer(t, cancel2, runErr2, "phase 5")

	// ---- Phase 6: zero-loss / zero-dup across the stop/restart, per namespace ----
	srcServerDSN := serverDSN(t, srcServer)
	srcA := mysqlDBRowCount(t, srcServerDSN, "source_db", "widgets")
	srcB := mysqlDBRowCount(t, srcServerDSN, "shop_db", "widgets")
	tgtA := mysqlDBRowCount(t, tgtServerDSN, "source_db", "widgets")
	tgtB := mysqlDBRowCount(t, tgtServerDSN, "shop_db", "widgets")
	t.Logf("phase 6 counts: source_db src=%d tgt=%d | shop_db src=%d tgt=%d", srcA, tgtA, srcB, tgtB)
	if srcA != tgtA {
		t.Errorf("source_db zero-loss/dup FAIL across stop/restart: src=%d tgt=%d", srcA, tgtA)
	}
	if srcB != tgtB {
		t.Errorf("shop_db zero-loss/dup FAIL across stop/restart: src=%d tgt=%d", srcB, tgtB)
	}

	// Routing correctness across the restart: the post-restart UPDATE landed
	// in source_db only; the DELETE landed in shop_db only; no cross-DB bleed.
	if v := queryStringMySQL(t, tgtServerDSN, "source_db", "SELECT COUNT(*) FROM widgets WHERE name='a-one-upd'"); v != "1" {
		t.Errorf("phase 6: post-restart UPDATE not routed to source_db: a-one-upd count = %s; want 1", v)
	}
	if v := queryStringMySQL(t, tgtServerDSN, "shop_db", "SELECT COUNT(*) FROM widgets WHERE name='b-two'"); v != "0" {
		t.Errorf("phase 6: post-restart DELETE not routed to shop_db: b-two count = %s; want 0", v)
	}
	if v := queryStringMySQL(t, tgtServerDSN, "shop_db", "SELECT COUNT(*) FROM widgets WHERE name='a-one-upd'"); v != "0" {
		t.Errorf("phase 6: cross-DB bleed after restart: shop_db has source_db's a-one-upd (%s); want 0", v)
	}
}

// TestStreamer_MultiDatabase_StopRestart_MySQLToPostgres is scenario (b):
// the same stop/restart zero-loss pin across two same-named PG schemas.
func TestStreamer_MultiDatabase_StopRestart_MySQLToPostgres(t *testing.T) {
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
	const streamID = "multidb-stop-restart-m2pg"
	newStreamer := func() *Streamer {
		return &Streamer{
			Source:         mysqlEng,
			Target:         pgEng,
			SourceDSN:      serverDSN(t, srcServer),
			TargetDSN:      pgTarget,
			StreamID:       streamID,
			DatabaseFilter: DatabaseFilter{Include: []string{"source_db", "shop_db"}},
		}
	}

	// ---- Phase 1: cold-start ----
	streamer1 := newStreamer()
	ctx1, cancel1 := context.WithCancel(context.Background())
	runErr1 := make(chan error, 1)
	go func() { runErr1 <- streamer1.Run(ctx1) }()

	if !waitForPGSchemaCount(t, pgTarget, "source_db", "widgets", 2, 60*time.Second) {
		cancel1()
		<-runErr1
		t.Fatalf("phase 1: cold-start never delivered source_db.widgets to PG")
	}
	if !waitForPGSchemaCount(t, pgTarget, "shop_db", "widgets", 3, 60*time.Second) {
		cancel1()
		<-runErr1
		t.Fatalf("phase 1: cold-start never delivered shop_db.widgets to PG")
	}

	applyDDLMySQL(t, srcServer, "INSERT INTO source_db.widgets (name) VALUES ('a-three');")
	applyDDLMySQL(t, shopDSN, "INSERT INTO widgets (name) VALUES ('b-four');")
	if !waitForPGSchemaCount(t, pgTarget, "source_db", "widgets", 3, 30*time.Second) {
		cancel1()
		<-runErr1
		t.Fatalf("phase 1: CDC never delivered source_db pre-stop insert to PG")
	}
	if !waitForPGSchemaCount(t, pgTarget, "shop_db", "widgets", 4, 30*time.Second) {
		cancel1()
		<-runErr1
		t.Fatalf("phase 1: CDC never delivered shop_db pre-stop insert to PG")
	}

	// ---- Phase 2: drain-stop ----
	drainStopStreamer(t, cancel1, runErr1, "phase 2")

	// ---- Phase 3: WARM-RESUME restart ----
	streamer2 := newStreamer()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	runErr2 := make(chan error, 1)
	go func() { runErr2 <- streamer2.Run(ctx2) }()

	// Stable counts post-restart (no re-copy).
	time.Sleep(3 * time.Second)
	if got := pgScalarCount(pgTarget, "SELECT COUNT(*) FROM source_db.widgets"); got != 3 {
		cancel2()
		<-runErr2
		t.Fatalf("phase 3: source_db.widgets = %d after warm-resume; want 3 (no re-copy)", got)
	}
	if got := pgScalarCount(pgTarget, "SELECT COUNT(*) FROM shop_db.widgets"); got != 4 {
		cancel2()
		<-runErr2
		t.Fatalf("phase 3: shop_db.widgets = %d after warm-resume; want 4 (no re-copy)", got)
	}

	// ---- Phase 4: write MORE to BOTH databases through the resumed stream ----
	applyDDLMySQL(t, srcServer, "INSERT INTO source_db.widgets (name) VALUES ('a-four');")
	applyDDLMySQL(t, shopDSN, "INSERT INTO widgets (name) VALUES ('b-five');")
	if !waitForPGSchemaCount(t, pgTarget, "source_db", "widgets", 4, 30*time.Second) {
		cancel2()
		<-runErr2
		t.Fatalf("phase 4: resumed CDC never delivered source_db post-restart insert to PG")
	}
	if !waitForPGSchemaCount(t, pgTarget, "shop_db", "widgets", 5, 30*time.Second) {
		cancel2()
		<-runErr2
		t.Fatalf("phase 4: resumed CDC never delivered shop_db post-restart insert to PG")
	}
	time.Sleep(2 * time.Second)

	// ---- Phase 5: drain-stop ----
	drainStopStreamer(t, cancel2, runErr2, "phase 5")

	// ---- Phase 6: zero-loss / zero-dup across stop/restart, per schema ----
	srcServerDSN := serverDSN(t, srcServer)
	srcA := mysqlDBRowCount(t, srcServerDSN, "source_db", "widgets")
	srcB := mysqlDBRowCount(t, srcServerDSN, "shop_db", "widgets")
	tgtA := pgScalarCount(pgTarget, "SELECT COUNT(*) FROM source_db.widgets")
	tgtB := pgScalarCount(pgTarget, "SELECT COUNT(*) FROM shop_db.widgets")
	t.Logf("phase 6 counts: source_db src=%d tgt=%d | shop_db src=%d tgt=%d", srcA, tgtA, srcB, tgtB)
	if srcA != tgtA {
		t.Errorf("source_db zero-loss/dup FAIL across stop/restart: src=%d tgt(pg)=%d", srcA, tgtA)
	}
	if srcB != tgtB {
		t.Errorf("shop_db zero-loss/dup FAIL across stop/restart: src=%d tgt(pg)=%d", srcB, tgtB)
	}

	// Cross-schema bleed guard: source_db must carry only a-* names.
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
		t.Errorf("phase 6: cross-schema bleed after restart: source_db has %d b-* rows; want 0", bleed)
	}
}
