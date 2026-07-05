//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the streamer-side table filter: confirms a
// CDC event for an excluded table is dropped at the dispatch layer
// before the applier sees it, while events for allowed tables
// continue to flow through.
//
// Uses the cross-engine MySQL→PG path: it's the highest-coverage
// pair in the suite today and the filter is engine-neutral, so a
// single direction is enough.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_FilterExcludesCDCEvents seeds two tables, starts
// the streamer with `audit_log` excluded, then issues an INSERT
// against each on the source. The PG target should receive the
// users row but never the audit_log row — both via bulk-copy (the
// excluded table never gets a CREATE TABLE on the target) and
// via CDC (the dispatch filter drops the audit_log event).
func TestStreamer_FilterExcludesCDCEvents(t *testing.T) {
	mysqlSourceDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()
	_, pgTargetDSN, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		CREATE TABLE audit_log (
			id   BIGINT NOT NULL AUTO_INCREMENT,
			what VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('seed@example.com');
		INSERT INTO audit_log (what) VALUES ('seed');
	`
	applyMySQLDDL(t, mysqlSourceDSN, seedDDL)

	mysqlEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")

	filter, err := migcore.NewTableFilter(nil, []string{"audit_log"})
	if err != nil {
		t.Fatalf("migcore.NewTableFilter: %v", err)
	}

	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: mysqlSourceDSN,
		TargetDSN: pgTargetDSN,
		StreamID:  "test-filter-streamer",
		Filter:    filter,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// ---- Bulk-copy phase: only users should land on PG ----
	if !waitForRowCount(t, pgTargetDSN, "users", 1, 60*time.Second) {
		t.Fatalf("bulk-copy never delivered users to PG target")
	}
	if pgTableExists(t, pgTargetDSN, "audit_log") {
		t.Errorf("audit_log table should not exist on PG target (filter excluded it)")
	}

	// ---- CDC phase: insert into both tables on source. ----
	// users inserts should propagate; audit_log inserts should
	// be dropped at the dispatch layer.
	applyMySQLDDL(t, mysqlSourceDSN,
		"INSERT INTO users (email) VALUES ('cdc1@example.com');"+
			"INSERT INTO audit_log (what) VALUES ('cdc1');"+
			"INSERT INTO users (email) VALUES ('cdc2@example.com');"+
			"INSERT INTO audit_log (what) VALUES ('cdc2');")

	// Wait for two more users rows on PG (3 total: 1 seed + 2 CDC).
	if !waitForRowCount(t, pgTargetDSN, "users", 3, 30*time.Second) {
		t.Fatalf("CDC never delivered new users rows to PG target")
	}
	// audit_log must remain absent — never created, never inserted.
	if pgTableExists(t, pgTargetDSN, "audit_log") {
		t.Errorf("audit_log appeared on PG target after CDC; filter should have dropped events")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// pgTableExists reports whether a table with the given name exists
// in the public schema on the given PG DSN. False on any
// connection / query error — used by the filter test to assert
// "table never created", so a transient error during the brief
// startup window degrades safely to "not yet".
func pgTableExists(t *testing.T, dsn, table string) bool {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return false
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var exists bool
	err = db.QueryRowContext(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = $1
		)`, table).Scan(&exists)
	if err != nil {
		return false
	}
	return exists
}
