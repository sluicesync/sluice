//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0091 (F7a) — default-on schema-change forwarding, DROP COLUMN
// cross-engine: MySQL → PG and PG → MySQL live CDC. DROP carries no
// type, so the cross-engine retarget (ADR-0091 §5) is a column-identity
// scrub only; the pin is that the destructive shape converges on the
// other engine's catalog and post-DROP DML lands.
//
// Both directions use the prime-then-mutate pattern (ADR-0091 §5b): the
// DROP is seed-guarded at the first post-cold-start boundary, so a
// non-destructive ADD is applied first to flip the cache entry seed→CDC.

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

// TestStreamer_SchemaForward_DropColumn_Cross_MySQLToPG forwards a DROP
// COLUMN from a MySQL source to a PG target.
func TestStreamer_SchemaForward_DropColumn_Cross_MySQLToPG(t *testing.T) {
	mysqlDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()

	applyDDLMySQL(t, mysqlDSN, `
		CREATE TABLE widgets (
			id BIGINT NOT NULL PRIMARY KEY,
			name VARCHAR(64) NOT NULL,
			doomed VARCHAR(64)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, doomed) VALUES (1, 'alpha', 'x'), (2, 'beta', 'y');")

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:    myEng,
		Target:    pgEng,
		SourceDSN: mysqlDSN,
		TargetDSN: pgDSN,
		StreamID:  "test-fwd-drop-mypg",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForPGRowCount(t, pgDSN, "widgets", 2, 60*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows on PG target")
	}

	tgtDB, err := sql.Open("pgx", pgDSN)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// PRIME on the MySQL source; wait for it on the PG target.
	applyDDLMySQL(t, mysqlDSN, "ALTER TABLE widgets ADD COLUMN _prime_col INT;")
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, _prime_col) VALUES (100, 'prime', 1);")
	if !waitForPGColumn(t, tgtDB, "widgets", "_prime_col", true, 60*time.Second) {
		t.Fatalf("prime: _prime_col never appeared on PG target — seed→CDC boundary not processed")
	}

	applyDDLMySQL(t, mysqlDSN, "ALTER TABLE widgets DROP COLUMN doomed;")
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name) VALUES (3, 'gamma');")

	if !waitForPGRowID(t, tgtDB, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-DROP row never landed on PG target — cross-engine DROP forwarding broken")
	}

	if !waitForPGColumn(t, tgtDB, "widgets", "doomed", false, 60*time.Second) {
		t.Errorf("PG target widgets.doomed still present — cross-engine DROP did not forward")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_SchemaForward_DropColumn_Cross_PGToMySQL forwards a DROP
// COLUMN from a PG source to a MySQL target.
func TestStreamer_SchemaForward_DropColumn_Cross_PGToMySQL(t *testing.T) {
	// ADR-0091 F7a GAP #1 (fixed): the PG CDC reader now lets DROP COLUMN
	// reach the forward intercept under --schema-changes=forward, so the
	// PG-source direction works the same way the MySQL→PG direction already
	// did. DROP carries no type, so the cross-engine retarget is a column-
	// identity scrub only.
	pgDSN, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	_, mysqlDSN, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgDSN, `
		CREATE TABLE widgets (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			doomed TEXT
		);
		ALTER TABLE widgets REPLICA IDENTITY FULL;
		INSERT INTO widgets (id, name, doomed) VALUES (1, 'alpha', 'x'), (2, 'beta', 'y');
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	streamer := &Streamer{
		Source:    pgEng,
		Target:    myEng,
		SourceDSN: pgDSN,
		TargetDSN: mysqlDSN,
		StreamID:  "test-fwd-drop-pgmy",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, mysqlDSN, "widgets", 2, 60*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows on MySQL target")
	}

	tgtDB, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// PRIME on the PG source; wait for it on the MySQL target. The
	// prime-row INSERT is REQUIRED on PG (pure DDL emits no logical-
	// replication message; the next DML carries the Relation that
	// surfaces the schema change).
	applyPGDDL(t, pgDSN, `
		ALTER TABLE widgets ADD COLUMN _prime_col INT;
		INSERT INTO widgets (id, name, _prime_col) VALUES (100, 'prime', 1);
	`)
	if !waitForMySQLColumn(t, tgtDB, "widgets", "_prime_col", true, 60*time.Second) {
		t.Fatalf("prime: _prime_col never appeared on MySQL target — seed→CDC boundary not processed")
	}

	applyPGDDL(t, pgDSN, `
		ALTER TABLE widgets DROP COLUMN doomed;
		INSERT INTO widgets (id, name) VALUES (3, 'gamma');
	`)

	if !waitForMySQLRowID(t, tgtDB, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-DROP row never landed on MySQL target — cross-engine DROP forwarding broken")
	}

	if !waitForMySQLColumn(t, tgtDB, "widgets", "doomed", false, 60*time.Second) {
		t.Errorf("MySQL target widgets.doomed still present — cross-engine DROP did not forward")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
