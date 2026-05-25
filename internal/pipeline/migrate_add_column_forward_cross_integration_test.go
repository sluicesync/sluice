//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0058 — Online ADD COLUMN forwarding cross-engine: MySQL → PG
// and PG → MySQL. Exercises the [translate.RetargetForEngine] path on
// the live CDC forwarding code, matching the existing chain-restore
// cross-engine pin (chain_restore_cross_test.go) but on the live apply
// path rather than the drained chain-restore path.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestStreamer_AddColumnForward_Cross_MySQLToPG verifies that an ALTER TABLE
// ADD COLUMN on a MySQL source forwards through the live CDC apply
// to a PG target. The translate.RetargetForEngine rewrite handles
// MySQL's type → PG dialect mapping (e.g. DECIMAL(10,2) stays
// NUMERIC(10,2) — both engines spell it identically in IR, but
// VARCHAR/TEXT/etc. would translate per the existing path).
func TestStreamer_AddColumnForward_Cross_MySQLToPG(t *testing.T) {
	mysqlDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()

	applyDDLMySQL(t, mysqlDSN, `
		CREATE TABLE widgets (
			id BIGINT NOT NULL PRIMARY KEY,
			name VARCHAR(64) NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name) VALUES (1, 'alpha'), (2, 'beta');")

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:                 myEng,
		Target:                 pgEng,
		SourceDSN:              mysqlDSN,
		TargetDSN:              pgDSN,
		StreamID:               "test-addcol-fwd-mypg",
		ForwardSchemaAddColumn: true,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForPGRowCount(t, pgDSN, "widgets", 2, 60*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows on PG target")
	}

	applyDDLMySQL(t, mysqlDSN, "ALTER TABLE widgets ADD COLUMN price DECIMAL(10,2);")
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, price) VALUES (3, 'gamma', 3.75);")

	if !waitForPGRowCount(t, pgDSN, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-ALTER row never landed on PG target — cross-engine forwarding broken")
	}

	tgtDB, err := sql.Open("pgx", pgDSN)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var hasPrice int
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema='public' AND table_name='widgets' AND column_name='price'
	`).Scan(&hasPrice); err != nil {
		t.Fatalf("check column: %v", err)
	}
	if hasPrice != 1 {
		t.Errorf("PG target widgets.price column missing — intercept didn't forward the ALTER cross-engine")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_AddColumnForward_Cross_PGToMySQL verifies the mirror: PG source,
// MySQL target. Cross-engine column-def emission flows through the
// SchemaWriter's MySQL dialect.
func TestStreamer_AddColumnForward_Cross_PGToMySQL(t *testing.T) {
	pgDSN, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	_, mysqlDSN, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgDSN, `
		CREATE TABLE widgets (
			id INT PRIMARY KEY,
			name TEXT NOT NULL
		);
		ALTER TABLE widgets REPLICA IDENTITY FULL;
		INSERT INTO widgets (id, name) VALUES (1, 'alpha'), (2, 'beta');
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
		Source:                 pgEng,
		Target:                 myEng,
		SourceDSN:              pgDSN,
		TargetDSN:              mysqlDSN,
		StreamID:               "test-addcol-fwd-pgmy",
		ForwardSchemaAddColumn: true,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, mysqlDSN, "widgets", 2, 60*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows on MySQL target")
	}

	applyPGDDL(t, pgDSN, `
		ALTER TABLE widgets ADD COLUMN price NUMERIC(10,2);
		INSERT INTO widgets (id, name, price) VALUES (3, 'gamma', 3.75);
	`)

	if !waitForRowCountMySQL(t, mysqlDSN, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-ALTER row never landed on MySQL target — cross-engine forwarding broken")
	}

	tgtDB, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var hasPrice int
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema=DATABASE() AND table_name='widgets' AND column_name='price'
	`).Scan(&hasPrice); err != nil {
		t.Fatalf("check column: %v", err)
	}
	if hasPrice != 1 {
		t.Errorf("MySQL target widgets.price column missing — intercept didn't forward the ALTER cross-engine")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
