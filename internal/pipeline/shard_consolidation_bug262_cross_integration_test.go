//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 262 — Shape A (--inject-shard-column) forwarded every source DDL
// with the target DDL qualified by the SOURCE's namespace: the boundary
// router handed the raw CDC-projected IR (MySQL database name, MySQL
// column types) straight to the target SchemaWriter, whose qualifyTable
// honours Table.Schema over its own bound schema. The cold start had
// CREATE'd the table through the bound schema (PG `public`), so the
// first forwardable DDL died with `schema "source_db" does not exist`
// and the stream stopped. Loud, no silent loss — but Shape A's headline
// promise (forwarded DDL) never worked cross-engine from a MySQL source.
//
// Every other consumer of the shared per-shape dispatch already ran the
// post IR through [retargetShapeForTarget] (types → target dialect,
// Schema scrubbed so the writer's bound namespace wins — the same rule
// the row applier's appliershared.Schema uses); the Shape A router was
// the one that did not. This file pins the fix on the real engine pair
// the regression cycle found it on.

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

// TestStreamer_Bug262_ShapeA_MySQLToPG_ForwardsDDLIntoTheBoundSchema: a
// MySQL source database `source_db` under Shape A onto a PG target whose
// bound schema is `public`. Two forwardable boundaries on two tables —
// ADD COLUMN on widgets, DROP COLUMN on gadgets — each followed by a row
// the stream can only apply if the DDL landed in the schema the table
// was created in. (Two tables rather than two boundaries on one: the
// lease row of an APPLIED boundary is re-acquirable only after the GC
// sweep retires it, so a second DDL on the same table is a different,
// pre-existing lease-FSM question and not what this pin grades.)
//
// Anti-vacuity: run against the parent commit this fails in phase B with
// the stream error `apply shape add-column: alter add column "price" on
// source_db.widgets: ERROR: schema "source_db" does not exist (SQLSTATE
// 3F000)` — recorded in the fix commit.
func TestStreamer_Bug262_ShapeA_MySQLToPG_ForwardsDDLIntoTheBoundSchema(t *testing.T) {
	mysqlDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()

	applyDDLMySQL(t, mysqlDSN, `
		CREATE TABLE widgets (
			id BIGINT NOT NULL PRIMARY KEY,
			name VARCHAR(64) NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO widgets (id, name) VALUES (1, 'alpha'), (2, 'beta');
		CREATE TABLE gadgets (
			id BIGINT NOT NULL PRIMARY KEY,
			name VARCHAR(64) NOT NULL,
			note VARCHAR(64) NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO gadgets (id, name, note) VALUES (1, 'gear', 'g'), (2, 'gizmo', NULL);
	`)

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
		StreamID:  "test-bug262-mypg",
		InjectShardColumn: ShardColumnSpec{
			Name:  "source_shard_id",
			Value: "shard_a",
		},
		ShardCoordinationLease: LeaseConfig{
			LeaseDuration: 30 * time.Second,
			RenewDeadline: 20 * time.Second,
			RetryPeriod:   5 * time.Second,
		},
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForPGRowCount(t, pgDSN, "widgets", 2, 60*time.Second) ||
		!waitForPGRowCount(t, pgDSN, "gadgets", 2, 60*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed the seed rows on the PG target")
	}

	// Phase B: ADD COLUMN — the shape the regression cycle died on —
	// then a row that needs the new column on the target.
	applyDDLMySQL(t, mysqlDSN, "ALTER TABLE widgets ADD COLUMN price DECIMAL(10,2);")
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, price) VALUES (3, 'gamma', 3.75);")
	if !waitForPGRowCountOrStreamExit(t, pgDSN, "widgets", 3, runErr, 60*time.Second) {
		t.Fatalf("phase B: the post-ADD-COLUMN row never landed on the PG target")
	}

	// Phase C: DROP COLUMN — a second arm of the same dispatch — then a
	// row shaped for the narrowed table.
	applyDDLMySQL(t, mysqlDSN, "ALTER TABLE gadgets DROP COLUMN note;")
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO gadgets (id, name) VALUES (3, 'gauge');")
	if !waitForPGRowCountOrStreamExit(t, pgDSN, "gadgets", 3, runErr, 60*time.Second) {
		t.Fatalf("phase C: the post-DROP-COLUMN row never landed on the PG target")
	}

	tgtDB, err := sql.Open("pgx", pgDSN)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The DDL landed in the schema the cold start created the table in
	// — and nowhere else. The independent expected value is the
	// catalog itself, not the stream's own accounting.
	assertPGColumnPresence(t, ctx, tgtDB, "public", "widgets", "price", true)
	assertPGColumnPresence(t, ctx, tgtDB, "public", "gadgets", "note", false)
	var sourceNamedSchemas int
	if err := tgtDB.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'source_db'`,
	).Scan(&sourceNamedSchemas); err != nil {
		t.Fatalf("schemata probe: %v", err)
	}
	if sourceNamedSchemas != 0 {
		t.Errorf("a schema named after the MySQL source database exists on the PG target; the DDL was routed by source namespace")
	}

	// Both post-DDL rows carry the discriminator and the value the
	// source wrote.
	var price string
	if err := tgtDB.QueryRowContext(
		ctx,
		`SELECT price::text FROM public.widgets WHERE id = 3 AND source_shard_id = 'shard_a'`,
	).Scan(&price); err != nil {
		t.Fatalf("read the post-ADD row: %v", err)
	}
	if price != "3.75" {
		t.Errorf("post-ADD row price = %q; want 3.75", price)
	}
	var gauge string
	if err := tgtDB.QueryRowContext(
		ctx,
		`SELECT name FROM public.gadgets WHERE id = 3 AND source_shard_id = 'shard_a'`,
	).Scan(&gauge); err != nil {
		t.Fatalf("read the post-DROP row: %v", err)
	}
	if gauge != "gauge" {
		t.Errorf("post-DROP row name = %q; want gauge", gauge)
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// waitForPGRowCountOrStreamExit polls like [waitForPGRowCount] but fails
// the test immediately — naming the stream's own error — if the streamer
// exits first. Without this, a stream that dies on the boundary looks
// like a 60-second timeout and the load-bearing error text is lost.
func waitForPGRowCountOrStreamExit(t *testing.T, dsn, table string, n int, runErr <-chan error, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-runErr:
			t.Fatalf("streamer exited while waiting for %s rows >= %d: %v", table, n, err)
		default:
		}
		if pollPGRowCount(dsn, table) >= n {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// assertPGColumnPresence checks information_schema.columns on the PG
// target for exactly the expected presence of schema.table.column.
func assertPGColumnPresence(t *testing.T, ctx context.Context, db *sql.DB, schema, table, column string, want bool) {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
	`, schema, table, column).Scan(&n); err != nil {
		t.Fatalf("column probe %s.%s.%s: %v", schema, table, column, err)
	}
	if (n == 1) != want {
		t.Errorf("%s.%s.%s present = %v; want %v", schema, table, column, n == 1, want)
	}
}
