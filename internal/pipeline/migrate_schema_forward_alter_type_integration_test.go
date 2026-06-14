//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0091 (F7a) — default-on schema-change forwarding. ALTER COLUMN
// TYPE (a safe widening, INT→BIGINT) end-to-end across PG → PG,
// MySQL → MySQL, and one cross direction (MySQL → PG, exercising the
// translate.RetargetForEngine type rewrite on the destructive shape per
// ADR-0091 §5).
//
// ALTER COLUMN TYPE is a MUTATING shape and is seed-guarded at the first
// post-cold-start boundary (ADR-0091 §5b), so every test uses the
// prime-then-mutate pattern: a non-destructive ADD COLUMN flips the
// cache entry seed→CDC first, then the ALTER TYPE forwards on a genuine
// CDC→CDC boundary. The post-ALTER INSERT carries a value that overflows
// the OLD (INT) type but fits the NEW (BIGINT) type, proving the target
// column actually widened (not merely that the row landed).

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

// bigVal overflows a signed 32-bit INT but fits a BIGINT — used to prove
// the target column genuinely widened.
const bigVal int64 = 5_000_000_000

// TestStreamer_SchemaForward_AlterType_PG widens an INT column to BIGINT
// on PG → PG and verifies the target column type changed and a
// post-ALTER row with a >32-bit value lands.
func TestStreamer_SchemaForward_AlterType_PG(t *testing.T) {
	// BLOCKED — F7a GAP #1 (PG source). The PG CDC reader's checkSchemaRace
	// refuses ALTER COLUMN TYPE (existing column OID changed) before the
	// boundary reaches the ADR-0091 intercept (same root cause as
	// TestStreamer_SchemaForward_DropColumn_PG).
	t.Skip("BLOCKED: F7a GAP #1 — PG CDC checkSchemaRace refuses ALTER COLUMN TYPE before the ADR-0091 intercept (see report)")

	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE widgets (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			counter INT
		);
		ALTER TABLE widgets REPLICA IDENTITY FULL;
		INSERT INTO widgets (id, name, counter) VALUES (1, 'alpha', 10), (2, 'beta', 20);
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "test-fwd-altertype-pg",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForPGRowCount(t, targetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows")
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// PRIME: flip seed→CDC (PG pure DDL needs a follow-on DML to surface).
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets ADD COLUMN _prime_col INT;
		INSERT INTO widgets (id, name, counter, _prime_col) VALUES (100, 'prime', 1, 1);
	`)
	if !waitForPGColumn(t, tgtDB, "widgets", "_prime_col", true, 60*time.Second) {
		t.Fatalf("prime: _prime_col never appeared on target — seed→CDC boundary not processed")
	}

	// Widen counter INT→BIGINT on a genuine CDC→CDC boundary; post-ALTER
	// INSERT carries a value that only fits BIGINT.
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets ALTER COLUMN counter TYPE BIGINT;
		INSERT INTO widgets (id, name, counter) VALUES (3, 'gamma', 5000000000);
	`)

	if !waitForPGRowID(t, tgtDB, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-ALTER row never landed — ALTER TYPE forwarding broken")
	}

	if !waitForPGColumnType(t, tgtDB, "widgets", "counter", "bigint", 60*time.Second) {
		t.Errorf("target widgets.counter did not widen to bigint — ALTER TYPE did not forward")
	}

	assertPGCounter(t, tgtDB, 3, bigVal)

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_SchemaForward_AlterType_MySQL widens an INT column to
// BIGINT on MySQL → MySQL (MySQL emits MODIFY COLUMN; the apply path
// differs from PG's ALTER COLUMN … TYPE — pinned per target per the
// Bug 74 class discipline).
func TestStreamer_SchemaForward_AlterType_MySQL(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyDDLMySQL(t, sourceDSN, `
		CREATE TABLE widgets (
			id BIGINT NOT NULL PRIMARY KEY,
			name VARCHAR(64) NOT NULL,
			counter INT
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)
	applyDDLMySQL(t, sourceDSN, "INSERT INTO widgets (id, name, counter) VALUES (1, 'alpha', 10), (2, 'beta', 20);")

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	streamer := &Streamer{
		Source:    myEng,
		Target:    myEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "test-fwd-altertype-mysql",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, targetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows")
	}

	tgtDB, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	applyDDLMySQL(t, sourceDSN, "ALTER TABLE widgets ADD COLUMN _prime_col INT;")
	applyDDLMySQL(t, sourceDSN, "INSERT INTO widgets (id, name, counter, _prime_col) VALUES (100, 'prime', 1, 1);")
	if !waitForMySQLColumn(t, tgtDB, "widgets", "_prime_col", true, 60*time.Second) {
		t.Fatalf("prime: _prime_col never appeared on target — seed→CDC boundary not processed")
	}

	applyDDLMySQL(t, sourceDSN, "ALTER TABLE widgets MODIFY COLUMN counter BIGINT;")
	applyDDLMySQL(t, sourceDSN, "INSERT INTO widgets (id, name, counter) VALUES (3, 'gamma', 5000000000);")

	if !waitForMySQLRowID(t, tgtDB, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-ALTER row never landed — ALTER TYPE forwarding broken")
	}

	if !waitForMySQLColumnType(t, tgtDB, "widgets", "counter", "bigint", 60*time.Second) {
		t.Errorf("target widgets.counter did not widen to bigint — MODIFY COLUMN did not forward")
	}

	assertMySQLCounter(t, tgtDB, 3, bigVal)

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_SchemaForward_AlterType_Cross_MySQLToPG widens INT→BIGINT
// from a MySQL source to a PG target — the cross-engine ALTER TYPE
// retarget (ADR-0091 §5): the source MySQL BIGINT is translated to the
// PG dialect before the target ALTER is issued.
func TestStreamer_SchemaForward_AlterType_Cross_MySQLToPG(t *testing.T) {
	// BLOCKED — F7a GAP #3 (CRITICAL, cross-engine ALTER TYPE convergence).
	// On MySQL→PG the intercept logs "schema-forward: target DDL applied
	// shape=alter-column-type", but the PG target column does NOT actually
	// widen — it stays int4. The post-ALTER row carrying a >32-bit value
	// then fails to apply with a LOUD encode error:
	//   "unable to encode 5000000000 into binary format for int4 (OID 23):
	//    5000000000 is greater than maximum value for int4"
	// So the cross-engine ALTER TYPE retarget/apply (ADR-0091 §5,
	// translate.RetargetForEngine + AlterColumnType on the PG SchemaWriter)
	// reports success while the target schema diverges. The loud row-encode
	// failure prevents silent corruption (good), but the DDL forward is a
	// false-success. The same-engine MySQL→MySQL ALTER TYPE converges, so
	// the defect is in the cross-engine retarget/apply path. This test pins
	// the convergence + the >32-bit value landing; un-skip when fixed.

	mysqlDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()

	applyDDLMySQL(t, mysqlDSN, `
		CREATE TABLE widgets (
			id BIGINT NOT NULL PRIMARY KEY,
			name VARCHAR(64) NOT NULL,
			counter INT
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, counter) VALUES (1, 'alpha', 10), (2, 'beta', 20);")

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
		StreamID:  "test-fwd-altertype-mypg",
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

	applyDDLMySQL(t, mysqlDSN, "ALTER TABLE widgets ADD COLUMN _prime_col INT;")
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, counter, _prime_col) VALUES (100, 'prime', 1, 1);")
	if !waitForPGColumn(t, tgtDB, "widgets", "_prime_col", true, 60*time.Second) {
		t.Fatalf("prime: _prime_col never appeared on PG target — seed→CDC boundary not processed")
	}

	applyDDLMySQL(t, mysqlDSN, "ALTER TABLE widgets MODIFY COLUMN counter BIGINT;")
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, counter) VALUES (3, 'gamma', 5000000000);")

	if !waitForPGRowID(t, tgtDB, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-ALTER row never landed on PG target — cross-engine ALTER TYPE forwarding broken")
	}

	if !waitForPGColumnType(t, tgtDB, "widgets", "counter", "bigint", 60*time.Second) {
		t.Errorf("PG target widgets.counter did not widen to bigint — cross-engine ALTER TYPE did not forward")
	}

	assertPGCounter(t, tgtDB, 3, bigVal)

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

func assertPGCounter(t *testing.T, db *sql.DB, id int, want int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var got int64
	if err := db.QueryRowContext(ctx, "SELECT counter FROM widgets WHERE id=$1", id).Scan(&got); err != nil {
		t.Fatalf("scan counter id=%d: %v", id, err)
	}
	if got != want {
		t.Errorf("widgets.counter for id=%d = %d; want %d (>32-bit value lost — column did not widen)", id, got, want)
	}
}

func assertMySQLCounter(t *testing.T, db *sql.DB, id int, want int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var got int64
	if err := db.QueryRowContext(ctx, "SELECT counter FROM widgets WHERE id=?", id).Scan(&got); err != nil {
		t.Fatalf("scan counter id=%d: %v", id, err)
	}
	if got != want {
		t.Errorf("widgets.counter for id=%d = %d; want %d (>32-bit value lost — column did not widen)", id, got, want)
	}
}
