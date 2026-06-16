//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 145 — MySQL ENUM schema changes forward to a PG target. Two
// manifestations, both fixed in the PG SchemaWriter's forward DDL emission:
//
//   (a) ADD COLUMN <enum>: the PG column def renders the named enum type
//       ident, so the type must be CREATE-d first or PG raises 42704
//       "type does not exist". AlterAddColumn now ensures the type
//       (idempotent DO-block CREATE TYPE) before the ADD COLUMN.
//   (b) MODIFY <enum> adding a value: arrives as an alter-column-type
//       shape, but on PG an enum value change is ALTER TYPE ... ADD VALUE,
//       not ALTER COLUMN ... TYPE (which can't render an enum). AlterColumnType
//       now routes enum wants to ADD VALUE IF NOT EXISTS.
//
// MySQL → PG only: MySQL ENUM is column-inline (the reader projects ir.Enum
// cleanly), so the bug was purely the PG writer's forward emission. A
// PG-source enum is a custom catalog OID the CDC reader doesn't resolve —
// a separate, out-of-scope gap.

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

// TestStreamer_SchemaForward_Enum_MySQLToPG forwards (a) an ADD COLUMN of a
// MySQL ENUM and (b) a MODIFY that appends an ENUM value, end-to-end to a PG
// target under the default --schema-changes=forward. The post-change INSERTs
// carry the new column / new value, so a landed row proves the PG enum type
// was created (a) and the value appended (b) — an unforwarded change would
// make the INSERT fail on PG and the row would never land.
func TestStreamer_SchemaForward_Enum_MySQLToPG(t *testing.T) {
	mysqlDSN, _, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()

	applyDDLMySQL(t, mysqlDSN, `
		CREATE TABLE widgets (
			id   BIGINT NOT NULL PRIMARY KEY,
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
		Source:    myEng,
		Target:    pgEng,
		SourceDSN: mysqlDSN,
		TargetDSN: pgDSN,
		StreamID:  "test-fwd-enum-mypg",
		// SchemaChanges defaults to "forward" (ADR-0091); left unset.
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

	// --- (a) ADD COLUMN <enum> (additive → forwards on the first boundary,
	// and flips the table seed→CDC for the mutate below). The INSERT carries
	// status='active'; the row lands only if the PG enum type was created and
	// the column added. ---
	applyDDLMySQL(t, mysqlDSN, "ALTER TABLE widgets ADD COLUMN status ENUM('new','active') NOT NULL DEFAULT 'new';")
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, status) VALUES (3, 'gamma', 'active');")

	if !waitForPGColumn(t, tgtDB, "widgets", "status", true, 60*time.Second) {
		t.Fatalf("(a) PG widgets.status enum column never appeared — ADD COLUMN enum did not forward (missing CREATE TYPE → 42704)")
	}
	if !waitForPGRowID(t, tgtDB, "widgets", 3, 60*time.Second) {
		select {
		case e := <-runErr:
			t.Fatalf("(a) post-ADD row never landed; stream exited: %v", e)
		default:
			t.Fatalf("(a) post-ADD row (status='active') never landed on PG — enum column forward broken")
		}
	}
	if got := pgScalarString(t, tgtDB, "SELECT status::text FROM widgets WHERE id=3"); got != "active" {
		t.Errorf("(a) PG widgets[id=3].status = %q; want \"active\"", got)
	}

	// --- (b) MODIFY <enum> appending a value (mutating → seed-guarded, but
	// the ADD above already flipped seed→CDC, so this forwards on a genuine
	// CDC→CDC boundary). The INSERT carries status='archived' — the new value;
	// the row lands only if 'archived' was appended to the PG enum type. ---
	applyDDLMySQL(t, mysqlDSN, "ALTER TABLE widgets MODIFY COLUMN status ENUM('new','active','archived') NOT NULL DEFAULT 'new';")
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, status) VALUES (4, 'delta', 'archived');")

	if !waitForPGRowID(t, tgtDB, "widgets", 4, 60*time.Second) {
		t.Fatalf("(b) post-MODIFY row (status='archived') never landed on PG — enum value-add did not forward (ALTER TYPE ADD VALUE)")
	}
	if got := pgScalarString(t, tgtDB, "SELECT status::text FROM widgets WHERE id=4"); got != "archived" {
		t.Errorf("(b) PG widgets[id=4].status = %q; want \"archived\"", got)
	}
	// Direct catalog check: the appended label is present on the PG enum type.
	if got := pgScalarString(t, tgtDB, `
		SELECT COUNT(*)::text FROM pg_enum e
		JOIN pg_type t ON e.enumtypid = t.oid
		WHERE t.typname = 'widgets_status_enum' AND e.enumlabel = 'archived'
	`); got != "1" {
		t.Errorf("(b) PG enum type widgets_status_enum missing label 'archived' (count=%s) — ALTER TYPE ADD VALUE did not forward", got)
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// pgScalarString runs a single-column single-row query and returns the value
// as a string (NULL → ""). Fails the test on query error.
func pgScalarString(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var v sql.NullString
	if err := db.QueryRowContext(ctx, query).Scan(&v); err != nil {
		t.Fatalf("pgScalarString %q: %v", query, err)
	}
	return v.String
}
