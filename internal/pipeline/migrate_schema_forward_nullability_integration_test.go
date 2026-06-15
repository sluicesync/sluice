//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0091 (F7a) — default-on schema-change forwarding. ALTER COLUMN
// NULLABILITY (DROP NOT NULL — the safe direction; SET NOT NULL only
// holds if existing rows comply) end-to-end on PG → PG and
// MySQL → MySQL.
//
// ALTER NULLABILITY is a MUTATING shape and is seed-guarded at the first
// post-cold-start boundary (ADR-0091 §5b), so both tests use the
// prime-then-mutate pattern: a non-destructive ADD COLUMN flips the
// cache entry seed→CDC first, then the nullability change forwards on a
// genuine CDC→CDC boundary. The post-ALTER INSERT supplies NULL for the
// now-nullable column, proving the constraint was actually dropped (a
// NULL would be rejected if NOT NULL still stood).

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

// TestStreamer_SchemaForward_DropNotNull_PG forwards an ALTER COLUMN …
// DROP NOT NULL on PG → PG; the post-ALTER INSERT carries a NULL.
func TestStreamer_SchemaForward_DropNotNull_PG(t *testing.T) {
	// DOCUMENTED LIMITATION (ADR-0091 §1d footnote 2), not a TODO:
	// pgoutput's RelationMessage carries no nullability flag, so a
	// DROP NOT NULL on a PG source produces no CDC boundary and cannot be
	// forwarded without a separate out-of-band catalog subscription
	// (future F47-class work). A resulting incompatibility surfaces as a
	// loud apply error, never silent corruption. The MySQL-source case
	// (below) DOES forward — GAP #2 — because MySQL's information_schema
	// re-read carries nullability. This skip pins the asymmetry.
	t.Skip("documented limitation (ADR-0091 §1d): PG pgoutput omits the nullability flag — DROP NOT NULL cannot be forwarded on a PG source; MySQL source forwards it")

	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE widgets (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			label TEXT NOT NULL
		);
		ALTER TABLE widgets REPLICA IDENTITY FULL;
		INSERT INTO widgets (id, name, label) VALUES (1, 'alpha', 'a'), (2, 'beta', 'b');
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
		StreamID:  "test-fwd-dropnn-pg",
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
		INSERT INTO widgets (id, name, label, _prime_col) VALUES (100, 'prime', 'p', 1);
	`)
	if !waitForPGColumn(t, tgtDB, "widgets", "_prime_col", true, 60*time.Second) {
		t.Fatalf("prime: _prime_col never appeared on target — seed→CDC boundary not processed")
	}

	// DROP NOT NULL on label, then INSERT a row whose label is NULL.
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets ALTER COLUMN label DROP NOT NULL;
		INSERT INTO widgets (id, name, label) VALUES (3, 'gamma', NULL);
	`)

	if !waitForPGRowID(t, tgtDB, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-ALTER NULL row never landed — DROP NOT NULL forwarding broken")
	}

	if !waitForPGColumnNullable(t, tgtDB, "widgets", "label", true, 60*time.Second) {
		t.Errorf("target widgets.label still NOT NULL — DROP NOT NULL did not forward")
	}

	// The NULL value must have landed (would be impossible under NOT NULL).
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var label sql.NullString
	if err := tgtDB.QueryRowContext(ctx, "SELECT label FROM widgets WHERE id=3").Scan(&label); err != nil {
		t.Fatalf("scan label id=3: %v", err)
	}
	if label.Valid {
		t.Errorf("widgets.label for id=3 = %q; want NULL", label.String)
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_SchemaForward_DropNotNull_MySQL forwards a MODIFY COLUMN …
// NULL on MySQL → MySQL (MySQL spells DROP NOT NULL as a MODIFY without
// the NOT NULL clause).
func TestStreamer_SchemaForward_DropNotNull_MySQL(t *testing.T) {
	// ADR-0091 F7a GAP #2 (fixed). A MODIFY COLUMN that changes only
	// nullability does NOT move ir.SchemaSignatureOf (column NAME+TYPE only
	// — nullability is deliberately excluded from the ADR-0049 decode
	// contract). Before the fix the MySQL CDC reader gated SchemaSnapshot
	// emission on that signature alone, so a nullability-only ALTER emitted
	// no boundary, the forward intercept never classified it, the target
	// stayed NOT NULL, and the post-ALTER NULL INSERT never landed. The fix
	// adds a SEPARATE forward signal in the reader: when schemaForward is on
	// (the default --schema-changes=forward), a per-column nullability delta
	// also emits a boundary, leaving the decode signature untouched.
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyDDLMySQL(t, sourceDSN, `
		CREATE TABLE widgets (
			id BIGINT NOT NULL PRIMARY KEY,
			name VARCHAR(64) NOT NULL,
			label VARCHAR(64) NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)
	applyDDLMySQL(t, sourceDSN, "INSERT INTO widgets (id, name, label) VALUES (1, 'alpha', 'a'), (2, 'beta', 'b');")

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	streamer := &Streamer{
		Source:    myEng,
		Target:    myEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "test-fwd-dropnn-mysql",
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
	applyDDLMySQL(t, sourceDSN, "INSERT INTO widgets (id, name, label, _prime_col) VALUES (100, 'prime', 'p', 1);")
	if !waitForMySQLColumn(t, tgtDB, "widgets", "_prime_col", true, 60*time.Second) {
		t.Fatalf("prime: _prime_col never appeared on target — seed→CDC boundary not processed")
	}

	applyDDLMySQL(t, sourceDSN, "ALTER TABLE widgets MODIFY COLUMN label VARCHAR(64) NULL;")
	applyDDLMySQL(t, sourceDSN, "INSERT INTO widgets (id, name, label) VALUES (3, 'gamma', NULL);")

	if !waitForMySQLRowID(t, tgtDB, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-ALTER NULL row never landed — DROP NOT NULL forwarding broken")
	}

	if !waitForMySQLColumnNullable(t, tgtDB, "widgets", "label", true, 60*time.Second) {
		t.Errorf("target widgets.label still NOT NULL — MODIFY … NULL did not forward")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var label sql.NullString
	if err := tgtDB.QueryRowContext(ctx, "SELECT label FROM widgets WHERE id=3").Scan(&label); err != nil {
		t.Fatalf("scan label id=3: %v", err)
	}
	if label.Valid {
		t.Errorf("widgets.label for id=3 = %q; want NULL", label.String)
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
