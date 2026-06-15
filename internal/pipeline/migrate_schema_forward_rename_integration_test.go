//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0091 (F7b) — PG-attnum-proven RENAME COLUMN forwarding on the
// single-stream CDC path. RENAME is the one shape that is ambiguous from
// the IR delta alone (a rename and a drop+add-of-same-type are
// indistinguishable). The ONLY safe disambiguation is a stable column id
// that survives a rename: PG's pg_attribute.attnum, carried as
// ir.Column.StableID by the PG CDC reader. Same non-zero attnum on
// before & after = PROVEN rename → forward (data preserved); a different
// attnum (real drop+add) or a zero attnum (MySQL — no stable id) →
// refuse loudly.
//
// THE PRIME-THEN-MUTATE PATTERN (seed-guard, ADR-0091 §5b): RENAME is a
// destructive/mutating shape under the seed-guard (F7b), so against the
// cold-start SEED (no attnum on the seed side) it is SKIPPED rather than
// forwarded. To exercise a forwarded rename we first establish a CDC→CDC
// boundary by priming with an ADD COLUMN (additive, not seed-guarded),
// then issue the real RENAME on the now-CDC-sourced cache entry.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_SchemaForward_RenameColumn_PG pins the forwarded,
// attnum-PROVEN RENAME COLUMN on PG → PG. The target column is renamed,
// the renamed column's data is PRESERVED, and post-rename INSERTs land.
func TestStreamer_SchemaForward_RenameColumn_PG(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE widgets (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			old_label TEXT
		);
		ALTER TABLE widgets REPLICA IDENTITY FULL;
		INSERT INTO widgets (id, name, old_label) VALUES (1, 'alpha', 'keep-me'), (2, 'beta', 'keep-me-too');
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
		StreamID:  "test-fwd-rename-pg",
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

	// PRIME: an ADD COLUMN flips the cache entry seed→CDC so the rename
	// lands on a genuine CDC→CDC boundary (where both sides carry attnum).
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets ADD COLUMN _prime_col INT;
		INSERT INTO widgets (id, name, _prime_col) VALUES (100, 'prime', 1);
	`)
	if !waitForPGColumn(t, tgtDB, "widgets", "_prime_col", true, 60*time.Second) {
		t.Fatalf("prime: _prime_col never appeared on target — seed→CDC boundary not processed")
	}

	// The real RENAME on a CDC→CDC boundary, plus a post-rename INSERT to
	// prove DML still flows under the new name. attnum is stable across
	// the rename → proven → forwards.
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets RENAME COLUMN old_label TO new_label;
		INSERT INTO widgets (id, name, new_label) VALUES (3, 'gamma', 'fresh');
	`)

	if !waitForPGRowID(t, tgtDB, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-RENAME row never landed — RENAME forwarding broken")
	}
	if !waitForPGColumn(t, tgtDB, "widgets", "new_label", true, 60*time.Second) {
		t.Errorf("target widgets.new_label absent — RENAME did not forward")
	}
	if !waitForPGColumn(t, tgtDB, "widgets", "old_label", false, 60*time.Second) {
		t.Errorf("target widgets.old_label still present — RENAME did not forward (drop+add fallback?)")
	}

	// CRITICAL: the renamed column's data must be PRESERVED (a rename
	// keeps the old data under the new name; a drop+add would lose it).
	var label string
	if err := tgtDB.QueryRow(`SELECT new_label FROM widgets WHERE id=1`).Scan(&label); err != nil {
		t.Fatalf("read preserved data: %v", err)
	}
	if label != "keep-me" {
		t.Errorf("renamed column data lost: new_label for id=1 = %q; want %q (data NOT preserved — drop+add, not rename)", label, "keep-me")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_SchemaForward_DropAddSameType_PG_Refuses pins the attnum
// DISAMBIGUATION: a DROP COLUMN x + ADD COLUMN y (same type) in ONE
// boundary is NOT a rename (the attnums differ), so it must NOT forward
// as a rename. It refuses loudly (the IR delta is a one-drop+one-add of
// the same type, classified as a rename shape, but unproven by attnum).
func TestStreamer_SchemaForward_DropAddSameType_PG_Refuses(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE widgets (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			gone TEXT
		);
		ALTER TABLE widgets REPLICA IDENTITY FULL;
		INSERT INTO widgets (id, name, gone) VALUES (1, 'alpha', 'x'), (2, 'beta', 'y');
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
		StreamID:  "test-fwd-dropadd-pg",
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

	// PRIME to a CDC→CDC boundary.
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets ADD COLUMN _prime_col INT;
		INSERT INTO widgets (id, name, _prime_col) VALUES (100, 'prime', 1);
	`)
	if !waitForPGColumn(t, tgtDB, "widgets", "_prime_col", true, 60*time.Second) {
		t.Fatalf("prime: _prime_col never appeared on target")
	}

	// DROP gone + ADD fresh (same TEXT type) in ONE boundary. New column
	// gets a fresh attnum != the dropped column's → unproven → refuse.
	applyPGDDL(t, sourceDSN, `
		ALTER TABLE widgets DROP COLUMN gone, ADD COLUMN fresh TEXT;
		INSERT INTO widgets (id, name, fresh) VALUES (3, 'gamma', 'z');
	`)

	var streamErr error
	select {
	case streamErr = <-runErr:
	case <-time.After(60 * time.Second):
		t.Fatal("streamer did not surface refuse-loudly within timeout (drop+add mis-forwarded as rename?)")
	}
	if streamErr == nil {
		t.Fatal("streamer returned nil; expected refuse on unproven (different-attnum) drop+add")
	}
	if !strings.Contains(streamErr.Error(), "RENAME COLUMN") ||
		!strings.Contains(streamErr.Error(), "cannot be auto-forwarded") {
		t.Fatalf("unexpected error (want ambiguous-rename refusal): %v", streamErr)
	}
}

// TestStreamer_SchemaForward_RenameColumn_Cross_PGToMySQL pins that a
// PG-source proven RENAME forwards to a MySQL target via
// AlterRenameColumn — the PROOF is the PG-source attnum; the target
// engine is irrelevant to the proof.
func TestStreamer_SchemaForward_RenameColumn_Cross_PGToMySQL(t *testing.T) {
	pgDSN, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	_, mysqlDSN, mysqlCleanup := startMySQLBinlog(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgDSN, `
		CREATE TABLE widgets (
			id INT PRIMARY KEY,
			name TEXT NOT NULL,
			old_label TEXT
		);
		ALTER TABLE widgets REPLICA IDENTITY FULL;
		INSERT INTO widgets (id, name, old_label) VALUES (1, 'alpha', 'keep-me'), (2, 'beta', 'keep-me-too');
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
		StreamID:  "test-fwd-rename-pgmy",
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

	applyPGDDL(t, pgDSN, `
		ALTER TABLE widgets ADD COLUMN _prime_col INT;
		INSERT INTO widgets (id, name, _prime_col) VALUES (100, 'prime', 1);
	`)
	if !waitForMySQLColumn(t, tgtDB, "widgets", "_prime_col", true, 60*time.Second) {
		t.Fatalf("prime: _prime_col never appeared on MySQL target")
	}

	applyPGDDL(t, pgDSN, `
		ALTER TABLE widgets RENAME COLUMN old_label TO new_label;
		INSERT INTO widgets (id, name, new_label) VALUES (3, 'gamma', 'fresh');
	`)

	if !waitForMySQLRowID(t, tgtDB, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-RENAME row never landed on MySQL target")
	}
	if !waitForMySQLColumn(t, tgtDB, "widgets", "new_label", true, 60*time.Second) {
		t.Errorf("MySQL target widgets.new_label absent — cross-engine RENAME did not forward")
	}
	if !waitForMySQLColumn(t, tgtDB, "widgets", "old_label", false, 60*time.Second) {
		t.Errorf("MySQL target widgets.old_label still present — cross-engine RENAME did not forward")
	}

	var label string
	if err := tgtDB.QueryRow(`SELECT new_label FROM widgets WHERE id=1`).Scan(&label); err != nil {
		t.Fatalf("read preserved data: %v", err)
	}
	if label != "keep-me" {
		t.Errorf("cross-engine renamed column data lost: new_label id=1 = %q; want %q", label, "keep-me")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_SchemaForward_RenameColumn_MySQL_Refuses is the
// regression pin that F7b did NOT loosen MySQL: a MySQL source has no
// stable column id (StableID=0), so a MySQL RENAME stays unprovable and
// must refuse loudly.
func TestStreamer_SchemaForward_RenameColumn_MySQL_Refuses(t *testing.T) {
	mysqlDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyDDLMySQL(t, mysqlDSN, `
		CREATE TABLE widgets (
			id BIGINT NOT NULL PRIMARY KEY,
			name VARCHAR(64) NOT NULL,
			old_label VARCHAR(64)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, old_label) VALUES (1, 'alpha', 'x'), (2, 'beta', 'y');")

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	streamer := &Streamer{
		Source:    myEng,
		Target:    myEng,
		SourceDSN: mysqlDSN,
		TargetDSN: targetDSN,
		StreamID:  "test-fwd-rename-mysql-refuse",
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, targetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows")
	}

	// PRIME to a CDC→CDC boundary (a rename against the seed would be
	// SKIPPED by the seed-guard, not refused).
	applyDDLMySQL(t, mysqlDSN, "ALTER TABLE widgets ADD COLUMN _prime_col INT;")
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, _prime_col) VALUES (100, 'prime', 1);")
	if !waitForRowCountMySQL(t, targetDSN, "widgets", 3, 60*time.Second) {
		t.Fatalf("prime: prime row never landed — seed→CDC boundary not processed")
	}

	// The RENAME on a CDC→CDC boundary: MySQL has no stable id, so it is
	// unprovable → refuse loudly.
	applyDDLMySQL(t, mysqlDSN, "ALTER TABLE widgets RENAME COLUMN old_label TO new_label;")
	applyDDLMySQL(t, mysqlDSN, "INSERT INTO widgets (id, name, new_label) VALUES (3, 'gamma', 'z');")

	var streamErr error
	select {
	case streamErr = <-runErr:
	case <-time.After(60 * time.Second):
		t.Fatal("streamer did not surface refuse-loudly on MySQL RENAME within timeout")
	}
	if streamErr == nil {
		t.Fatal("streamer returned nil; expected refuse on unprovable MySQL RENAME")
	}
	if !strings.Contains(streamErr.Error(), "RENAME COLUMN") ||
		!strings.Contains(streamErr.Error(), "cannot be auto-forwarded") {
		t.Fatalf("unexpected error (want ambiguous-rename refusal): %v", streamErr)
	}

	// The intercept refused BEFORE issuing the target DDL — old_label
	// stays, new_label never appears.
	tgtDB, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()
	if waitForMySQLColumn(t, tgtDB, "widgets", "new_label", true, 5*time.Second) {
		t.Errorf("MySQL target widgets.new_label exists — intercept did NOT refuse (regression: F7b loosened MySQL)")
	}
}
