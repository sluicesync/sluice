//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The CDC half of the MySQL array-leaf fix, against real servers.
//
// The unit test next door proves preflightArrayBytesLeafOnCDC's
// predicate; this proves the wiring — that a real PG source with a
// `json[]` column and a real MySQL target actually reaches it, and that
// the refusal arrives BEFORE the copy rather than mid-stream. A preflight
// that is correct and unreachable is the shape this file exists to rule
// out (see the pipeline's optional-interface note in CLAUDE.md: a stub
// that satisfies an interface by construction proves nothing about a
// real engine).
//
// The companion assertion is the one that would catch an over-refusal:
// the SAME harness with a `text[]` column must sync normally.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

func TestSync_PGToMySQL_ByteShapedArrayLeafRefusedAtPreflight(t *testing.T) {
	pgSource, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, `
		CREATE TABLE docs (
			id INT PRIMARY KEY,
			j  json[],
			t  text[]
		);
		INSERT INTO docs VALUES (1, ARRAY[$${"a":1}$$::json], ARRAY['x','y']);
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	streamer := &Streamer{
		Source:    pgEng,
		Target:    myEng,
		SourceDSN: pgSource,
		TargetDSN: mysqlTarget,
		StreamID:  "test-array-bytes-leaf-refusal",
	}
	err := streamer.Run(ctx)
	if err == nil {
		t.Fatal("sync accepted a json[] source column against a MySQL target. The applier reads column " +
			"types from the target, where json[] and bytea[] are the same JSON column, so it cannot " +
			"reproduce the cold copy's per-family encoding — the run must be refused, not started")
	}
	if !errors.Is(err, errArrayBytesLeafOnCDC) {
		t.Fatalf("sync failed for some other reason than the array-leaf refusal: %v", err)
	}
	if !strings.Contains(err.Error(), "docs.j") {
		t.Errorf("refusal does not name the offending column: %v", err)
	}
	// The over-refusal direction, in the same message: the text[] column
	// beside it is perfectly syncable and must not be implicated.
	if strings.Contains(err.Error(), "docs.t ") {
		t.Errorf("refusal implicates the text[] column, which syncs fine: %v", err)
	}
}

// TestSync_PGToMySQL_StringLeafArraySyncsNormally is the control. It uses
// the same harness with the byte-shaped column removed, so a preflight
// that grew too broad — refusing every array rather than the two families
// whose leaf is byte-shaped — fails here rather than shipping.
func TestSync_PGToMySQL_StringLeafArraySyncsNormally(t *testing.T) {
	pgSource, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, `
		CREATE TABLE plain (
			id INT PRIMARY KEY,
			t  text[],
			n  int[]
		);
		INSERT INTO plain VALUES (1, ARRAY['x','y'], ARRAY[1,2]);
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	streamer := &Streamer{
		Source:    pgEng,
		Target:    myEng,
		SourceDSN: pgSource,
		TargetDSN: mysqlTarget,
		StreamID:  "test-array-string-leaf-ok",
	}
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	db, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = db.Close() }()

	if !waitForMySQLRowID(t, db, "plain", 1, 90*time.Second) {
		select {
		case e := <-runErr:
			t.Fatalf("string-leaf array sync exited instead of copying: %v", e)
		default:
			t.Fatal("string-leaf array sync never landed the seed row")
		}
	}
	// And the values arrived as the documented JSON projection, so the
	// control is proving a copy rather than an empty table.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if got := mysqlString(t, ctx, db, `SELECT CAST(t AS CHAR) FROM plain WHERE id = 1`); got != `["x", "y"]` {
		t.Errorf("plain.t = %q; want [\"x\", \"y\"]", got)
	}
}
