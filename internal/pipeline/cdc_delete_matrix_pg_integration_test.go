//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Task #44 — hard-delete CDC family-matrix pin (Postgres source axis).
//
// Pins ADR-0057's hard-delete matrix per the Bug-74 family-pin
// discipline. The family-dispatch under test is the pgoutput OLD-tuple
// content, which varies by REPLICA IDENTITY:
//
//   - DEFAULT      → OLD tuple carries primary-key columns only.
//   - FULL         → OLD tuple carries every column.
//   - USING INDEX  → OLD tuple carries the named index's columns.
//   - NOTHING      → no OLD tuple at all; sluice REFUSES LOUDLY.
//
// NOTHING is intentionally excluded from this matrix — emitDelete at
// internal/engines/postgres/cdc_reader.go:1003-1013 refuses by
// surfacing an operator-actionable error, and unit-level coverage
// already lives at TestSynthesizeKeyOnlyBeforeRejectsReplicaIdentityNothing
// (internal/engines/postgres/cdc_reader_test.go:539-555). A direct
// emitDelete-level unit test would duplicate that coverage; the
// refusal path is exercised at the same level the bug class lives at.
//
// A regression in the "always-emit-DELETE" property could surface
// per setting (e.g. FULL → DEFAULT divergence in PG could break
// composite-PK targets if the WHERE was built from the full OLD tuple;
// USING INDEX on a non-PK column requires the apply path to honour
// the operator's choice of identity index). The matrix exists
// precisely so the next codec-dispatched regression in any cell
// fails LOUDLY. See docs/adr/adr-0057-hard-delete-semantics-across-engines.md.
//
// F18 Reddit-research triage context: silently dropping hard deletes
// was the most-cited operator pain in the dataset; sluice's structural
// always-emit-DELETE behaviour is the answer, and this pin keeps it
// honest across REPLICA IDENTITY cells.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_CDCDeleteMatrix_PostgresToPostgres pins the
// hard-delete always-propagates property across REPLICA IDENTITY ∈
// {DEFAULT, FULL, USING INDEX} × {plain DELETE, UPDATE-then-DELETE,
// DELETE of a row with TOAST'd column}.
//
// PG TOAST kicks in for variable-length values larger than ~2KB —
// 100KB TEXT is comfortably TOAST'd. Under DEFAULT replica identity
// the OLD tuple only carries the PK, so the TOAST'd column is absent
// regardless; under FULL the OLD tuple carries it (with the TOAST
// pointer or the inline value depending on column storage); under
// USING INDEX the OLD tuple carries the indexed column only. In every
// case the DELETE must propagate.
func TestStreamer_CDCDeleteMatrix_PostgresToPostgres(t *testing.T) {
	const big = 100 * 1024 // 100KB TEXT — exercise PG TOAST path.

	cells := []struct {
		identity string // "DEFAULT", "FULL", "USING_INDEX"
		shape    string
	}{
		{identity: "DEFAULT", shape: "plain-delete"},
		{identity: "DEFAULT", shape: "update-then-delete"},
		{identity: "DEFAULT", shape: "toast-delete"},
		{identity: "FULL", shape: "plain-delete"},
		{identity: "FULL", shape: "update-then-delete"},
		{identity: "FULL", shape: "toast-delete"},
		{identity: "USING_INDEX", shape: "plain-delete"},
		{identity: "USING_INDEX", shape: "update-then-delete"},
		{identity: "USING_INDEX", shape: "toast-delete"},
	}

	for _, c := range cells {
		c := c
		name := fmt.Sprintf("identity=%s/shape=%s", c.identity, c.shape)
		t.Run(name, func(t *testing.T) {
			runPostgresToPostgresDeleteCell(t, c.identity, c.shape, big)
		})
	}
}

// runPostgresToPostgresDeleteCell is the per-cell driver. Extracted so
// the matrix iteration stays readable.
func runPostgresToPostgresDeleteCell(t *testing.T, identity, shape string, blobLen int) {
	t.Helper()
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	// Schema includes `alt_key` for the USING INDEX cell (we create a
	// UNIQUE INDEX on it and point REPLICA IDENTITY at that index).
	// payload is TEXT so the TOAST'd-row shape can fit a 100KB blob.
	const seedDDL = `
		CREATE TABLE widgets (
			id      BIGINT       NOT NULL,
			alt_key BIGINT       NOT NULL UNIQUE,
			name    VARCHAR(64)  NOT NULL,
			payload TEXT,
			PRIMARY KEY (id)
		);
	`
	applyDDL(t, sourceDSN, seedDDL)

	// Configure REPLICA IDENTITY per cell. DEFAULT is the server-default;
	// we still ALTER explicitly so the test name's claim is hermetic.
	switch identity {
	case "DEFAULT":
		applyDDL(t, sourceDSN, "ALTER TABLE widgets REPLICA IDENTITY DEFAULT;")
	case "FULL":
		applyDDL(t, sourceDSN, "ALTER TABLE widgets REPLICA IDENTITY FULL;")
	case "USING_INDEX":
		// alt_key already has a UNIQUE constraint (which creates a
		// matching unique index named widgets_alt_key_key). Point
		// REPLICA IDENTITY at that index.
		applyDDL(t, sourceDSN,
			"ALTER TABLE widgets REPLICA IDENTITY USING INDEX widgets_alt_key_key;")
	default:
		t.Fatalf("unknown identity %q", identity)
	}

	// Seed 3 rows. id=2 is the one we DELETE; ids 1 and 3 are
	// witnesses that the WHERE clause didn't over-match.
	bigVal := strings.Repeat("x", blobLen)
	applyDDL(t, sourceDSN, fmt.Sprintf(
		"INSERT INTO widgets (id, alt_key, name, payload) VALUES "+
			"(1, 101, 'one', NULL), "+
			"(2, 102, 'two', '%s'), "+
			"(3, 103, 'three', NULL);",
		bigVal,
	))

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  fmt.Sprintf("delete-matrix-pg-%s-%s", identity, shape),
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForExactRowCount(targetDSN, "widgets", 3, 90*time.Second) {
		t.Fatalf("bulk copy never delivered the 3 seed rows (REPLICA IDENTITY=%s); target rows = %d",
			identity, pollRowCount(targetDSN, "widgets"))
	}

	switch shape {
	case "plain-delete":
		applyDDL(t, sourceDSN, "DELETE FROM widgets WHERE id = 2;")
		if !waitForExactRowCount(targetDSN, "widgets", 2, 30*time.Second) {
			t.Fatalf("plain DELETE never propagated (REPLICA IDENTITY=%s); target rows = %d, want 2",
				identity, pollRowCount(targetDSN, "widgets"))
		}

	case "update-then-delete":
		// The UPDATE event emits an OLD tuple whose content varies by
		// REPLICA IDENTITY (DEFAULT/USING_INDEX → only identity cols;
		// FULL → all cols). The DELETE must still propagate regardless.
		applyDDL(t, sourceDSN, "UPDATE widgets SET name = 'TWO_UPDATED' WHERE id = 2;")
		applyDDL(t, sourceDSN, "DELETE FROM widgets WHERE id = 2;")
		if !waitForExactRowCount(targetDSN, "widgets", 2, 30*time.Second) {
			t.Fatalf("UPDATE-then-DELETE never settled at row count 2 (REPLICA IDENTITY=%s); target rows = %d",
				identity, pollRowCount(targetDSN, "widgets"))
		}

	case "toast-delete":
		// DELETE the row whose payload is 100KB. Under DEFAULT the
		// OLD tuple only carries PK; under FULL it carries every
		// column (with the TOAST pointer); under USING_INDEX it
		// carries alt_key only. In every case sluice's
		// filterBeforeToKeyCols narrows to identity-key columns, so the
		// WHERE clause should be PK-only (DEFAULT/FULL) or alt_key
		// (USING_INDEX) — never include the TOAST'd payload column.
		applyDDL(t, sourceDSN, "DELETE FROM widgets WHERE id = 2;")
		if !waitForExactRowCount(targetDSN, "widgets", 2, 30*time.Second) {
			t.Fatalf("DELETE of TOAST'd row never propagated (REPLICA IDENTITY=%s); target rows = %d",
				identity, pollRowCount(targetDSN, "widgets"))
		}

	default:
		t.Fatalf("unknown shape %q", shape)
	}

	// Witness assertion: ids 1 and 3 still present (the WHERE clause
	// didn't over-match).
	if !pgRowExistsByID(t, streamCtx, targetDSN, "widgets", 1) {
		t.Errorf("witness row id=1 unexpectedly gone from target")
	}
	if !pgRowExistsByID(t, streamCtx, targetDSN, "widgets", 3) {
		t.Errorf("witness row id=3 unexpectedly gone from target")
	}
	if pgRowExistsByID(t, streamCtx, targetDSN, "widgets", 2) {
		t.Errorf("deleted row id=2 still present on target (silent-drop regression?)")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_CDCDeleteMatrix_PostgresToMySQL is the cross-engine
// sanity cell. PG→MySQL with REPLICA IDENTITY FULL × plain DELETE.
//
// FULL is chosen for the cross-engine sanity because it's the most
// content-rich variant on the wire — if the MySQL applier's WHERE
// construction silently drifted to include non-PK columns under FULL,
// this would catch it.
func TestStreamer_CDCDeleteMatrix_PostgresToMySQL(t *testing.T) {
	pgSourceDSN, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()

	_, mysqlTargetDSN, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	const seedDDL = `
		CREATE TABLE widgets (
			id   BIGINT       NOT NULL,
			name VARCHAR(64)  NOT NULL,
			PRIMARY KEY (id)
		);
		ALTER TABLE widgets REPLICA IDENTITY FULL;
		INSERT INTO widgets (id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three');
	`
	applyDDL(t, pgSourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	streamer := &Streamer{
		Source:    pgEng,
		Target:    mysqlEng,
		SourceDSN: pgSourceDSN,
		TargetDSN: mysqlTargetDSN,
		StreamID:  "delete-matrix-pg-to-mysql-FULL",
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForExactRowCountMySQL(mysqlTargetDSN, "widgets", 3, 90*time.Second) {
		t.Fatalf("bulk copy never delivered the 3 seed rows to MySQL (got %d)", pollRowCountMySQL(mysqlTargetDSN, "widgets"))
	}

	applyDDL(t, pgSourceDSN, "DELETE FROM widgets WHERE id = 2;")
	if !waitForExactRowCountMySQL(mysqlTargetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("cross-engine CDC DELETE never propagated to MySQL target; rows = %d, want 2",
			pollRowCountMySQL(mysqlTargetDSN, "widgets"))
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// pgRowExistsByID returns true when a row with the given integer id
// exists in the named PG table on the target. Used as a witness check
// that DELETE WHERE clauses don't over-match.
func pgRowExistsByID(t *testing.T, ctx context.Context, dsn, table string, id int) bool {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return false
	}
	defer func() { _ = db.Close() }()
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var exists bool
	q := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE id = $1)", table)
	if err := db.QueryRowContext(c, q, id).Scan(&exists); err != nil {
		return false
	}
	return exists
}
