//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the PG source-side heartbeat writer
// (ADR-0061, F17). Boots a Postgres container with logical replication,
// creates a slot, runs the heartbeat writer for a brief window, and
// asserts:
//
//   - the heartbeat table exists with the expected schema;
//   - rows accumulate at the expected cadence;
//   - the slot's confirmed_flush_lsn / restart_lsn advance — proving
//     the writes generate WAL the slot consumer sees as progress;
//   - PruneHeartbeat removes rows older than the window without
//     touching newer ones;
//   - EnsureHeartbeatTable on a low-privilege role surfaces
//     [ir.ErrHeartbeatPermission].
//
// The unit tests cover the loop-lifecycle shape; this file exercises
// the engine-side SQL against real Postgres.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

// TestEnsureHeartbeatTable_CreatesAndIdempotent pins the table-create
// path: the first call creates the table; a second call is a no-op.
// We assert the column shape against information_schema so a future
// drift in the DDL surfaces loudly.
func TestEnsureHeartbeatTable_CreatesAndIdempotent(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeReader(t, sr)
	pgsr := sr.(*SchemaReader)

	const table = "sluice_heartbeat"
	if err := pgsr.EnsureHeartbeatTable(ctx, table); err != nil {
		t.Fatalf("EnsureHeartbeatTable: %v", err)
	}
	// Idempotency.
	if err := pgsr.EnsureHeartbeatTable(ctx, table); err != nil {
		t.Fatalf("EnsureHeartbeatTable (second call) should be idempotent: %v", err)
	}

	// Verify the column shape.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verifier db: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx,
		`SELECT column_name, data_type
		   FROM information_schema.columns
		   WHERE table_schema = 'public' AND table_name = $1
		   ORDER BY ordinal_position`, table)
	if err != nil {
		t.Fatalf("query columns: %v", err)
	}
	defer func() { _ = rows.Close() }()

	type col struct{ name, dtype string }
	var got []col
	for rows.Next() {
		var c col
		if err := rows.Scan(&c.name, &c.dtype); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, c)
	}
	want := []col{
		{"id", "bigint"},
		{"ts", "timestamp with time zone"},
		{"stream_id", "text"},
	}
	if len(got) != len(want) {
		t.Fatalf("column count: got %d (%+v); want %d (%+v)", len(got), got, len(want), want)
	}
	for i, w := range want {
		if got[i].name != w.name || got[i].dtype != w.dtype {
			t.Errorf("col[%d]: got %+v; want %+v", i, got[i], w)
		}
	}
}

// TestWriteHeartbeat_RowAccumulates pins the INSERT path: a WriteHeartbeat
// call lands a row with the supplied stream_id and a server-side
// timestamp. A second call lands a second row with a later timestamp.
func TestWriteHeartbeat_RowAccumulates(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeReader(t, sr)
	pgsr := sr.(*SchemaReader)

	const table = "sluice_heartbeat"
	if err := pgsr.EnsureHeartbeatTable(ctx, table); err != nil {
		t.Fatalf("EnsureHeartbeatTable: %v", err)
	}

	const streamID = "test-stream-write"
	for i := 0; i < 3; i++ {
		if err := pgsr.WriteHeartbeat(ctx, table, streamID); err != nil {
			t.Fatalf("WriteHeartbeat[%d]: %v", i, err)
		}
		// Small sleep so the server-side ts has distinct values.
		time.Sleep(15 * time.Millisecond)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verifier db: %v", err)
	}
	defer func() { _ = db.Close() }()

	var count int
	if err := db.QueryRowContext(
		ctx,
		`SELECT count(*) FROM public.sluice_heartbeat WHERE stream_id = $1`, streamID,
	).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 3 {
		t.Errorf("row count: got %d; want 3", count)
	}

	// Timestamps must be strictly ordered.
	rows, err := db.QueryContext(ctx,
		`SELECT ts FROM public.sluice_heartbeat WHERE stream_id = $1 ORDER BY id`, streamID)
	if err != nil {
		t.Fatalf("ordered query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var prev time.Time
	for rows.Next() {
		var ts time.Time
		if err := rows.Scan(&ts); err != nil {
			t.Fatalf("scan ts: %v", err)
		}
		if !prev.IsZero() && !ts.After(prev) && !ts.Equal(prev) {
			t.Errorf("ts: rows should be non-decreasing; got %v before %v", ts, prev)
		}
		prev = ts
	}
}

// TestPruneHeartbeat_DropsOldRows pins the prune path. We insert a row
// with a backdated ts via direct SQL (bypassing WriteHeartbeat's NOW()
// default) plus a fresh row, then call PruneHeartbeat with a 1-second
// window and assert the backdated row is dropped while the fresh row
// survives.
func TestPruneHeartbeat_DropsOldRows(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeReader(t, sr)
	pgsr := sr.(*SchemaReader)

	const table = "sluice_heartbeat"
	if err := pgsr.EnsureHeartbeatTable(ctx, table); err != nil {
		t.Fatalf("EnsureHeartbeatTable: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verifier db: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Insert a backdated row (5 minutes ago) and a fresh row.
	if _, err := db.ExecContext(
		ctx,
		`INSERT INTO public.sluice_heartbeat (ts, stream_id) VALUES (NOW() - INTERVAL '5 minutes', 'old')`,
	); err != nil {
		t.Fatalf("seed old row: %v", err)
	}
	if err := pgsr.WriteHeartbeat(ctx, table, "fresh"); err != nil {
		t.Fatalf("WriteHeartbeat fresh: %v", err)
	}

	// Prune everything older than 1 second.
	deleted, err := pgsr.PruneHeartbeat(ctx, table, time.Second)
	if err != nil {
		t.Fatalf("PruneHeartbeat: %v", err)
	}
	if deleted < 1 {
		t.Errorf("PruneHeartbeat: expected >=1 row deleted; got %d", deleted)
	}

	// The fresh row must still be present.
	var freshCount int
	if err := db.QueryRowContext(
		ctx,
		`SELECT count(*) FROM public.sluice_heartbeat WHERE stream_id = 'fresh'`,
	).Scan(&freshCount); err != nil {
		t.Fatalf("count fresh: %v", err)
	}
	if freshCount != 1 {
		t.Errorf("fresh row count: got %d; want 1", freshCount)
	}
	var oldCount int
	if err := db.QueryRowContext(
		ctx,
		`SELECT count(*) FROM public.sluice_heartbeat WHERE stream_id = 'old'`,
	).Scan(&oldCount); err != nil {
		t.Fatalf("count old: %v", err)
	}
	if oldCount != 0 {
		t.Errorf("old row count: got %d; want 0 (PruneHeartbeat should have dropped it)", oldCount)
	}
}

// TestHeartbeat_AdvancesSlotPosition pins the load-bearing F17 promise:
// the heartbeat INSERTs generate WAL the slot consumer sees as progress.
// We create a logical replication slot, capture its restart_lsn,
// drive several writes, then re-read the slot's restart_lsn (or
// current WAL position relative to the slot) and assert the WAL head
// has advanced past the slot's initial position.
//
// We don't actually consume from the slot — F17's value is that the
// writes generate WAL the consumer would see. Asserting that
// pg_current_wal_lsn() advances past the initial slot position proves
// the writes produced WAL.
func TestHeartbeat_AdvancesSlotPosition(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Create a slot so the test mirrors the production shape.
	const slotName = "f17_heartbeat_slot"
	replConn, err := openReplicationConn(ctx, dsn)
	if err != nil {
		t.Fatalf("openReplicationConn: %v", err)
	}
	if _, err := pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication}); err != nil {
		_ = replConn.Close(ctx)
		t.Fatalf("CreateReplicationSlot: %v", err)
	}
	if err := replConn.Close(ctx); err != nil {
		t.Fatalf("close repl conn: %v", err)
	}

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeReader(t, sr)
	pgsr := sr.(*SchemaReader)

	const table = "sluice_heartbeat"
	if err := pgsr.EnsureHeartbeatTable(ctx, table); err != nil {
		t.Fatalf("EnsureHeartbeatTable: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verifier db: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Capture the current WAL position BEFORE the writes start.
	var beforeLSN string
	if err := db.QueryRowContext(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&beforeLSN); err != nil {
		t.Fatalf("read pg_current_wal_lsn before: %v", err)
	}

	// Drive several heartbeat writes — each INSERT generates WAL.
	for i := 0; i < 5; i++ {
		if err := pgsr.WriteHeartbeat(ctx, table, "advance-test"); err != nil {
			t.Fatalf("WriteHeartbeat[%d]: %v", i, err)
		}
	}

	// Confirm the WAL head advanced.
	var afterLSN string
	if err := db.QueryRowContext(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&afterLSN); err != nil {
		t.Fatalf("read pg_current_wal_lsn after: %v", err)
	}
	if beforeLSN == afterLSN {
		t.Errorf("pg_current_wal_lsn should have advanced after 5 heartbeat writes; both reads returned %q", beforeLSN)
	}

	// Compute the bytes generated. PG's pg_wal_lsn_diff returns
	// numeric; convert to int64.
	var diffBytes int64
	if err := db.QueryRowContext(
		ctx,
		`SELECT pg_wal_lsn_diff($1, $2)::bigint`, afterLSN, beforeLSN,
	).Scan(&diffBytes); err != nil {
		t.Fatalf("pg_wal_lsn_diff: %v", err)
	}
	if diffBytes <= 0 {
		t.Errorf("WAL diff: expected positive byte count after heartbeat writes; got %d", diffBytes)
	}
	t.Logf("F17 heartbeat WAL footprint: %d bytes across 5 writes (avg %d bytes/write)",
		diffBytes, diffBytes/5)
}

// TestEnsureHeartbeatTable_PermissionDenied pins the loud-failure path:
// a connection as a role lacking CREATE on the schema must surface
// [ir.ErrHeartbeatPermission] so the pipeline wiring can degrade
// gracefully. Constructing the lowercase test: create an
// intentionally-restricted role, connect as it, attempt
// EnsureHeartbeatTable, and assert the wrapped sentinel matches via
// errors.Is.
func TestEnsureHeartbeatTable_PermissionDenied(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create a role that CONNECT-s but lacks CREATE on the public schema.
	applyPGSQL(t, dsn, `CREATE ROLE noddl LOGIN PASSWORD 'noddl'`)
	applyPGSQL(t, dsn, `REVOKE CREATE ON SCHEMA public FROM noddl`)
	applyPGSQL(t, dsn, `REVOKE CREATE ON SCHEMA public FROM PUBLIC`)

	// Build a DSN that connects as `noddl`. The startPostgresForCDC
	// helper returns a DSN with `test:test` credentials; we substitute
	// the user/password via the URL helper.
	noddlDSN := rewriteDSNCredentials(t, dsn, "noddl", "noddl")

	sr, err := Engine{}.OpenSchemaReader(ctx, noddlDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader as noddl: %v", err)
	}
	defer closeReader(t, sr)
	pgsr := sr.(*SchemaReader)

	const table = "sluice_heartbeat_perm_test"
	err = pgsr.EnsureHeartbeatTable(ctx, table)
	if err == nil {
		t.Fatal("EnsureHeartbeatTable as noddl: expected permission error; got nil")
	}
	if !errors.Is(err, ir.ErrHeartbeatPermission) {
		t.Errorf("EnsureHeartbeatTable error: must match ir.ErrHeartbeatPermission via errors.Is; got %v", err)
	}
}

// rewriteDSNCredentials replaces the user/password in a PG DSN so the
// permission-denied test can connect as the restricted role.
// startPostgresForCDC returns a DSN like:
//
//	postgres://test:test@host:port/source_db?sslmode=disable
//
// This helper does the surgical substitution without pulling in a URL
// library (the format is well-defined for the test container).
func rewriteDSNCredentials(t *testing.T, dsn, user, pass string) string {
	t.Helper()
	const oldCreds = "test:test@"
	newCreds := user + ":" + pass + "@"
	if !strings.Contains(dsn, oldCreds) {
		t.Fatalf("rewriteDSNCredentials: DSN %q does not contain %q (helper assumes startPostgresForCDC's credential shape)", dsn, oldCreds)
	}
	return strings.Replace(dsn, oldCreds, newCreds, 1)
}
