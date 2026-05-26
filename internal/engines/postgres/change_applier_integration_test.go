//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the Postgres ChangeApplier. Boots a Postgres
// container, opens the applier directly (bypassing CDC — we feed
// hand-built ir.Change events through the channel), and asserts:
//
//   - Insert/Update/Delete/Truncate land correctly on the target.
//   - Replaying the same event stream is idempotent (the
//     load-bearing property for CDC resume).
//   - Tables without a PRIMARY KEY fall back to plain INSERT.
//   - NULL columns in Before images produce IS NULL predicates.

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// startPostgresForApplier returns a DSN pointed at a freshly-reset
// database on the shared PG container booted by TestMain (see
// shared_container_integration_test.go). The cleanup is a no-op;
// container teardown is owned by TestMain.
func startPostgresForApplier(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	return newSharedPGDB(t, "target_db")
}

func applyPGApplier(t *testing.T, dsn, sqlText string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, sqlText); err != nil {
		t.Fatalf("apply sql: %v", err)
	}
}

// testStreamID is the fixed stream_id the applier integration tests
// use. Position writes from these tests all land on this single row.
const testStreamID = "test-stream"

func pumpChanges(t *testing.T, ctx context.Context, applier ir.ChangeApplier, events []ir.Change) {
	t.Helper()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	ch := make(chan ir.Change, len(events))
	for _, e := range events {
		ch <- e
	}
	close(ch)
	if err := applier.Apply(ctx, testStreamID, ch); err != nil {
		t.Fatalf("Apply: %v", err)
	}
}

// TestChangeApplier_ApplyAndIdempotency walks the canonical proof:
// apply Insert/Update/Delete, assert state, replay the same events,
// assert state UNCHANGED. Idempotency is the property that lets
// continuous-sync resume work safely.
func TestChangeApplier_ApplyAndIdempotency(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE users (
			id     BIGINT       PRIMARY KEY,
			email  VARCHAR(255) NOT NULL UNIQUE,
			active BOOLEAN      NOT NULL DEFAULT true
		);
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	events := []ir.Change{
		ir.Insert{Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "email": "r1@example.com", "active": true}},
		ir.Insert{Schema: "public", Table: "users", Row: ir.Row{"id": int64(2), "email": "r2@example.com", "active": true}},
		ir.Insert{Schema: "public", Table: "users", Row: ir.Row{"id": int64(3), "email": "r3@example.com", "active": true}},
		ir.Update{
			Schema: "public", Table: "users",
			Before: ir.Row{"id": int64(1), "email": "r1@example.com", "active": true},
			After:  ir.Row{"id": int64(1), "email": "r1@example.com", "active": false},
		},
		ir.Delete{
			Schema: "public", Table: "users",
			Before: ir.Row{"id": int64(2), "email": "r2@example.com", "active": true},
		},
	}
	pumpChanges(t, ctx, applier, events)

	got := selectAllUsers(t, dsn)
	want := []userRow{
		{ID: 1, Email: "r1@example.com", Active: false},
		{ID: 3, Email: "r3@example.com", Active: true},
	}
	if !equalUsers(got, want) {
		t.Fatalf("after first apply: got %+v; want %+v", got, want)
	}

	// Replay the SAME events. Upsert + tolerant Update/Delete keeps
	// state unchanged.
	pumpChanges(t, ctx, applier, events)

	got2 := selectAllUsers(t, dsn)
	if !equalUsers(got2, want) {
		t.Fatalf("after replay: got %+v; want %+v (idempotency violated)", got2, want)
	}
}

// TestChangeApplier_NoPKInsert verifies the documented fallback for
// tables without a PRIMARY KEY: plain INSERT. Replaying inserts on
// such a table produces duplicates — that's the documented best-
// effort behavior.
func TestChangeApplier_NoPKInsert(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE events (
			payload VARCHAR(255) NOT NULL
		);
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Insert{Schema: "public", Table: "events", Row: ir.Row{"payload": "first"}},
		ir.Insert{Schema: "public", Table: "events", Row: ir.Row{"payload": "second"}},
	})

	if got := countAllRows(t, dsn, "events"); got != 2 {
		t.Errorf("after first apply: rows = %d; want 2", got)
	}

	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Insert{Schema: "public", Table: "events", Row: ir.Row{"payload": "first"}},
	})
	if got := countAllRows(t, dsn, "events"); got != 3 {
		t.Errorf("after replay on no-PK table: rows = %d; want 3 (best-effort behavior — replays duplicate)", got)
	}
}

// TestChangeApplier_NullInWhere verifies the IS-NULL predicate path:
// a Delete whose Before image carries a NULL column must produce a
// WHERE clause that actually matches that row.
func TestChangeApplier_NullInWhere(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE notes (
			id    BIGINT       PRIMARY KEY,
			title VARCHAR(255) NULL,
			body  VARCHAR(255) NOT NULL
		);
		INSERT INTO notes (id, title, body) VALUES (1, NULL, 'untitled');
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Delete{
			Schema: "public", Table: "notes",
			Before: ir.Row{"id": int64(1), "title": nil, "body": "untitled"},
		},
	})

	if got := countAllRows(t, dsn, "notes"); got != 0 {
		t.Errorf("after delete: rows = %d; want 0 (WHERE IS NULL match failed)", got)
	}
}

// TestChangeApplier_Truncate verifies the TRUNCATE path empties the
// table and that replaying TRUNCATE on an already-empty table is a
// no-op (idempotent).
func TestChangeApplier_Truncate(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE users (
			id    BIGINT       PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		INSERT INTO users VALUES (1, 'a@x'), (2, 'b@x'), (3, 'c@x');
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Truncate{Schema: "public", Table: "users"},
	})
	if got := countAllRows(t, dsn, "users"); got != 0 {
		t.Fatalf("after truncate: rows = %d; want 0", got)
	}

	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Truncate{Schema: "public", Table: "users"},
	})
	if got := countAllRows(t, dsn, "users"); got != 0 {
		t.Errorf("after replay truncate: rows = %d; want 0", got)
	}
}

// TestChangeApplier_JSONColumn is the PG-side mirror of the MySQL
// applier's JSON regression test. PG / pgx doesn't surface the
// `_binary` charset failure mode that drives Bug 6 on MySQL — pgx
// inspects the per-column type metadata before binding — but the
// structural fix is symmetric, and a regression here would still
// indicate the applier's prepareValue routing has broken.
//
// The test feeds a JSONB-column Insert + Update + Delete with
// hand-built ir.Change events whose values match what a PG CDC
// reader emits ([]byte). Asserts that the destination row reflects
// the events end-to-end.
func TestChangeApplier_JSONColumn(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE docs (
			id   BIGINT PRIMARY KEY,
			data JSONB  NOT NULL
		);
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	insertedJSON := []byte(`{"k":"v","n":1}`)
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Insert{
			Schema: "public", Table: "docs",
			Row: ir.Row{"id": int64(1), "data": insertedJSON},
		},
	})
	if got := selectJSONByID(t, dsn, 1); !jsonEqual(got, string(insertedJSON)) {
		t.Fatalf("after insert: data = %q; want %q", got, string(insertedJSON))
	}

	updatedJSON := []byte(`{"k":"v2","n":2}`)
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Update{
			Schema: "public", Table: "docs",
			Before: ir.Row{"id": int64(1), "data": insertedJSON},
			After:  ir.Row{"id": int64(1), "data": updatedJSON},
		},
	})
	if got := selectJSONByID(t, dsn, 1); !jsonEqual(got, string(updatedJSON)) {
		t.Fatalf("after update: data = %q; want %q", got, string(updatedJSON))
	}

	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Delete{
			Schema: "public", Table: "docs",
			Before: ir.Row{"id": int64(1), "data": updatedJSON},
		},
	})
	if got := countAllRows(t, dsn, "docs"); got != 0 {
		t.Errorf("after delete: rows = %d; want 0", got)
	}
}

// TestChangeApplier_GeneratedColumn is the GitHub issue #12 regression
// guard. A table with a STORED generated column previously caused the
// CDC apply path to emit the generated column's value in the INSERT
// statement, which PG rejected with SQLSTATE 428C9 ("cannot insert
// a non-DEFAULT value into column 'margin'"). The fix filters
// generated columns out of the apply path's column lists / SET /
// WHERE — mirroring the bulk-load writer's existing filter
// (ADR-0026:100).
//
// The test sends Insert + Update events whose Row maps include the
// generated column's value (matching what the PG CDC reader's
// pgoutput decoder emits for a STORED column). Without the fix, the
// applier issues an INSERT with `margin` in the column list and PG
// returns 428C9; with the fix, the INSERT omits `margin` and PG
// recomputes it on its side. The assertion proves the destination's
// `margin` matches what PG would compute, not what the source sent.
func TestChangeApplier_GeneratedColumn(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE products (
			id     BIGINT         PRIMARY KEY,
			price  NUMERIC(12,2)  NOT NULL,
			cost   NUMERIC(12,2),
			margin NUMERIC(12,2)  GENERATED ALWAYS AS (price - COALESCE(cost, 0)) STORED
		);
	`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// Insert event whose Row carries the generated column's value
	// (what the pgoutput decoder produces). Without the fix this
	// would fail with PG SQLSTATE 428C9 — the test would fail in
	// Apply, not in the assertion. With the fix the column is
	// filtered out and PG computes margin itself.
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Insert{
			Schema: "public", Table: "products",
			Row: ir.Row{"id": int64(1), "price": "9.99", "cost": "4.50", "margin": "5.49"},
		},
	})

	got := selectProductMargin(t, dsn, 1)
	if got != "5.49" {
		t.Errorf("after insert: margin = %q; want %q (PG-computed)", got, "5.49")
	}

	// Update changes price; PG should recompute margin.
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Update{
			Schema: "public", Table: "products",
			Before: ir.Row{"id": int64(1), "price": "9.99", "cost": "4.50", "margin": "5.49"},
			After:  ir.Row{"id": int64(1), "price": "12.99", "cost": "4.50", "margin": "8.49"},
		},
	})
	got = selectProductMargin(t, dsn, 1)
	if got != "8.49" {
		t.Errorf("after update: margin = %q; want %q (PG-recomputed from new price)", got, "8.49")
	}

	// Delete using a Before image that includes the generated
	// column's value. WHERE must filter it out — including it
	// risks silent zero-rows-affected when the target's
	// recomputation differs from the source's stored value.
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Delete{
			Schema: "public", Table: "products",
			Before: ir.Row{"id": int64(1), "price": "12.99", "cost": "4.50", "margin": "8.49"},
		},
	})
	if got := countAllRows(t, dsn, "products"); got != 0 {
		t.Errorf("after delete: rows = %d; want 0", got)
	}
}

// selectProductMargin reads the `margin` column for a single row.
// Returns the textual NUMERIC representation so test assertions can
// match exactly without floating-point rounding.
func selectProductMargin(t *testing.T, dsn string, id int64) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var margin string
	if err := db.QueryRowContext(ctx, `SELECT margin::text FROM products WHERE id = $1`, id).Scan(&margin); err != nil {
		t.Fatalf("select margin: %v", err)
	}
	return margin
}

// selectJSONByID reads docs.data for the row with the given id.
func selectJSONByID(t *testing.T, dsn string, id int64) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var out string
	err = db.QueryRowContext(ctx, "SELECT data::text FROM docs WHERE id = $1", id).Scan(&out)
	if err != nil {
		t.Fatalf("select data: %v", err)
	}
	return out
}

// jsonEqual reports whether two JSON documents are semantically
// equal. Postgres re-serialises stored JSON in canonical form (and
// jsonb especially normalises key order), so byte-equal would
// over-fail; comparing parsed maps avoids that.
func jsonEqual(got, want string) bool {
	var gotV, wantV any
	if err := json.Unmarshal([]byte(got), &gotV); err != nil {
		return false
	}
	if err := json.Unmarshal([]byte(want), &wantV); err != nil {
		return false
	}
	return reflect.DeepEqual(gotV, wantV)
}

// ---- Test helpers ----

type userRow struct {
	ID     int64
	Email  string
	Active bool
}

func selectAllUsers(t *testing.T, dsn string) []userRow {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SELECT id, email, active FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var out []userRow
	for rows.Next() {
		var u userRow
		if err := rows.Scan(&u.ID, &u.Email, &u.Active); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func countAllRows(t *testing.T, dsn, table string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func equalUsers(a, b []userRow) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
